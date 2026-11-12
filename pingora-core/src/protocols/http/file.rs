// Copyright 2026 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Optional immutable file-body transport. No TLS or HTTP/2 raw-fd shortcut.

use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{fs::File, io, sync::Arc};

/// 每次后台任务最多发送 1 MiB，减少小量子反复切换线程，同时保留公平边界。
pub const FILE_SEND_QUANTUM: usize = 1024 * 1024;

/// 进程级固定维度计数，不以 URL/key 产生高基数指标。
/// `eligible_bytes` 只在协议和模块检查全部通过后记录，不包含 MISS/L1/TLS。
#[derive(Clone, Copy, Debug, Default)]
pub struct FileSendStats {
    pub eligible_bytes: u64,
    pub sent_bytes: u64,
    pub calls: u64,
    pub jobs: u64,
    pub wait_count: u64,
    pub wait_ns: u64,
    pub waiters: u64,
    pub active: u64,
    pub queue_full: u64,
    pub wait_timeout: u64,
    pub syscall_unsupported: u64,
    pub transport_unsupported: u64,
    pub module_disallowed: u64,
    pub syscall_errors: u64,
}

macro_rules! counters {
    ($($name:ident),* $(,)?) => { $(static $name: AtomicU64 = AtomicU64::new(0);)* };
}
counters!(
    ELIGIBLE,
    SENT,
    CALLS,
    JOBS,
    WAIT_COUNT,
    WAIT_NS,
    WAITERS,
    ACTIVE,
    QUEUE_FULL,
    WAIT_TIMEOUT,
    SYSCALL_UNSUPPORTED,
    TRANSPORT_UNSUPPORTED,
    MODULE_DISALLOWED,
    SYSCALL_ERRORS
);

pub fn file_send_stats() -> FileSendStats {
    FileSendStats {
        eligible_bytes: ELIGIBLE.load(Ordering::Relaxed),
        sent_bytes: SENT.load(Ordering::Relaxed),
        calls: CALLS.load(Ordering::Relaxed),
        jobs: JOBS.load(Ordering::Relaxed),
        wait_count: WAIT_COUNT.load(Ordering::Relaxed),
        wait_ns: WAIT_NS.load(Ordering::Relaxed),
        waiters: WAITERS.load(Ordering::Relaxed),
        active: ACTIVE.load(Ordering::Relaxed),
        queue_full: QUEUE_FULL.load(Ordering::Relaxed),
        wait_timeout: WAIT_TIMEOUT.load(Ordering::Relaxed),
        syscall_unsupported: SYSCALL_UNSUPPORTED.load(Ordering::Relaxed),
        transport_unsupported: TRANSPORT_UNSUPPORTED.load(Ordering::Relaxed),
        module_disallowed: MODULE_DISALLOWED.load(Ordering::Relaxed),
        syscall_errors: SYSCALL_ERRORS.load(Ordering::Relaxed),
    }
}

/// 由 cache adapter 在确认完整 HIT 和精确长度后记录候选分母。
pub fn record_file_eligible(bytes: usize) {
    ELIGIBLE.fetch_add(bytes as u64, Ordering::Relaxed);
}

/// 分母排除不支持的 transport/module；单独记录降级原因，方便解释覆盖率。
pub fn record_file_capability(transport: bool, modules: bool) {
    if !transport {
        TRANSPORT_UNSUPPORTED.fetch_add(1, Ordering::Relaxed);
    }
    if !modules {
        MODULE_DISALLOWED.fetch_add(1, Ordering::Relaxed);
    }
}

/// 记录文件发送候选检查失败的固定原因。
pub fn record_file_candidate_failure(transport: bool, modules: bool) {
    record_file_capability(transport, modules);
}

/// 读取当前累计值；节点只每十秒采样一次，避免在热路径输出日志。
pub fn stats() -> FileSendStats {
    file_send_stats()
}

/// 只读、不可变的文件区间。发布后禁止修改 inode，即使 sendfile 已返回；
/// TCP 发送路径仍可能因重传继续引用 page cache 页面。
#[derive(Clone)]
pub struct FileRegion {
    pub file: Arc<File>,
    pub offset: u64,
    pub len: usize,
    // 由缓存 adapter 传入的 reader 租约。sendfile 任务克隆 FileRegion 时一并
    // 持有它，直到最后一个任务释放文件引用后才归还 reader 槽。该字段不参与
    // 文件内容或偏移语义，只负责延长底层资源生命周期。
    pub lease: Option<Arc<dyn Any + Send + Sync>>,
}

impl FileRegion {
    /// Validate bounds before exposing headers. Files must not subsequently shrink.
    pub fn new(file: Arc<File>, offset: u64, len: usize) -> io::Result<Self> {
        let end = offset
            .checked_add(len as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "file region overflow"))?;
        if end > i64::MAX as u64 || end > file.metadata()?.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "file region outside file",
            ));
        }
        Ok(Self {
            file,
            offset,
            len,
            lease: None,
        })
    }
}

#[cfg(target_os = "linux")]
pub(crate) mod linux {
    use super::*;
    use nix::{
        errno::Errno,
        sys::{
            sendfile::sendfile,
            socket::{shutdown, Shutdown},
        },
    };
    use std::os::fd::AsFd;
    use tokio::sync::Semaphore;

    use std::sync::OnceLock;
    use std::time::Instant;

    // 发送槽在进程启动时配置，限制真正并行进入内核的 sendfile 数量；每个
    // 请求仍只有一个写者。最多 32 个异步等待者；由 HTTP 写超时兜底，不再
    // 因单核调度超过 5 ms 就永久失去文件路径。只有获得 jobs 才可提交后台
    // 工作，不把任务堆进 blocking pool，慢客户端也不占槽等网络可写。
    pub(crate) struct Executor {
        jobs: Semaphore,
        queue: Semaphore,
    }
    static EXECUTOR: OnceLock<Executor> = OnceLock::new();
    #[derive(Debug, PartialEq, Eq)]
    pub(crate) enum Busy {
        QueueFull,
    }

    struct WaitObservation(Instant);
    impl WaitObservation {
        fn new() -> Self {
            WAIT_COUNT.fetch_add(1, Ordering::Relaxed);
            WAITERS.fetch_add(1, Ordering::Relaxed);
            Self(Instant::now())
        }
    }
    impl Drop for WaitObservation {
        fn drop(&mut self) {
            WAITERS.fetch_sub(1, Ordering::Relaxed);
            WAIT_NS.fetch_add(
                self.0.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                Ordering::Relaxed,
            );
        }
    }
    impl Executor {
        const fn new(slots: usize) -> Self {
            Self {
                jobs: Semaphore::const_new(slots),
                queue: Semaphore::const_new(32),
            }
        }
        async fn acquire(&self) -> Result<tokio::sync::SemaphorePermit<'_>, Busy> {
            if let Ok(permit) = self.jobs.try_acquire() {
                return Ok(permit);
            }
            let _queue = self.queue.try_acquire().map_err(|_| {
                QUEUE_FULL.fetch_add(1, Ordering::Relaxed);
                Busy::QueueFull
            })?;
            // RAII 覆盖正常唤醒、超时、取消三条路径；取消等待者不能泄漏队列额度。
            let _observation = WaitObservation::new();
            // 取消时 Tokio 负责撤销等待；queue 与 observation 的 RAII 一并归还。
            // jobs 不会 close，因此这里不需要额外的永久降级分支。
            Ok(self.jobs.acquire().await.expect("文件执行器不会关闭"))
        }
    }

    /// 在创建 HTTP service 前配置进程级 sendfile 并发槽；启动后不允许热改，
    /// 避免正在运行的请求看到不一致的资源边界。
    pub fn configure(slots: usize) -> bool {
        (1..=16).contains(&slots) && EXECUTOR.set(Executor::new(slots)).is_ok()
    }

    fn executor() -> &'static Executor {
        EXECUTOR.get_or_init(|| Executor::new(1))
    }

    pub(crate) async fn permit() -> Result<tokio::sync::SemaphorePermit<'static>, Busy> {
        executor().acquire().await
    }

    // 在 blocking closure 内构造，外部 future 取消后仍由真实任务归还 active。
    pub(crate) struct Running;
    impl Running {
        pub(crate) fn new() -> Self {
            JOBS.fetch_add(1, Ordering::Relaxed);
            ACTIVE.fetch_add(1, Ordering::Relaxed);
            Self
        }
    }
    impl Drop for Running {
        fn drop(&mut self) {
            ACTIVE.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// 保留同一个 reactor socket 的身份。取消时 shutdown，使尚未返回的后台
    /// 任务不能写入下一条 keepalive 响应；Arc 避免每量子 dup/close 一个 FD。
    pub(crate) struct CancelSocket(Option<Arc<tokio::net::TcpStream>>);
    impl CancelSocket {
        pub(crate) fn new(socket: Arc<tokio::net::TcpStream>) -> Self {
            Self(Some(socket))
        }
        pub(crate) fn disarm(&mut self) {
            self.0.take();
        }
    }
    impl Drop for CancelSocket {
        fn drop(&mut self) {
            if let Some(socket) = &self.0 {
                let _ = shutdown(std::os::fd::AsRawFd::as_raw_fd(&**socket), Shutdown::Both);
            }
        }
    }

    pub(crate) fn step(socket: &impl AsFd, region: &FileRegion, count: usize) -> io::Result<usize> {
        let mut offset = i64::try_from(region.offset)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file offset overflow"))?;
        CALLS.fetch_add(1, Ordering::Relaxed);
        let result = sendfile(
            socket.as_fd(),
            region.file.as_fd(),
            Some(&mut offset),
            count,
        )
        .map_err(|e| io::Error::from_raw_os_error(e as i32));
        match &result {
            Ok(n) => {
                SENT.fetch_add(*n as u64, Ordering::Relaxed);
            }
            Err(e) if unsupported(e) => {
                SYSCALL_UNSUPPORTED.fetch_add(1, Ordering::Relaxed);
            }
            Err(e)
                if !matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) =>
            {
                SYSCALL_ERRORS.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        result
    }

    pub(crate) fn unsupported(error: &io::Error) -> bool {
        matches!(error.raw_os_error(), Some(x) if x == Errno::ENOSYS as i32 || x == Errno::EINVAL as i32 || x == Errno::EOPNOTSUPP as i32)
    }

    #[cfg(test)]
    mod executor_tests {
        use super::*;

        #[tokio::test]
        async fn contention_waits_then_succeeds_and_cancellation_returns_capacity() {
            let executor = Executor::new(1);
            let held = executor.acquire().await.unwrap();
            let mut waiting = Box::pin(executor.acquire());
            assert!(futures::poll!(&mut waiting).is_pending());
            assert_eq!(executor.queue.available_permits(), 31);
            drop(held);
            let permit = waiting.await.unwrap();
            assert_eq!(executor.queue.available_permits(), 32);
            let mut cancelled = Box::pin(executor.acquire());
            assert!(futures::poll!(&mut cancelled).is_pending());
            drop(cancelled);
            assert_eq!(executor.queue.available_permits(), 32);
            drop(permit);
            assert!(executor.acquire().await.is_ok());
        }

        #[tokio::test]
        async fn saturated_queue_is_bounded_without_micro_timeout() {
            let executor = Executor::new(1);
            let _held = executor.acquire().await.unwrap();
            let _all = executor.queue.acquire_many(32).await.unwrap();
            assert_eq!(executor.acquire().await.unwrap_err(), Busy::QueueFull);
            drop(_all);
            let mut waiting = Box::pin(executor.acquire());
            assert!(futures::poll!(&mut waiting).is_pending());
            // 真实 HTTP 写超时仍在外层；单核下排队超过旧阈值不是能力丢失。
            tokio::time::sleep(std::time::Duration::from_millis(12)).await;
            assert!(futures::poll!(&mut waiting).is_pending());
            drop(_held);
            assert!(waiting.await.is_ok());
            assert_eq!(executor.queue.available_permits(), 32);
        }

        #[tokio::test]
        async fn parallel_slots_are_independent_and_never_exceed_capacity() {
            let executor = Executor::new(2);
            let a = executor.acquire().await.unwrap();
            let b = executor.acquire().await.unwrap();
            assert_eq!(executor.jobs.available_permits(), 0);
            let mut c = Box::pin(executor.acquire());
            assert!(futures::poll!(&mut c).is_pending());
            drop(a);
            let c = c.await.unwrap();
            assert_eq!(executor.jobs.available_permits(), 0);
            drop(b);
            drop(c);
            assert_eq!(executor.jobs.available_permits(), 2);
        }
    }
}

/// 配置进程级 sendfile 并发槽；必须在创建 HTTP service 前调用。
#[cfg(target_os = "linux")]
pub fn configure(slots: usize) -> bool {
    linux::configure(slots)
}

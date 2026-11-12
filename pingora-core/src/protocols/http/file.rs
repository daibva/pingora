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

use std::{fs::File, io, sync::Arc};

/// A read-only, immutable file region. The producer must never modify the inode
/// after publication, including after a send completes (TCP may retransmit).
#[derive(Clone, Debug)]
pub struct FileRegion {
    pub file: Arc<File>,
    pub offset: u64,
    pub len: usize,
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
        Ok(Self { file, offset, len })
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
    use std::os::fd::{AsFd, OwnedFd};
    use tokio::sync::Semaphore;

    // Shared across runtimes/workers. A permit stays with a running syscall even
    // if its async waiter is cancelled. No unbounded blocking-pool submission.
    static JOBS: Semaphore = Semaphore::const_new(1);

    pub(crate) fn permit() -> Option<tokio::sync::SemaphorePermit<'static>> {
        JOBS.try_acquire().ok()
    }

    /// Owns the socket identity. Cancellation shuts it down, preventing stale
    /// blocking jobs from ever writing into a reused connection.
    pub(crate) struct CancelSocket(Option<OwnedFd>);
    impl CancelSocket {
        pub(crate) fn new(fd: OwnedFd) -> Self {
            Self(Some(fd))
        }
        pub(crate) fn disarm(&mut self) {
            self.0.take();
        }
    }
    impl Drop for CancelSocket {
        fn drop(&mut self) {
            if let Some(fd) = &self.0 {
                let _ = shutdown(std::os::fd::AsRawFd::as_raw_fd(fd), Shutdown::Both);
            }
        }
    }

    pub(crate) fn step(socket: &impl AsFd, region: &FileRegion, count: usize) -> io::Result<usize> {
        let mut offset = i64::try_from(region.offset)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file offset overflow"))?;
        sendfile(
            socket.as_fd(),
            region.file.as_fd(),
            Some(&mut offset),
            count,
        )
        .map_err(|e| io::Error::from_raw_os_error(e as i32))
    }

    pub(crate) fn unsupported(error: &io::Error) -> bool {
        matches!(error.raw_os_error(), Some(x) if x == Errno::ENOSYS as i32 || x == Errno::EINVAL as i32 || x == Errno::EOPNOTSUPP as i32)
    }
}

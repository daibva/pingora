# cloud-node file-send fork

Fork: <https://github.com/daibva/pingora>, branch `cloud-node/sendfile-v0.9.1`.
Base: upstream `f86ffda5d3ae5c3baf5125682138b33b26c01b08`, the VCS revision
recorded in the published Pingora 0.9.1 crates.

This is an experimental, default-off Linux plaintext HTTP/1.1 cache-HIT path.
It is not an implementation of kTLS, HTTP/2 sendfile, or QUIC zero-copy.
No performance claim is made.

## Enablement

- Build `pingora-proxy` with feature `file-send` (propagates to core/cache).
- Implement `HandleHit::file_region/consume_file` for a complete immutable file.
- Explicitly opt in with `ProxyHttp::allow_cache_file_send`.
- All downstream modules must return true from `supports_response_file`.
  Unknown modules return false. Disabled compression is safe to bypass.
- Only fresh complete GET/200 HITs, no Range, at least 128 KiB, and an exact
  Content-Length qualify. Selection happens after header filters.

Opt-in certifies that body/trailer/done callbacks are not needed for policy,
transforms, throttling or accounting. Byte accounting in the H1 session remains
active; `file_bytes_sent()` distinguishes bytes actually accepted by sendfile.
Any future body-dependent policy must revoke opt-in until integrated safely.

## Ownership and backpressure

The ordinary H1 writer still handles headers and finish/keepalive. Transport
buffers are flushed before a file quantum. An optional shared TCP wrapper keeps
the original Tokio reactor registration alive for blocking jobs. No additional
socket reactor is registered.

文件发送使用显式偏移，接受短发送，每次后台任务最多 1 MiB。冷文件 syscall
仍在 `spawn_blocking` 执行；提交前申请进程级并发额度，启动时可配置 1..=16 槽。
最多 32 个等待者异步排队，取消后归还队列额度，不在 blocking pool 无界排队。
不再用 5 ms 的排队超时永久撤销文件能力：单核调度延迟不等于 transport 失效，
HTTP 写超时仍兜底。队列满或平台能力不支持才按原游标回退。
socket readiness 在异步侧等待，不占执行槽；`try_io` 仅在真实 EAGAIN 时清除就绪。

取消保护共享同一个 `Arc<TcpStream>` 并关闭连接，不再每个量子 dup/close FD。
在途任务继续拥有 socket、文件、reader 租约及执行额度。H1 disables reuse around
the await and refuses incomplete file-body reuse. This prevents a detached job
from writing into the next keepalive response. Running file I/O is not forcibly
abortable.

File content must never be modified/truncated after publication, even after
sendfile returns: TCP retransmission may retain references to pages. Unlink is
allowed with an already-open file. Producers validate file identity and bounds
before headers. ENOSYS/EINVAL/EOPNOTSUPP use the original buffered path at the
exact remaining cursor; fatal errors close the response instead of appending
another version or another HTTP response.

## Validation

`pingora-core/tests/file_send.rs` has eight Linux contract tests covering
bounds, module opt-in, actual file bytes, non-TCP fallback, offset/header ordering,
unlink, keepalive, mixed file/buffered delivery, truncated-file failure and
cancellation under socket backpressure. 默认未配置时仍只有一槽；串行执行合约测试
避免并行测试互相耗尽测试执行器：

```sh
cargo test -p pingora-core --features file-send --test file_send -- --test-threads=1
cargo test -p pingora-core --features file-send --lib executor_tests -- --test-threads=1
```

The consumer also has Python end-to-end tests using `--file-send`, including a
large L2 object with a nonconstant body, repeated keepalive hits and actual
file-send byte counters. Diagnostics and benchmarks belong in Python, not a new
Rust CLI.

Consumer tests also verified repeated TLS/H1 and negotiated TLS/H2 L2 hits with
file-send enabled in the build; kernel file-byte counters stayed zero.

Still needed before production: comprehensive forced syscall error injection,
broader TLS/H2 regression, concurrent/slow-reader resource profiling,
configurable executor/FD/buffer budgets and measured hot/cold performance. This
prototype has no FD cache and does not pretend to implement the entire target
resource planner.

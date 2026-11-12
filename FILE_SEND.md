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

sendfile uses an explicit input offset, accepts partial writes and yields after
at most 256 KiB. Cold-file syscalls run on `spawn_blocking`, with a process-wide
single-job semaphore acquired before submission and held by the job. A busy
executor returns to the normal buffered reader at the unchanged cursor; it does
not create an unbounded blocking submission queue. Socket readiness is waited
asynchronously; `try_io` clears readiness only when the syscall returns EAGAIN.

Cancellation owns a duplicate socket identity and shuts down the connection;
an in-flight job retains its socket, file and permit. H1 disables reuse around
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

`pingora-core/tests/file_send.rs` has seven passing Linux contract tests covering
bounds, module opt-in, actual file bytes, non-TCP fallback, offset/header ordering,
unlink, keepalive, mixed file/buffered delivery, truncated-file failure and
cancellation under socket backpressure. Run serially because this prototype deliberately allows only one
global file syscall at a time:

```sh
cargo test -p pingora-core --features file-send --test file_send -- --test-threads=1
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

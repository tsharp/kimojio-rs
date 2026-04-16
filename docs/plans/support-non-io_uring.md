# Plan: Support Non-io_uring Backends (epoll on Linux, IOCP on Windows)

## Problem Statement

Kimojio is currently a Linux-only runtime that is tightly coupled to `io_uring`. This prevents
use in two important scenarios:

1. **Linux containers** that run on kernels too old for io_uring, or that disable io_uring via
   `seccomp` policy (common in Kubernetes environments on GKE, EKS, AKS, etc.).
2. **Windows** development and deployment environments.

The goal is to introduce an **epoll** backend for Linux (selectable at compile time via a cargo
feature flag) and an **IOCP** (I/O Completion Ports) backend for Windows, while keeping io_uring
as the default on Linux and retaining all existing semantics.

---

## Codebase Assessment

### Architecture Summary

Kimojio is a **thread-per-core** async runtime built around io_uring. The async machinery is
clean and well-layered:

| Layer | Files | io_uring coupling |
|---|---|---|
| Public API / futures | `operations.rs` | **Strong** – every I/O operation builds an `opcode::*` SQE and wraps it in `RingFuture` |
| Completion plumbing | `ring_future.rs`, `lib.rs` (`Completion`, `CompletionState`) | **Strong** – SQE/CQE types from `rustix_uring` wired throughout |
| Event loop | `runtime.rs` | **Strong** – calls `submit_and_complete_io`, reads CQEs from `Ring` |
| Per-thread state | `task.rs` (`TaskState`, `Ring`) | **Strong** – owns two `IoUring` ring handles (`ring` and `ring_poll`) |
| Task / scheduler | `task.rs` (`Task`, `TaskReadyState`, wakers) | **No coupling** – pure Rust async machinery |
| Async primitives | `async_event.rs`, `async_lock.rs`, `async_channel.rs`, etc. | **No coupling** |
| Timers | `timer.rs` | **Mild** – uses TSC ticks for busy-poll accounting; timer futures use io_uring `LinkTimeout` SQEs |
| Pipes / streams | `pipe.rs`, `async_stream.rs` | **Mild** – `pipe.rs` uses `socketpair`/`pipe2` (Linux); streams call `operations::read/write` |
| TLS | `tlsstream.rs`, `tlscontext.rs` | **No direct coupling** – delegates I/O to the stream abstraction |

### Key io_uring-Coupled Types

- `rustix_uring::IoUring<SQE, CQE>` — the ring handle, wrapped in `task::Ring`.
- `rustix_uring::squeue::Entry` (SQE) — submission queue entries built by `opcode::*`.
- `rustix_uring::cqueue::Entry` (CQE) — completion queue entries consumed by `process_completions`.
- `rustix_uring::Probe` — used to check which opcodes the kernel supports.
- `rustix_uring::Errno` — re-exported as `kimojio::Errno`; must be preserved on all platforms.
- `rustix::io_uring::io_uring_user_data` — carries the `Rc<Completion>` pointer through the kernel.

### Dependencies to Replace/Extend

```toml
# Currently unconditional – must become cfg-gated
rustix    = { version = "1", features = ["io_uring", ...] }
rustix-uring = { version = "0.6" }
```

On Linux/epoll: replace `rustix_uring` with `rustix` epoll APIs (already in `rustix`).  
On Windows: use `windows-sys` or `windows` crate for IOCP.

---

## Proposed Design

### 1. Feature Flags

Add the following feature flags to `kimojio/Cargo.toml`:

```toml
[features]
# New flags
io_uring    = []          # io_uring backend (default on Linux)
epoll       = []          # epoll backend (opt-in on Linux; required for containers)
windows-iocp = []         # IOCP backend (automatically selected on Windows via build.rs)

default = ["tls", "io_uring"]   # Linux default unchanged
```

A `build.rs` script selects the correct default backend based on `target_os`:

- `linux` → `io_uring` unless `epoll` feature is explicitly requested.
- `windows` → `windows-iocp` automatically.

Mutual exclusion between `io_uring` / `epoll` / `windows-iocp` is enforced with `static_assertions`
or a `build.rs` error.

### 2. Introduce a Backend Abstraction Trait

Create `kimojio/src/backend/mod.rs` with a sealed trait:

```rust
/// Backend-private trait implemented by io_uring, epoll, and IOCP drivers.
pub(crate) trait Driver: Sized {
    /// Opaque handle to a pending I/O operation. Stored inside `Completion`.
    type PendingHandle: 'static;

    /// Submit a batch of pending operations and drain any completions.
    /// `want` controls blocking: 0 = non-blocking, 1 = block until one CQE arrives.
    fn submit_and_wait(
        &mut self,
        want: usize,
    ) -> Result<usize, DriverError>;

    /// Cancel a previously submitted operation.
    fn cancel(&mut self, handle: &Self::PendingHandle);

    /// Drain all available completions, calling `on_complete` for each.
    fn drain_completions(
        &mut self,
        on_complete: impl FnMut(CompletionKey, Result<u32, Errno>),
    );
}
```

`CompletionKey` is a `u64` / pointer-sized token matching a `Rc<Completion>`.

### 3. Refactor `Completion` and `CompletionState`

Currently, `CompletionState::Idle` owns an `Option<SQE>`. Under the new design:

```rust
enum CompletionState {
    Idle {
        request: Box<dyn PendingRequest>,  // backend-specific I/O description
        timespec: bool,
    },
    Submitted { waker: Waker, activity_id: Uuid, tag: u32, canceled: bool },
    Completed { result: Result<u32, Errno> },
    Terminated,
}
```

`PendingRequest` is a backend-private trait whose implementations hold either an io_uring SQE, an
epoll registration request, or an IOCP overlapped buffer pointer.

### 4. `RingFuture` → `IoFuture`

Rename `ring_future.rs` to `io_future.rs`. The generic `RingFuture<T, C>` becomes `IoFuture<T, C>`.
The `poll` implementation changes from:

```rust
ring.submit(&entries[..]);
```

to a backend-agnostic call:

```rust
task_state.driver.submit(completion.clone(), request);
```

### 5. Module Layout

```
kimojio/src/
  backend/
    mod.rs          # Driver trait, CompletionKey, DriverError
    io_uring.rs     # io_uring Driver impl (cfg(feature = "io_uring"))
    epoll.rs        # epoll Driver impl   (cfg(feature = "epoll"))
    iocp.rs         # IOCP Driver impl    (cfg(feature = "windows-iocp"))
  io_future.rs      # (renamed from ring_future.rs)
  operations/
    mod.rs          # re-exports, shared helpers
    linux.rs        # Linux-specific: open, stat, fallocate, madvise, etc.
    common.rs       # cross-platform: connect, accept, send, recv, read, write
    windows.rs      # Windows-specific: named pipes, file I/O via IOCP
  task.rs           # TaskState gains `driver: Box<dyn Driver>` field
  ...
```

### 6. `TaskState` Changes

```rust
pub struct TaskState {
    // replaces `ring: Ring` and `ring_poll: Ring`
    pub driver: Box<dyn Driver>,
    // everything else unchanged
    ...
}
```

The `Ring` struct in `task.rs` is moved into `backend/io_uring.rs`.

### 7. `operations.rs` Changes

Operations that are **Linux-only** (open, stat, unlink, rename, mkdir, fallocate, fsync, etc.)
are gated with `#[cfg(target_os = "linux")]`.

Cross-platform operations (read, write, send, recv, connect, accept, shutdown) are implemented
for each backend in `backend/*.rs`.

On Windows, filesystem operations use `CreateFileW` + IOCP overlapped I/O. Sockets use
`WSARecv`/`WSASend` + IOCP.

### 8. `pipe.rs` Changes

`socketpair` and `pipe2` are Linux-only. For Windows:

- `bipipe()` → uses `CreateNamedPipeW` or an anonymous `PIPE_ACCESS_DUPLEX` pair.
- `pipe()` → uses `CreatePipe`.

### 9. Timer Changes

`timer.rs` uses `_rdtsc` (x86_64) and `cntvct_el0` (aarch64). On Windows the same intrinsics
are available. The `__rdtsc` intrinsic is available via the `windows` crate or MSVC intrinsics.
The fallback `Instant::now()` path already handles other architectures. No changes needed here.

The io_uring `LinkTimeout` SQE is replaced per backend:

- **epoll**: `timerfd_create` + `epoll_ctl` for the timeout fd, or `epoll_pwait2` with a timeout.
- **IOCP**: `SetWaitableTimer` or `CreateThreadpoolTimer`.

### 10. `Errno` / Error Types

`rustix_uring::Errno` is currently re-exported as `kimojio::Errno`. On Windows this type does not
exist. Options:

1. Keep `rustix_uring::Errno` on Linux, introduce a `kimojio::Error` newtype that wraps either
   `rustix_uring::Errno` or a Windows `HRESULT`/`DWORD` on Windows.
2. Unify behind a single `kimojio::Error` enum from the start with POSIX-style codes translated
   from Win32.

**Recommended:** Option 2. Introduce `kimojio::Error` as a stable error type with a POSIX-style
mapping on all platforms. The Win32 socket errors map cleanly to POSIX codes via `WSAGetLastError`.
This avoids leaking `rustix_uring` into the public API.

### 11. `lib.rs` Changes

- Remove unconditional `pub use rustix_uring::Errno`.
- Replace with `pub use crate::error::Errno` (the new unified type).
- Gate all `libc::` re-exports (`EAGAIN`, `EPIPE`, etc.) with `#[cfg(unix)]`; on Windows expose
  equivalent constants from `windows-sys`.
- `run_with_configuration` currently panics if kernel version < 5.15. This check becomes
  `#[cfg(feature = "io_uring")]` only.

### 12. `kimojio-macros`

The `#[kimojio::main]` and `#[kimojio::test]` macros expand to `kimojio::run(...)`. These are
platform-agnostic and require no changes. The expanded code calls `run()` which is already an
entry point; the macro crate needs no io_uring knowledge.

### 13. TLS (`kimojio-tls`)

TLS is built on top of `AsyncStreamRead + AsyncStreamWrite`. It does not touch io_uring directly.
The TLS layer should work without modification once the underlying stream traits are implemented
for the new backends.

### 14. `Cargo.toml` Workspace Changes

```toml
[workspace.dependencies]
# io_uring (Linux default)
rustix    = { version = "1", features = ["fs", "mm", "process", "rand", "system", "time", "thread"] }
rustix    = { version = "1", features = ["io_uring", ...], optional = true }  # with io_uring feature
rustix-uring = { version = "0.6", optional = true }

# epoll (Linux, opt-in)
# No new crates needed; epoll is in rustix already

# Windows IOCP
windows-sys = { version = "0.52", features = ["Win32_System_IO", "Win32_Networking_WinSock", ...], optional = true }
```

---

## Migration Strategy

The work is naturally split into phases that can be merged incrementally:

### Phase 1 – Introduce Abstraction Without Breaking io_uring

1. Create `backend/mod.rs` with the `Driver` trait.
2. Implement `backend/io_uring.rs`: extract the `Ring` struct from `task.rs` into this module,
   implement `Driver` for it. No behaviour change.
3. Update `TaskState` to hold `Box<dyn Driver>` (or an enum for zero-cost dispatch).
4. Refactor `ring_future.rs` → `io_future.rs`, switching from direct ring calls to `Driver` calls.
5. All existing tests pass; this is a pure refactor.

### Phase 2 – epoll Backend

1. Implement `backend/epoll.rs`.
   - Use `epoll_create1` + `epoll_wait` via `rustix::event::epoll`.
   - Map operations to non-blocking syscalls + readiness notification:
     - `read`/`write`/`recv`/`send` → non-blocking `read(2)`/`write(2)` + epoll wait on EPOLLIN/EPOLLOUT.
     - `connect` → non-blocking `connect(2)` + epoll EPOLLOUT.
     - `accept` → non-blocking `accept4(2)` + epoll EPOLLIN.
     - `open`/`stat`/`fsync` etc. → spawn a blocking thread via a small thread pool (these are
       inherently blocking in epoll; no kernel support for async file I/O without io_uring).
   - Timers: use `timerfd_create` + `epoll_ctl` for deadline support.
2. Add `feature = "epoll"` guard throughout.
3. Update `Cargo.toml` to add the `epoll` feature.
4. Add CI matrix: `--features epoll --no-default-features` on Linux.

### Phase 3 – Windows IOCP Backend

1. Add `backend/iocp.rs`.
   - Create an IOCP handle with `CreateIoCompletionPort`.
   - Implement `Driver::submit_and_wait` using `GetQueuedCompletionStatusEx`.
   - Associate sockets with the IOCP via `CreateIoCompletionPort` with a socket handle.
   - Network I/O: `AcceptEx`, `ConnectEx`, `WSARecv`, `WSASend` with `OVERLAPPED`.
   - File I/O: `ReadFile`/`WriteFile` with `OVERLAPPED`.
   - Timers: use a `CreateWaitableTimerEx` + post a fake completion on expiry.
2. Port `pipe.rs` to Windows using named pipes or `CreatePipe`.
3. Port `socket_helpers.rs` to use `WS2_32` APIs on Windows.
4. Gate filesystem operations (`open`, `stat`, `unlink`, etc.) under `#[cfg(unix)]`; stub or
   replace with Windows equivalents.
5. Update `Cargo.toml` to add `windows-sys` dependency, feature-gated.
6. Add CI matrix: Windows runners with `--features windows-iocp --no-default-features`.

### Phase 4 – Unified Error Type

1. Create `kimojio/src/error.rs` with a `kimojio::Error` type (POSIX-style codes).
2. Provide `From<rustix_uring::Errno>` and `From<windows_sys::Win32::Foundation::WIN32_ERROR>`.
3. Update `operations.rs` return types to use `kimojio::Error`.
4. Deprecate the `pub use rustix_uring::Errno` re-export; keep it as a type alias temporarily
   to avoid a breaking change for existing users.

### Phase 5 – Cleanup and Documentation

1. Remove Linux kernel version check from non-io_uring paths.
2. Update `kimojio/Cargo.toml` description (no longer "Linux io_uring only").
3. Update `README.md` with platform support matrix and feature flag documentation.
4. Write integration tests that compile and run on both Linux (epoll) and Windows (IOCP).

---

## Scope Boundaries

| Item | In scope | Notes |
|---|---|---|
| Linux epoll backend | ✅ | Full parity for network I/O; file I/O via thread pool |
| Windows IOCP backend | ✅ | Network + file I/O |
| io_uring remains default on Linux | ✅ | No behaviour change for existing users |
| `io_uring_cmd` / NVMe passthrough | ❌ | Linux + io_uring only; keep existing feature flag |
| `setup_single_issuer` | ❌ | io_uring only; keep as-is |
| Polled I/O (`ring_poll`, `iopoll`) | ❌ | io_uring only; no epoll/IOCP equivalent |
| TLS changes | ❌ | TLS already platform-agnostic |
| macOS / kqueue | ❌ | Not requested; can follow the same pattern later |

---

## Risks and Considerations

- **epoll is readiness-based, io_uring is completion-based.** The `Completion`/`RingFuture` model
  assumes completion semantics. For epoll, each "completion" is a readiness event followed by a
  non-blocking syscall. The `Driver` trait must expose this difference, or the epoll driver must
  internally wrap each (fd, readiness, syscall) triple to look like a completion.

- **File I/O on epoll.** Linux epoll does not support regular files; `read(2)` on a file always
  returns `POLLIN`. Async file I/O on epoll requires either `io_uring` or a blocking thread pool.
  This is a known limitation and should be documented clearly.

- **`Rc<Completion>` pointer trick.** The io_uring backend encodes `Rc::into_raw` as user data
  in the SQE. IOCP uses `OVERLAPPED` with a similar embedded pointer trick. epoll requires a
  separate map from fd to `Rc<Completion>`.

- **Thread-per-core model.** The runtime is `!Send` and uses thread-local `TaskState`. This is
  preserved on all backends; no changes to the threading model are needed.

- **Cancellation.** io_uring has first-class `AsyncCancel`. epoll cancellation means removing the
  fd from epoll and abandoning the non-blocking syscall (usually safe). IOCP cancellation uses
  `CancelIoEx`. Each backend's `Driver::cancel` will handle this correctly.

- **`URingStats`.** The stats type is named after io_uring but is generic enough to reuse. It
  should be renamed to `RuntimeStats` and the field names updated (`in_flight_io_poll` becomes
  meaningful only on io_uring; on other backends it can always be zero).

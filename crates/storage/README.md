# lenso-storage

Streaming local object storage for Tokio. `LocalStorage` is usable without an
application or plugin; `ObjectStorage` is a small object-safe trait for business
services. `LocalStoragePlugin` only acquires the service and registers resource
cleanup with the existing `lenso-plugin-core` lifecycle.

```rust
use lenso_storage::{LocalStorage, LocalStorageConfig, PutObject, ReadOptions, ReadRange};
use std::io::Cursor;
use tokio::io::AsyncReadExt;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let temp = tempfile::tempdir()?;
let root = temp.path().canonicalize()?.join("objects");
let storage = LocalStorage::open(LocalStorageConfig::new(root)).await?;

let mut upload = PutObject::new("notes/hello.txt", Cursor::new(b"hello storage"));
upload.content_type = "text/plain".into();
upload.size = Some(13);
upload.max_bytes = Some(1024);
let metadata = storage.put(upload).await?;

let mut download = storage.get(&metadata.key, ReadOptions {
    if_match: Some(metadata.etag),
    range: Some(ReadRange { offset: 6, length: None }),
    ..Default::default()
}).await?;
let mut text = String::new();
download.body.read_to_string(&mut text).await?;
assert_eq!(text, "storage");
storage.close().await?;
# Ok(())
# }
# tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(example()).unwrap();
```

Run the standalone and exact-instance injection examples:

```sh
cargo run -p lenso-storage --example local
cargo run -p lenso-storage --example injected
```

## API and semantics

- `put(PutObject)` creates a new key, never overwrites one. The upload owns a
  `Send + 'static` Tokio `AsyncRead`; non-`Unpin` readers are accepted. Payloads
  pass through a fixed 64 KiB buffer. `size`, when supplied, must match the actual
  byte count. Effective maximum size is the smaller of per-upload `max_bytes`
  and configured `max_upload_bytes`; zero allows only an empty object.
- `get(key, ReadOptions)` returns full object metadata and an `AsyncRead` body.
  `if_match` compares the quoted SHA-256 ETag. A range uses an inclusive offset
  and an optional positive length, clamped to EOF. A range starting at or past
  EOF is invalid, including offset zero on an empty object. A full empty read
  succeeds. An opened download retains its snapshot after delete/recreate.
- `head(key, OperationOptions)` returns `None` for an absent object. Corrupt,
  incomplete or unsafe objects return errors, never apparent absence.
- `delete(key, OperationOptions)` is idempotent and reports `Deleted` or
  `NotFound`. Logical deletion renames to a stable per-key tomb. A retry also
  removes a tomb left after a partial deletion.
- `list(ListOptions)` has a literal key prefix, exclusive key cursor and limit
  1–1000 (default 100). Keys use Rust lexical order. The cursor is present only
  if another matching object existed during that call. Each page scans the root;
  it retains only `limit + 1` metadata records. Pagination across calls is not a
  transaction. Another writer may change subsequent pages.

Metadata includes content type, arbitrary string-to-string custom metadata, full
size, ETag and a `SystemTime` modification time. The serialized representation is
limited to 64 KiB. Keys must be relative, nonempty, at most 1024 UTF-8 bytes, and
have no empty/dot/traversal components, backslashes, ASCII controls or drive-letter
prefix. Content types must be nonempty, at most 1024 UTF-8 bytes and contain no
ASCII control characters.

`StorageError::code()` is a stable domain classification. `Display` and `Debug`
do not expose paths or provider/source details. `std::error::Error::source()` is
for trusted diagnostics. Download `io::Error` values contain a downcastable
`StorageError`; cancellation is terminal, not an automatically retried
`Interrupted` IO error.

Downloads can feed `PutObject::new` directly without collecting the payload.
Copy content type, size and custom metadata from the download when needed; the
destination computes its own ETag and modification time. Upload source IO errors
wrapping a `StorageError` retain that classification (including `Aborted` from a
cancelled download). Other source IO errors remain `Provider`; their details are
available only through the diagnostic source chain. Failed copies are not published.

## Cancellation and lifecycle

Each operation accepts an optional `CancellationToken`. Cancelling it interrupts
a stalled upload read and drops the owned source. Dropping an in-progress API
future also requests cancellation. Owned Tokio tasks finish cleanup even when
their original caller has gone away. Cancellation near a publication or deletion
boundary may be observed after the effect completed; reconcile with `head` if
that distinction matters.

`close().await` atomically seals this service, cancels all of its operations and
downloads, and waits for its tasks and upload cleanup. It also closes unconsumed
download workers. Repeated/concurrent calls join the same state; dropping a close
waiter does not stop cleanup. Retained service clones reject calls with `Aborted`.
Successful objects and other instances' temporary files are preserved. Failed
upload cleanup is returned by the upload and makes subsequent close return a
provider error. Keep the Tokio runtime alive until close completes. Process
termination, abort-on-panic and hung filesystem syscalls cannot be cleaned up by
an asynchronous lifecycle. A panicking user reader is not a supported input
error protocol; use `io::Error` for source failures.

Download workers read only when the body requests data, with one request pending
at a time and chunks at most 64 KiB. Dropping a body closes its request channel;
caller cancellation and service closure also terminate an unpolled download.
Finite filesystem operations finish before their worker exits. No whole-payload
buffering or unbounded prefetch is used.

Plugin resource cleanup runs **after** the existing core work drain. An outer
business entry should hold the core `WorkGuard` through its operation. Storage
dependency calls do not reenter that gate, so already admitted work can finish
during application shutdown. Cancel a stalled admitted request at the business
entrance if it needs to stop before drain. `examples/injected.rs` separates the
pure business service from that entry adapter and declares the exact storage
handle in `dependencies()`.

## Filesystem contract

The local provider supports Unix filesystems with working directory advisory
locks and same-directory atomic rename. It is verified on macOS; Linux follows
the same Unix implementation but has not been executed in this task. Non-Unix
configuration returns `Unsupported`. Use a trusted, dedicated root and trusted
ancestors. Root creation rejects symlink ancestry instead of canonicalizing it;
on macOS, canonicalize a trusted temporary base before choosing its child root.

Keys are mapped to lowercase SHA-256 directory names. Each committed directory
contains only `payload` and `metadata.json`. Payload and metadata are written and
synced before the directory is atomically published. This gives atomic visibility,
not a promise of durability across power loss: directory entries are not fsynced.
Rust instances coordinate publication, reads/list snapshots and deletion with an
advisory lock on the root directory. Upload streaming happens outside that lock.
Applications must not mutate this directory through other writers.

Every operation checks ancestry and the root's original inode identity. Object
directories must not be symlinks. Files are opened with `O_NOFOLLOW | O_NONBLOCK`
and must be regular with exactly one hard link, preventing FIFO reads and
symlink/hardlink escapes. Metadata key identity and payload size are checked on
load. Unexpected root or object entries are errors. Valid owned staging entries
(`.tmp-*`) and deletion tombs are omitted from listings. A staging directory that
disappears during its owner's cleanup is safely skipped.

These portable path checks do not prevent a hostile writer from swapping ancestor
directories between checks and use. Same-size out-of-band payload modification
is not detected by rehashing on each read. ETags describe uploaded bytes, and are
not an integrity guarantee against later filesystem tampering. Do not use a
world-writable shared tree, or share this on-disk format with the TS adapter.
Orphan staging directories from process crashes are not swept automatically.

See [PARITY.md](PARITY.md) for the reference mapping and [VERIFICATION.md](VERIFICATION.md)
for review, checks and integration notes.

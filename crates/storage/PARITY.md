# TS semantic parity and deferred scope

Reference: `LioRael/lenso` main
`c6d2820513878a8b24d40c015da613b9918b5d6b`, `packages/storage/src/{index,local,stream}.ts`
and `packages/storage/test/{local,stream}.test.ts`. Read-only inventory snapshot:
`/tmp/lenso-plugin-inventory-20261015/ts`.

Rust base: `LioRael/lenso-rust` main
`e5dfafbf9e7e96ec7593c66ae108b227e6629d74`.

| TS behavior | Rust implementation / validation | Status |
| --- | --- | --- |
| Create-only streaming `put` | Owned Tokio `AsyncRead`, staged directory, locked atomic publication; 24 competing instances test | Implemented |
| `size` and `maxBytes` enforce actual bytes | Unsigned lengths, checked counter, mismatch rejection, early over-limit source drop | Implemented |
| Per-provider upload maximum | `LocalStorageConfig::max_upload_bytes`, intersected with request limit | Added explicit configuration |
| Metadata, quoted SHA-256 ETag, modification time | `ObjectMetadata`; roundtrip across separate instances | Implemented |
| 64 KiB metadata limit | Serialized metadata checked before polling source and before publication; bounded metadata reads | Implemented |
| `get` and `ifMatch` | Exact ETag match, typed `Conflict` | Implemented |
| Offset/length range; clamp beyond EOF | `ReadRange`/`ResolvedRange`; empty/full reads, EOF and `u64::MAX` boundaries tested | Implemented |
| Download survives delete/recreate | Open file snapshot; bounded pull-driven worker | Implemented |
| `head` absent versus damaged object | `Option<ObjectMetadata>`; corrupt/incomplete/forged metadata tests | Implemented |
| Idempotent delete and partial tomb recovery | `DeleteOutcome`; stable `.delete-<hash>` tomb, safe file-by-file retry | Implemented |
| List prefix, key cursor, default 100 / max 1000 | `ListOptions`, bounded retained metadata; unexpected root entries rejected | Implemented |
| Abort stalled upload and cancel source | `CancellationToken` selects against pending read, source dropped by ownership | Implemented with Rust ownership |
| Abort unconsumed download on cleanup | Worker cancellation and task tracking; repeated close joins cleanup | Implemented |
| Caller abandons operation | Dropping API future cancels owned worker; close is cleanup barrier | Added Rust-specific behavior |
| Cleanup errors surfaced | Upload keeps primary classification and cleanup cause; close reports cleanup failure | Implemented; disk-failure injection not exercised |
| Cleanup also observes pending operation errors | Business errors stay with their request; close reports owned resource cleanup failures | Rust lifecycle adaptation |
| Key validation and root ancestry safety | UTF-8 key limits, control/traversal rejection, symlink ancestry/root replacement tests | Implemented |
| No file symlinks/hardlinks/FIFOs or extra entries | Safe open and file type/link-count checks | Implemented |
| Per-instance plugin ownership | Existing typed `Handle<LocalStorage>`, exact dependency resolution, resources cleanup | Implemented |
| No extra dependency container | Standalone service plus small `ObjectStorage` trait; no core edits | Implemented |
| `StorageError` stable diagnostic surface | Typed `ErrorCode`, redacted Display/Debug, internal source chain | Implemented |
| Local signed URLs unsupported | Capabilities report false; signing methods are absent from the scoped trait | Deliberately not exposed |
| Provider identity/capability strings | Rust type/instance handle supplies identity; capabilities are typed fields | Rust API adaptation |
| JS numbers and AbortSignal | `u64`/`usize` eliminate negative/fractional sizes; cancellation token replaces signal | Rust API adaptation |
| TS UTF-16 key sort | Rust Unicode scalar / UTF-8 lexical order; differs for some supplementary-plane keys | Documented difference |
| TS content-type length in UTF-16 units | Rust limit is 1024 UTF-8 bytes | Documented stricter byte bound |
| TS disk JSON date strings | Rust serde representation of `SystemTime`; dedicated Rust root only | No on-disk interoperability |
| Windows behavior | Provider returns `Unsupported` | Deferred |
| S3 / R2 providers and signing | No network provider, credentials or permissions changes | Out of scope |
| DBFiles, SQLite, Postgres | No schema, index or database dependency | Out of scope |
| Fetch/HTTP upload-download routing | Provider only; no HTTP auth or routing added | Out of scope |
| Crash orphan staging sweep; fsync of parent directories | No automatic sweeping; atomic visibility rather than power-loss durability guarantee | Deferred |

Filesystem trust matches the TS local adapter: an application-controlled dedicated
root with no hostile concurrent ancestor replacement. The Rust advisory lock
strengthens coordination between cooperating Rust instances but is not a security
container and does not coordinate writers that ignore it.

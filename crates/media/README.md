# Native image media

`MediaPlugin` depends on an exact `Handle<S: ObjectStorage>`. It borrows Storage;
Core drains accepted Media work before closing its SQLite journal and before the
Storage owner closes. No Core/Storage changes, task worker or external services.

Host-controlled named/versioned presets support PNG/JPEG header inspection,
aspect-preserving thumbnail resize, optional pixel crop, RGB conversion and fresh
PNG/JPEG encoding (metadata stripped; JPEG output converts to RGB). The host's
`Authority` owns verified identity and checks the source scope on every operation,
before publication and after reading. Source references carry an ETag; conditional
source reads and fresh head checks reject changed versions. Raw object keys are
not authorization capabilities: applications must serve images via `Media::read`
and keep Storage private. A revocation concurrent with the final policy check
retains the usual check/use boundary; there is no distributed authorization lock.

SQLite records a pending random artifact key before upload and publishes the
complete result afterwards. Reopening removes pending objects, retaining ready
objects. An exclusive manifest file lock prevents concurrent owners. Each manifest
is bound to a stable host-supplied Storage identity. The application must reserve
`media-derived/` for Media and retain that identity for the same Storage root.
Journal and its parent are trusted host paths, not user input. Recovery fails
closed if delete fails; retry startup after repairing the provider. Failed puts
may have committed and are cleaned through the pending record. A committed ready
result survives caller cancellation or a lost response. No eviction/GC of ready
objects is implemented; deleting originals makes old derived references stale.

Cache identity includes Storage identity, source key/ETag/scope, full named preset
and processor version. Public reads additionally match the stored record exactly.
One nonqueued job per Media instance bounds concurrency; excess requests return
Busy. Caller cancellation does not stop accepted work: its owned Tokio task holds
Core's WorkGuard and the slot through codec/journal/storage completion.

Input and output bytes, image dimensions and pixels are bounded. The configuration
reserves 32 bytes per maximum pixel plus input/output buffers; the decoder receives
image's allocation limit. This is a conservative working-buffer policy, **not an
OS-enforced RSS ceiling** (codec/library overhead and allocator behavior vary).
Only PNG/JPEG codecs are enabled. No URLs, videos, animation processing, EXIF
orientation normalization, CDN, Files metadata layer or public upload endpoint.
Inspection validates headers/dimensions, not full image integrity. Decode/encode
and SQLite operations run on blocking workers. Tiny fixture example:

```sh
cargo run --locked -p lenso-media --example thumbnail
cargo test --locked -p lenso-media
```

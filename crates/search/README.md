# lenso-search

Native SQLite FTS5 persistent note projections, independent of Tasks. Explicitly
`provision(path)` before opening `Search` or installing `SearchPlugin` with Core.
Startup opens read/write without creating files or migrating schemas. Bundled
SQLite is pinned to the workspace's existing 0.38.0. Missing FTS5 fails explicitly.

The host derives `Scope(namespace, tenant, owner)` from current authorization for
**each** operation; scopes and cursors are not credentials. Updates replace a
projection only within the supplied scope. Ownership transfers require deleting
the old scope's projection, and authoritative deletes must be projected here.
No automatic source-of-truth synchronization or authorization caching is supplied.

Queries AND literal whitespace-separated terms using FTS5 unicode61. SQL values
are bound, including scope, query, limits and offsets. The exact scope predicate
applies before sorting and pagination. Title substring matches rank first, then
binary note ID ascending; no corpus-wide BM25 scores expose other partitions.
Summaries are plain text (24-token snippet, at most 512 Unicode characters), not
HTML. Escape them when rendering HTML. This is neither vector search nor Chinese
word segmentation. Titles are at most 4096 bytes; bodies at most 1 MiB.

Page size is 1–100, query size 1024 bytes/32 terms, cursor size 8192 bytes, and
pagination stops at 10,000 hits. Cursors bind scope, query, size and database
revision. Any write invalidates outstanding cursors, including writes through
another connection; restart without writes preserves them. Cursors are transparent
JSON and may be tampered with; they never grant access and every query repeats
scope filtering. These are bounded offset cursors, not stable snapshot handles.

Operations are synchronous and serialized, with a two-second SQLite busy timeout.
Use a host blocking thread for substantial workloads; no workers are created.
Plugin methods enter Core WorkGate once and retain the guard until SQLite settles.
Shutdown drains admitted work and closes the connection; retained services reject
new calls. Ordinary-service users explicitly call `close()` (also closed on drop).

Run the public consumer with:
`cargo run --locked -p lenso-search --example notes_search`.

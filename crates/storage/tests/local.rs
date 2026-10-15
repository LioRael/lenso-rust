#![cfg(unix)]

use lenso_storage::*;
use sha2::{Digest, Sha256};
use std::{
    fs, io,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncRead, AsyncReadExt, ReadBuf},
    sync::Semaphore,
};

async fn fixture() -> (TempDir, LocalStorage) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap().join("objects");
    let storage = LocalStorage::open(LocalStorageConfig::new(root))
        .await
        .unwrap();
    (temp, storage)
}
fn root(temp: &TempDir) -> std::path::PathBuf {
    temp.path().canonicalize().unwrap().join("objects")
}
fn name(key: &str) -> String {
    format!("{:x}", Sha256::digest(key.as_bytes()))
}
fn code<T>(result: Result<T, StorageError>, expected: ErrorCode) {
    match result {
        Ok(_) => panic!("expected {expected:?}"),
        Err(error) => assert_eq!(error.code(), expected, "{error:?}"),
    }
}
fn input(key: &str, value: &str) -> PutObject {
    PutObject::new(key, io::Cursor::new(value.as_bytes().to_vec()))
}
async fn text(storage: &LocalStorage, key: &str) -> String {
    let mut value = String::new();
    storage
        .get(key, ReadOptions::default())
        .await
        .unwrap()
        .body
        .read_to_string(&mut value)
        .await
        .unwrap();
    value
}
fn assert_empty(temp: &TempDir) {
    assert_eq!(fs::read_dir(root(temp)).unwrap().count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listing_tolerates_owned_staging_cleanup() {
    let (temp, storage) = fixture().await;
    for _ in 0..8 {
        let token = CancellationToken::new();
        let mut uploads = tokio::task::JoinSet::new();
        for index in 0..32 {
            let entered = Arc::new(Semaphore::new(0));
            let mut upload = PutObject::new(
                format!("pending-{index}"),
                StalledReader {
                    entered: entered.clone(),
                    dropped: Arc::new(AtomicBool::new(false)),
                },
            );
            upload.cancellation = Some(token.clone());
            let worker = storage.clone();
            uploads.spawn(async move { worker.put(upload).await });
            bounded(entered.acquire()).await.unwrap().forget();
        }
        let reader = storage.clone();
        let list = tokio::spawn(async move {
            for _ in 0..16 {
                assert!(reader
                    .list(ListOptions::default())
                    .await
                    .unwrap()
                    .objects
                    .is_empty());
                tokio::task::yield_now().await;
            }
        });
        tokio::time::sleep(Duration::from_micros(50)).await;
        token.cancel();
        bounded(list).await.unwrap();
        while let Some(result) = bounded(uploads.join_next()).await {
            code(result.unwrap(), ErrorCode::Aborted);
        }
        assert_empty(&temp);
    }
    storage.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_recreation_deletion_and_snapshot_reads_stay_consistent() {
    let (temp, storage) = fixture().await;
    let writer = LocalStorage::open(LocalStorageConfig::new(root(&temp)))
        .await
        .unwrap();
    let deleter = LocalStorage::open(LocalStorageConfig::new(root(&temp)))
        .await
        .unwrap();
    let reader = storage.clone();
    let writes = tokio::spawn(async move {
        for index in 0..40 {
            let result = writer.put(input("key", &format!("value-{index}"))).await;
            if let Err(error) = result {
                assert_eq!(error.code(), ErrorCode::Conflict);
            }
        }
        writer.close().await.unwrap();
    });
    let deletes = tokio::spawn(async move {
        for _ in 0..40 {
            deleter
                .delete("key", OperationOptions::default())
                .await
                .unwrap();
        }
        deleter.close().await.unwrap();
    });
    let reads = tokio::spawn(async move {
        for _ in 0..80 {
            match reader.get("key", ReadOptions::default()).await {
                Ok(mut download) => {
                    let mut bytes = Vec::new();
                    download.body.read_to_end(&mut bytes).await.unwrap();
                    assert_eq!(bytes.len() as u64, download.metadata.size);
                    assert_eq!(
                        format!("\"{:x}\"", Sha256::digest(&bytes)),
                        download.metadata.etag
                    );
                }
                Err(error) => assert_eq!(error.code(), ErrorCode::NotFound),
            }
        }
    });
    let (a, b, c) = bounded(async { tokio::join!(writes, deletes, reads) }).await;
    a.unwrap();
    b.unwrap();
    c.unwrap();
    assert!(fs::read_dir(root(&temp)).unwrap().count() <= 1);
    storage.close().await.unwrap();
}
async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("operation must make progress")
}

#[tokio::test]
async fn streaming_roundtrip_metadata_persistence_and_trait_injection() {
    let (temp, storage) = fixture().await;
    let value = "a".repeat(200_000) + "middle" + &"z".repeat(100_000);
    let mut upload = input("photos/é.jpg", &value);
    upload.size = Some(value.len() as u64);
    upload.content_type = "image/jpeg".into();
    upload
        .custom_metadata
        .insert("owner".into(), "alice".into());
    let metadata = storage.put(upload).await.unwrap();
    assert_eq!(
        metadata.etag,
        format!("\"{:x}\"", Sha256::digest(value.as_bytes()))
    );
    let other: Arc<dyn ObjectStorage> = Arc::new(
        LocalStorage::open(LocalStorageConfig::new(root(&temp)))
            .await
            .unwrap(),
    );
    assert_eq!(
        other
            .head(&metadata.key, OperationOptions::default())
            .await
            .unwrap(),
        Some(metadata.clone())
    );
    let mut download = other
        .get(
            &metadata.key,
            ReadOptions {
                if_match: Some(metadata.etag.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(download.metadata, metadata);
    let mut actual = String::new();
    download.body.read_to_string(&mut actual).await.unwrap();
    assert_eq!(actual, value);
    assert_eq!(fs::read_dir(root(&temp)).unwrap().count(), 1);
    storage.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn competing_instances_publish_exactly_one_complete_object() {
    let (temp, storage) = fixture().await;
    let mut uploads = tokio::task::JoinSet::new();
    for index in 0..24 {
        let other = LocalStorage::open(LocalStorageConfig::new(root(&temp)))
            .await
            .unwrap();
        uploads.spawn(async move {
            let result = other.put(input("same", &format!("value-{index}"))).await;
            other.close().await.unwrap();
            result
        });
    }
    let mut winner = None;
    while let Some(result) = bounded(uploads.join_next()).await {
        match result.unwrap() {
            Ok(metadata) => {
                assert!(winner.replace(metadata).is_none());
            }
            Err(error) => assert_eq!(error.code(), ErrorCode::Conflict),
        }
    }
    let metadata = winner.unwrap();
    assert_eq!(
        storage
            .head("same", OperationOptions::default())
            .await
            .unwrap(),
        Some(metadata)
    );
    let original = text(&storage, "same").await;
    code(
        storage.put(input("same", "replacement")).await,
        ErrorCode::Conflict,
    );
    assert_eq!(text(&storage, "same").await, original);
    assert_eq!(fs::read_dir(root(&temp)).unwrap().count(), 1);
    storage.close().await.unwrap();
}

#[tokio::test]
async fn range_conditions_empty_objects_and_open_snapshot() {
    let (_temp, storage) = fixture().await;
    let metadata = storage.put(input("a", "abcdef")).await.unwrap();
    for (offset, length, expected) in [
        (2, Some(u64::MAX), "cdef"),
        (1, Some(2), "bc"),
        (5, None, "f"),
    ] {
        let mut download = storage
            .get(
                "a",
                ReadOptions {
                    range: Some(ReadRange { offset, length }),
                    if_match: Some(metadata.etag.clone()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            download.range,
            Some(ResolvedRange {
                offset,
                length: expected.len() as u64
            })
        );
        assert_eq!(download.metadata.size, 6);
        let mut actual = String::new();
        download.body.read_to_string(&mut actual).await.unwrap();
        assert_eq!(actual, expected);
    }
    code(
        storage
            .get(
                "a",
                ReadOptions {
                    if_match: Some("wrong".into()),
                    ..Default::default()
                },
            )
            .await,
        ErrorCode::Conflict,
    );
    for range in [
        ReadRange {
            offset: 6,
            length: None,
        },
        ReadRange {
            offset: u64::MAX,
            length: Some(2),
        },
        ReadRange {
            offset: 0,
            length: Some(0),
        },
    ] {
        code(
            storage
                .get(
                    "a",
                    ReadOptions {
                        range: Some(range),
                        ..Default::default()
                    },
                )
                .await,
            ErrorCode::InvalidInput,
        );
    }
    let mut upload = input("empty", "");
    upload.size = Some(0);
    upload.max_bytes = Some(0);
    storage.put(upload).await.unwrap();
    assert_eq!(text(&storage, "empty").await, "");
    code(
        storage
            .get(
                "empty",
                ReadOptions {
                    range: Some(ReadRange {
                        offset: 0,
                        length: None,
                    }),
                    ..Default::default()
                },
            )
            .await,
        ErrorCode::InvalidInput,
    );
    let mut snapshot = storage.get("a", ReadOptions::default()).await.unwrap();
    storage
        .delete("a", OperationOptions::default())
        .await
        .unwrap();
    storage.put(input("a", "replacement")).await.unwrap();
    let mut old = String::new();
    snapshot.body.read_to_string(&mut old).await.unwrap();
    assert_eq!(old, "abcdef");
    assert_eq!(text(&storage, "a").await, "replacement");
    storage.close().await.unwrap();
}

#[tokio::test]
async fn pagination_delete_and_partial_tomb_recovery() {
    let (temp, storage) = fixture().await;
    for key in ["a/3", "b", "a/1", "a/2"] {
        storage.put(input(key, key)).await.unwrap();
    }
    fs::create_dir(root(&temp).join(".tmp-owned")).unwrap();
    let page = storage
        .list(ListOptions {
            prefix: "a/".into(),
            limit: 2,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        page.objects
            .iter()
            .map(|m| m.key.as_str())
            .collect::<Vec<_>>(),
        ["a/1", "a/2"]
    );
    assert_eq!(page.cursor.as_deref(), Some("a/2"));
    let second = storage
        .list(ListOptions {
            prefix: "a/".into(),
            limit: 2,
            cursor: page.cursor,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        second
            .objects
            .iter()
            .map(|m| m.key.as_str())
            .collect::<Vec<_>>(),
        ["a/3"]
    );
    assert!(second.cursor.is_none());
    for partial in [false, true] {
        let key = if partial { "a/1" } else { "a/2" };
        let tomb = root(&temp).join(format!(".delete-{}", name(key)));
        fs::rename(root(&temp).join(name(key)), &tomb).unwrap();
        if partial {
            fs::remove_file(tomb.join("metadata.json")).unwrap();
        }
        assert_eq!(
            storage
                .delete(key, OperationOptions::default())
                .await
                .unwrap(),
            DeleteOutcome::Deleted
        );
        assert_eq!(
            storage
                .delete(key, OperationOptions::default())
                .await
                .unwrap(),
            DeleteOutcome::NotFound
        );
        assert!(storage
            .head(key, OperationOptions::default())
            .await
            .unwrap()
            .is_none());
        code(
            storage.get(key, ReadOptions::default()).await,
            ErrorCode::NotFound,
        );
    }
    for limit in [0, 1001] {
        code(
            storage
                .list(ListOptions {
                    limit,
                    ..Default::default()
                })
                .await,
            ErrorCode::InvalidInput,
        );
    }
    code(
        storage
            .list(ListOptions {
                cursor: Some("../a".into()),
                ..Default::default()
            })
            .await,
        ErrorCode::InvalidKey,
    );
    storage.close().await.unwrap();
}

struct CountingReader {
    reads: Arc<AtomicUsize>,
    dropped: Arc<AtomicBool>,
    remaining: usize,
    fail: bool,
}
impl AsyncRead for CountingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            return Poll::Ready(Err(io::Error::other("private source failure")));
        }
        let count = self.remaining.min(output.remaining());
        output.put_slice(&vec![7; count]);
        self.remaining -= count;
        Poll::Ready(Ok(()))
    }
}
impl Drop for CountingReader {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn actual_size_limits_metadata_validation_and_source_failures_leave_no_artifacts() {
    let (temp, storage) = fixture().await;
    for (size, max_bytes, expected) in [
        (None, Some(4), ErrorCode::TooLarge),
        (Some(5), None, ErrorCode::InvalidInput),
        (Some(9), None, ErrorCode::InvalidInput),
        (Some(6), Some(4), ErrorCode::TooLarge),
    ] {
        let mut upload = input("a", "abcdef");
        upload.size = size;
        upload.max_bytes = max_bytes;
        code(storage.put(upload).await, expected);
        assert_empty(&temp);
    }
    for content_type in ["", "text/plain\r\nfoo: bar", "\u{7f}"] {
        let mut upload = input("a", "x");
        upload.content_type = content_type.into();
        code(storage.put(upload).await, ErrorCode::InvalidInput);
    }
    let mut upload = input("a", "x");
    upload
        .custom_metadata
        .insert("large".into(), "x".repeat(65536));
    code(storage.put(upload).await, ErrorCode::InvalidInput);
    for fail in [false, true] {
        let reads = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let mut upload = PutObject::new(
            "a",
            CountingReader {
                reads: reads.clone(),
                dropped: dropped.clone(),
                remaining: 1_000_000,
                fail,
            },
        );
        upload.max_bytes = Some(1);
        code(
            storage.put(upload).await,
            if fail {
                ErrorCode::Provider
            } else {
                ErrorCode::TooLarge
            },
        );
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        assert!(dropped.load(Ordering::SeqCst));
        assert_empty(&temp);
    }
    let mut config = LocalStorageConfig::new(root(&temp));
    config.max_upload_bytes = Some(2);
    let limited = LocalStorage::open(config).await.unwrap();
    let mut upload = input("a", "abc");
    upload.max_bytes = Some(1000);
    code(limited.put(upload).await, ErrorCode::TooLarge);
    assert_eq!(limited.capabilities().max_upload_bytes, Some(2));
    assert_empty(&temp);
    limited.close().await.unwrap();
    storage.close().await.unwrap();
}

struct StalledReader {
    entered: Arc<Semaphore>,
    dropped: Arc<AtomicBool>,
}
impl AsyncRead for StalledReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.entered.add_permits(1);
        Poll::Pending
    }
}
impl Drop for StalledReader {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn cancellation_close_and_dropped_callers_interrupt_stalled_uploads() {
    for mode in ["token", "close", "drop"] {
        let (temp, storage) = fixture().await;
        let other = LocalStorage::open(LocalStorageConfig::new(root(&temp)))
            .await
            .unwrap();
        other.put(input("committed", "keep")).await.unwrap();
        let entered = Arc::new(Semaphore::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let token = CancellationToken::new();
        let mut upload = PutObject::new(
            "blocked",
            StalledReader {
                entered: entered.clone(),
                dropped: dropped.clone(),
            },
        );
        upload.cancellation = Some(token.clone());
        let instance = storage.clone();
        let waiter = tokio::spawn(async move { instance.put(upload).await });
        bounded(entered.acquire()).await.unwrap().forget();
        match mode {
            "token" => token.cancel(),
            "close" => bounded(storage.close()).await.unwrap(),
            _ => waiter.abort(),
        }
        if mode == "drop" {
            assert!(waiter.await.unwrap_err().is_cancelled());
        } else {
            code(bounded(waiter).await.unwrap(), ErrorCode::Aborted);
        }
        bounded(storage.close()).await.unwrap();
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(fs::read_dir(root(&temp)).unwrap().count(), 1);
        assert_eq!(text(&other, "committed").await, "keep");
        code(
            storage.head("committed", OperationOptions::default()).await,
            ErrorCode::Aborted,
        );
        other.close().await.unwrap();
    }
    let (temp, storage) = fixture().await;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let mut upload = input("a", "x");
    upload.cancellation = Some(cancelled);
    code(storage.put(upload).await, ErrorCode::Aborted);
    assert_empty(&temp);
    storage.close().await.unwrap();
}

#[tokio::test]
async fn close_and_caller_cancel_invalidate_unconsumed_downloads() {
    let (_temp, storage) = fixture().await;
    storage.put(input("a", "value")).await.unwrap();
    let mut completed = storage.get("a", ReadOptions::default()).await.unwrap();
    completed.body.read_to_end(&mut Vec::new()).await.unwrap();
    let token = CancellationToken::new();
    let mut first = storage
        .get(
            "a",
            ReadOptions {
                cancellation: Some(token.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    token.cancel();
    let error = bounded(first.body.read_u8()).await.unwrap_err();
    assert_eq!(
        error
            .get_ref()
            .unwrap()
            .downcast_ref::<StorageError>()
            .unwrap()
            .code(),
        ErrorCode::Aborted
    );
    let mut second = storage.get("a", ReadOptions::default()).await.unwrap();
    let (a, b) = tokio::join!(storage.close(), storage.close());
    a.unwrap();
    b.unwrap();
    // Once EOF was observed, shutdown cannot retroactively abort that stream.
    assert_eq!(completed.body.read(&mut [0; 1]).await.unwrap(), 0);
    let error = bounded(second.body.read_u8()).await.unwrap_err();
    assert_eq!(
        error
            .get_ref()
            .unwrap()
            .downcast_ref::<StorageError>()
            .unwrap()
            .code(),
        ErrorCode::Aborted
    );
}

#[tokio::test]
async fn rejects_traversal_symlink_ancestry_and_replaced_root() {
    use std::os::unix::fs::symlink;
    let (temp, storage) = fixture().await;
    for key in [
        "../secret",
        "/absolute",
        "a/../b",
        "a//b",
        "a\\b",
        "C:foo",
        "a\0b",
        "",
        "a/",
        "a/./b",
        "x\u{7f}",
    ] {
        code(storage.put(input(key, "x")).await, ErrorCode::InvalidKey);
        code(
            storage.get(key, ReadOptions::default()).await,
            ErrorCode::InvalidKey,
        );
        code(
            storage.head(key, OperationOptions::default()).await,
            ErrorCode::InvalidKey,
        );
        code(
            storage.delete(key, OperationOptions::default()).await,
            ErrorCode::InvalidKey,
        );
    }
    code(
        storage.put(input(&"é".repeat(513), "x")).await,
        ErrorCode::InvalidKey,
    );
    let base = temp.path().canonicalize().unwrap();
    symlink(root(&temp), base.join("link")).unwrap();
    code(
        LocalStorage::open(LocalStorageConfig::new(base.join("link/nested"))).await,
        ErrorCode::Forbidden,
    );
    fs::rename(root(&temp), base.join("real")).unwrap();
    symlink(base.join("real"), root(&temp)).unwrap();
    code(
        storage.head("a", OperationOptions::default()).await,
        ErrorCode::Forbidden,
    );
    fs::remove_file(root(&temp)).unwrap();
    fs::create_dir(root(&temp)).unwrap();
    code(
        storage.head("a", OperationOptions::default()).await,
        ErrorCode::Conflict,
    );
    storage.close().await.unwrap();
}

#[tokio::test]
async fn rejects_symlinks_hardlinks_special_files_and_unexpected_entries() {
    use std::os::unix::fs::symlink;
    for target in [
        "payload",
        "metadata.json",
        "object",
        "hardlink",
        "fifo",
        "extra",
    ] {
        let (temp, storage) = fixture().await;
        storage.put(input("a", "value")).await.unwrap();
        let base = temp.path().canonicalize().unwrap();
        let object = root(&temp).join(name("a"));
        fs::write(base.join("secret"), "secret").unwrap();
        match target {
            "payload" | "metadata.json" => {
                fs::remove_file(object.join(target)).unwrap();
                symlink(base.join("secret"), object.join(target)).unwrap();
            }
            "object" => {
                fs::rename(&object, base.join("real")).unwrap();
                symlink(base.join("real"), &object).unwrap();
            }
            "hardlink" => {
                fs::hard_link(object.join("payload"), base.join("linked")).unwrap();
            }
            "fifo" => {
                fs::remove_file(object.join("payload")).unwrap();
                let path =
                    std::ffi::CString::new(object.join("payload").as_os_str().as_encoded_bytes())
                        .unwrap();
                // Test-only fixture creates a FIFO; provider open must never block on it.
                assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
            }
            _ => {
                fs::write(object.join("unexpected"), "x").unwrap();
            }
        }
        code(
            bounded(storage.get("a", ReadOptions::default())).await,
            ErrorCode::Forbidden,
        );
        code(
            storage.head("a", OperationOptions::default()).await,
            ErrorCode::Forbidden,
        );
        code(
            storage.delete("a", OperationOptions::default()).await,
            ErrorCode::Forbidden,
        );
        code(
            storage.list(ListOptions::default()).await,
            ErrorCode::Forbidden,
        );
        assert_eq!(fs::read(base.join("secret")).unwrap(), b"secret");
        storage.close().await.unwrap();
    }
    let (temp, storage) = fixture().await;
    fs::write(root(&temp).join("unexpected"), "x").unwrap();
    code(
        storage.list(ListOptions::default()).await,
        ErrorCode::Forbidden,
    );
    fs::remove_file(root(&temp).join("unexpected")).unwrap();
    symlink(temp.path(), root(&temp).join(".tmp-unsafe")).unwrap();
    code(
        storage.list(ListOptions::default()).await,
        ErrorCode::Forbidden,
    );
    storage.close().await.unwrap();
}

#[tokio::test]
async fn incomplete_forged_and_corrupt_objects_are_not_absence() {
    for mutation in ["missing", "wrong-key", "size", "json", "large", "etag"] {
        let (temp, storage) = fixture().await;
        storage.put(input("a", "value")).await.unwrap();
        let path = root(&temp).join(name("a"));
        match mutation {
            "missing" => fs::remove_file(path.join("payload")).unwrap(),
            "wrong-key" => {
                fs::rename(&path, root(&temp).join(name("b"))).unwrap();
            }
            "size" => fs::write(path.join("payload"), "changed size").unwrap(),
            "large" => fs::write(path.join("metadata.json"), "x".repeat(65537)).unwrap(),
            "etag" => {
                let mut metadata: serde_json::Value =
                    serde_json::from_slice(&fs::read(path.join("metadata.json")).unwrap()).unwrap();
                metadata["etag"] =
                    serde_json::Value::String("\"".to_owned() + &"é".repeat(32) + "\"");
                fs::write(
                    path.join("metadata.json"),
                    serde_json::to_vec(&metadata).unwrap(),
                )
                .unwrap();
            }
            _ => fs::write(path.join("metadata.json"), "not json").unwrap(),
        }
        let key = if mutation == "wrong-key" { "b" } else { "a" };
        let expected = if mutation == "missing" {
            ErrorCode::Forbidden
        } else {
            ErrorCode::Provider
        };
        code(
            storage.head(key, OperationOptions::default()).await,
            expected,
        );
        code(storage.get(key, ReadOptions::default()).await, expected);
        code(
            storage.delete(key, OperationOptions::default()).await,
            expected,
        );
        code(storage.list(ListOptions::default()).await, expected);
        storage.close().await.unwrap();
    }
}

use image::{DynamicImage, GenericImageView, ImageFormat, Rgb, RgbImage};
use lenso_media::{Authority, Config, Error, Format, Limits, Media, MediaPlugin, Preset, Source};
use lenso_plugin_core::{App, BoxError, Instance, RunningApp};
use lenso_storage::{LocalStorage, LocalStorageConfig, LocalStoragePlugin, PutObject};
use std::{
    collections::BTreeMap,
    io::Cursor,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
struct Policy(AtomicBool);
impl Authority for Policy {
    fn authorize(&self, s: &Source) -> bool {
        self.0.load(Ordering::SeqCst) && (s.scope == "owner" || s.scope == "other-owner")
    }
}
async fn start(
    root: &Path,
    policy: Arc<Policy>,
    limits: Limits,
) -> Result<(RunningApp, Arc<LocalStorage>, Arc<Media>), BoxError> {
    let storage = Instance::new(
        "storage",
        LocalStoragePlugin::new(LocalStorageConfig::new(root.join("objects"))),
    );
    let sh = storage.handle();
    let media = Instance::new(
        "media",
        MediaPlugin::new(
            sh.clone(),
            Config {
                storage_id: "fixture-storage".into(),
                journal: root.join("media.db"),
                limits,
                presets: BTreeMap::from([
                    (
                        "thumb".into(),
                        Preset {
                            version: "1".into(),
                            width: 8,
                            height: 8,
                            crop: None,
                            format: Format::Png,
                        },
                    ),
                    (
                        "crop".into(),
                        Preset {
                            version: "2".into(),
                            width: 4,
                            height: 4,
                            crop: Some([0, 0, 8, 8]),
                            format: Format::Jpeg,
                        },
                    ),
                    (
                        "bad-crop".into(),
                        Preset {
                            version: "1".into(),
                            width: 4,
                            height: 4,
                            crop: Some([u32::MAX, 0, 8, 8]),
                            format: Format::Png,
                        },
                    ),
                ]),
            },
            policy,
        ),
    );
    let mh = media.handle();
    let app = App::new().with(storage).with(media).start().await?;
    let s = app.get(&sh)?;
    let m = app.get(&mh)?;
    Ok((app, s, m))
}
async fn upload(storage: &LocalStorage, format: ImageFormat, color: u8) -> Source {
    let mut encoded = Cursor::new(Vec::new());
    DynamicImage::ImageRgb8(RgbImage::from_pixel(32, 16, Rgb([color, 20, 40])))
        .write_to(&mut encoded, format)
        .unwrap();
    let meta = storage
        .put(PutObject::new(
            "original",
            Cursor::new(encoded.into_inner()),
        ))
        .await
        .unwrap();
    Source {
        key: meta.key,
        etag: meta.etag,
        scope: "owner".into(),
    }
}
#[tokio::test]
async fn transformations_cache_authority_versions_and_restart() -> Result<(), BoxError> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().canonicalize()?;
    let policy = Arc::new(Policy(AtomicBool::new(true)));
    let (app, s, m) = start(&root, policy.clone(), Limits::default()).await?;
    let src = upload(&s, ImageFormat::Png, 220).await;
    assert_eq!(m.inspect(src.clone()).await?, (32, 16));
    let d = m.derive(src.clone(), "thumb".into()).await?;
    let bytes = m.read(&d).await?;
    let img = image::load_from_memory(&bytes)?;
    assert_eq!(img.dimensions(), (8, 4));
    assert_eq!(img.to_rgb8().get_pixel(0, 0)[0], 220);
    assert_eq!(m.derive(src.clone(), "thumb".into()).await?.key(), d.key());
    let crop = m.derive(src.clone(), "crop".into()).await?;
    assert_eq!((crop.width, crop.height), (4, 4));
    assert_ne!(crop.key(), d.key());
    assert_eq!(
        image::guess_format(&m.read(&crop).await?)?,
        ImageFormat::Jpeg
    );
    assert!(matches!(
        m.derive(src.clone(), "bad-crop".into()).await,
        Err(Error::Invalid)
    ));
    let mut scoped = src.clone();
    scoped.scope = "other-owner".into();
    assert_ne!(m.derive(scoped, "thumb".into()).await?.key(), d.key());
    let mut bad = src.clone();
    bad.scope = "forbidden".into();
    assert!(matches!(
        m.derive(bad, "thumb".into()).await,
        Err(Error::Forbidden)
    ));
    policy.0.store(false, Ordering::SeqCst);
    assert!(matches!(m.read(&d).await, Err(Error::Forbidden)));
    policy.0.store(true, Ordering::SeqCst);
    app.shutdown().await?;
    assert!(matches!(m.read(&d).await, Err(Error::Busy)));
    let (app, s, m) = start(&root, policy, Limits::default()).await?;
    assert_eq!(m.derive(src.clone(), "thumb".into()).await?.key(), d.key());
    s.delete("original", Default::default()).await?;
    let changed = upload(&s, ImageFormat::Jpeg, 150).await;
    assert!(matches!(m.read(&d).await, Err(Error::Stale)));
    assert_ne!(m.derive(changed, "thumb".into()).await?.key(), d.key());
    app.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn bounds_and_publish_failure_recovery() -> Result<(), BoxError> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().canonicalize()?;
    let policy = Arc::new(Policy(AtomicBool::new(true)));
    let (app, s, m) = start(&root, policy.clone(), Limits::default()).await?;
    let src = upload(&s, ImageFormat::Png, 220).await;
    let db = rusqlite::Connection::open(root.join("media.db"))?;
    db.execute_batch("CREATE TRIGGER fail_publish BEFORE UPDATE ON media_artifacts BEGIN SELECT RAISE(FAIL,'fixture publish failure'); END;")?;
    assert!(matches!(
        m.derive(src.clone(), "thumb".into()).await,
        Err(Error::Journal)
    ));
    assert_eq!(s.list(Default::default()).await?.objects.len(), 1);
    db.execute_batch("DROP TRIGGER fail_publish")?;
    let ready = m.derive(src.clone(), "thumb".into()).await?;
    // Simulate process death after uploading a journaled artifact, before publishing.
    let pending = "media-derived/fixture-crash";
    s.put(PutObject::new(pending, Cursor::new(b"unpublished")))
        .await?;
    db.execute(
        "INSERT INTO media_artifacts VALUES ('fixture-crash',?,NULL)",
        [pending],
    )?;
    drop(db);
    app.shutdown().await?;
    let (app, s, m) = start(&root, policy.clone(), Limits::default()).await?;
    assert!(s.head(pending, Default::default()).await?.is_none());
    assert!(!m.read(&ready).await?.is_empty());
    app.shutdown().await?;
    let (app, _, m) = start(
        &root,
        policy.clone(),
        Limits {
            max_pixels: 64,
            ..Limits::default()
        },
    )
    .await?;
    assert!(matches!(m.inspect(src.clone()).await, Err(Error::Limit)));
    app.shutdown().await?;
    let (app, _, m) = start(
        &root,
        policy,
        Limits {
            input_bytes: 8,
            ..Limits::default()
        },
    )
    .await?;
    assert!(matches!(m.inspect(src).await, Err(Error::Limit)));
    app.shutdown().await?;
    Ok(())
}

struct PausedStorage {
    local: Arc<LocalStorage>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}
impl lenso_storage::ObjectStorage for PausedStorage {
    fn capabilities(&self) -> lenso_storage::StorageCapabilities {
        self.local.capabilities()
    }
    fn put(
        &self,
        p: lenso_storage::PutObject,
    ) -> lenso_storage::StorageFuture<'_, lenso_storage::ObjectMetadata> {
        Box::pin(self.local.put(p))
    }
    fn get(
        &self,
        key: &str,
        opts: lenso_storage::ReadOptions,
    ) -> lenso_storage::StorageFuture<'_, lenso_storage::ObjectDownload> {
        let key = key.to_owned();
        Box::pin(async move {
            if key == "original" {
                self.entered.notify_one();
                self.release.notified().await;
            }
            self.local.get(&key, opts).await
        })
    }
    fn head(
        &self,
        key: &str,
        opts: lenso_storage::OperationOptions,
    ) -> lenso_storage::StorageFuture<'_, Option<lenso_storage::ObjectMetadata>> {
        let key = key.to_owned();
        Box::pin(async move { self.local.head(&key, opts).await })
    }
    fn delete(
        &self,
        key: &str,
        opts: lenso_storage::OperationOptions,
    ) -> lenso_storage::StorageFuture<'_, lenso_storage::DeleteOutcome> {
        let key = key.to_owned();
        Box::pin(async move { self.local.delete(&key, opts).await })
    }
    fn list(
        &self,
        opts: lenso_storage::ListOptions,
    ) -> lenso_storage::StorageFuture<'_, lenso_storage::ObjectPage> {
        Box::pin(self.local.list(opts))
    }
}
struct PausedPlugin {
    local: lenso_plugin_core::Handle<LocalStorage>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}
impl lenso_plugin_core::Plugin for PausedPlugin {
    type Service = PausedStorage;
    fn dependencies(&self) -> Vec<lenso_plugin_core::InstanceKey> {
        vec![self.local.key()]
    }
    async fn setup(
        self,
        setup: &mut lenso_plugin_core::Setup,
    ) -> Result<Arc<PausedStorage>, BoxError> {
        Ok(Arc::new(PausedStorage {
            local: setup.get(&self.local)?,
            entered: self.entered,
            release: self.release,
        }))
    }
}
#[tokio::test]
async fn cancellation_keeps_admission_and_shutdown_waits() -> Result<(), BoxError> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().canonicalize()?;
    let local = Instance::new(
        "local",
        LocalStoragePlugin::new(LocalStorageConfig::new(root.join("objects"))),
    );
    let sh = local.handle();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let paused = Instance::new(
        "paused",
        PausedPlugin {
            local: sh.clone(),
            entered: entered.clone(),
            release: release.clone(),
        },
    );
    let media = Instance::new(
        "media",
        MediaPlugin::new(
            paused.handle(),
            Config {
                storage_id: "fixture-storage".into(),
                journal: root.join("media.db"),
                limits: Limits::default(),
                presets: BTreeMap::from([(
                    "thumb".into(),
                    Preset {
                        version: "1".into(),
                        width: 8,
                        height: 8,
                        crop: None,
                        format: Format::Png,
                    },
                )]),
            },
            Arc::new(Policy(AtomicBool::new(true))),
        ),
    );
    let mh = media.handle();
    let app = App::new()
        .with(local)
        .with(paused)
        .with(media)
        .start()
        .await?;
    let storage = app.get(&sh)?;
    let media = app.get(&mh)?;
    let src = upload(&storage, ImageFormat::Png, 200).await;
    let m = media.clone();
    let s = src.clone();
    let caller = tokio::spawn(async move { m.derive(s, "thumb".into()).await });
    entered.notified().await;
    assert!(matches!(media.inspect(src.clone()).await, Err(Error::Busy)));
    caller.abort();
    let _ = caller.await;
    let mut shutdown = tokio::spawn(async move { app.shutdown().await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(30), &mut shutdown)
            .await
            .is_err()
    );
    release.notify_one();
    shutdown.await??;
    assert!(matches!(
        media.derive(src.clone(), "thumb".into()).await,
        Err(Error::Busy)
    ));
    let (app, _, media) = start(
        &root,
        Arc::new(Policy(AtomicBool::new(true))),
        Limits::default(),
    )
    .await?;
    let cached = media.derive(src, "thumb".into()).await?;
    assert_eq!(cached.width, 8);
    let db = rusqlite::Connection::open(root.join("media.db"))?;
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM media_artifacts WHERE result IS NOT NULL",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        1
    );
    app.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn invalid_images_output_budget_and_config_fail_closed() -> Result<(), BoxError> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().canonicalize()?;
    let policy = Arc::new(Policy(AtomicBool::new(true)));
    assert!(start(
        &root,
        policy.clone(),
        Limits {
            memory_bytes: 1,
            ..Limits::default()
        }
    )
    .await
    .is_err());
    assert!(!root.join("media.db").exists());
    let (app, s, m) = start(
        &root,
        policy,
        Limits {
            output_bytes: 8,
            ..Limits::default()
        },
    )
    .await?;
    let src = upload(&s, ImageFormat::Png, 220).await;
    assert!(m.derive(src, "thumb".into()).await.is_err());
    assert_eq!(s.list(Default::default()).await?.objects.len(), 1);
    s.delete("original", Default::default()).await?;
    let meta = s
        .put(PutObject::new("original", Cursor::new(b"not an image")))
        .await?;
    assert!(matches!(
        m.inspect(Source {
            key: meta.key,
            etag: meta.etag,
            scope: "owner".into()
        })
        .await,
        Err(Error::Image)
    ));
    app.shutdown().await?;
    Ok(())
}

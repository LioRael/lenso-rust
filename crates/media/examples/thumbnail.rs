use image::{DynamicImage, GenericImageView, ImageFormat, Rgb, RgbImage};
use lenso_media::{Authority, Config, Format, Limits, MediaPlugin, Preset, Source};
use lenso_plugin_core::{App, BoxError, Instance};
use lenso_storage::{LocalStorageConfig, LocalStoragePlugin, PutObject};
use std::{collections::BTreeMap, io::Cursor, sync::Arc};
// Fixture policy only. A real host resolves its verified principal and current ACL.
struct NotesPolicy;
impl Authority for NotesPolicy {
    fn authorize(&self, source: &Source) -> bool {
        source.scope == "fixture-notes-owner" && source.key == "notes/original.png"
    }
}
#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().canonicalize()?;
    let storage = Instance::new(
        "attachments",
        LocalStoragePlugin::new(LocalStorageConfig::new(root.join("objects"))),
    );
    let sh = storage.handle();
    let media = Instance::new(
        "note-images",
        MediaPlugin::new(
            sh.clone(),
            Config {
                storage_id: "fixture-notes-storage-v1".into(),
                journal: root.join("media.sqlite"),
                limits: Limits::default(),
                presets: BTreeMap::from([(
                    "note-card".into(),
                    Preset {
                        version: "1".into(),
                        width: 16,
                        height: 16,
                        crop: None,
                        format: Format::Jpeg,
                    },
                )]),
            },
            Arc::new(NotesPolicy),
        ),
    );
    let mh = media.handle();
    let app = App::new().with(storage).with(media).start().await?;
    let result: Result<(), BoxError> = async {
        let mut bytes = Cursor::new(Vec::new());
        DynamicImage::ImageRgb8(RgbImage::from_pixel(48, 24, Rgb([210, 40, 20])))
            .write_to(&mut bytes, ImageFormat::Png)?;
        let original = app
            .get(&sh)?
            .put(PutObject::new(
                "notes/original.png",
                Cursor::new(bytes.into_inner()),
            ))
            .await?;
        let source = Source {
            key: original.key,
            etag: original.etag,
            scope: "fixture-notes-owner".into(),
        };
        let service = app.get(&mh)?;
        assert_eq!(service.inspect(source.clone()).await?, (48, 24));
        let card = service.derive(source.clone(), "note-card".into()).await?;
        let image = image::load_from_memory(&service.read(&card).await?)?;
        assert_eq!(image.dimensions(), (16, 8));
        assert!(image.to_rgb8().get_pixel(0, 0)[0] > 190);
        let cached = service.derive(source, "note-card".into()).await?;
        assert_eq!(card.key(), cached.key());
        println!(
            "Note card: PNG 48x24 -> JPEG {}x{}; cache reused {}",
            card.width,
            card.height,
            card.key()
        );
        Ok(())
    }
    .await;
    let shutdown = app.shutdown().await;
    match (result, shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(e), Ok(())) => Err(e),
        (Ok(()), Err(e)) => Err(e.into()),
        (Err(e), Err(cleanup)) => Err(format!("{e}; cleanup: {cleanup}").into()),
    }
}

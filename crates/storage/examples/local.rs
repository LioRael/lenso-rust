use lenso_storage::{
    LocalStorage, LocalStorageConfig, PutObject, ReadOptions, ReadRange, ResolvedRange,
};
use std::io::Cursor;
use tokio::io::AsyncReadExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    // /tmp and /var can be symlinks on macOS; resolve the trusted test base first.
    let storage = LocalStorage::open(LocalStorageConfig::new(
        temp.path().canonicalize()?.join("objects"),
    ))
    .await?;
    let mut input = PutObject::new("notes/hello.txt", Cursor::new(b"hello storage"));
    input.content_type = "text/plain".into();
    input.size = Some(13);
    input.max_bytes = Some(1024);
    let metadata = storage.put(input).await?;
    let mut download = storage
        .get(
            &metadata.key,
            ReadOptions {
                range: Some(ReadRange {
                    offset: 6,
                    length: None,
                }),
                if_match: Some(metadata.etag.clone()),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(
        download.range,
        Some(ResolvedRange {
            offset: 6,
            length: 7
        })
    );
    let mut text = String::new();
    download.body.read_to_string(&mut text).await?;
    assert_eq!(text, "storage");
    println!(
        "{}: {} bytes, selected text: {text}",
        metadata.key, metadata.size
    );
    storage.close().await?;
    Ok(())
}

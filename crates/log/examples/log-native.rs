use lenso_log::{LogError, Logger, LoggerOptions};
use serde_json::json;

fn greet(log: &Logger, name: &str) -> Result<String, LogError> {
    log.info(json!({"event": "greet", "name": name}), "greeting")?;
    Ok(format!("Hello, {name}!"))
}

#[tokio::main]
async fn main() -> Result<(), LogError> {
    let log = Logger::new("greeting", "native", LoggerOptions::default());
    let _greeting = greet(&log, "Ada")?;
    log.close().await
}

use lenso_log::{Logger, LoggerOptions, LoggerPlugin};
use lenso_plugin_core::{App, BoxError, Handle, InstanceKey, Plugin, Setup};
use serde_json::json;
use std::sync::Arc;

struct Greeting {
    log: Arc<Logger>,
}
impl Greeting {
    fn greet(&self, name: &str) -> Result<String, BoxError> {
        self.log
            .info(json!({"event": "greet", "name": name}), "greeting")?;
        Ok(format!("Hello, {name}!"))
    }
}

struct GreetingPlugin {
    log: Handle<Logger>,
}
impl Plugin for GreetingPlugin {
    type Service = Greeting;

    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.log.key()]
    }

    async fn setup(self, setup: &mut Setup) -> Result<Arc<Greeting>, BoxError> {
        Ok(Arc::new(Greeting {
            log: setup.get(&self.log)?,
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let mut app = App::new();
    let log = app.install(
        "application-log",
        LoggerPlugin::new("greeting", LoggerOptions::default()),
    );
    let greeting = app.install("greeting", GreetingPlugin { log });
    let running = app.start().await?;
    let _greeting = running.get(&greeting)?.greet("Ada")?;
    running.shutdown().await?;
    Ok(())
}

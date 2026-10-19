//! Static public-API consumer; run with `cargo run -p lenso-plugin-core --example inspect --locked`.
use lenso_plugin_core::{App, BoxError, Instance, InstanceKey, Plugin, Setup};
use std::sync::Arc;

struct StaticPlugin(Vec<InstanceKey>);
impl Plugin for StaticPlugin {
    type Service = ();
    fn dependencies(&self) -> Vec<InstanceKey> {
        self.0.clone()
    }
    async fn setup(self, _: &mut Setup) -> Result<Arc<()>, BoxError> {
        panic!("this consumer must never acquire resources or invoke setup")
    }
}

fn main() -> Result<(), BoxError> {
    let source = Instance::new("source", StaticPlugin(vec![]));
    let consumer = Instance::new("consumer", StaticPlugin(vec![source.handle().key()]));
    let app = App::new().with(consumer).with(source);
    let report = app.inspect();
    assert!(report.is_valid());
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

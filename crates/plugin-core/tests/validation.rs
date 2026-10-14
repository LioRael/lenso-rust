use lenso_plugin_core::{App, BoxError, Plugin, Setup};
use std::sync::Arc;

#[tokio::test]
async fn arbitrary_validation_errors_do_not_expose_rejected_config_values() {
    struct Invalid;
    impl Plugin for Invalid {
        type Service = ();
        fn validate(&self) -> Result<(), BoxError> {
            Err("credential=private-value".into())
        }
        async fn setup(self, _: &mut Setup) -> Result<Arc<()>, BoxError> {
            panic!("invalid plugin must never reach setup");
        }
    }
    let mut app = App::new();
    app.install("private-plugin", Invalid);
    let error = app.start().await.err().unwrap();
    let text = format!("{error} {error:?}");
    assert!(text.contains("private-plugin"));
    assert!(!text.contains("private-value"));
}

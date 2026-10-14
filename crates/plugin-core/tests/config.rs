use lenso_plugin_core::{Config, ConfigSource};
use serde::Deserialize;
use serde_json::json;
use std::sync::Mutex;

static ENVIRONMENT: Mutex<()> = Mutex::new(());

#[derive(Debug, Deserialize, PartialEq)]
#[serde(default)]
struct Settings {
    limit: usize,
    nested: Nested,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            limit: 10,
            nested: Nested::default(),
        }
    }
}
#[derive(Debug, Deserialize, PartialEq)]
#[serde(default)]
struct Nested {
    label: String,
}
impl Default for Nested {
    fn default() -> Self {
        Self {
            label: "default".into(),
        }
    }
}

#[test]
fn selected_json_environment_overrides_explicit_and_serde_defaults() {
    let _lock = ENVIRONMENT.lock().unwrap();
    std::env::set_var("LENSO_CORE_TEST_LIMIT", "30");
    std::env::set_var("LENSO_CORE_TEST_LABEL", "\"environment\"");
    std::env::set_var("LENSO_CORE_TEST_UNSELECTED", "not JSON");
    let settings = Config::<Settings>::new("worker")
        .explicit_json(json!({"limit": 20, "nested": {}}))
        .env_json("/limit", "LENSO_CORE_TEST_LIMIT")
        .env_json("/nested/label", "LENSO_CORE_TEST_LABEL")
        .resolve()
        .unwrap();
    assert_eq!(settings.limit, 30);
    assert_eq!(settings.nested.label, "environment");
    assert_eq!(
        Config::<Settings>::new("default").resolve().unwrap(),
        Settings::default()
    );
    std::env::remove_var("LENSO_CORE_TEST_LIMIT");
    std::env::remove_var("LENSO_CORE_TEST_LABEL");
    std::env::remove_var("LENSO_CORE_TEST_UNSELECTED");
}

#[test]
fn absent_selected_environment_preserves_explicit_value() {
    let _lock = ENVIRONMENT.lock().unwrap();
    std::env::remove_var("LENSO_CORE_TEST_ABSENT");
    let settings = Config::<Settings>::new("worker")
        .explicit_json(json!({"limit": 20}))
        .env_json("/limit", "LENSO_CORE_TEST_ABSENT")
        .resolve()
        .unwrap();
    assert_eq!(settings.limit, 20);
    assert_eq!(settings.nested, Nested::default());
}

#[test]
fn errors_identify_instance_and_field_without_retaining_sensitive_input() {
    let _lock = ENVIRONMENT.lock().unwrap();
    for raw in ["secret-invalid-json", "\"secret-type-mismatch\""] {
        std::env::set_var("LENSO_CORE_TEST_BAD", raw);
        let error = Config::<Settings>::new("private-worker")
            .env_json("/limit", "LENSO_CORE_TEST_BAD")
            .resolve()
            .unwrap_err();
        assert_eq!(error.instance, "private-worker");
        assert_eq!(error.field_path, "/limit");
        assert_eq!(
            error.origin,
            ConfigSource::Environment {
                variable: "LENSO_CORE_TEST_BAD".into()
            }
        );
        assert!(!format!("{error} {error:?}").contains("secret"));
        assert!(std::error::Error::source(&error).is_none());
    }
    std::env::remove_var("LENSO_CORE_TEST_BAD");
    let config = Config::<Settings>::new("explicit")
        .diagnostic_field("/nested/label")
        .explicit_json(json!({"nested": {"label": ["secret"]}}));
    assert!(!format!("{config:?}").contains("secret"));
    let error = config.resolve().unwrap_err();
    assert_eq!(error.field_path, "/nested/label");
    assert_eq!(error.origin, ConfigSource::Explicit);
    assert!(!format!("{error} {error:?}").contains("secret"));
}

#[test]
fn missing_required_fields_identify_the_field_and_default_source() {
    #[derive(Debug, Deserialize)]
    struct Required {
        #[serde(rename = "required")]
        _required: usize,
    }
    let error = Config::<Required>::new("required-instance")
        .diagnostic_field("/required")
        .resolve()
        .unwrap_err();
    assert_eq!(error.field_path, "/required");
    assert_eq!(error.origin, ConfigSource::Defaults);
}

#[test]
fn alphanumeric_dynamic_map_keys_are_not_diagnostic_field_names() {
    let error = Config::<std::collections::HashMap<String, usize>>::new("map")
        .explicit_json(json!({"secret_api_key_123": "wrong-type"}))
        .resolve()
        .unwrap_err();
    assert_eq!(error.field_path, "/<redacted>");
    assert!(!format!("{error} {error:?}").contains("secret_api_key_123"));

    #[derive(Debug, Deserialize)]
    struct NestedMap {
        #[serde(rename = "nested")]
        _nested: std::collections::HashMap<String, usize>,
    }
    let error = Config::<NestedMap>::new("nested")
        .diagnostic_field("/nested")
        .explicit_json(json!({"nested": {"privatekey123": "wrong-type"}}))
        .resolve()
        .unwrap_err();
    assert_eq!(error.field_path, "/nested/<redacted>");
    assert!(!format!("{error} {error:?}").contains("privatekey123"));
}

#[test]
fn sensitive_subtree_masks_success_metadata_debug_and_early_env_errors() {
    let _lock = ENVIRONMENT.lock().unwrap();
    #[derive(Debug, Deserialize)]
    struct Credentials {
        #[serde(rename = "credentials")]
        _credentials: std::collections::HashMap<String, String>,
    }
    std::env::set_var("LENSO_CORE_TEST_SENSITIVE", "\"private-value\"");
    let config = || {
        Config::<Credentials>::new("credentials")
            .env_json("/credentials/privatekey123", "LENSO_CORE_TEST_SENSITIVE")
            .sensitive("/credentials")
    };
    assert!(!format!("{:?}", config()).contains("privatekey123"));
    let (_, sources) = config().resolve_with_sources().unwrap();
    assert_eq!(sources[0].field_path, "/credentials/<redacted>");
    assert!(!format!("{sources:?}").contains("private-value"));
    std::env::set_var("LENSO_CORE_TEST_SENSITIVE", "private-invalid-json");
    let error = config().resolve().unwrap_err();
    assert_eq!(error.field_path, "/credentials/<redacted>");
    assert!(!format!("{error} {error:?}").contains("privatekey123"));
    assert!(!format!("{error} {error:?}").contains("private-invalid-json"));
    std::env::remove_var("LENSO_CORE_TEST_SENSITIVE");
}

#[test]
fn success_provenance_reports_selected_values_and_missing_serde_defaults() {
    let _lock = ENVIRONMENT.lock().unwrap();
    std::env::set_var("LENSO_CORE_TEST_PROVENANCE", "{}");
    let (settings, sources) = Config::<Settings>::new("worker")
        .explicit_json(json!({"limit": 20, "nested": {"label": "explicit"}}))
        .diagnostic_field("/limit")
        .diagnostic_field("/nested/label")
        .env_json("/nested", "LENSO_CORE_TEST_PROVENANCE")
        .resolve_with_sources()
        .unwrap();
    assert_eq!(settings.limit, 20);
    assert_eq!(settings.nested.label, "default");
    assert_eq!(sources[0].source, ConfigSource::Explicit);
    assert_eq!(sources[1].source, ConfigSource::Defaults);
    assert_eq!(
        sources[2].source,
        ConfigSource::Environment {
            variable: "LENSO_CORE_TEST_PROVENANCE".into()
        }
    );
    assert!(!format!("{sources:?}").contains("explicit"));
    std::env::remove_var("LENSO_CORE_TEST_PROVENANCE");
}

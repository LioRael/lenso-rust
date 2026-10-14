use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use std::{fmt, marker::PhantomData};

/// Redacted provenance, never configuration values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfigSource {
    /// A value supplied in the explicit JSON object.
    Explicit,
    /// A specifically selected environment variable containing JSON.
    Environment {
        /// Environment variable name, not its value.
        variable: String,
    },
    /// No override was supplied; serde applies the type's defaults.
    Defaults,
}

/// A sanitized error. Raw serde errors and input are not retained.
#[derive(Debug, thiserror::Error)]
#[error("invalid config for {instance} at {field_path} ({origin:?}): {reason}")]
pub struct ConfigError {
    /// Instance owning this configuration.
    pub instance: String,
    /// JSON pointer identifying the field; the empty pointer means the root.
    pub field_path: String,
    /// Redacted source of the failing value.
    pub origin: ConfigSource,
    /// Fixed diagnostic text that never contains an input value.
    pub reason: &'static str,
}

/// Redacted source metadata for a resolved field; contains no configuration value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigFieldSource {
    /// Safe JSON pointer, or `/<redacted>` when not declared.
    pub field_path: String,
    /// The layer that supplied the field.
    pub source: ConfigSource,
}

/// Resolve ordinary serde configuration with environment > explicit > serde defaults.
///
/// Only selected environment variables are read. Each is parsed as JSON (strings
/// must be quoted). Field paths are object-only JSON pointers, not a config DSL.
/// Absent environment variables leave explicit values/defaults unchanged.
pub struct Config<T> {
    instance: String,
    explicit: Value,
    environment: Vec<(String, String)>,
    diagnostic_fields: Vec<String>,
    sensitive_fields: Vec<String>,
    marker: PhantomData<fn() -> T>,
}

impl<T> fmt::Debug for Config<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("instance", &self.instance)
            .field("explicit", &"<redacted>")
            .field(
                "environment",
                &self
                    .environment
                    .iter()
                    .map(|(path, variable)| {
                        (
                            project_path(path, &self.diagnostic_fields, &self.sensitive_fields),
                            variable,
                        )
                    })
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl<T: DeserializeOwned> Config<T> {
    /// Start with an empty JSON object so serde owns defaults.
    pub fn new(instance: impl Into<String>) -> Self {
        Self {
            instance: instance.into(),
            explicit: Value::Object(Map::new()),
            environment: Vec::new(),
            diagnostic_fields: Vec::new(),
            sensitive_fields: Vec::new(),
            marker: PhantomData,
        }
    }

    /// Supply a JSON object. Missing fields remain eligible for serde defaults.
    pub fn explicit_json(mut self, value: Value) -> Self {
        self.explicit = value;
        self
    }

    /// Select one environment variable to override an object field JSON pointer.
    pub fn env_json(mut self, field_path: impl Into<String>, variable: impl Into<String>) -> Self {
        let field_path = field_path.into();
        if !self.diagnostic_fields.contains(&field_path) {
            self.diagnostic_fields.push(field_path.clone());
        }
        self.environment.push((field_path, variable.into()));
        self
    }

    /// Allow this field to appear in diagnostics.
    pub fn diagnostic_field(mut self, field_path: impl Into<String>) -> Self {
        let field_path = field_path.into();
        if !self.diagnostic_fields.contains(&field_path) {
            self.diagnostic_fields.push(field_path);
        }
        self
    }

    /// Redact descendants of this field from diagnostics.
    pub fn sensitive(mut self, field_path: impl Into<String>) -> Self {
        self.sensitive_fields.push(field_path.into());
        self
    }

    /// Resolve and deserialize without exposing values in any error chain.
    pub fn resolve(self) -> Result<T, ConfigError> {
        self.resolve_with_sources().map(|(value, _)| value)
    }

    /// Resolve and return source metadata without configuration values.
    pub fn resolve_with_sources(self) -> Result<(T, Vec<ConfigFieldSource>), ConfigError> {
        let instance = self.instance;
        let error = |path: String, origin: ConfigSource, reason| ConfigError {
            instance: instance.clone(),
            field_path: project_path(&path, &self.diagnostic_fields, &self.sensitive_fields),
            origin,
            reason,
        };
        if !self.explicit.is_object() {
            return Err(error(
                String::new(),
                ConfigSource::Explicit,
                "expected a JSON object",
            ));
        }
        let mut value = self.explicit;
        let mut applied = Vec::new();
        for (path, variable) in self.environment {
            let origin = ConfigSource::Environment {
                variable: variable.clone(),
            };
            let fields = object_path(&path).ok_or_else(|| {
                error(
                    path.clone(),
                    origin.clone(),
                    "expected an object field JSON pointer",
                )
            })?;
            let raw = match std::env::var(&variable) {
                Ok(raw) => raw,
                Err(std::env::VarError::NotPresent) => continue,
                Err(std::env::VarError::NotUnicode(_)) => {
                    return Err(error(path, origin, "environment value is not Unicode"));
                }
            };
            let replacement = serde_json::from_str(&raw)
                .map_err(|_| error(path.clone(), origin.clone(), "expected valid JSON"))?;
            set_field(&mut value, &fields, replacement).map_err(|_| {
                error(
                    path.clone(),
                    origin.clone(),
                    "parent field is not an object",
                )
            })?;
            applied.push((path, origin));
        }
        let resolved: T = serde_path_to_error::deserialize(&value).map_err(|failure| {
            // Map keys can be secrets; only declared metadata supplies diagnostic names.
            let mut path = String::new();
            for segment in failure.path() {
                match segment {
                    serde_path_to_error::Segment::Map { key } => {
                        path.push('/');
                        path.push_str(&pointer_segment(key));
                    }
                    serde_path_to_error::Segment::Seq { index } => {
                        path.push('/');
                        path.push_str(&index.to_string());
                    }
                    _ => path.push_str("/<redacted>"),
                }
            }
            let message = failure.inner().to_string();
            if let Some(field) = message
                .strip_prefix("missing field `")
                .and_then(|message| message.strip_suffix('`'))
                .filter(|field| {
                    self.diagnostic_fields
                        .iter()
                        .any(|p| p.ends_with(&format!("/{field}")))
                })
            {
                path.push('/');
                path.push_str(&pointer_segment(field));
            }
            let origin = source_for(&path, &value, &applied);
            error(path, origin, "value does not match the configuration type")
        })?;
        let mut sources = Vec::new();
        for path in &self.diagnostic_fields {
            let source = source_for(path, &value, &applied);
            sources.push(ConfigFieldSource {
                field_path: project_path(path, &self.diagnostic_fields, &self.sensitive_fields),
                source,
            });
        }
        Ok((resolved, sources))
    }
}

fn source_for(path: &str, value: &Value, applied: &[(String, ConfigSource)]) -> ConfigSource {
    if value.pointer(path).is_none() {
        return ConfigSource::Defaults;
    }
    applied
        .iter()
        .rev()
        .find(|(selected, _)| path == selected || path.starts_with(&format!("{selected}/")))
        .map(|(_, source)| source.clone())
        .unwrap_or(ConfigSource::Explicit)
}

fn pointer_segment(field: &str) -> String {
    field.replace('~', "~0").replace('/', "~1")
}

fn project_path(path: &str, known: &[String], sensitive: &[String]) -> String {
    if let Some(root) = sensitive
        .iter()
        .filter(|root| object_path(root).is_some() && path.starts_with(&format!("{root}/")))
        .min_by_key(|root| root.len())
    {
        return format!("{root}/<redacted>");
    }
    let mut projected = String::new();
    for (depth, part) in path.split('/').skip(1).enumerate() {
        if depth >= 8 || part.len() > 128 {
            return format!("{projected}/<redacted>");
        }
        if part.is_empty() {
            continue;
        }
        let candidate = format!("{projected}/{part}");
        if known.iter().any(|known| {
            object_path(known).is_some()
                && (known == &candidate || known.starts_with(&format!("{candidate}/")))
        }) {
            projected = candidate;
        } else {
            return format!("{projected}/<redacted>");
        }
    }
    projected
}

fn object_path(path: &str) -> Option<Vec<String>> {
    let path = path.strip_prefix('/')?;
    let mut fields = Vec::new();
    for (depth, part) in path.split('/').enumerate() {
        if depth >= 8 || part.len() > 128 {
            return None;
        }
        let mut field = String::new();
        let mut chars = part.chars();
        while let Some(c) = chars.next() {
            if c == '~' {
                field.push(match chars.next()? {
                    '0' => '~',
                    '1' => '/',
                    _ => return None,
                });
            } else {
                field.push(c);
            }
        }
        if field.is_empty() {
            return None;
        }
        fields.push(field);
    }
    Some(fields)
}

fn set_field(value: &mut Value, fields: &[String], replacement: Value) -> Result<(), ()> {
    let (field, rest) = fields.split_first().ok_or(())?;
    let object = value.as_object_mut().ok_or(())?;
    if rest.is_empty() {
        object.insert(field.clone(), replacement);
        Ok(())
    } else {
        set_field(
            object
                .entry(field.clone())
                .or_insert_with(|| Value::Object(Map::new())),
            rest,
            replacement,
        )
    }
}

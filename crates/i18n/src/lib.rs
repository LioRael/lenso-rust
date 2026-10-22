//! Small versioned plain-message catalogs; no ICU, markup, plural or date formatting.
use lenso_plugin_core::{BoxError, CleanupPhase, Plugin, Setup, WorkGate};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
};
pub type Variables = BTreeMap<String, String>;
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    #[error("invalid catalog, language header, or variables")]
    Invalid,
    #[error("message missing")]
    Missing,
    #[error("service closed")]
    Closed,
    #[error("catalog version must increase")]
    Version,
}
#[derive(Clone, Debug)]
pub struct Catalog {
    pub version: u32,
    /// Lowercase ASCII language tags; keys form the supported language whitelist.
    pub languages: BTreeMap<String, BTreeMap<String, String>>,
    pub fallback: String,
}
fn tag(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.split('-').all(|p| {
            !p.is_empty()
                && p.len() <= 8
                && p.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        })
}
fn keys(s: &str) -> Result<BTreeSet<String>, Error> {
    if s.len() > 65536 {
        return Err(Error::Invalid);
    }
    let mut keys = BTreeSet::new();
    let mut rest = s;
    while let Some((head, tail)) = rest.split_once("{{") {
        if head.contains(['{', '}']) {
            return Err(Error::Invalid);
        }
        let (key, tail) = tail.split_once("}}").ok_or(Error::Invalid)?;
        if key.is_empty()
            || key.len() > 64
            || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            return Err(Error::Invalid);
        }
        keys.insert(key.into());
        rest = tail;
    }
    if rest.contains(['{', '}']) {
        return Err(Error::Invalid);
    }
    Ok(keys)
}
impl Catalog {
    fn validate(&self) -> Result<(), Error> {
        let base = self.languages.get(&self.fallback).ok_or(Error::Invalid)?;
        if self.version == 0 || base.is_empty() || self.languages.len() > 100 {
            return Err(Error::Invalid);
        }
        for (locale, messages) in &self.languages {
            if !tag(locale) || messages.len() > 10000 {
                return Err(Error::Invalid);
            }
            for (key, text) in messages {
                if key.is_empty()
                    || key.len() > 256
                    || keys(text)? != keys(base.get(key).ok_or(Error::Invalid)?)?
                {
                    return Err(Error::Invalid);
                }
            }
        }
        Ok(())
    }
    fn negotiate(&self, header: &str) -> Result<String, Error> {
        if header.len() > 8192 {
            return Err(Error::Invalid);
        }
        let mut ranges = Vec::new();
        for (order, item) in header
            .split(',')
            .filter(|s| !s.trim().is_empty())
            .enumerate()
        {
            let mut parts = item.trim().split(';');
            let range = parts.next().unwrap().trim().to_ascii_lowercase();
            if range != "*" && !tag(&range) {
                return Err(Error::Invalid);
            }
            let q = match parts.next() {
                None => 1000,
                Some(p) => quality(p.trim().strip_prefix("q=").ok_or(Error::Invalid)?)?,
            };
            if parts.next().is_some() {
                return Err(Error::Invalid);
            }
            ranges.push((range, q, order));
        }
        // For each supported language, the most specific range wins, including q=0.
        let mut candidates = Vec::new();
        for locale in self.languages.keys() {
            let matched = ranges
                .iter()
                .filter(|(r, _, _)| {
                    r == "*"
                        || locale == r
                        || locale.starts_with(&format!("{r}-"))
                        || r.starts_with(&format!("{locale}-"))
                })
                .max_by_key(|(r, _, order)| {
                    (
                        if r == "*" { 0 } else { r.len() },
                        std::cmp::Reverse(*order),
                    )
                });
            if let Some((_, q, order)) = matched {
                if *q > 0 {
                    candidates.push((
                        std::cmp::Reverse(*q),
                        *order,
                        locale != &self.fallback,
                        locale.clone(),
                    ));
                }
            }
        }
        candidates.sort();
        // Explicit server policy: if nothing is acceptable, use configured fallback (not HTTP 406).
        Ok(candidates
            .first()
            .map(|x| x.3.clone())
            .unwrap_or_else(|| self.fallback.clone()))
    }
}
fn quality(s: &str) -> Result<u16, Error> {
    let (whole, fraction) = s.split_once('.').unwrap_or((s, ""));
    if !matches!(whole, "0" | "1")
        || fraction.len() > 3
        || !fraction.bytes().all(|b| b.is_ascii_digit())
        || (whole == "1" && fraction.bytes().any(|b| b != b'0'))
    {
        return Err(Error::Invalid);
    }
    Ok(if whole == "1" {
        1000
    } else {
        fraction.parse::<u16>().unwrap_or(0) * 10u16.pow(3 - fraction.len() as u32)
    })
}
pub struct I18n {
    catalog: Arc<RwLock<Option<Arc<Catalog>>>>,
    gate: WorkGate,
}
impl I18n {
    pub fn request(&self, accept_language: &str) -> Result<RequestLocale, Error> {
        let _guard = self.gate.enter().map_err(|_| Error::Closed)?;
        let catalog = self
            .catalog
            .read()
            .map_err(|_| Error::Closed)?
            .clone()
            .ok_or(Error::Closed)?;
        Ok(RequestLocale {
            locale: catalog.negotiate(accept_language)?,
            catalog,
            gate: self.gate.clone(),
        })
    }
    /// Validate completely before atomically installing a strictly newer snapshot.
    pub fn replace(&self, catalog: Catalog) -> Result<(), Error> {
        let _guard = self.gate.enter().map_err(|_| Error::Closed)?;
        catalog.validate()?;
        let mut current = self.catalog.write().map_err(|_| Error::Closed)?;
        if catalog.version <= current.as_ref().ok_or(Error::Closed)?.version {
            return Err(Error::Version);
        }
        *current = Some(Arc::new(catalog));
        Ok(())
    }
}
pub struct RequestLocale {
    locale: String,
    catalog: Arc<Catalog>,
    gate: WorkGate,
}
impl RequestLocale {
    pub fn locale(&self) -> &str {
        &self.locale
    }
    pub fn version(&self) -> u32 {
        self.catalog.version
    }
    fn message(&self, key: &str) -> Result<&str, Error> {
        self.catalog.languages[&self.locale]
            .get(key)
            .or_else(|| self.catalog.languages[&self.catalog.fallback].get(key))
            .map(String::as_str)
            .ok_or(Error::Missing)
    }
    pub fn text(&self, key: &str, vars: &Variables) -> Result<String, Error> {
        let _guard = self.gate.enter().map_err(|_| Error::Closed)?;
        let source = self.message(key)?;
        if keys(source)? != vars.keys().cloned().collect() || vars.values().any(|s| s.len() > 65536)
        {
            return Err(Error::Invalid);
        }
        let mut out = String::new();
        let mut rest = source;
        while let Some((head, tail)) = rest.split_once("{{") {
            let (key, tail) = tail.split_once("}}").ok_or(Error::Invalid)?;
            out.push_str(head);
            out.push_str(&vars[key]);
            rest = tail;
            if out.len() > 65536 {
                return Err(Error::Invalid);
            }
        }
        out.push_str(rest);
        if out.len() > 65536 {
            return Err(Error::Invalid);
        }
        Ok(out)
    }
    /// Escapes the entire message for an HTML text node. Not for URLs, JS, CSS or attributes.
    pub fn html_text(&self, key: &str, vars: &Variables) -> Result<String, Error> {
        Ok(self
            .text(key, vars)?
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
            .replace('\'', "&#39;"))
    }
    /// Selects raw, validated plain-text templates for Notifications' own single-pass interpolation.
    /// The host owns name/category/required; catalog version identifies immutable message content.
    pub fn notification(
        &self,
        mut template: lenso_notifications::Template,
        subject_key: &str,
        text_key: &str,
    ) -> Result<lenso_notifications::Template, Error> {
        let _guard = self.gate.enter().map_err(|_| Error::Closed)?;
        template.version = self.version();
        template.name = format!("{}:{}", template.name, self.locale);
        template.subject = self.message(subject_key)?.into();
        template.text = self.message(text_key)?.into();
        Ok(template)
    }
}
pub struct I18nPlugin(pub Catalog);
impl Plugin for I18nPlugin {
    type Service = I18n;
    fn validate(&self) -> Result<(), BoxError> {
        self.0.validate().map_err(Into::into)
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<I18n>, BoxError> {
        let catalog = Arc::new(RwLock::new(Some(Arc::new(self.0))));
        let cleanup = catalog.clone();
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            cleanup.write().map_err(|_| Error::Closed)?.take();
            Ok(())
        });
        Ok(Arc::new(I18n {
            catalog,
            gate: setup.work_gate(),
        }))
    }
}

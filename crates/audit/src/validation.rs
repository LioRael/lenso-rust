use crate::*;

const MAX_TIME: u64 = 8_640_000_000_000_000;
const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;
const FORBIDDEN: &[&str] = &[
    "secret",
    "password",
    "token",
    "credential",
    "cookie",
    "authorization",
    "digest",
    "hash",
    "body",
    "payload",
    "email",
    "phone",
    "address",
    "__proto__",
    "constructor",
    "prototype",
];

fn check(valid: bool) -> Result<(), AuditError> {
    valid.then_some(()).ok_or(AuditError::InvalidInput)
}

pub(crate) fn identifier(value: &str, max: usize) -> Result<(), AuditError> {
    check(
        value.len() <= max
            && value
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphanumeric)
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._:/-".contains(&b))
            && !value.contains("://"),
    )
}

pub(crate) fn uuid(value: &str) -> Result<(), AuditError> {
    check(
        value.len() == 36
            && value.bytes().enumerate().all(|(i, b)| {
                if [8, 13, 18, 23].contains(&i) {
                    b == b'-'
                } else {
                    b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
                }
            }),
    )
}

pub(crate) fn time(value: u64) -> Result<(), AuditError> {
    check(value <= MAX_TIME)
}

pub(crate) fn scope(value: &AuditScope) -> Result<(), AuditError> {
    if let Some(tenant) = &value.tenant_id {
        identifier(tenant, 128)?;
    }
    identifier(&value.scope_id, 128)
}

pub(crate) fn actor(value: &AuditActor) -> Result<(), AuditError> {
    match value {
        AuditActor::System { system_id } => identifier(system_id, 128),
        AuditActor::Subject {
            realm_id,
            subject_id,
            ..
        } => check(
            !realm_id.trim().is_empty()
                && realm_id.encode_utf16().count() <= 256
                && !realm_id.chars().any(char::is_whitespace)
                && !subject_id.trim().is_empty()
                && subject_id.encode_utf16().count() <= 512,
        ),
    }
}

fn allowed_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    !FORBIDDEN.iter().any(|word| lower.contains(word))
}

pub(crate) fn config(config: &AuditConfig) -> Result<(), AuditError> {
    check((1..=500).contains(&config.max_page_size))?;
    check(config.summary_policy.len() <= 100)?;
    for (action, fields) in &config.summary_policy {
        identifier(action, 128)?;
        check(fields.len() <= 16)?;
        for (key, rule) in fields {
            identifier(key, 64)?;
            check(allowed_key(key))?;
            match rule {
                SummaryRule::Boolean => {}
                SummaryRule::Integer { min, max } => {
                    check(min <= max && *min >= -MAX_SAFE_INTEGER && *max <= MAX_SAFE_INTEGER)?
                }
                SummaryRule::Enum(values) => {
                    check(!values.is_empty() && values.len() <= 32)?;
                    for value in values {
                        identifier(value, 64)?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn target(value: &AuditTarget) -> Result<(), AuditError> {
    identifier(&value.kind, 64)?;
    identifier(&value.id, 256)
}

pub(crate) fn input(input: &AuditInput, policy: &SummaryPolicy) -> Result<(), AuditError> {
    uuid(&input.id)?;
    time(input.occurred_at)?;
    scope(&input.scope)?;
    identifier(&input.action, 128)?;
    target(&input.target)?;
    identifier(&input.reason_code, 64)?;
    if let Some(correlation) = &input.correlation_id {
        identifier(correlation, 128)?;
    }
    if let Some(relation) = &input.relation {
        uuid(&relation.event_id)?;
        check(relation.event_id != input.id)?;
    }
    for (key, value) in &input.summary {
        let rule = policy
            .get(&input.action)
            .and_then(|fields| fields.get(key))
            .ok_or(AuditError::InvalidInput)?;
        check(allowed_key(key))?;
        check(match (rule, value) {
            (SummaryRule::Boolean, SummaryValue::Boolean(_)) => true,
            (SummaryRule::Integer { min, max }, SummaryValue::Integer(value)) => {
                (min..=max).contains(&value)
            }
            (SummaryRule::Enum(values), SummaryValue::Enum(value)) => values.contains(value),
            _ => false,
        })?;
    }
    Ok(())
}

pub(crate) fn query(query: &AuditQuery, max_page_size: usize) -> Result<usize, AuditError> {
    scope(&query.scope)?;
    let limit = query.limit.unwrap_or(50.min(max_page_size));
    check((1..=max_page_size).contains(&limit))?;
    if let Some(cursor) = &query.cursor {
        time(cursor.recorded_at)?;
        uuid(&cursor.id)?;
    }
    if let Some(action) = &query.action {
        identifier(action, 128)?;
    }
    if let Some(value) = &query.target {
        target(value)?;
    }
    if let Some(correlation) = &query.correlation_id {
        identifier(correlation, 128)?;
    }
    if let Some(from) = query.recorded_from {
        time(from)?;
    }
    if let Some(to) = query.recorded_to {
        time(to)?;
    }
    if let (Some(from), Some(to)) = (query.recorded_from, query.recorded_to) {
        check(from <= to)?;
    }
    Ok(limit)
}

pub(crate) fn matches_query(event: &AuditEvent, query: &AuditQuery) -> bool {
    event.input.scope == query.scope
        && query
            .action
            .as_ref()
            .is_none_or(|v| *v == event.input.action)
        && query
            .target
            .as_ref()
            .is_none_or(|v| *v == event.input.target)
        && query.result.is_none_or(|v| v == event.input.result)
        && query
            .correlation_id
            .as_ref()
            .is_none_or(|v| Some(v) == event.input.correlation_id.as_ref())
        && query.recorded_from.is_none_or(|v| event.recorded_at >= v)
        && query.recorded_to.is_none_or(|v| event.recorded_at <= v)
        && query
            .cursor
            .as_ref()
            .is_none_or(|v| (event.recorded_at, &event.input.id) < (v.recorded_at, &v.id))
}

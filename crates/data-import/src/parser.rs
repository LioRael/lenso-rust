use crate::{Error, Format, Kind, Plan};
use serde_json::{Map, Value};
use tokio::io::{AsyncRead, AsyncReadExt};

// CSV framing tracks quoted newlines and doubled quotes; malformed quote syntax
// is fatal because the next record boundary cannot safely be inferred.
pub async fn record<R: AsyncRead + Unpin>(
    reader: &mut R,
    format: &Format,
    max: usize,
) -> Result<Option<(Vec<u8>, u64)>, Error> {
    let mut out = Vec::new();
    let mut state = 0; // field start, unquoted, quoted, after quote
    loop {
        let mut byte = [0];
        if reader.read(&mut byte).await? == 0 {
            if out.is_empty() {
                return Ok(None);
            }
            if state == 2 {
                return Err(Error::Framing);
            }
            let n = out.len() as u64;
            return Ok(Some((out, n)));
        }
        let b = byte[0];
        if out.len() == max {
            return Err(Error::Limit);
        }
        out.push(b);
        if matches!(format, Format::Jsonl) {
            if b == b'\n' {
                let n = out.len() as u64;
                return Ok(Some((out, n)));
            }
            continue;
        }
        state = match (state, b) {
            (0, b'"') => 2,
            (2, b'"') => 3,
            (2, _) => 2,
            (3, b'"') => 2,
            (4, b'\n') => {
                let n = out.len() as u64;
                return Ok(Some((out, n)));
            }
            (4, _) => return Err(Error::Framing),
            (_, b'\n') => {
                let n = out.len() as u64;
                return Ok(Some((out, n)));
            }
            (_, b',') => 0,
            (3, b'\r') => 4,
            (3, _) | (1, b'"') => return Err(Error::Framing),
            _ => 1,
        };
    }
}
type MappingError = (&'static str, Option<String>);
pub fn map(bytes: &[u8], plan: &Plan) -> Result<Map<String, Value>, MappingError> {
    let text = std::str::from_utf8(bytes).map_err(|_| ("utf8", None))?;
    let source = match plan.format {
        Format::Jsonl => {
            serde_json::from_str::<Map<String, Value>>(text).map_err(|_| ("json-object", None))?
        }
        Format::Csv => {
            let text = text.strip_suffix('\n').unwrap_or(text);
            let text = text.strip_suffix('\r').unwrap_or(text);
            let mut fields = Map::new();
            let mut field = String::new();
            let mut chars = text.chars().peekable();
            let mut quoted = false;
            let mut index = 0;
            while let Some(c) = chars.next() {
                match c {
                    '"' if quoted && chars.peek() == Some(&'"') => {
                        chars.next();
                        field.push('"');
                    }
                    '"' => quoted = !quoted,
                    ',' if !quoted => {
                        fields.insert(index.to_string(), Value::String(std::mem::take(&mut field)));
                        index += 1;
                    }
                    _ => field.push(c),
                }
            }
            fields.insert(index.to_string(), Value::String(field));
            fields
        }
    };
    let mut result = Map::new();
    for field in &plan.fields {
        let fail = |code| (code, Some(field.target.clone()));
        let value = source.get(&field.source).unwrap_or(&Value::Null);
        if value.is_null() || value.as_str() == Some("") {
            if field.required {
                return Err(fail("required"));
            }
            result.insert(field.target.clone(), Value::Null);
            continue;
        }
        let typed = match field.kind {
            Kind::Text => {
                let s = value.as_str().ok_or_else(|| fail("text"))?;
                if s.chars().count() > field.max_chars {
                    return Err(fail("length"));
                }
                Value::String(s.into())
            }
            Kind::Integer => {
                let n = value
                    .as_i64()
                    .or_else(|| value.as_str()?.parse::<i64>().ok())
                    .ok_or_else(|| fail("integer"))?;
                Value::from(n)
            }
            Kind::Boolean => {
                let b = value
                    .as_bool()
                    .or_else(|| match value.as_str()? {
                        "true" => Some(true),
                        "false" => Some(false),
                        _ => None,
                    })
                    .ok_or_else(|| fail("boolean"))?;
                Value::Bool(b)
            }
        };
        result.insert(field.target.clone(), typed);
    }
    Ok(result)
}

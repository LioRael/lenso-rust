use crate::LogRecord;
use std::{
    convert::Infallible,
    error::Error,
    fmt,
    fs::File,
    io::{self, BufRead, BufReader},
    path::PathBuf,
};

/// Replaceable snapshot provider used by the generic aggregation plugin.
pub trait LogSource {
    type Error: Error + Send + Sync + 'static;

    fn read_logs(&self) -> Result<Vec<LogRecord>, Self::Error>;
}

/// An owned snapshot with no global state or file access.
#[derive(Clone, Debug, Default)]
pub struct MemoryLogSource {
    records: Vec<LogRecord>,
}

impl MemoryLogSource {
    pub fn new(records: Vec<LogRecord>) -> Self {
        Self { records }
    }

    pub fn replace(&mut self, records: Vec<LogRecord>) {
        self.records = records;
    }
}

impl LogSource for MemoryLogSource {
    type Error = Infallible;

    fn read_logs(&self) -> Result<Vec<LogRecord>, Self::Error> {
        Ok(self.records.clone())
    }
}

/// Explicit file configuration. Each read opens a fresh snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonLinesConfig {
    pub path: PathBuf,
    pub skip_blank_lines: bool,
}

impl JsonLinesConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            skip_blank_lines: true,
        }
    }
}

/// File and parse errors include the configured path and physical line number.
#[derive(Debug)]
pub enum JsonLinesError {
    Io {
        path: PathBuf,
        line: Option<usize>,
        source: io::Error,
    },
    Parse {
        path: PathBuf,
        line: usize,
        source: serde_json::Error,
    },
}

impl fmt::Display for JsonLinesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, line, source } => {
                write!(f, "reading {}", path.display())?;
                if let Some(line) = line {
                    write!(f, " at line {line}")?;
                }
                write!(f, ": {source}")
            }
            Self::Parse { path, line, source } => {
                write!(f, "parsing {} at line {line}: {source}", path.display())
            }
        }
    }
}

impl Error for JsonLinesError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Parse { source, .. } => Some(source),
        }
    }
}

pub(crate) fn read_json_lines(config: &JsonLinesConfig) -> Result<Vec<LogRecord>, JsonLinesError> {
    let file = File::open(&config.path).map_err(|source| JsonLinesError::Io {
        path: config.path.clone(),
        line: None,
        source,
    })?;
    let mut records = Vec::new();
    for (index, line) in BufReader::new(file).lines().enumerate() {
        let line_number = index + 1;
        let line = line.map_err(|source| JsonLinesError::Io {
            path: config.path.clone(),
            line: Some(line_number),
            source,
        })?;
        if config.skip_blank_lines && line.trim().is_empty() {
            continue;
        }
        records.push(
            serde_json::from_str(&line).map_err(|source| JsonLinesError::Parse {
                path: config.path.clone(),
                line: line_number,
                source,
            })?,
        );
    }
    Ok(records)
}

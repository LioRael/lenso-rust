use std::{error::Error, fmt, io};

/// Stable domain classifications. Numeric input is represented with unsigned Rust types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorCode {
    InvalidKey,
    InvalidInput,
    NotFound,
    Forbidden,
    Unsupported,
    Conflict,
    TooLarge,
    Aborted,
    Provider,
}

impl ErrorCode {
    pub fn message(self) -> &'static str {
        match self {
            Self::InvalidKey => "Object key is invalid.",
            Self::InvalidInput => "Storage input is invalid.",
            Self::NotFound => "Object was not found.",
            Self::Forbidden => "Storage access denied.",
            Self::Unsupported => "Storage operation is not supported.",
            Self::Conflict => "Storage state conflicts with the operation.",
            Self::TooLarge => "Object exceeds the size limit.",
            Self::Aborted => "Storage request cancelled; effects may have completed.",
            Self::Provider => "Storage provider operation failed.",
        }
    }
}

/// Display/Debug expose only a fixed classification. `source()` is internal diagnostic data.
pub struct StorageError {
    code: ErrorCode,
    source: Option<Box<dyn Error + Send + Sync>>,
}

impl StorageError {
    pub fn new(code: ErrorCode) -> Self {
        Self { code, source: None }
    }
    pub fn code(&self) -> ErrorCode {
        self.code
    }
    pub(crate) fn caused(code: ErrorCode, source: impl Error + Send + Sync + 'static) -> Self {
        Self {
            code,
            source: Some(Box::new(source)),
        }
    }
    pub(crate) fn io(error: io::Error) -> Self {
        let code = match error.kind() {
            io::ErrorKind::NotFound => ErrorCode::NotFound,
            io::ErrorKind::PermissionDenied => ErrorCode::Forbidden,
            _ => ErrorCode::Provider,
        };
        #[cfg(unix)]
        let code = if matches!(error.raw_os_error(), Some(libc::ELOOP | libc::ENOTDIR)) {
            ErrorCode::Forbidden
        } else {
            code
        };
        Self::caused(code, error)
    }
    pub(crate) fn upload_source(error: io::Error) -> Self {
        // A DownloadBody can feed another upload. Preserve its domain error,
        // including cancellation, without treating arbitrary source IO failures
        // as failures of the destination's filesystem or access policy.
        let code = error
            .get_ref()
            .and_then(|source| source.downcast_ref::<Self>())
            .map_or(ErrorCode::Provider, Self::code);
        Self::caused(code, error)
    }
    pub(crate) fn into_io(self) -> io::Error {
        // Interrupted is retried by reader helpers; cancellation is terminal.
        io::Error::other(self)
    }
}
impl fmt::Debug for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageError")
            .field("code", &self.code)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code.message())
    }
}
impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(|error| error as _)
    }
}

pub(crate) type Result<T> = std::result::Result<T, StorageError>;

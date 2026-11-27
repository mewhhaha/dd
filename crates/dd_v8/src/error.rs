use std::fmt;

/// A failure while running JavaScript: an uncaught exception, termination,
/// or a module that could not be loaded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    message: String,
    terminated: bool,
}

impl Error {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            terminated: false,
        }
    }

    pub(crate) fn terminated() -> Self {
        Self {
            message: "execution terminated".to_string(),
            terminated: true,
        }
    }

    /// Whether the isolate's execution was terminated (by the heap limit, or
    /// through a runtime handle) rather than failing on its own.
    pub fn is_terminated(&self) -> bool {
        self.terminated
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

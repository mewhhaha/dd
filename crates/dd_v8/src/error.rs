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
    /// through an isolate handle) rather than failing on its own.
    pub fn is_terminated(&self) -> bool {
        self.terminated
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    /// Formats an exception as `"{prefix} {stack or value}"`, so an uncaught
    /// error reads `Uncaught Error: boom` followed by its stack frames.
    pub(crate) fn from_exception<'s>(
        scope: &mut v8::PinScope<'s, '_>,
        exception: v8::Local<'s, v8::Value>,
        prefix: &str,
    ) -> Self {
        if scope.is_execution_terminating() {
            return Self::terminated();
        }
        Self::new(format!("{prefix} {}", describe_exception(scope, exception)))
    }

    pub(crate) fn from_try_catch<'obj, 'i>(
        scope: &mut v8::PinnedRef<'_, v8::TryCatch<'_, 'obj, v8::HandleScope<'i>>>,
        prefix: &str,
    ) -> Self {
        if scope.has_terminated() || scope.is_execution_terminating() {
            return Self::terminated();
        }
        match scope.exception() {
            Some(exception) => Self::from_exception(scope, exception, prefix),
            None => Self::new(format!("{prefix} exception")),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

pub(crate) fn describe_exception<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    exception: v8::Local<'s, v8::Value>,
) -> String {
    v8::tc_scope!(let scope, scope);
    if let Ok(object) = v8::Local::<v8::Object>::try_from(exception) {
        let key = crate::serde_v8::key(scope, "stack");
        if let Some(stack) = object.get(scope, key.into())
            && stack.is_string()
        {
            let stack = stack.to_rust_string_lossy(scope);
            if !stack.is_empty() {
                return stack;
            }
        }
    }
    match exception.to_string(scope) {
        Some(text) => text.to_rust_string_lossy(scope),
        None => exception.type_repr().to_string(),
    }
}

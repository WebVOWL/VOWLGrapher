use std::panic::Location;

use vowlgrapher_util::prelude::{
    ErrorRecord, ErrorSeverity, ErrorType, VOWLGrapherError, get_timestamp,
};

#[derive(Debug)]
/// The different error types the serializer may raise.
pub enum QueryAssemblyErrorKind {
    RegexError(Box<regex::Error>),
    InvalidTripleDecl(String),
}

impl From<QueryAssemblyErrorKind> for VOWLGrapherError {
    #[track_caller]
    fn from(value: QueryAssemblyErrorKind) -> Self {
        <QueryAssemblyError as Into<Self>>::into(value.into())
    }
}

impl From<QueryAssemblyErrorKind> for ErrorRecord {
    #[track_caller]
    fn from(value: QueryAssemblyErrorKind) -> Self {
        <QueryAssemblyError as Into<Self>>::into(value.into())
    }
}

/// Wrapper for errors raised by the serializer.
#[derive(Debug)]
pub struct QueryAssemblyError {
    /// The contained error type.
    inner: QueryAssemblyErrorKind,
    #[cfg(debug_assertions)]
    /// The error's location in the source code.
    location: &'static Location<'static>,
    /// When the error occurred.
    timestamp: String,
}

impl std::fmt::Display for QueryAssemblyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl From<QueryAssemblyErrorKind> for QueryAssemblyError {
    #[track_caller]
    fn from(error: QueryAssemblyErrorKind) -> Self {
        Self {
            inner: error,
            #[cfg(debug_assertions)]
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}

impl From<regex::Error> for QueryAssemblyError {
    #[track_caller]
    fn from(value: regex::Error) -> Self {
        Self {
            inner: QueryAssemblyErrorKind::RegexError(Box::new(value)),
            #[cfg(debug_assertions)]
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}

impl From<QueryAssemblyError> for ErrorRecord {
    fn from(value: QueryAssemblyError) -> Self {
        let (message, severity) = match value.inner {
            QueryAssemblyErrorKind::RegexError(regex_error) => {
                (format!("{regex_error}"), ErrorSeverity::Critical)
            }
            QueryAssemblyErrorKind::InvalidTripleDecl(e) => (e, ErrorSeverity::Error),
        };

        Self::new(
            value.timestamp,
            severity,
            ErrorType::Database,
            message,
            #[cfg(debug_assertions)]
            Some(value.location.to_string()),
        )
    }
}

impl From<QueryAssemblyError> for VOWLGrapherError {
    fn from(value: QueryAssemblyError) -> Self {
        <ErrorRecord as Into<Self>>::into(value.into())
    }
}

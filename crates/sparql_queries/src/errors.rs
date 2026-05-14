use std::{backtrace::Backtrace, error::request_ref, panic::Location};

use thiserror::Error;
use vowlgrapher_util::prelude::{
    ErrorRecord, ErrorSeverity, ErrorType, VOWLGrapherError, get_timestamp,
};

#[derive(Error, Debug)]

/// The different error types the serializer may raise.
pub enum QueryAssemblyError {
    #[error("{0}")]
    RegexError(
        #[from]
        #[backtrace]
        regex::Error,
    ),
    #[error("{}", .0)]
    InvalidTripleDecl(String),
}

impl From<QueryAssemblyError> for ErrorRecord {
    #[track_caller]
    fn from(value: QueryAssemblyError) -> Self {
        let severity = match value {
            QueryAssemblyError::RegexError(_) => ErrorSeverity::Critical,
            QueryAssemblyError::InvalidTripleDecl { .. } => ErrorSeverity::Error,
        };

        // TODO: When refactoring erorr handling, move `message` and `location` to the ErrorRecord struct.
        // (they can be auto-generated there, no need for client code to handle that)
        Self::new(
            get_timestamp(),
            severity,
            ErrorType::Database,
            value.to_string(),
            #[cfg(debug_assertions)]
            #[cfg(debug_assertions)]
            {
                let dyn_error = &value as &dyn std::error::Error;
                request_ref::<Backtrace>(dyn_error).map_or_else(
                    || Some(Location::caller().to_string()),
                    |backtrace_ref| Some(backtrace_ref.to_string()),
                )
            },
        )
    }
}

impl From<QueryAssemblyError> for VOWLGrapherError {
    fn from(value: QueryAssemblyError) -> Self {
        <ErrorRecord as Into<Self>>::into(value.into())
    }
}

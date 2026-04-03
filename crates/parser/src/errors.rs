//! Errors thrown by the parser.

use std::{
    io::{self},
    panic::Location,
};

use horned_owl::error::HornedError;

use rdf_fusion::{
    error::LoaderError,
    execution::sparql::error::QueryEvaluationError,
    model::{IriParseError, StorageError},
};
use tokio::task::JoinError;
use vowlr_util::prelude::{ErrorRecord, ErrorSeverity, ErrorType, VOWLRError, get_timestamp};

/// The type of errors thrown by the database.
#[derive(Debug)]
pub enum VOWLRStoreErrorKind {
    /// The file type is not supported by the server.
    ///
    /// Example: server only supports `.owl` and is given `.png`
    InvalidFileType(String),
    /// An error raised by Horned-OWL during parsing (of OWL files).
    HornedError(Box<HornedError>),
    /// Generic IO error.
    IOError(Box<io::Error>),
    /// An error raised while trying to parse an invalid IRI.
    IriParseError(Box<IriParseError>),
    /// An error raised while loading a file into a Store (database).
    LoaderError(Box<LoaderError>),
    /// A SPARQL evaluation error.
    QueryEvaluationError(Box<QueryEvaluationError>),
    /// A Tokio task failed to execute to completion.
    JoinError(Box<JoinError>),
    /// An error related to (database) storage operations (reads, writes...).
    StorageError(Box<StorageError>),
}

/// Encapsulates the error with metadata.
#[derive(Debug)]
pub struct VOWLRStoreError {
    /// The contained error type.
    inner: VOWLRStoreErrorKind,
    /// The error's location in the source code.
    location: &'static Location<'static>,
    /// When the error occurred.
    timestamp: String,
}

impl From<VOWLRStoreError> for io::Error {
    fn from(val: VOWLRStoreError) -> Self {
        io::Error::other(val.to_string())
    }
}
impl From<String> for VOWLRStoreError {
    #[track_caller]
    fn from(error: String) -> Self {
        Self {
            inner: VOWLRStoreErrorKind::InvalidFileType(error),
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}

impl From<HornedError> for VOWLRStoreError {
    #[track_caller]
    fn from(error: HornedError) -> Self {
        Self {
            inner: VOWLRStoreErrorKind::HornedError(Box::new(error)),
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}

impl From<IriParseError> for VOWLRStoreError {
    #[track_caller]
    fn from(error: IriParseError) -> Self {
        Self {
            inner: VOWLRStoreErrorKind::IriParseError(Box::new(error)),
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}

impl From<LoaderError> for VOWLRStoreError {
    #[track_caller]
    fn from(error: LoaderError) -> Self {
        Self {
            inner: VOWLRStoreErrorKind::LoaderError(Box::new(error)),
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}
impl From<VOWLRStoreErrorKind> for VOWLRStoreError {
    #[track_caller]
    fn from(error: VOWLRStoreErrorKind) -> Self {
        Self {
            inner: error,
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}

impl From<io::Error> for VOWLRStoreError {
    #[track_caller]
    fn from(error: io::Error) -> Self {
        Self {
            inner: VOWLRStoreErrorKind::IOError(Box::new(error)),
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}
impl From<QueryEvaluationError> for VOWLRStoreError {
    #[track_caller]
    fn from(error: QueryEvaluationError) -> Self {
        Self {
            inner: VOWLRStoreErrorKind::QueryEvaluationError(Box::new(error)),
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}
impl From<JoinError> for VOWLRStoreError {
    #[track_caller]
    fn from(error: JoinError) -> Self {
        Self {
            inner: VOWLRStoreErrorKind::JoinError(Box::new(error)),
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}

impl From<StorageError> for VOWLRStoreError {
    #[track_caller]
    fn from(error: StorageError) -> Self {
        Self {
            inner: VOWLRStoreErrorKind::StorageError(Box::new(error)),
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}

impl std::fmt::Display for VOWLRStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl std::error::Error for VOWLRStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.inner {
            VOWLRStoreErrorKind::InvalidFileType(_) => None,
            VOWLRStoreErrorKind::HornedError(e) => Some(e),
            VOWLRStoreErrorKind::IOError(e) => Some(e),
            VOWLRStoreErrorKind::IriParseError(e) => Some(e),
            VOWLRStoreErrorKind::LoaderError(e) => Some(e),
            VOWLRStoreErrorKind::QueryEvaluationError(e) => Some(e),
            VOWLRStoreErrorKind::JoinError(e) => Some(e),
            VOWLRStoreErrorKind::StorageError(e) => Some(e),
        }
    }
}

impl From<VOWLRStoreError> for ErrorRecord {
    fn from(value: VOWLRStoreError) -> Self {
        let (message, error_type) = match value.inner {
            VOWLRStoreErrorKind::InvalidFileType(e) => (e, ErrorType::Parser),
            VOWLRStoreErrorKind::HornedError(horned_error) => {
                (horned_error.to_string(), ErrorType::Parser)
            }
            VOWLRStoreErrorKind::IOError(error) => {
                (error.to_string(), ErrorType::InternalServerError)
            }
            VOWLRStoreErrorKind::IriParseError(iri_parse_error) => {
                (iri_parse_error.to_string(), ErrorType::Parser)
            }
            VOWLRStoreErrorKind::LoaderError(loader_error) => {
                (loader_error.to_string(), ErrorType::Database)
            }
            VOWLRStoreErrorKind::QueryEvaluationError(query_evaluation_error) => {
                (query_evaluation_error.to_string(), ErrorType::Database)
            }
            VOWLRStoreErrorKind::JoinError(join_error) => {
                (join_error.to_string(), ErrorType::InternalServerError)
            }
            VOWLRStoreErrorKind::StorageError(storage_error) => {
                (storage_error.to_string(), ErrorType::Database)
            }
        };
        ErrorRecord::new(
            value.timestamp,
            ErrorSeverity::Critical,
            error_type,
            message,
            #[cfg(debug_assertions)]
            Some(value.location.to_string()),
        )
    }
}

impl From<VOWLRStoreError> for VOWLRError {
    fn from(value: VOWLRStoreError) -> Self {
        <ErrorRecord as Into<VOWLRError>>::into(value.into())
    }
}

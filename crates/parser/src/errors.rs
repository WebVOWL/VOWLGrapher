//! Errors thrown by the parser.

use std::{
    io::{self},
    panic::Location,
};

use horned_owl::error::HornedError;

use rdf_fusion::{
    error::{LoaderError, SerializerError},
    execution::sparql::error::QueryEvaluationError,
    model::{IriParseError, StorageError},
};
use tokio::task::JoinError;
use vowlgrapher_util::prelude::{
    ErrorRecord, ErrorSeverity, ErrorType, VOWLGrapherError, get_timestamp,
};

/// The type of errors thrown by the database.
#[derive(Debug)]
pub enum VOWLGrapherStoreErrorKind {
    /// The file type is not supported by the server.
    ///
    /// Example: server only supports `.owl` and is given `.png`
    InvalidFileType(String),
    /// The file extension does not match the file type.
    ///
    /// Example: the file is parsed as `.owl` but has a `.ttl` extension.
    IncorrectFileExtension(String),
    /// Error on file extension for imported ontology
    ImportResolutionError(String),
    /// Error on fetch of imported ontology
    RemoteFetchError(String),
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
    /// An error raised if the query type is not supported.
    ///
    /// Some types are: SELECT, ASK, CONSTRUCT.
    UnsupportedQueryType(String),
    /// Seralizer error
    SerializerError(Box<SerializerError>),
}

impl From<VOWLGrapherStoreErrorKind> for VOWLGrapherError {
    #[track_caller]
    fn from(value: VOWLGrapherStoreErrorKind) -> Self {
        <VOWLGrapherStoreError as Into<Self>>::into(value.into())
    }
}

/// Encapsulates the error with metadata.
#[derive(Debug)]
pub struct VOWLGrapherStoreError {
    /// The contained error type.
    inner: VOWLGrapherStoreErrorKind,
    /// The error's location in the source code.
    location: &'static Location<'static>,
    /// When the error occurred.
    timestamp: String,
}

impl From<VOWLGrapherStoreError> for io::Error {
    fn from(val: VOWLGrapherStoreError) -> Self {
        Self::other(val.to_string())
    }
}
impl From<String> for VOWLGrapherStoreError {
    #[track_caller]
    fn from(error: String) -> Self {
        Self {
            inner: VOWLGrapherStoreErrorKind::InvalidFileType(error),
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}

impl From<HornedError> for VOWLGrapherStoreError {
    #[track_caller]
    fn from(error: HornedError) -> Self {
        Self {
            inner: VOWLGrapherStoreErrorKind::HornedError(Box::new(error)),
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}

impl From<SerializerError> for VOWLGrapherStoreError {
    #[track_caller]
    fn from(error: SerializerError) -> Self {
        Self {
            inner: VOWLGrapherStoreErrorKind::SerializerError(Box::new(error)),
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}

impl From<IriParseError> for VOWLGrapherStoreError {
    #[track_caller]
    fn from(error: IriParseError) -> Self {
        Self {
            inner: VOWLGrapherStoreErrorKind::IriParseError(Box::new(error)),
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}

impl From<LoaderError> for VOWLGrapherStoreError {
    #[track_caller]
    fn from(error: LoaderError) -> Self {
        Self {
            inner: VOWLGrapherStoreErrorKind::LoaderError(Box::new(error)),
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}
impl From<VOWLGrapherStoreErrorKind> for VOWLGrapherStoreError {
    #[track_caller]
    fn from(error: VOWLGrapherStoreErrorKind) -> Self {
        Self {
            inner: error,
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}

impl From<io::Error> for VOWLGrapherStoreError {
    #[track_caller]
    fn from(error: io::Error) -> Self {
        Self {
            inner: VOWLGrapherStoreErrorKind::IOError(Box::new(error)),
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}
impl From<QueryEvaluationError> for VOWLGrapherStoreError {
    #[track_caller]
    fn from(error: QueryEvaluationError) -> Self {
        Self {
            inner: VOWLGrapherStoreErrorKind::QueryEvaluationError(Box::new(error)),
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}
impl From<JoinError> for VOWLGrapherStoreError {
    #[track_caller]
    fn from(error: JoinError) -> Self {
        Self {
            inner: VOWLGrapherStoreErrorKind::JoinError(Box::new(error)),
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}

impl From<StorageError> for VOWLGrapherStoreError {
    #[track_caller]
    fn from(error: StorageError) -> Self {
        Self {
            inner: VOWLGrapherStoreErrorKind::StorageError(Box::new(error)),
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}

impl std::fmt::Display for VOWLGrapherStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl std::error::Error for VOWLGrapherStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.inner {
            VOWLGrapherStoreErrorKind::HornedError(e) => Some(e),
            VOWLGrapherStoreErrorKind::IOError(e) => Some(e),
            VOWLGrapherStoreErrorKind::IriParseError(e) => Some(e),
            VOWLGrapherStoreErrorKind::InvalidFileType(_)
            | VOWLGrapherStoreErrorKind::IncorrectFileExtension(_)
            | VOWLGrapherStoreErrorKind::ImportResolutionError(_)
            | VOWLGrapherStoreErrorKind::RemoteFetchError(_)
            | VOWLGrapherStoreErrorKind::UnsupportedQueryType(_) => None,
            VOWLGrapherStoreErrorKind::LoaderError(e) => Some(e),
            VOWLGrapherStoreErrorKind::QueryEvaluationError(e) => Some(e),
            VOWLGrapherStoreErrorKind::JoinError(e) => Some(e),
            VOWLGrapherStoreErrorKind::StorageError(e) => Some(e),
            VOWLGrapherStoreErrorKind::SerializerError(e) => Some(e),
        }
    }
}

impl From<VOWLGrapherStoreError> for ErrorRecord {
    fn from(value: VOWLGrapherStoreError) -> Self {
        let (message, severity, error_type) = match value.inner {
            VOWLGrapherStoreErrorKind::InvalidFileType(e) => {
                (e, ErrorSeverity::Critical, ErrorType::Parser)
            }
            VOWLGrapherStoreErrorKind::IncorrectFileExtension(e)
            | VOWLGrapherStoreErrorKind::ImportResolutionError(e)
            | VOWLGrapherStoreErrorKind::RemoteFetchError(e) => {
                (e, ErrorSeverity::Warning, ErrorType::Parser)
            }
            VOWLGrapherStoreErrorKind::HornedError(horned_error) => (
                horned_error.to_string(),
                ErrorSeverity::Critical,
                ErrorType::Parser,
            ),
            VOWLGrapherStoreErrorKind::IOError(error) => (
                error.to_string(),
                ErrorSeverity::Critical,
                ErrorType::InternalServerError,
            ),
            VOWLGrapherStoreErrorKind::IriParseError(iri_parse_error) => (
                iri_parse_error.to_string(),
                ErrorSeverity::Critical,
                ErrorType::Parser,
            ),
            VOWLGrapherStoreErrorKind::LoaderError(loader_error) => (
                loader_error.to_string(),
                ErrorSeverity::Critical,
                ErrorType::Database,
            ),
            VOWLGrapherStoreErrorKind::QueryEvaluationError(query_evaluation_error) => (
                query_evaluation_error.to_string(),
                ErrorSeverity::Critical,
                ErrorType::Database,
            ),
            VOWLGrapherStoreErrorKind::JoinError(join_error) => (
                join_error.to_string(),
                ErrorSeverity::Critical,
                ErrorType::InternalServerError,
            ),
            VOWLGrapherStoreErrorKind::StorageError(storage_error) => (
                storage_error.to_string(),
                ErrorSeverity::Critical,
                ErrorType::Database,
            ),
            VOWLGrapherStoreErrorKind::UnsupportedQueryType(e) => {
                (e, ErrorSeverity::Critical, ErrorType::Database)
            }
            VOWLGrapherStoreErrorKind::SerializerError(serializer_error) => (
                serializer_error.to_string(),
                ErrorSeverity::Critical,
                ErrorType::Parser,
            ),
        };

        Self::new(
            value.timestamp,
            severity,
            error_type,
            message,
            #[cfg(debug_assertions)]
            Some(value.location.to_string()),
        )
    }
}

impl From<VOWLGrapherStoreError> for VOWLGrapherError {
    fn from(value: VOWLGrapherStoreError) -> Self {
        <ErrorRecord as Into<Self>>::into(value.into())
    }
}

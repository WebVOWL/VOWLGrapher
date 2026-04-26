use std::{panic::Location, sync::PoisonError};

use oxrdf::{BlankNodeIdParseError, IriParseError};
use rayon::ThreadPoolBuildError;
use vowlgrapher_util::prelude::{
    ErrorRecord, ErrorSeverity, ErrorType, VOWLGrapherError, get_timestamp,
};

#[derive(Debug)]
/// The different error types the serializer may raise.
pub enum SerializationErrorKind {
    /// An error raised when the object of a triple is required but missing.
    ///
    /// String #1 is the triple, translated from term ids to terms.
    ///
    /// String #2 is the error message.
    MissingObject(String, String),
    /// An error raised when the subject of a triple is required but missing.
    ///
    /// String #1 is the triple, translated from term ids to terms.
    ///
    /// String #2 is the error message.
    MissingSubject(String, String),
    /// An error raised when the predicate of a triple is required but missing.
    ///
    /// String #1 is the triple, translated from term ids to terms.
    ///
    /// String #2 is the error message.
    MissingPredicate(String, String),
    /// An error raised when the range of an edge is required but missing.
    ///
    /// String #1 is the edge, translated from term ids to terms.
    ///
    /// String #2 is the error message.
    MissingRange(String, String),
    /// An error raised when the domain of an edge is required but missing.
    ///
    /// String #1 is the edge, translated from term ids to terms.
    ///
    /// String #2 is the error message.
    MissingDomain(String, String),
    /// An error raised when the label of a term is required but missing.
    MissingLabel(String),
    /// An error raised when the property term of an edge is required but missing.
    MissingProperty(String),
    /// An error raised when the characteristics of a node term is required but missing.
    MisisngCharacteristic(String),
    /// An error raised when the individuals count for a node term is required but missing.
    MissingIndividualsCount(String),
    /// An error raised when the document base is required but missing.
    MissingDocumentBase(String),
    /// An error raised when the serializer encountered an unrecoverable problem.
    ///
    /// String #1 is the triple, translated from term ids to terms.
    ///
    /// String #2 is the error message.
    SerializationFailedTriple(String, String),
    /// An error raised when the serializer encountered an unrecoverable problem.
    SerializationFailed(String),
    /// A warning emitted when the serializer encountered a recoverable problem.
    /// However, the outcome may not be as expected!
    ///
    /// String #1 is the triple, translated from term ids to terms.
    ///
    /// String #2 is the error message.
    SerializationWarningTriple(String, String),
    /// A warning emitted when the serializer encountered a recoverable problem.
    /// However, the outcome may not be as expected!
    SerializationWarning(String),
    /// An error raised during `Iri` or `IriRef` validation.
    IriParseError(String, Box<IriParseError>),
    /// An error raised during `BlankNode` IDs validation.
    BlankNodeParseError(String, Box<BlankNodeIdParseError>),
    /// Errors related to the term index.
    TermIndexError(String),
    /// An error raised if a lock becomes poisoned, e.g., if a thread panics
    /// while holding a write lock.
    LockPoisoned(String),
    /// A warning emitted when a triple is not supported by the serializer.
    ///
    /// String #1 is the triple, translated from term ids to terms.
    ///
    /// String #2 is the error message.
    SerialiationNotSupported(String, String),
    /// An error raised if the threadpool fails to build.
    ThreadPoolFailure(String),
}

impl From<SerializationErrorKind> for VOWLGrapherError {
    #[track_caller]
    fn from(value: SerializationErrorKind) -> Self {
        <SerializationError as Into<Self>>::into(value.into())
    }
}

impl From<SerializationErrorKind> for ErrorRecord {
    #[track_caller]
    fn from(value: SerializationErrorKind) -> Self {
        <SerializationError as Into<Self>>::into(value.into())
    }
}

/// Wrapper for errors raised by the serializer.
#[derive(Debug)]
pub struct SerializationError {
    /// The contained error type.
    inner: SerializationErrorKind,
    /// The error's location in the source code.
    location: &'static Location<'static>,
    /// When the error occurred.
    timestamp: String,
}

impl std::fmt::Display for SerializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl From<SerializationErrorKind> for SerializationError {
    #[track_caller]
    fn from(error: SerializationErrorKind) -> Self {
        Self {
            inner: error,
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}

impl<T> From<PoisonError<T>> for SerializationError {
    #[track_caller]
    fn from(value: PoisonError<T>) -> Self {
        Self {
            inner: SerializationErrorKind::LockPoisoned(value.to_string()),
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}

impl From<ThreadPoolBuildError> for SerializationError {
    #[track_caller]
    fn from(value: ThreadPoolBuildError) -> Self {
        Self {
            inner: SerializationErrorKind::ThreadPoolFailure(format!("{value}")),
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}

impl From<SerializationError> for ErrorRecord {
    fn from(value: SerializationError) -> Self {
        let (message, severity) = match value.inner {
            SerializationErrorKind::MissingDomain(edge, e)
            | SerializationErrorKind::MissingRange(edge, e) => {
                (format!("{e}:\n{edge}"), ErrorSeverity::Warning)
            }
            SerializationErrorKind::MissingLabel(e)
            | SerializationErrorKind::MissingProperty(e)
            | SerializationErrorKind::MisisngCharacteristic(e)
            | SerializationErrorKind::MissingIndividualsCount(e)
            | SerializationErrorKind::MissingDocumentBase(e)
            | SerializationErrorKind::SerializationWarning(e) => (e, ErrorSeverity::Warning),
            SerializationErrorKind::SerializationFailedTriple(triple, e) => {
                (format!("{e}:\n{triple}"), ErrorSeverity::Critical)
            }
            SerializationErrorKind::MissingObject(triple, e)
            | SerializationErrorKind::MissingPredicate(triple, e)
            | SerializationErrorKind::MissingSubject(triple, e)
            | SerializationErrorKind::SerializationWarningTriple(triple, e)
            | SerializationErrorKind::SerialiationNotSupported(triple, e) => {
                (format!("{e}:\n{triple}"), ErrorSeverity::Warning)
            }
            SerializationErrorKind::IriParseError(iri, iri_parse_error) => (
                format!("{iri_parse_error}\nIRI: {iri}"),
                ErrorSeverity::Error,
            ),
            SerializationErrorKind::BlankNodeParseError(id, blank_node_id_parse_error) => (
                format!("{blank_node_id_parse_error}\nID: {id}"),
                ErrorSeverity::Error,
            ),
            SerializationErrorKind::SerializationFailed(e)
            | SerializationErrorKind::TermIndexError(e)
            | SerializationErrorKind::LockPoisoned(e)
            | SerializationErrorKind::ThreadPoolFailure(e) => (e, ErrorSeverity::Critical),
        };

        Self::new(
            value.timestamp,
            severity,
            ErrorType::Serializer,
            message,
            #[cfg(debug_assertions)]
            Some(value.location.to_string()),
        )
    }
}

impl From<SerializationError> for VOWLGrapherError {
    fn from(value: SerializationError) -> Self {
        <ErrorRecord as Into<Self>>::into(value.into())
    }
}

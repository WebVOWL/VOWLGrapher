use std::{panic::Location, rc::Rc};

use crate::serializers::Triple;
use oxrdf::{BlankNodeIdParseError, IriParseError};
use vowlr_util::prelude::{ErrorRecord, ErrorSeverity, ErrorType, VOWLRError, get_timestamp};

#[derive(Debug)]
pub enum SerializationErrorKind {
    /// An error raised when the object of a triple is required but missing.
    MissingObject(Rc<Triple>, String),
    /// An error raised when the subject of a triple is required but missing.
    MissingSubject(Rc<Triple>, String),
    /// An error raised when the serializer encountered an unrecoverable problem.
    SerializationFailed(Rc<Triple>, String),
    /// An error raised during Iri or IriRef validation.
    IriParseError(String, Box<IriParseError>),
    /// An error raised during BlankNode IDs validation.
    BlankNodeParseError(String, Box<BlankNodeIdParseError>),
    /// An error raised if the query type is not supported.
    ///
    /// Some types are: SELECT, ASK, CONSTRUCT.
    UnsupportedQueryType(String),
}

impl From<SerializationErrorKind> for VOWLRError {
    fn from(value: SerializationErrorKind) -> Self {
        <SerializationError as Into<VOWLRError>>::into(value.into())
    }
}

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
        SerializationError {
            inner: error,
            location: Location::caller(),
            timestamp: get_timestamp(),
        }
    }
}

impl From<SerializationError> for ErrorRecord {
    fn from(value: SerializationError) -> Self {
        let (message, severity) = match value.inner {
            SerializationErrorKind::MissingObject(triple, e) => {
                (format!("{e}:\n{triple}"), ErrorSeverity::Warning)
            }
            SerializationErrorKind::MissingSubject(triple, e) => {
                (format!("{e}:\n{triple}"), ErrorSeverity::Warning)
            }
            SerializationErrorKind::SerializationFailed(triple, e) => {
                (format!("{e}:\n{triple}"), ErrorSeverity::Critical)
            }
            SerializationErrorKind::IriParseError(iri, iri_parse_error) => (
                format!("{iri_parse_error} (IRI: {iri})"),
                ErrorSeverity::Error,
            ),
            SerializationErrorKind::BlankNodeParseError(id, blank_node_id_parse_error) => (
                format!("{blank_node_id_parse_error} (ID: {id})"),
                ErrorSeverity::Error,
            ),
            SerializationErrorKind::UnsupportedQueryType(e) => (e, ErrorSeverity::Critical),
        };
        ErrorRecord::new(
            value.timestamp,
            severity,
            ErrorType::Serializer,
            message,
            #[cfg(debug_assertions)]
            Some(value.location.to_string()),
        )
    }
}

impl From<SerializationError> for VOWLRError {
    fn from(value: SerializationError) -> Self {
        <ErrorRecord as Into<VOWLRError>>::into(value.into())
    }
}

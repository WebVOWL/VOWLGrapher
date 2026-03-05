use crate::serializers::Triple;
use oxrdf::{BlankNodeIdParseError, IriParseError};
use vowlr_util::prelude::{ErrorRecord, ErrorSeverity, ErrorType};

#[derive(Debug)]
pub enum SerializationErrorKind {
    /// An error raised when the object of a triple is required but missing.
    MissingObject(Triple, String),
    /// An error raised when the subject of a triple is required but missing.
    MissingSubject(Triple, String),
    /// An error raised when the serializer encountered an unrecoverable problem.
    SerializationFailed(Triple, String),
    /// An error raised during Iri or IriRef validation.
    IriParseError(String, IriParseError),
    /// An error raised during BlankNode IDs validation
    BlankNodeParseError(String, BlankNodeIdParseError),
}

#[derive(Debug)]
pub struct SerializationError {
    inner: SerializationErrorKind,
}
impl std::fmt::Display for SerializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl From<SerializationErrorKind> for SerializationError {
    fn from(error: SerializationErrorKind) -> Self {
        SerializationError { inner: error }
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
        };
        ErrorRecord::new(
            severity,
            ErrorType::Serializer,
            message,
            #[cfg(debug_assertions)]
            "N/A".to_string(),
        )
    }
}

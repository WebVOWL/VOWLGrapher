#[cfg(not(feature = "server"))]
use std::fmt::Write;

use leptos::{
    prelude::{ElementChild, FromServerFnError, IntoView, ServerFnError, ServerFnErrorErr, Signal},
    server_fn::{Decodes, Encodes, codec::RkyvEncoding, error::IntoAppError},
    view,
};

// use leptos_struct_table::*;
use serde::{Deserialize, Serialize};

#[cfg(feature = "server")]
use tabled::{
    Table, Tabled,
    settings::{Settings, Style},
};

use crate::layout::TableHTML;

#[derive(
    Debug,
    Copy,
    Clone,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    Serialize,
    Deserialize,
    strum::Display,
)]
#[strum(serialize_all = "title_case")]
pub enum ErrorSeverity {
    Critical,
    Severe,
    Medium,
    Low,
    Warning,
    Unset,
}

impl ErrorSeverity {
    // TODO: Work in progress. Pls don't remove format!
    pub fn description(&self) -> String {
        match self {
            Self::Critical => format!(
                "an unrecoverable error which makes VOWL-R unusable (do not use the output of VOWL-R!)"
            ),
            Self::Severe => format!(
                "an error which highly disrupts the user experience (the output of VOWL-R is likely incorrect)"
            ),
            // TODO
            Self::Medium => format!("error desc goes here"),
            // TODO
            Self::Low => format!(
                "error desc goes here (part of the output of VOWL-R could be incorrect, but should be \"insignificant\")"
            ),
            Self::Warning => format!(
                "something happened which may reduce the user experience (but can otherwise be ignored)"
            ),
            Self::Unset => format!("unknown severity"),
        }
    }
}

// impl CellValue for ErrorSeverity {
//     type RenderOptions = ();

//     // #[allow(unused)]
//     fn render_value(self, _options: Self::RenderOptions) -> impl IntoView {
//         self.to_string()
//     }
// }

#[derive(
    Debug,
    Copy,
    Clone,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    Serialize,
    Deserialize,
    strum::Display,
)]
pub enum ErrorType {
    /// Errors related to database operations.
    Database,
    /// Errors related to serializing data from backend to frontend (server -> client).
    Serializer,
    /// Errors related to parsing data (e.g. a `.owl` file).
    Parser,
    /// Errors related to the graph renderer (i.e. WasmGrapher)
    Renderer,
    #[strum(serialize = "GUI")]
    /// Errors related to the frontend GUI.
    Gui,
    /// Server errors without a type.
    InternalServerError,
    /// Client errors without a type.
    ClientError,
}

// impl CellValue for ErrorType {
//     type RenderOptions = ();

//     fn render_value(self, _options: Self::RenderOptions) -> impl IntoView {
//         self.to_string()
//     }
// }

#[derive(
    Debug,
    Clone,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    Serialize,
    Deserialize,
    // TableRow,
)]
#[cfg_attr(feature = "server", derive(Tabled))]
// #[table(sortable, classes_provider = "TailwindClassesPreset")]
// TODO: implement a leptos struct table looking like: https://datatables.net/
/// The fundamental building block of the error handling system.
///
/// It stores the data of a single error event.
///
/// # Note
/// Every error type in use should implement [`From<T> for ErrorRecord`].
pub struct ErrorRecord {
    /// The severity of an error.
    ///
    /// Useful for grouping errors by severity and applying custom color schemes in the GUI.
    pub severity: ErrorSeverity,
    /// The type of an error.
    ///
    /// Useful for grouping errors by type and debugging for devs.
    pub error_type: ErrorType,
    /// The actual error message to show.
    pub message: String,

    #[cfg(debug_assertions)]
    /// The location in the source code where the error originated.
    ///
    /// Only enabled with [cfg.debug_assertions]
    pub location: String,
}

impl ErrorRecord {
    pub fn new(
        severity: ErrorSeverity,
        error_type: ErrorType,
        message: String,
        #[cfg(debug_assertions)] location: String,
    ) -> Self {
        Self {
            severity,
            error_type,
            message,
            #[cfg(debug_assertions)]
            location,
        }
    }

    #[cfg(feature = "server")]
    /// Only available on the server.
    pub fn format_records(records: &[ErrorRecord]) -> String {
        let table_config = Settings::default().with(Style::modern_rounded());
        Table::new(records).with(table_config).to_string()
    }
}

impl TableHTML for ErrorRecord {
    fn header(&self) -> impl IntoView {
        view! {
            <thead>
                <tr>
                    <th>{"Severity"}</th>
                    <th>{"Error Type"}</th>
                    <th>{"Message"}</th>
                    {
                        #[cfg(debug_assertions)]
                        view! {<th>{"Code Location"}</th>}
                    }
                </tr>
            </thead>
        }
    }

    fn row(&self) -> impl IntoView {
        view! {
            <tr>
                <td>
                    {self.severity.to_string()}
                </td>
                <td>
                    {self.error_type.to_string()}
                </td>
                <td>
                    {self.message.clone()}
                </td>
                {
                    #[cfg(debug_assertions)]
                    view! {
                        <td>
                            {self.location.to_string()}
                        </td>
                    }
                }
            </tr>
        }
    }
}

impl From<ServerFnError> for ErrorRecord {
    fn from(value: ServerFnError) -> Self {
        let (error_type, message) = match value {
            ServerFnError::WrappedServerError(_) => (
                // TODO: Remove in Leptos v0.9
                ErrorType::InternalServerError,
                "deprecated WrappedServerError".to_string(),
            ),
            ServerFnError::Registration(e) => (ErrorType::InternalServerError, e),
            ServerFnError::Request(e) => (ErrorType::ClientError, e),
            ServerFnError::Response(e) => (ErrorType::InternalServerError, e),
            ServerFnError::ServerError(e) => (ErrorType::InternalServerError, e),
            ServerFnError::MiddlewareError(e) => (ErrorType::InternalServerError, e),
            ServerFnError::Deserialization(e) => (ErrorType::ClientError, e),
            ServerFnError::Serialization(e) => (ErrorType::ClientError, e),
            ServerFnError::Args(e) => (ErrorType::InternalServerError, e),
            ServerFnError::MissingArg(e) => (ErrorType::InternalServerError, e),
        };

        ErrorRecord::new(
            ErrorSeverity::Unset,
            error_type,
            message,
            #[cfg(debug_assertions)]
            "N/A".to_string(),
        )
    }
}

impl From<ServerFnErrorErr> for ErrorRecord {
    fn from(value: ServerFnErrorErr) -> Self {
        let a: ServerFnError = value.into();
        a.into()
    }
}

#[cfg(feature = "server")]
impl std::fmt::Display for ErrorRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let table_config = Settings::default().with(Style::modern_rounded());
        let table = Table::new([self]).with(table_config).to_string();
        write!(f, "{}", table)
    }
}

#[cfg(not(feature = "server"))]
impl std::fmt::Display for ErrorRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[cfg(debug_assertions)]
        {
            write!(
                f,
                "{} | {} | {} | {}",
                self.severity, self.error_type, self.message, self.location
            )
        }

        #[cfg(not(debug_assertions))]
        {
            write!(
                f,
                "{} | {} | {}",
                self.severity, self.error_type, self.message
            )
        }
    }
}

#[derive(
    Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Serialize, Deserialize,
)]
/// The struct sent by the server when things go south.
///
/// # Note
/// Every error type in use should implement [`From<T> for VOWLRServerError`].
pub struct VOWLRServerError {
    pub records: Vec<ErrorRecord>,
}

impl FromServerFnError for VOWLRServerError {
    type Encoder = RkyvEncoding;

    fn from_server_fn_error(value: ServerFnErrorErr) -> Self {
        value.into()
    }

    fn ser(&self) -> leptos::server_fn::Bytes {
        Self::Encoder::encode(self).unwrap_or_else(|e| {
            Self::Encoder::encode(&Self::from_server_fn_error(
                ServerFnErrorErr::Serialization(e.to_string()),
            ))
            .expect("serializing should at least succeed with the serialization error type")
        })
    }

    fn de(data: leptos::server_fn::Bytes) -> Self {
        Self::Encoder::decode(data)
            .unwrap_or_else(|e| ServerFnErrorErr::Deserialization(e.to_string()).into_app_error())
    }
}

impl From<ServerFnError> for VOWLRServerError {
    fn from(value: ServerFnError) -> Self {
        let record: ErrorRecord = value.into();
        record.into()
    }
}

impl From<ServerFnErrorErr> for VOWLRServerError {
    fn from(value: ServerFnErrorErr) -> Self {
        let record: ErrorRecord = value.into();
        record.into()
    }
}

impl From<ErrorRecord> for VOWLRServerError {
    fn from(value: ErrorRecord) -> Self {
        VOWLRServerError {
            records: vec![value],
        }
    }
}

impl From<Vec<ErrorRecord>> for VOWLRServerError {
    fn from(value: Vec<ErrorRecord>) -> Self {
        VOWLRServerError { records: value }
    }
}

#[cfg(feature = "server")]
impl std::fmt::Display for VOWLRServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", ErrorRecord::format_records(&self.records))
    }
}

#[cfg(not(feature = "server"))]
impl std::fmt::Display for VOWLRServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[cfg(debug_assertions)]
        {
            writeln!(f, "Severity | Error Type | Message | Location")?;
        }

        #[cfg(not(debug_assertions))]
        {
            writeln!(f, "Severity | Error Type | Message")?;
        }

        let mut buffer = String::new();
        for record in self.records.iter() {
            writeln!(buffer, "{}", record)?;
        }
        write!(f, "{buffer}")
    }
}

#[cfg(not(feature = "server"))]
use std::fmt::Write;

use leptos::{
    prelude::*,
    server_fn::{Decodes, Encodes, codec::RkyvEncoding, error::IntoAppError},
    view,
};
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
    Error,
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
            Self::Error => format!(
                "an error which highly disrupts the user experience (the output of VOWL-R is likely incorrect)"
            ),

            Self::Warning => format!(
                "something happened which may reduce the user experience (but can otherwise be ignored)"
            ),
            Self::Unset => format!("unknown severity"),
        }
    }
}

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

#[derive(
    Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Serialize, Deserialize,
)]
#[cfg_attr(feature = "server", derive(Tabled))]
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
        #[cfg(debug_assertions)] location: Option<String>,
    ) -> Self {
        Self {
            severity,
            error_type,
            message,
            #[cfg(debug_assertions)]
            location: location.unwrap_or("Unknown".to_string()),
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
    // TODO: implement a leptos struct table looking like: https://datatables.net/
    // Tailwind Table: https://www.material-tailwind.com/docs/html/table#table-with-hover
    fn header(&self) -> impl IntoView {
        let th_css =
            "p-1 font-sans text-sm antialiased font-normal leading-normal text-blue-gray-900";
        view! {
            <tr class="border-b">
                <th class=th_css>{"Severity"}</th>
                <th class=th_css>{"Error Type"}</th>
                <th class=th_css>{"Message"}</th>
                {
                    #[cfg(debug_assertions)]
                    view! { <th class=th_css>{"Code Location"}</th> }
                }
            </tr>
        }
    }

    fn row(&self) -> impl IntoView {
        let tr_color = match self.severity {
            ErrorSeverity::Critical => "border-red-300 bg-red-100 text-red-700",
            ErrorSeverity::Error => "border-red-200 bg-red-50 text-red-700",
            ErrorSeverity::Warning => "border-yellow-200 bg-yellow-50 text-yellow-700",
            ErrorSeverity::Unset => "border-slate-200 bg-slate-50 text-slate-700",
        };

        #[cfg(debug_assertions)]
        let td_css = "p-2 whitespace-pre-wrap font-sans text-sm antialiased font-normal leading-normal text-blue-gray-900";

        #[cfg(not(debug_assertions))]
        let td_css = "p-2 mr-2 whitespace-pre-wrap font-sans text-sm antialiased font-normal leading-normal text-blue-gray-900";

        view! {
            <tr class=format!("border-b hover:bg-slate-200 {tr_color}")>
                <td class=td_css>{self.severity.to_string()}</td>
                <td class=td_css>{self.error_type.to_string()}</td>
                <td class=td_css>{self.message.clone()}</td>
                {
                    #[cfg(debug_assertions)]
                    view! { <td class=td_css>{self.location.to_string()}</td> }
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
            None,
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
/// The struct used by VOWL-R when things go south.
///
/// # Note
/// Every error type in use should implement [`From<T> for VOWLRServerError`].
pub struct VOWLRError {
    pub records: Vec<ErrorRecord>,
}

impl FromServerFnError for VOWLRError {
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

impl From<ServerFnError> for VOWLRError {
    fn from(value: ServerFnError) -> Self {
        let record: ErrorRecord = value.into();
        record.into()
    }
}

impl From<ServerFnErrorErr> for VOWLRError {
    fn from(value: ServerFnErrorErr) -> Self {
        let record: ErrorRecord = value.into();
        record.into()
    }
}

impl From<ErrorRecord> for VOWLRError {
    fn from(value: ErrorRecord) -> Self {
        VOWLRError {
            records: vec![value],
        }
    }
}

impl From<Vec<ErrorRecord>> for VOWLRError {
    fn from(value: Vec<ErrorRecord>) -> Self {
        VOWLRError { records: value }
    }
}

#[cfg(feature = "server")]
impl std::fmt::Display for VOWLRError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", ErrorRecord::format_records(&self.records))
    }
}

#[cfg(not(feature = "server"))]
impl std::fmt::Display for VOWLRError {
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

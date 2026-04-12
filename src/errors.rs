use leptos::prelude::*;
use std::panic::Location;
use vowlr_util::prelude::{ErrorRecord, ErrorSeverity, ErrorType, VOWLRError, get_timestamp};

#[derive(Debug)]
pub enum ClientErrorKind {
    /// An error raised when an unexpected value was received from JS-land.
    JavaScriptError(String),
    /// Errors related to the graph renderer (i.e. ``WasmGrapher``)
    RenderError(String),
    /// Errors related to file upload
    FileUploadError(String),
    /// An error raised when the event handler fails to send or receive events.
    EventHandlingError(String),
    /// An error raised when the server's environment wasn't received.
    ///
    /// #1 argument is the client error message.
    ///
    /// #2 argument is the received error message.
    EnvironmentFetchError(String, ErrorRecord),
}

impl From<ClientErrorKind> for ErrorRecord {
    #[track_caller]
    fn from(value: ClientErrorKind) -> Self {
        let (message, error_type, severity) = match value {
            ClientErrorKind::JavaScriptError(e) => (e, ErrorType::Gui, ErrorSeverity::Error),
            ClientErrorKind::RenderError(e) => (e, ErrorType::Renderer, ErrorSeverity::Critical),
            ClientErrorKind::FileUploadError(e) | ClientErrorKind::EventHandlingError(e) => {
                (e, ErrorType::ClientError, ErrorSeverity::Error)
            }
        };
        Self::new(
            get_timestamp(),
            severity,
            error_type,
            message,
            #[cfg(debug_assertions)]
            Some(Location::caller().to_string()),
        )
    }
}

impl From<ClientErrorKind> for VOWLRError {
    fn from(value: ClientErrorKind) -> Self {
        let a: ErrorRecord = value.into();
        a.into()
    }
}

// #[derive(Debug)]
// pub struct VOWLRClientError {
//     /// The contained error type.
//     inner: ClientErrorKind,
//     /// The error's location in the source code.
//     location: &'static Location<'static>,
// }
// impl std::fmt::Display for VOWLRClientError {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "{:?}", self.inner)
//     }
// }

// impl From<ClientErrorKind> for VOWLRClientError {
//     #[track_caller]
//     fn from(error: ClientErrorKind) -> Self {
//         VOWLRClientError {
//             inner: error,
//             location: Location::caller(),
//         }
//     }
// }

// impl From<VOWLRClientError> for ErrorRecord {
//     fn from(value: VOWLRClientError) -> Self {
//         let (message, error_type, severity) = match value.inner {
//             ClientErrorKind::JavaScriptError(e) => (e, ErrorType::Gui, ErrorSeverity::Error),
//             ClientErrorKind::RenderError(e) => (e, ErrorType::Renderer, ErrorSeverity::Critical),
//         };
//         ErrorRecord::new(
//             severity,
//             error_type,
//             message,
//             #[cfg(debug_assertions)]
//             Some(value.location.to_string()),
//         )
//     }
// }

// impl From<VOWLRClientError> for VOWLRError {
//     fn from(value: VOWLRClientError) -> Self {
//         let a: ErrorRecord = value.into();
//         a.into()
//     }
// }

#[derive(Debug, Copy, Clone)]
pub struct ErrorLogContext {
    pub records: RwSignal<Vec<ErrorRecord>>,
}

impl ErrorLogContext {
    pub fn new(records: Vec<ErrorRecord>) -> Self {
        Self {
            records: RwSignal::new(records),
        }
    }

    /// Appends an element to the back of a collection.
    ///
    /// # Panics
    /// Panics if you update the value of the signal of `self` before this function returns.
    pub fn push(&self, record: ErrorRecord) {
        self.records.update(|records| records.push(record));
    }

    /// Extends a collection with the contents of an iterator.
    ///
    /// # Panics
    /// Panics if you update the value of the signal of `self` before this function returns.
    pub fn extend(&self, records: Vec<ErrorRecord>) {
        self.records.update(|records_| records_.extend(records));
    }

    /// Clears the collection, removing all values.
    ///
    /// Note that this method has no effect on the allocated capacity of the vector.
    ///
    /// # Panics
    /// Panics if you update the value of the signal of `self` before this function returns.
    pub fn clear(&self) {
        // self.records.update(|records| records.clear());
        self.records.update(std::vec::Vec::clear);
    }

    /// Returns the number of elements in the collection, also referred to as its 'length'
    ///
    /// # Panics
    /// Panics if you try to access the signal of `self` when it has been disposed.
    pub fn len(&self) -> usize {
        self.records.read().len()
    }

    /// Returns `true` if the vector contains no elements.
    ///
    /// # Panics
    /// Panics if you try to access the signal of `self` when it has been disposed.
    pub fn is_empty(&self) -> bool {
        self.records.read().is_empty()
    }
}

impl Default for ErrorLogContext {
    fn default() -> Self {
        Self {
            records: RwSignal::new(Vec::new()),
        }
    }
}

impl From<VOWLRError> for ErrorLogContext {
    fn from(value: VOWLRError) -> Self {
        Self::new(value.records)
    }
}

mod datatypes;
pub mod error_handler;

pub mod prelude {
    pub use crate::datatypes::DataType;
    pub use crate::error_handler::{ErrorRecord, ErrorSeverity, ErrorType, VOWLRServerError};
}

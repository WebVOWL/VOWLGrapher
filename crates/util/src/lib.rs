//! Utility code used by the entire VOWL-R workspace

mod datatypes;
mod error_handler;
mod layout;
mod time;

pub mod prelude {
    //! Export all types of the crate.
    pub use crate::datatypes::DataType;
    pub use crate::error_handler::{ErrorRecord, ErrorSeverity, ErrorType, VOWLRError};
    pub use crate::layout::TableHTML;
    pub use crate::time::get_timestamp;
}

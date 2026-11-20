// Needed for async functions in the store
#![recursion_limit = "256"]

//! The graph database.

mod store;

pub mod prelude {
    //! Export all types of the crate.
    pub use crate::store::{DATABASE_RUNTIME, VOWLGrapherStore};
}

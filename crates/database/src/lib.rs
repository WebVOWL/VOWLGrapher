//! The graph database.

mod errors;
mod serializers;
mod store;
mod vocab;

pub mod prelude {
    //! Export all types of the crate.
    pub use crate::store::VOWLRStore;
}

//! The serializer.

mod datastructures;
mod errors;
mod serializer_util;
mod serializers;
mod vocab;

pub mod prelude {
    //! Export all types of the crate.
    pub use crate::errors::{SerializationError, SerializationErrorKind};
    pub use crate::serializers::frontend::GraphDisplayDataSolutionSerializer;
}

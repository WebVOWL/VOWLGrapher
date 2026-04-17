use std::sync::{Arc, RwLock};

use oxrdf::Term;

use crate::datastructures::{edge_data::Edge, restriction_data::RestrictionState, triple::Triple};

pub mod edge_data;
pub mod graph_metadata_buffer;
pub mod index;
pub mod restriction_data;
pub mod serialization_data_buffer;
pub mod triple;

pub type ArcTerm = Arc<Term>;
pub type ArcTriple = Arc<Triple>;
pub type ArcEdge = Arc<Edge>;
pub type ArcLockRestrictionState = Arc<RwLock<RestrictionState>>;

pub enum SerializationStatus {
    Serialized,
    Deferred,
    NotSupported,
}

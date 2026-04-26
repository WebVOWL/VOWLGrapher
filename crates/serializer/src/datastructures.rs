use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

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

/// The corresponding ID of a [`Term`].
///
/// Created by the [`TermIndex`](crate::datastructures::index::TermIndex).
type TermID = usize;
/// A language tag, e.g., `en`.
type LanguageTag = String;
/// A term's corresponding id specific for metadata, e.g., `<http://www.w3.org/2000/01/rdf-schema#comment>`
type MetadataTermID = usize;
/// The metadata content term's corresponding id, e.g., the data of `rdfs:comment`.
type MetadataContent = HashSet<TermID>;
/// Stores the metadata associated with a language tag.
///
/// The same metadata can be written in multiple languages.
type TaggedMetadata = HashMap<LanguageTag, MetadataContent>;
/// Stores the metadata associated with a metadata type.
///
/// There exist many types of metadata, e.g., `rdfs:comment`, `dc:creator`.
type MetadataType = HashMap<MetadataTermID, TaggedMetadata>;
/// Stores the metadata of a term's corresponding id.
///
/// A term does not necessarily have metadata associated with it.
type ElementTypeMetadata = HashMap<TermID, MetadataType>;
// /// Stores language-tagged labels.
// ///
// /// The same label can be written in multiple languages.
// type TaggedLabel = HashMap<LanguageTag, String>;

pub enum SerializationStatus {
    Serialized,
    Deferred,
    // NotSupported
}

#[derive(Clone)]
pub struct DocumentBase {
    pub base_term: ArcTerm,
    pub base: String,
}

impl DocumentBase {
    pub const fn new(base_term: ArcTerm, base: String) -> Self {
        Self { base_term, base }
    }
}

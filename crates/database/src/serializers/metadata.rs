use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

#[derive(Default)]
pub struct GraphMetadata {
    /// Stores comments of terms, keyed by the term's corresponding id.
    ///
    /// rdfs:comment
    pub comments: Arc<RwLock<HashMap<usize, usize>>>,
    /// Maps from a term's cooresponding id to the term's corresponding id which defines it.
    ///
    /// rdfs:isDefinedBy
    pub is_defined_by: Arc<RwLock<HashMap<usize, usize>>>,
    /// Maps from a term's cooresponding id to the term's corresponding id which
    /// provides additional info about it.
    ///
    /// rdfs:seeAlso
    pub see_also: Arc<RwLock<HashMap<usize, HashSet<usize>>>>,
    /// Stores the version of terms, keyed by the term's cooresponding id.
    ///
    /// owl:versionInfo
    pub version_info: Arc<RwLock<HashMap<usize, usize>>>,
    /// The term's corresponding id which describes the version of an ontology.
    ///
    /// owl:versionIRI
    pub version_iri: Arc<RwLock<Option<usize>>>,
    /// The term's corresponding id which describes the prior version of an ontology.
    ///
    /// owl:priorVersion
    ///
    /// ## Note
    /// The usage of this annotation property on entities other than ontologies is [discouraged](https://www.w3.org/TR/owl-syntax/#Ontology_Annotations).
    /// As such, we notify the client if this is violated.
    pub prior_version: Arc<RwLock<Option<usize>>>,
    /// The term's cooresponding id which describes the prior version of the ontology that is incompatible with the current version, i.e., [`Self::version_iri`]-
    ///
    /// owl:incompatibleWith
    ///
    /// ## Note
    /// The usage of this annotation property on entities other than ontologies is [discouraged](https://www.w3.org/TR/owl-syntax/#Ontology_Annotations).
    /// As such, we notify the client if this is violated.
    pub incompatible_with: Arc<RwLock<Option<usize>>>,
    /// The term's cooresponding id which describes the prior version of the ontology that is compatible with the current version, i.e., [`Self::version_iri`]-
    ///
    ///
    /// owl:backwardCompatibleWith
    ///
    /// ## Note
    /// The usage of this annotation property on entities other than ontologies is [discouraged](https://www.w3.org/TR/owl-syntax/#Ontology_Annotations).
    /// As such, we notify the client if this is violated.
    pub backward_compatible_with: Arc<RwLock<Option<usize>>>,
    pub ontology_property: Arc<RwLock<String>>,
}

impl GraphMetadata {
    pub fn new() -> Self {
        Self::default()
    }
}

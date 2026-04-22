use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::{Arc, RwLock},
};

use crate::datastructures::index::TermIndex;

#[derive(Default)]
pub struct GraphMetadataBuffer {
    /// Maps terms to integer ids and vice-versa.
    ///
    /// Reduces memory usage and allocations.
    pub term_index: Arc<TermIndex>,
    /// The authors of the ontology.
    pub author_buffer: Arc<RwLock<HashMap<usize, usize>>>,
    /// Stores comments of terms, keyed by the term's corresponding id.
    ///
    /// rdfs:comment
    pub comment_buffer: Arc<RwLock<HashMap<usize, usize>>>,
    /// Maps from a term's cooresponding id to the term's corresponding id which defines it.
    ///
    /// rdfs:isDefinedBy
    pub defined_by_buffer: Arc<RwLock<HashMap<usize, usize>>>,
    /// Maps from a term's cooresponding id to the term's corresponding id which
    /// provides additional info about it.
    ///
    /// rdfs:seeAlso
    pub see_also_buffer: Arc<RwLock<HashMap<usize, HashSet<usize>>>>,
    /// Stores the version of terms, keyed by the term's cooresponding id.
    ///
    /// owl:versionInfo
    pub version_info_buffer: Arc<RwLock<HashMap<usize, usize>>>,
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
    /// As such, we should notify the client if this is violated.
    pub prior_version: Arc<RwLock<Option<usize>>>,
    /// The term's cooresponding id which describes the prior version of the ontology that is incompatible with the current version, i.e., [`Self::version_iri`]-
    ///
    /// owl:incompatibleWith
    ///
    /// ## Note
    /// The usage of this annotation property on entities other than ontologies is [discouraged](https://www.w3.org/TR/owl-syntax/#Ontology_Annotations).
    /// As such, we should notify the client if this is violated.
    pub incompatible_with: Arc<RwLock<Option<usize>>>,
    /// The term's cooresponding id which describes the prior version of the ontology that is compatible with the current version, i.e., [`Self::version_iri`]-
    ///
    ///
    /// owl:backwardCompatibleWith
    ///
    /// ## Note
    /// The usage of this annotation property on entities other than ontologies is [discouraged](https://www.w3.org/TR/owl-syntax/#Ontology_Annotations).
    /// As such, we should notify the client if this is violated.
    pub backward_compatible_with: Arc<RwLock<Option<usize>>>,
    /// Maps from `owl:annotatedSource` to a hashmap, mapping `owl:annotatedProperty` to `owl:annotatedTarget`.
    #[expect(unused, reason = "pending implementation")]
    pub annotations: Arc<RwLock<HashMap<usize, HashMap<usize, usize>>>>,
}

impl GraphMetadataBuffer {
    pub fn new(index: Arc<TermIndex>) -> Self {
        Self {
            term_index: index,
            ..Default::default()
        }
    }
}

impl Display for GraphMetadataBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "\tGraphMetadataBuffer {{")?;
        writeln!(f, "\t\tOntology")?;
        writeln!(
            f,
            "\t\t\tversion_iri: {}",
            self.version_iri
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .map_or_else(String::new, |term_id| {
                    self.term_index
                        .get(term_id)
                        .map_or_else(|e| e.to_string(), |term| term.to_string())
                })
        )?;
        writeln!(
            f,
            "\t\t\tprior_version: {}",
            self.prior_version
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .map_or_else(String::new, |term_id| {
                    self.term_index
                        .get(term_id)
                        .map_or_else(|e| e.to_string(), |term| term.to_string())
                })
        )?;
        writeln!(
            f,
            "\t\t\tincompatible_with: {}",
            self.incompatible_with
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .map_or_else(String::new, |term_id| {
                    self.term_index
                        .get(term_id)
                        .map_or_else(|e| e.to_string(), |term| term.to_string())
                })
        )?;
        writeln!(
            f,
            "\t\t\tbackward_compatible_with: {}",
            self.backward_compatible_with
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .map_or_else(String::new, |term_id| {
                    self.term_index
                        .get(term_id)
                        .map_or_else(|e| e.to_string(), |term| term.to_string())
                })
        )?;
        writeln!(f, "\t\tComments")?;
        for (term_id, comment_term_id) in self
            .comment_buffer
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            writeln!(
                f,
                "\t\t\t{} - {}",
                self.term_index
                    .get(*term_id)
                    .map_or_else(|e| e.to_string(), |term| term.to_string()),
                self.term_index
                    .get(*comment_term_id)
                    .map_or_else(|e| e.to_string(), |term| term.to_string())
            )?;
        }
        writeln!(f, "\t}}")
    }
}

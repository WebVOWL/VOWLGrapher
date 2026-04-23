use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::{Arc, RwLock},
};

use oxrdf::Term;

use crate::{
    datastructures::{
        ArcTerm, ElementTypeMetadata, LanguageTag, MetadataContent, MetadataTermID,
        index::TermIndex,
    },
    errors::SerializationError,
    serializer_util::{
        labels::{extract_label, insert_label},
        trim_tag_circumfix,
    },
};

#[derive(Default)]
pub struct GraphMetadataBuffer {
    /// Maps terms to integer ids and vice-versa.
    ///
    /// Reduces memory usage and allocations.
    pub term_index: Arc<TermIndex>,
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
    /// owl:backwardCompatibleWith
    ///
    /// ## Note
    /// The usage of this annotation property on entities other than ontologies is [discouraged](https://www.w3.org/TR/owl-syntax/#Ontology_Annotations).
    /// As such, we should notify the client if this is violated.
    pub backward_compatible_with: Arc<RwLock<Option<usize>>>,
    /// Stores the general metadata of a term's corresponding id.
    ///
    /// A term does not necessarily have metadata associated with it.
    pub element_metadata: Arc<RwLock<ElementTypeMetadata>>,
    /// Maps from annotation term (which is a blank node) to an [`AxiomAnnotation`]
    pub axiom_annotations: Arc<RwLock<HashMap<usize, AxiomAnnotation>>>,
}

impl GraphMetadataBuffer {
    pub fn new(index: Arc<TermIndex>) -> Self {
        Self {
            term_index: index,
            ..Default::default()
        }
    }

    pub fn get_general_metadata(
        &self,
        subject_term_id: usize,
        metadata_term: &ArcTerm,
        language_tag: LanguageTag,
    ) -> Result<Option<MetadataContent>, SerializationError> {
        if let Ok(metadata_term_id) = self.term_index.get_id(metadata_term) {
            if let Some(metadata_type) = self.element_metadata.read()?.get(&subject_term_id) {
                return Ok(metadata_type
                    .get(&metadata_term_id)
                    .map(|tagged_metadata| tagged_metadata.get(&language_tag))
                    .flatten()
                    .cloned());
            }
        }
        Ok(None)
    }

    fn fmt_element_metadata(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        element_metadata: &Arc<RwLock<ElementTypeMetadata>>,
    ) -> std::fmt::Result {
        let mut map: HashMap<usize, HashMap<usize, Vec<String>>> = HashMap::new();

        // Prepare data for display
        for (term_id, metadata_types) in element_metadata
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            for (metadata_term_id, tagged_metadata) in metadata_types {
                map.entry(*metadata_term_id)
                    .or_default()
                    .entry(*term_id)
                    .or_default()
                    .extend(tagged_metadata.iter().map(|(lang_tag, content)| {
                        content
                            .iter()
                            .map(|content_term_id| {
                                format!(
                                    "\t\t\t{lang_tag} - {}",
                                    self.term_index
                                        .get(*content_term_id)
                                        .map_or_else(|e| e.to_string(), |term| term.to_string())
                                )
                            })
                            .collect()
                    }));
            }
        }

        // Display data
        for (metadata_term_id, element_metadata) in map {
            let title = {
                match self.term_index.get(metadata_term_id) {
                    Ok(metadata_term) => extract_label(None, &metadata_term)
                        .unwrap_or_else(|| trim_tag_circumfix(&metadata_term.to_string())),
                    Err(e) => e.to_string(),
                }
            };
            writeln!(f, "\t\t{title}:")?;
            for (term_id, content) in element_metadata {
                writeln!(
                    f,
                    "\t\t\t{}",
                    self.term_index
                        .get(term_id)
                        .map_or_else(|e| e.to_string(), |term| term.to_string()),
                )?;
                for item in content {
                    writeln!(f, "\t\t\t\t{item}")?;
                }
            }
        }
        write!(f, "")
    }

    fn fmt_hashset_hashmap(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        map: &Arc<RwLock<HashMap<usize, HashSet<usize>>>>,
    ) -> std::fmt::Result {
        for (term_id, object_term_ids) in map
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            writeln!(
                f,
                "\t\t\t{} - {}",
                self.term_index.display_term(*term_id),
                self.term_index.display_term(*comment_term_id)
            )?;
        }
        writeln!(f, "\t\tAnnotations")?;
        for (term_id, annotation) in self
            .axiom_annotations
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            writeln!(
                f,
                "\t\t\t{} - ({}, {}, {}) - {:?}",
                self.term_index
                    .get(*term_id)
                    .map_or_else(|e| e.to_string(), |term| term.to_string()),
                annotation.source.map_or_else(
                    || "None".to_owned(),
                    |term_id| self.term_index.display_term(term_id)
                ),
                annotation.property.map_or_else(
                    || "None".to_owned(),
                    |term_id| self.term_index.display_term(term_id)
                ),
                annotation.target.map_or_else(
                    || "None".to_owned(),
                    |term_id| self.term_index.display_term(term_id)
                ),
                annotation
                    .annotations
                    .iter()
                    .map(|(k, v)| (
                        self.term_index.display_term(*k),
                        self.term_index.display_term(*v)
                    ))
                    .collect::<HashMap<_, _>>()
            )?;
            for object_term_id in object_term_ids {
                writeln!(
                    f,
                    "\t\t\t\t{}",
                    self.term_index
                        .get(*object_term_id)
                        .map_or_else(|e| e.to_string(), |term| term.to_string())
                )?;
            }
        }
        write!(f, "")
    }

    fn fmt_option_usize(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        title: &str,
        value: &Arc<RwLock<Option<usize>>>,
    ) -> std::fmt::Result {
        writeln!(
            f,
            "\t\t\t{title}: {}",
            value
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .map_or_else(String::new, |term_id| {
                    self.term_index
                        .get(term_id)
                        .map_or_else(|e| e.to_string(), |term| term.to_string())
                })
        )
    }
}

impl Display for GraphMetadataBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "\tGraphMetadataBuffer {{")?;
        writeln!(f, "\t\tOntology")?;
        self.fmt_option_usize(f, "version_iri", &self.version_iri)?;
        self.fmt_option_usize(f, "prior_version", &self.prior_version)?;
        self.fmt_option_usize(f, "incompatible_with", &self.incompatible_with)?;
        self.fmt_option_usize(f, "backward_compatible_wit", &self.backward_compatible_with)?;
        self.fmt_element_metadata(f, &self.element_metadata)?;
        writeln!(f, "\t\tAnnotations:")?;
        writeln!(f, "\t\t\t{:?}", self.annotations)?;
        writeln!(f, "\t}}")
    }
}

/// An annotation on an axiom
///
/// RDF represents these as the triples
/// ```plaintext
/// s p o   # The axiom itself
/// _:x rdf:type owl:Axiom .
/// _:x owl:annotatedSource s .
/// _:x owl:annotatedProperty p .
/// _:x owl:annotatedTarget o .
/// _:x AP av   # AP is the annotation property, e.g. `rdfs:comment`, and av is the annotation value
/// ```
#[derive(Default, Debug)]
pub struct AxiomAnnotation {
    pub source: Option<usize>,
    pub property: Option<usize>,
    pub target: Option<usize>,
    /// The annotation properties/values in the annotation.
    /// Maps from id of annotation property to id of annotation value.
    pub annotations: HashMap<usize, usize>,
}

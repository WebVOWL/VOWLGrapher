use crate::{
    datastructures::{
        ArcTriple, ElementTypeMetadata, LanguageTag, MetadataType, TermID, index::TermIndex,
    },
    errors::{SerializationError, SerializationErrorKind},
    serializer_util::{
        fmt_langtag, labels::extract_label, translate_metadata_content, trim_tag_circumfix,
    },
};
use std::fmt::Write;
use std::{
    collections::HashMap,
    fmt::Display,
    sync::{Arc, RwLock},
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
    pub version_iri: Arc<RwLock<Option<TermID>>>,
    /// The term's corresponding id which describes the prior version of an ontology.
    ///
    /// owl:priorVersion
    ///
    /// ## Note
    /// The usage of this annotation property on entities other than ontologies is [discouraged](https://www.w3.org/TR/owl-syntax/#Ontology_Annotations).
    /// As such, we should notify the client if this is violated.
    pub prior_version: Arc<RwLock<Option<TermID>>>,
    /// The term's cooresponding id which describes the prior version of the ontology that is incompatible with the current version, i.e., [`Self::version_iri`]-
    ///
    /// owl:incompatibleWith
    ///
    /// ## Note
    /// The usage of this annotation property on entities other than ontologies is [discouraged](https://www.w3.org/TR/owl-syntax/#Ontology_Annotations).
    /// As such, we should notify the client if this is violated.
    pub incompatible_with: Arc<RwLock<Option<TermID>>>,
    /// The term's cooresponding id which describes the prior version of the ontology that is compatible with the current version, i.e., [`Self::version_iri`]-
    ///
    /// owl:backwardCompatibleWith
    ///
    /// ## Note
    /// The usage of this annotation property on entities other than ontologies is [discouraged](https://www.w3.org/TR/owl-syntax/#Ontology_Annotations).
    /// As such, we should notify the client if this is violated.
    pub backward_compatible_with: Arc<RwLock<Option<TermID>>>,
    /// Stores the general metadata of a term's corresponding id.
    ///
    /// A term does not necessarily have metadata associated with it.
    pub element_metadata: Arc<RwLock<ElementTypeMetadata>>,
    /// Maps from annotation term (which is a blank node) to an [`AxiomAnnotation`]
    pub axiom_annotations: Arc<RwLock<HashMap<TermID, AxiomAnnotation>>>,
}

impl GraphMetadataBuffer {
    pub fn new(index: Arc<TermIndex>) -> Self {
        Self {
            term_index: index,
            ..Default::default()
        }
    }

    /// Returns the contents of a metadata type, translated to term strings.
    ///
    /// # Errors
    /// Returns an error if the underlying lock is poisoned when accessed.
    pub fn get_element_metadata_content(
        &self,
        metadata_type: &MetadataType,
        metadata_term_id: TermID,
    ) -> Option<HashMap<String, Vec<String>>> {
        metadata_type.get(&metadata_term_id).map(|tagged_metadata| {
            tagged_metadata
                .iter()
                .map(|(lang_tag, content)| {
                    (
                        fmt_langtag(lang_tag.clone()),
                        translate_metadata_content(&self.term_index, content),
                    )
                })
                .collect::<HashMap<_, _>>()
        })
    }

    /// Adds a language-tagged metadata triple to the metadata buffer.
    ///
    /// # Errors
    /// Returns an error if the triple does not contain subject, predicate, and object.
    ///
    /// Returns an error if the underlying lock is poisoned when accessed.
    pub fn insert_element_metadata(
        &self,
        triple: &ArcTriple,
        language: Option<LanguageTag>,
    ) -> Result<(), SerializationError> {
        match (triple.predicate_term_id, triple.object_term_id) {
            (Some(predicate_term_id), Some(object_term_id)) => {
                self.element_metadata
                    .write()?
                    .entry(triple.subject_term_id)
                    .or_default()
                    .entry(predicate_term_id)
                    .or_default()
                    .entry(language)
                    .or_default()
                    .insert(object_term_id);
            }
            (None, Some(_)) => {
                return Err(SerializationErrorKind::MissingPredicate(
                    self.term_index.display_triple(triple)?,
                    format!(
                        "Failed to insert metadata for element '{}': triple is missing a predicate",
                        self.term_index.display_term(triple.subject_term_id)
                    ),
                ))?;
            }
            (Some(_), None) => {
                return Err(SerializationErrorKind::MissingObject(
                    self.term_index.display_triple(triple)?,
                    format!(
                        "Failed to insert metadata for element '{}': triple is missing an object",
                        self.term_index.display_term(triple.subject_term_id)
                    ),
                ))?;
            }
            (None, None) => {
                return Err(SerializationErrorKind::MissingObject(
                    self.term_index.display_triple(triple)?,
                    format!(
                        "Failed to insert metadata for element '{}': triple is missing both predicate and object",
                        self.term_index.display_term(triple.subject_term_id)
                    ),
                ))?;
            }
        }

        Ok(())
    }

    fn fmt_element_metadata(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        element_metadata: &Arc<RwLock<ElementTypeMetadata>>,
    ) -> std::fmt::Result {
        let mut map: HashMap<TermID, HashMap<TermID, Vec<String>>> = HashMap::new();

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
                            .fold(String::new(), |mut buffer, content_term_id| {
                                // SAFETY: writing strings is infallible
                                // https://doc.rust-lang.org/stable/src/alloc/string.rs.html#2879
                                let _ = write!(
                                    buffer,
                                    "\t\t\t{} - {}",
                                    fmt_langtag(lang_tag.clone()),
                                    self.term_index
                                        .get(*content_term_id)
                                        .map_or_else(|e| e.to_string(), |term| term.to_string())
                                );
                                buffer
                            })
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

    fn fmt_option_usize(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        title: &str,
        value: &Arc<RwLock<Option<TermID>>>,
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
        }
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
    pub source: Option<TermID>,
    pub property: Option<TermID>,
    pub target: Option<TermID>,
    /// The annotation properties/values in the annotation.
    /// Maps from id of annotation property to id of annotation value.
    pub annotations: HashMap<TermID, TermID>,
}

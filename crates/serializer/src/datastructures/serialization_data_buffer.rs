use crate::{
    datastructures::{
        ArcEdge, ArcLockRestrictionState, ArcTriple, DocumentBase,
        graph_metadata_buffer::GraphMetadataBuffer, index::TermIndex,
    },
    errors::{SerializationError, SerializationErrorKind},
    serializer_util::trim_tag_circumfix,
};
use grapher::prelude::{
    Characteristic, ElementType, GraphDisplayData, GraphMetadata, OwlEdge, OwlType,
};
use log::debug;
use std::{
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter},
    mem::take,
    sync::{Arc, RwLock},
};
use vowlgrapher_util::prelude::{ErrorRecord, VOWLGrapherError};

/// An intermediate container for serialization data.
///
/// This data may mutate during serialization
/// if new information regarding them is found.
/// This also means an element can be completely removed!
#[derive(Default)]
pub struct SerializationDataBuffer {
    /// Maps terms to integer ids and vice-versa.
    ///
    /// Reduces memory usage and allocations.
    pub term_index: Arc<TermIndex>,
    /// Stores all resolved node elements.
    ///
    /// The key is a term's corresponding id.
    ///
    /// The value is a term's type, e.g., "Owl Class".
    pub node_element_buffer: Arc<RwLock<HashMap<usize, ElementType>>>,
    /// Stores all resolved edge elements.
    ///
    /// The key is a term's corresponding id.
    ///
    /// The value is a term's type, e.g., "Owl Class".
    pub edge_element_buffer: Arc<RwLock<HashMap<usize, ElementType>>>,
    /// Keeps track of edges that should point to a node different
    /// from their definition.
    ///
    /// This can happen if, e.g., two nodes are merged.
    ///
    /// The key is the range term of an edge triple, translated to that term's corresponding id.
    ///
    /// The value is the domain term of an edge triple, translated to that term's corresponding id.
    pub edge_redirection: Arc<RwLock<HashMap<usize, usize>>>,
    /// Maps a term's corresponding id to the set of edges that include it.
    ///
    /// Used to remap edges when nodes are merged.
    pub edges_include_map: Arc<RwLock<HashMap<usize, HashSet<ArcEdge>>>>,
    /// Canonical synthesized owl:Thing node per resolved domain.
    ///
    /// This lets structurally-defined ranges like complement/union expressions
    /// collapse to the same owl:Thing node that direct owl:Thing ranges use.
    pub anchor_thing_map: Arc<RwLock<HashMap<usize, usize>>>,
    /// Partially assembled restriction metadata keyed by the restriction node.
    pub restriction_buffer: Arc<RwLock<HashMap<usize, ArcLockRestrictionState>>>,
    #[expect(clippy::type_complexity)]
    /// Final display cardinalities keyed by the concrete edge that will be emitted.
    pub edge_cardinality_buffer: Arc<RwLock<HashMap<ArcEdge, (String, Option<String>)>>>,
    /// Stores the edges of a property, keyed by the property's corresponding id.
    pub property_edge_map: Arc<RwLock<HashMap<usize, ArcEdge>>>,
    /// Stores the domains of a property, keyed by the property's corresponding id.
    pub property_domain_map: Arc<RwLock<HashMap<usize, HashSet<usize>>>>,
    /// Stores the ranges of a property, keyed by the property's corresponding id.
    pub property_range_map: Arc<RwLock<HashMap<usize, HashSet<usize>>>>,
    /// Stores declared domains of a property, keyed by the property's corresponding id.
    ///
    /// This is used by owl:inverseOf resolution and should contain only query-level
    /// domain/range evidence, never endpoints inferred from rendered property edges.
    pub declared_property_domain_map: Arc<RwLock<HashMap<usize, HashSet<usize>>>>,
    /// Stores declared ranges of a property, keyed by the property's corresponding id.
    ///
    /// This is used by owl:inverseOf resolution and should contain only query-level
    /// domain/range evidence, never endpoints inferred from rendered property edges.
    pub declared_property_range_map: Arc<RwLock<HashMap<usize, HashSet<usize>>>>,
    /// Stores labels of terms, keyed by the term's corresponding id.
    pub label_buffer: Arc<RwLock<HashMap<usize, Option<String>>>>,
    /// Stores labels of edges, keyed by the edge it belongs to.
    pub edge_label_buffer: Arc<RwLock<HashMap<ArcEdge, Option<String>>>>,
    /// Edges in graph, to avoid duplicates
    pub edge_buffer: Arc<RwLock<HashSet<ArcEdge>>>,
    /// Maps from an edge to its characteristic.
    pub edge_characteristics: Arc<RwLock<HashMap<ArcEdge, HashSet<Characteristic>>>>,
    /// Maps from a node term's corresponding id to its characteristics.
    pub node_characteristics: Arc<RwLock<HashMap<usize, HashSet<Characteristic>>>>,
    /// Maps from node term's corresponding id to its number of individuals.
    pub individual_count_buffer: Arc<RwLock<HashMap<usize, u32>>>,
    /// Maps from a class term id to the set of canonical individual term ids already counted for it.
    pub counted_individual_members: Arc<RwLock<HashMap<usize, HashSet<usize>>>>,
    /// Stores unresolved triples.
    ///
    /// This is a mapping of a term's corresponding id to the set of triples referencing it.
    pub unknown_buffer: Arc<RwLock<HashMap<usize, HashSet<ArcTriple>>>>,
    /// Stores errors encountered during serialization.
    pub failed_buffer: Arc<RwLock<Vec<ErrorRecord>>>,
    /// The base IRI of the document.
    ///
    /// For instance: `http://purl.obolibrary.org/obo/envo.owl`
    pub document_base: Arc<RwLock<Option<DocumentBase>>>,
    /// Data not visualized in the graph.
    pub metadata: GraphMetadataBuffer,
}
impl SerializationDataBuffer {
    pub fn new() -> Self {
        let term_index: Arc<TermIndex> = TermIndex::new().into();
        Self {
            metadata: GraphMetadataBuffer::new(term_index.clone()),
            term_index,
            ..Default::default()
        }
    }

    /// Unpack the predicate term id of the triple.
    ///
    /// Returns an error if the term id is None.
    pub fn get_predicate(&self, triple: &ArcTriple) -> Result<usize, SerializationError> {
        match triple.predicate_term_id {
            Some(predicate_term_id) => Ok(predicate_term_id),
            None => Err(SerializationErrorKind::MissingPredicate(
                self.term_index.display_triple(triple)?,
                "Cannot serialize a triple with a missing predicate".to_string(),
            ))?,
        }
    }

    /// Converts [`self`] into [`GraphDisplayData`].
    ///
    /// Works like [`TryFrom`] except it also returns non-critical errors in [`Result::Ok`].
    pub fn convert_into(
        &self,
    ) -> Result<(GraphDisplayData, Option<VOWLGrapherError>), SerializationError> {
        let mut display_data = GraphDisplayData::new();
        let mut failed: Vec<ErrorRecord> = Vec::new();

        // Maps an RDF term's corresponding id to a [`GraphDisplayData`] index.
        let mut iricache: HashMap<usize, usize> = HashMap::new();

        // Maps an RDF term's corresponding id to a [`GraphDisplayData`] index.
        let mut inverse_edge_indices: HashMap<usize, usize> = HashMap::new();

        self.convert_graph_data(
            &mut display_data,
            &mut failed,
            &mut iricache,
            &mut inverse_edge_indices,
        )?;
        self.convert_metadata(&mut display_data, &mut failed, &iricache)?;

        if failed.is_empty() {
            Ok((display_data, None))
        } else {
            Ok((display_data, Some(failed.into())))
        }
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "this method runs single-threaded"
    )]
    fn convert_graph_data(
        &self,
        display_data: &mut GraphDisplayData,
        failed: &mut Vec<ErrorRecord>,
        iricache: &mut HashMap<usize, usize>,
        inverse_edge_indices: &mut HashMap<usize, usize>,
    ) -> Result<(), SerializationError> {
        let mut label_buffer = self.label_buffer.write()?;
        let mut node_element_buffer = self.node_element_buffer.write()?;
        for (term_id, element) in take(&mut *node_element_buffer) {
            let label = label_buffer.remove(&term_id);
            if label.is_none() && !self.term_index.is_blank_node(term_id)? {
                let msg = match self.term_index.get(term_id) {
                    Ok(term) => {
                        format!("Label not found for term '{term}'. Using None")
                    }
                    Err(e) => {
                        format!("Label not found for term '{e}'. Using None")
                    }
                };
                debug!("{msg}");
            }
            iricache.insert(term_id, display_data.elements.len());
            display_data.labels.push(label.flatten());
            display_data.elements.push(element);
        }

        let mut edge_label_buffer = self.edge_label_buffer.write()?;
        let mut edge_characteristics = self.edge_characteristics.write()?;
        let mut edge_cardinality_buffer = self.edge_cardinality_buffer.write()?;
        for edge in self.edge_buffer.read()?.iter() {
            let subject_idx = iricache.get(&edge.domain_term_id);
            let object_idx = iricache.get(&edge.range_term_id);
            let maybe_label = edge_label_buffer.remove(edge).flatten();
            let characteristics = edge_characteristics.remove(edge);
            let cardinality = edge_cardinality_buffer.remove(edge);

            match (subject_idx, object_idx) {
                (Some(subject_idx), Some(object_idx)) => {
                    let edge_idx =
                        if edge.edge_type == ElementType::Owl(OwlType::Edge(OwlEdge::InverseOf)) {
                            let Some(property_id) = edge.property_term_id else {
                                let msg = format!("Edge is missing merged property id\n{edge}");
                                failed.push(<SerializationError as Into<ErrorRecord>>::into(
                                    SerializationErrorKind::MissingProperty(msg).into(),
                                ));
                                continue;
                            };

                            if let Some(existing_idx) = inverse_edge_indices.get(&property_id) {
                                *existing_idx
                            } else {
                                display_data.elements.push(edge.edge_type);
                                display_data.labels.push(maybe_label.clone());
                                let new_idx = display_data.elements.len() - 1;
                                inverse_edge_indices.insert(property_id, new_idx);
                                new_idx
                            }
                        } else {
                            display_data.elements.push(edge.edge_type);
                            display_data.labels.push(maybe_label.clone());
                            display_data.elements.len() - 1
                        };

                    display_data
                        .edges
                        .push([*subject_idx, edge_idx, *object_idx]);

                    if let Some(characteristics) = characteristics {
                        display_data
                            .characteristics
                            .insert(edge_idx, characteristics);
                    }

                    if let Some(cardinality) = cardinality {
                        let display_edge_idx = u32::try_from(display_data.edges.len() - 1)
                            .map_err(|_| {
                                SerializationErrorKind::SerializationFailed(format!(
                                    "Cardinality edge index overflow ({}/{})",
                                    display_data.edges.len() - 1,
                                    u32::MAX
                                ))
                            })?;
                        display_data
                            .cardinalities
                            .push((display_edge_idx, cardinality));
                    }
                }
                (None, _) => {
                    let msg = "Domain of edge not found in iricache".to_string();
                    failed.push(<SerializationError as Into<ErrorRecord>>::into(
                        SerializationErrorKind::MissingDomain(
                            self.term_index.display_edge(edge)?,
                            msg,
                        )
                        .into(),
                    ));
                }
                (_, None) => {
                    let msg = "Range of edge not found in iricache".to_string();
                    failed.push(<SerializationError as Into<ErrorRecord>>::into(
                        SerializationErrorKind::MissingRange(
                            self.term_index.display_edge(edge)?,
                            msg,
                        )
                        .into(),
                    ));
                }
            }
        }

        let mut node_characteristics = self.node_characteristics.write()?;
        for (term_id, characteristics) in take(&mut *node_characteristics) {
            let idx = iricache.get(&term_id);
            if let Some(idx) = idx {
                display_data.characteristics.insert(*idx, characteristics);
            } else {
                let msg = match self.term_index.get(term_id) {
                    Ok(term) => {
                        format!("Characteristic not found for term '{term}' in iricache")
                    }
                    Err(e) => {
                        format!("Characteristic not found for term '{e}' in iricache")
                    }
                };
                debug!("{msg}");
            }
        }

        let mut individual_count_buffer = self.individual_count_buffer.write()?;
        for (term_id, count) in take(&mut *individual_count_buffer) {
            if let Some(idx) = iricache.get(&term_id) {
                display_data.individual_counts.insert(*idx, count);
            }
        }
        Ok(())
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "this method runs single-threaded"
    )]
    fn convert_metadata(
        &self,
        display_data: &mut GraphDisplayData,
        failed: &mut Vec<ErrorRecord>,
        iricache: &HashMap<usize, usize>,
    ) -> Result<(), SerializationError> {
        let mut metadata = GraphMetadata::new();

        metadata.document_base = self
            .document_base
            .read()?
            .clone()
            .map_or_else(String::new, |docbase| docbase.base);
        metadata.description = {
            let value = self.document_base.read()?.clone();
            if let Some(docbase) = value {
                if let Ok(base_term_id) = self.term_index.get_id(&docbase.base_term) {
                    self.metadata
                        .comment_buffer
                        .read()?
                        .get(&base_term_id)
                        .map_or_else(String::new, |comment_term_id| {
                            self.term_index
                                .get(*comment_term_id)
                                .map_or_else(|e| e.to_string(), |term| term.to_string())
                        })
                } else {
                    let msg = format!(
                        "Failed to create ontology description: Term id for document base '{}' not found in term index",
                        docbase.base_term
                    );
                    debug!("{msg}");
                    failed.push(SerializationErrorKind::TermIndexError(msg.clone()).into());
                    msg
                }
            } else {
                String::new()
            }
        };
        metadata.title = {
            let value = self.document_base.read()?.clone();
            if let Some(docbase) = value {
                if let Ok(base_term_id) = self.term_index.get_id(&docbase.base_term) {
                    match self.label_buffer.read()?.get(&base_term_id) {
                        Some(Some(label)) => label.clone(),
                        Some(None) => {
                            // No label declared
                            let msg = "Ontology title not found in ontology.".to_string();

                            debug!("{msg}");
                            failed.push(SerializationErrorKind::SerializationWarning(msg).into());
                            String::new()
                        }
                        None => {
                            // No label found in buffer
                            let msg = "Ontology title not found in label buffer".to_string();
                            debug!("{msg}");
                            failed.push(
                                SerializationErrorKind::SerializationWarning(msg.clone()).into(),
                            );
                            msg
                        }
                    }
                } else {
                    let msg = format!(
                        "Failed to create ontology title: Term id for document base '{}' not found in term index",
                        docbase.base_term
                    );
                    debug!("{msg}");
                    failed.push(SerializationErrorKind::TermIndexError(msg.clone()).into());
                    msg
                }
            } else {
                String::new()
            }
        };
        let mut author_buffer = self.metadata.author_buffer.write()?;
        for (term_id, author) in take(&mut *author_buffer) {
            // TODO: Implement
        }
        let mut comment_buffer = self.metadata.comment_buffer.write()?;
        for (term_id, comment_term_id) in take(&mut *comment_buffer) {
            if let Some(term_idx) = iricache.get(&term_id) {
                let comment = self
                    .term_index
                    .get(comment_term_id)
                    .map_or_else(|e| e.to_string(), |term| term.to_string());
                if let Some(old_comment) = metadata.comments.insert(*term_idx, comment.clone()) {
                    let term = self
                        .term_index
                        .get(term_id)
                        .map_or_else(|e| e.to_string(), |term| term.to_string());
                    let msg = format!(
                        "Overriding comment '{old_comment}' for term '{term}' with new comment '{comment}'"
                    );
                    debug!("{msg}");
                    failed.push(SerializationErrorKind::SerializationWarning(msg).into());
                }
            } else {
                let msg = match self.term_index.get(term_id) {
                    Ok(term) => {
                        format!("Failed to map comment: Term '{term}' not found in iricache")
                    }
                    Err(e) => {
                        format!("Failed to map comment: Term '{e}' not found in iricache")
                    }
                };
                debug!("{msg}");
            }
        }
        let mut defined_by_buffer = self.metadata.defined_by_buffer.write()?;
        for (term_id, defined_by_term_id) in take(&mut *defined_by_buffer) {
            let maybe_term_idx = iricache.get(&term_id);
            let maybe_defined_by_term = self.term_index.get(defined_by_term_id);

            match (maybe_term_idx, maybe_defined_by_term) {
                (Some(term_idx), Ok(defined_by_term)) => {
                    let new_defined_by = trim_tag_circumfix(&defined_by_term.to_string());
                    if let Some(old_defined_by) = metadata
                        .is_defined_by
                        .insert(*term_idx, new_defined_by.clone())
                    {
                        let term = self
                            .term_index
                            .get(term_id)
                            .map_or_else(|e| e.to_string(), |term| term.to_string());
                        let msg = format!(
                            "Overriding isDefinedBy '{old_defined_by}' for term '{term}' with new isDefinedBy '{new_defined_by}'"
                        );
                        debug!("{msg}");
                        failed.push(SerializationErrorKind::SerializationWarning(msg).into());
                    }
                }
                (None, Ok(_)) => {}
                (Some(_) | None, Err(e)) => {
                    failed.push(e.into());
                }
            }
        }
        let mut see_also_buffer = self.metadata.see_also_buffer.write()?;
        for (term_id, see_also) in take(&mut *see_also_buffer) {
            // TODO: Implement
        }
        metadata.version_iri = Some(self.metadata.version_iri.read()?.map_or_else(
            String::new,
            |version_term_id| {
                self.term_index
                    .get(version_term_id)
                    .map_or_else(|e| e.to_string(), |term| term.to_string())
            },
        ));
        metadata.prior_version = Some(self.metadata.prior_version.read()?.map_or_else(
            String::new,
            |version_term_id| {
                self.term_index
                    .get(version_term_id)
                    .map_or_else(|e| e.to_string(), |term| term.to_string())
            },
        ));
        metadata.incompatible_with = Some(self.metadata.prior_version.read()?.map_or_else(
            String::new,
            |version_term_id| {
                self.term_index
                    .get(version_term_id)
                    .map_or_else(|e| e.to_string(), |term| term.to_string())
            },
        ));
        metadata.backward_compatible_with = Some(self.metadata.prior_version.read()?.map_or_else(
            String::new,
            |version_term_id| {
                self.term_index
                    .get(version_term_id)
                    .map_or_else(|e| e.to_string(), |term| term.to_string())
            },
        ));

        display_data.graph_metadata = metadata;
        Ok(())
    }
}

impl Display for SerializationDataBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "SerializationDataBuffer {{")?;

        writeln!(
            f,
            "\tdocument_base: {}",
            self.document_base
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
                .map_or_else(String::new, |docbase| docbase.base)
        )?;
        writeln!(f, "\tnode_element_buffer:")?;
        for (term_id, element) in self
            .node_element_buffer
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            let term = self
                .term_index
                .get(*term_id)
                .map_or_else(|e| e.to_string(), |term| term.to_string());
            writeln!(f, "\t\t{term} : {element}")?;
        }
        writeln!(f, "\tedge_element_buffer (not used by into()):")?;
        for (term_id, element) in self
            .edge_element_buffer
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            let term = self
                .term_index
                .get(*term_id)
                .map_or_else(|e| e.to_string(), |term| term.to_string());
            writeln!(f, "\t\t{term} : {element}")?;
        }
        writeln!(f, "\tedge_redirection:")?;
        for (term_id, subject_term_id) in self
            .edge_redirection
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            let term = self
                .term_index
                .get(*term_id)
                .map_or_else(|e| e.to_string(), |term| term.to_string());
            let subject_term = self
                .term_index
                .get(*subject_term_id)
                .map_or_else(|e| e.to_string(), |term| term.to_string());
            writeln!(f, "\t\t{term} -> {subject_term}")?;
        }
        writeln!(f, "\tedges_include_map: ")?;
        for (term_id, edges) in self
            .edges_include_map
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            let term = self
                .term_index
                .get(*term_id)
                .map_or_else(|e| e.to_string(), |term| term.to_string());
            writeln!(f, "\t\t{term} : {{")?;
            for edge in edges {
                let display_edge = self
                    .term_index
                    .display_edge(edge)
                    .unwrap_or_else(|e| e.to_string());

                writeln!(f, "\t\t\t{display_edge}")?;
            }
            writeln!(f, "\t\t}}")?;
        }
        writeln!(f, "\tlabel_buffer:")?;
        for (term_id, label) in self
            .label_buffer
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            let term = self
                .term_index
                .get(*term_id)
                .map_or_else(|e| e.to_string(), |term| term.to_string());
            writeln!(f, "\t\t{term} : {label:?}")?;
        }
        writeln!(f, "\tedge_buffer:")?;
        for edge in self
            .edge_buffer
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            let display_edge = self
                .term_index
                .display_edge(edge)
                .unwrap_or_else(|e| e.to_string());
            writeln!(f, "\t\t{display_edge}")?;
        }

        writeln!(f, "\tedge_characteristics:")?;
        for (edge, characteristics) in self
            .edge_characteristics
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            let display_edge = self
                .term_index
                .display_edge(edge)
                .unwrap_or_else(|e| e.to_string());
            writeln!(f, "{display_edge}\n\t{characteristics:?}")?;
        }

        writeln!(f, "\tnode_characteristics:")?;
        for (term_id, characteristics) in self
            .node_characteristics
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            let term = self
                .term_index
                .get(*term_id)
                .map_or_else(|e| e.to_string(), |term| term.to_string());
            writeln!(f, "{term}\n\t{characteristics:?}")?;
        }

        writeln!(f, "\tindividual_count_buffer:")?;
        for (term_id, individual_count) in self
            .individual_count_buffer
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            let term = self
                .term_index
                .get(*term_id)
                .map_or_else(|e| e.to_string(), |term| term.to_string());
            writeln!(
                f,
                "\t\t{} : {} individual{}",
                term,
                individual_count,
                if *individual_count == 1 { "" } else { "s" }
            )?;
        }

        writeln!(f, "\tunknown_buffer:")?;
        for (term_id, triples) in self
            .unknown_buffer
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            let term = self
                .term_index
                .get(*term_id)
                .map_or_else(|e| e.to_string(), |term| term.to_string());
            write!(f, "\t\t{term} : ")?;
            for triple in triples {
                let display_triple = self
                    .term_index
                    .display_triple(triple)
                    .unwrap_or_else(|e| e.to_string());
                writeln!(f, "{display_triple}")?;
            }
        }
        // Not needed as it's displayed by the serializer
        // writeln!(f, "\tfailed_buffer:")?;
        // writeln!(f, "{}", ErrorRecord::format_records(&self.failed_buffer))?;
        write!(f, "{}", self.metadata)?;
        writeln!(f, "}}")
    }
}

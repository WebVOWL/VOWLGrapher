use crate::{
    datastructures::{
        ArcEdge, ArcLockRestrictionState, ArcTriple, graph_metadata_buffer::GraphMetadataBuffer,
        index::TermIndex,
    },
    errors::{SerializationError, SerializationErrorKind},
};
use grapher::prelude::{Characteristic, ElementType, GraphDisplayData, OwlEdge, OwlType};
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
    pub term_index: TermIndex,
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
    pub document_base: Arc<RwLock<Option<Arc<String>>>>,
    /// Data not visualized in the graph.
    pub metadata: GraphMetadataBuffer,
}
impl SerializationDataBuffer {
    pub fn new() -> Self {
        Self::default()
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

    #[expect(
        clippy::significant_drop_tightening,
        reason = "this method clears most buffers and is expected to be called at the end of processing. 
        Thus, keeping locks longer than necessary doesn't matter"
    )]
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

        if failed.is_empty() {
            Ok((display_data, None))
        } else {
            Ok((display_data, Some(failed.into())))
        }
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
                .unwrap_or_default()
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
        writeln!(f, "}}")
    }
}

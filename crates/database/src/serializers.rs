use std::{
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
    mem::take,
    sync::{Arc, RwLock},
};

use grapher::prelude::{Characteristic, ElementType, GraphDisplayData, OwlEdge, OwlType};
use oxrdf::Term;
use vowlr_util::prelude::{ErrorRecord, VOWLRError};

use crate::{
    errors::{SerializationError, SerializationErrorKind},
    serializers::{
        index::TermIndex,
        util::{PROPERTY_EDGE_TYPES, SYMMETRIC_EDGE_TYPES},
    },
};

pub mod frontend;
pub mod index;
pub mod util;

type ArcTerm = Arc<Term>;
type ArcTriple = Arc<Triple>;
type ArcEdge = Arc<Edge>;
type ArcLockRestrictionState = Arc<RwLock<RestrictionState>>;

#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub struct Triple {
    /// The subject.
    subject_term_id: usize,
    /// The predicate.
    predicate_term_id: usize,
    /// The object.
    object_term_id: Option<usize>,
}

impl Display for Triple {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Triple{{ ")?;
        write!(f, "{} - ", self.subject_term_id)?;
        write!(f, "{} - ", self.predicate_term_id)?;
        write!(
            f,
            "{}",
            self.object_term_id
                .as_ref()
                .map(|t| t.to_string())
                .unwrap_or_default(),
        )?;
        write!(f, "}}")
    }
}

impl Triple {
    pub fn new(
        subject_term_id: usize,
        predicate_term_id: usize,
        object_term_id: Option<usize>,
    ) -> Self {
        Self {
            subject_term_id,
            predicate_term_id,
            object_term_id,
        }
    }
}

#[derive(Debug, Clone, Eq)]
pub struct Edge {
    /// The domain of the edge.
    ///
    /// Also called the "source".
    domain_term_id: usize,
    /// The type of the edge, e.g., "Object Property".
    edge_type: ElementType,
    /// The range of the edge.
    ///
    /// Also called the "target".
    range_term_id: usize,
    /// The property.
    property_term_id: Option<usize>,
}

impl Edge {
    pub fn new(
        domain_term_id: usize,
        edge_type: ElementType,
        range_term_id: usize,
        property_term_id: Option<usize>,
    ) -> Self {
        Self {
            domain_term_id,
            edge_type,
            range_term_id,
            property_term_id,
        }
    }
}

impl PartialEq for Edge {
    fn eq(&self, other: &Self) -> bool {
        // Element type and property must always match
        if self.edge_type != other.edge_type || self.property_term_id != other.property_term_id {
            return false;
        }

        // For symmetric relations, treat (A, B) and (B, A) as equal
        if SYMMETRIC_EDGE_TYPES.contains(&self.edge_type) {
            (self.domain_term_id == other.domain_term_id
                && self.range_term_id == other.range_term_id)
                || (self.domain_term_id == other.range_term_id
                    && self.range_term_id == other.domain_term_id)
        } else {
            self.domain_term_id == other.domain_term_id && self.range_term_id == other.range_term_id
        }
    }
}

impl Hash for Edge {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if SYMMETRIC_EDGE_TYPES.contains(&self.edge_type) {
            // For symmetric relations, hash the sorted pair
            let (first, second) =
                if self.domain_term_id.to_string() <= self.range_term_id.to_string() {
                    (&self.domain_term_id, &self.range_term_id)
                } else {
                    (&self.range_term_id, &self.domain_term_id)
                };

            first.hash(state);
            second.hash(state);
            self.edge_type.hash(state);
        } else if PROPERTY_EDGE_TYPES.contains(&self.edge_type) {
            self.domain_term_id.hash(state);
            self.edge_type.hash(state);
            self.range_term_id.hash(state);
            self.property_term_id.hash(state);
        } else {
            self.domain_term_id.hash(state);
            self.edge_type.hash(state);
            self.range_term_id.hash(state);
        }
    }
}

impl Display for Edge {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Edge{{ {} - {:?} - {} }}",
            self.domain_term_id, self.edge_type, self.range_term_id
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub enum RestrictionRenderMode {
    #[default]
    ValuesFromEdge,
    ExistingPropertyEdge,
}

#[derive(Debug, Clone, Default)]
pub struct RestrictionState {
    pub on_property: Option<usize>,
    pub filler: Option<usize>,
    pub cardinality: Option<(String, Option<String>)>,
    pub self_restriction: bool,
    pub requires_filler: bool,
    pub render_mode: RestrictionRenderMode,
}

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
    term_index: TermIndex,
    /// Stores all resolved node elements.
    ///
    /// The key is a term's corresponding id.
    ///
    /// The value is a term's type, e.g., "Owl Class".
    node_element_buffer: Arc<RwLock<HashMap<usize, ElementType>>>,
    /// Stores all resolved edge elements.
    ///
    /// The key is a term's corresponding id.
    ///
    /// The value is a term's type, e.g., "Owl Class".
    edge_element_buffer: Arc<RwLock<HashMap<usize, ElementType>>>,
    /// Keeps track of edges that should point to a node different
    /// from their definition.
    ///
    /// This can happen if, e.g., to nodes are merged.
    ///
    /// The key is the range term of an edge triple, translated to that term's corresponding id.
    ///
    /// The value is the domain term of an edge triple, translated to that term's corresponding id.
    edge_redirection: Arc<RwLock<HashMap<usize, usize>>>,
    /// Maps a term's corresponding id to the set of edges that include it.
    ///
    /// Used to remap edges when nodes are merged.
    edges_include_map: Arc<RwLock<HashMap<usize, HashSet<ArcEdge>>>>,
    /// Canonical synthesized owl:Thing node per resolved domain.
    ///
    /// This lets structurally-defined ranges like complement/union expressions
    /// collapse to the same owl:Thing node that direct owl:Thing ranges use.
    anchor_thing_map: Arc<RwLock<HashMap<usize, usize>>>,
    /// Partially assembled restriction metadata keyed by the restriction node.
    restriction_buffer: Arc<RwLock<HashMap<usize, ArcLockRestrictionState>>>,
    /// Final display cardinalities keyed by the concrete edge that will be emitted.
    edge_cardinality_buffer: Arc<RwLock<HashMap<ArcEdge, (String, Option<String>)>>>,
    /// Stores the edges of a property, keyed by the property's corresponding id.
    property_edge_map: Arc<RwLock<HashMap<usize, ArcEdge>>>,
    /// Stores the domains of a property, keyed by the property's corresponding id.
    property_domain_map: Arc<RwLock<HashMap<usize, HashSet<usize>>>>,
    /// Stores the ranges of a property, keyed by the property's corresponding id.
    property_range_map: Arc<RwLock<HashMap<usize, HashSet<usize>>>>,
    /// Stores labels of terms, keyed by the term's corresponding id.
    label_buffer: Arc<RwLock<HashMap<usize, String>>>,
    /// Stores labels of edges, keyed by the edge it belongs to.
    edge_label_buffer: Arc<RwLock<HashMap<ArcEdge, String>>>,
    /// Edges in graph, to avoid duplicates
    edge_buffer: Arc<RwLock<HashSet<ArcEdge>>>,
    /// Maps from an edge to its characteristic.
    edge_characteristics: Arc<RwLock<HashMap<ArcEdge, HashSet<Characteristic>>>>,
    /// Maps from a node term's corresponding id to its characteristics.
    node_characteristics: Arc<RwLock<HashMap<usize, HashSet<Characteristic>>>>,
    /// Maps from node term's corresponding id to its number of individuals.
    individual_count_buffer: Arc<RwLock<HashMap<usize, u32>>>,
    /// Stores unresolved triples.
    ///
    /// This is a mapping of a term's corresponding id to the set of triples referencing it.
    unknown_buffer: Arc<RwLock<HashMap<usize, HashSet<ArcTriple>>>>,
    /// Stores errors encountered during serialization.
    failed_buffer: Arc<RwLock<Vec<ErrorRecord>>>,
    /// The base IRI of the document.
    ///
    /// For instance: `http://purl.obolibrary.org/obo/envo.owl`
    document_base: Arc<RwLock<Option<Arc<String>>>>,
}
impl SerializationDataBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Converts [`self`] into [`GraphDisplayData`].
    ///
    /// Works like [`TryFrom`] except it also returns non-critical errors in [`Result::Ok`].
    pub fn convert_into(
        &self,
    ) -> Result<(GraphDisplayData, Option<VOWLRError>), SerializationError> {
        let mut display_data = GraphDisplayData::new();
        let mut failed: Vec<ErrorRecord> = Vec::new();

        // Maps an RDF term's corresponding id to a [`GraphDisplayData`] index.
        let mut iricache: HashMap<usize, usize> = HashMap::new();

        // Maps an RDF term's corresponding id to a [`GraphDisplayData`] index.
        let mut inverse_edge_indices: HashMap<usize, usize> = HashMap::new();

        let mut label_buffer = self.label_buffer.write()?;
        let mut node_element_buffer = self.node_element_buffer.write()?;
        for (term_id, element) in take(&mut *node_element_buffer).into_iter() {
            let label = label_buffer.remove(&term_id);
            if label.is_none() {
                let msg = match self.term_index.get(&term_id) {
                    Ok(term) => {
                        format!("Label not found for term '{}'. Using None", term)
                    }
                    Err(e) => {
                        format!("Label not found for term '{}'. Using None", e)
                    }
                };
                failed.push(<SerializationError as Into<ErrorRecord>>::into(
                    SerializationErrorKind::MissingLabel(msg).into(),
                ));
            }
            iricache.insert(term_id, display_data.elements.len());
            display_data.labels.push(label);
            display_data.elements.push(element);
        }

        let mut edge_label_buffer = self.edge_label_buffer.write()?;
        let mut edge_characteristics = self.edge_characteristics.write()?;
        let mut edge_cardinality_buffer = self.edge_cardinality_buffer.write()?;
        for edge in self.edge_buffer.read()?.iter() {
            let subject_idx = iricache.get(&edge.domain_term_id);
            let object_idx = iricache.get(&edge.range_term_id);
            let maybe_label = edge_label_buffer.remove(edge);
            let characteristics = edge_characteristics.remove(edge);
            let cardinality = edge_cardinality_buffer.remove(edge);

            match (subject_idx, object_idx) {
                (Some(subject_idx), Some(object_idx)) => {
                    let edge_idx =
                        if edge.edge_type == ElementType::Owl(OwlType::Edge(OwlEdge::InverseOf)) {
                            let Some(property_id) = edge.property_term_id else {
                                let msg = format!("Edge is missing merged property id\n{}", edge);
                                failed.push(<SerializationError as Into<ErrorRecord>>::into(
                                    SerializationErrorKind::MissingProperty(msg).into(),
                                ));
                                continue;
                            };

                            match inverse_edge_indices.get(&property_id) {
                                Some(existing_idx) => *existing_idx,
                                None => {
                                    display_data.elements.push(edge.edge_type);
                                    display_data.labels.push(maybe_label.clone());
                                    let new_idx = display_data.elements.len() - 1;
                                    inverse_edge_indices.insert(property_id, new_idx);
                                    new_idx
                                }
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
                    let msg = format!("Domain of edge not found in iricache");
                    failed.push(<SerializationError as Into<ErrorRecord>>::into(
                        SerializationErrorKind::MissingDomain(edge.clone(), msg).into(),
                    ));
                }
                (_, None) => {
                    let msg = format!("Range of edge not found in iricache");
                    failed.push(<SerializationError as Into<ErrorRecord>>::into(
                        SerializationErrorKind::MissingRange(edge.clone(), msg).into(),
                    ));
                }
            }
        }

        let mut node_characteristics = self.node_characteristics.write()?;
        for (term_id, characteristics) in take(&mut *node_characteristics).into_iter() {
            let idx = iricache.get(&term_id);
            match idx {
                Some(idx) => {
                    display_data.characteristics.insert(*idx, characteristics);
                }
                None => {
                    let msg = match self.term_index.get(&term_id) {
                        Ok(term) => {
                            format!("Characteristic not found for term '{}' in iricache", term)
                        }
                        Err(e) => {
                            format!("Characteristic not found for term '{}' in iricache", e)
                        }
                    };
                    failed.push(<SerializationError as Into<ErrorRecord>>::into(
                        SerializationErrorKind::MisisngCharacteristic(msg).into(),
                    ));
                }
            }
        }

        let mut individual_count_buffer = self.individual_count_buffer.write()?;
        for (term_id, count) in take(&mut *individual_count_buffer).into_iter() {
            match iricache.get(&term_id) {
                Some(idx) => {
                    display_data.individual_counts.insert(*idx, count);
                }
                None => {
                    let msg = match self.term_index.get(&term_id) {
                        Ok(term) => {
                            format!("Individual count not found for term '{}' in iricache", term)
                        }
                        Err(e) => {
                            format!("Individual count not found for term '{}' in iricache", e)
                        }
                    };
                    failed.push(<SerializationError as Into<ErrorRecord>>::into(
                        SerializationErrorKind::MisisngCharacteristic(msg).into(),
                    ));
                }
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
                .map_or_else(|e| e.into_inner(), |v| v)
                .clone()
                .unwrap_or_default()
        )?;
        writeln!(f, "\tnode_element_buffer:")?;
        for (iri, element) in self
            .node_element_buffer
            .read()
            .map_or_else(|e| e.into_inner(), |v| v)
            .iter()
        {
            writeln!(f, "\t\t{} : {}", iri, element)?;
        }
        writeln!(f, "\tedge_element_buffer (not used by into()):")?;
        for (iri, element) in self
            .edge_element_buffer
            .read()
            .map_or_else(|e| e.into_inner(), |v| v)
            .iter()
        {
            writeln!(f, "\t\t{} : {}", iri, element)?;
        }
        writeln!(f, "\tedge_redirection:")?;
        for (iri, subject) in self
            .edge_redirection
            .read()
            .map_or_else(|e| e.into_inner(), |v| v)
            .iter()
        {
            writeln!(f, "\t\t{} -> {}", iri, subject)?;
        }
        writeln!(f, "\tedges_include_map: ")?;
        for (iri, edges) in self
            .edges_include_map
            .read()
            .map_or_else(|e| e.into_inner(), |v| v)
            .iter()
        {
            writeln!(f, "\t\t{} : {{", iri)?;
            for edge in edges.iter() {
                writeln!(f, "\t\t\t{}", edge)?;
            }
            writeln!(f, "\t\t}}")?;
        }
        writeln!(f, "\tlabel_buffer:")?;
        for (iri, label) in self
            .label_buffer
            .read()
            .map_or_else(|e| e.into_inner(), |v| v)
            .iter()
        {
            writeln!(f, "\t\t{} : {}", iri, label)?;
        }
        writeln!(f, "\tedge_buffer:")?;
        for edge in self
            .edge_buffer
            .read()
            .map_or_else(|e| e.into_inner(), |v| v)
            .iter()
        {
            writeln!(f, "\t\t{}", edge)?;
        }
        writeln!(f, "\tedge_characteristics: {:?}", self.edge_characteristics)?;
        writeln!(f, "\tnode_characteristics: {:?}", self.node_characteristics)?;
        writeln!(
            f,
            "\tindividual_count_buffer: {:?}",
            self.individual_count_buffer
        )?;
        writeln!(f, "\tunknown_buffer:")?;
        for (iri, triples) in self
            .unknown_buffer
            .read()
            .map_or_else(|e| e.into_inner(), |v| v)
            .iter()
        {
            write!(f, "\t\t{} : ", iri)?;
            writeln!(
                f,
                "{}",
                triples
                    .iter()
                    .map(|t| t.to_string())
                    .collect::<Vec<String>>()
                    .join("\n")
            )?;
        }
        // Not needed as it's displayed by the serializer
        // writeln!(f, "\tfailed_buffer:")?;
        // writeln!(f, "{}", ErrorRecord::format_records(&self.failed_buffer))?;
        writeln!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_disjoint_with_edge_symmetry() {
        // Create two edges with swapped subject and object
        let x = 1;
        let y = 2;
        let edge1 = Edge {
            domain_term_id: x,
            edge_type: ElementType::Owl(OwlType::Edge(OwlEdge::DisjointWith)),
            range_term_id: y,
            property_term_id: None,
        };

        let edge2 = Edge {
            domain_term_id: y,
            edge_type: ElementType::Owl(OwlType::Edge(OwlEdge::DisjointWith)),
            range_term_id: x,
            property_term_id: None,
        };

        // Test that they are equal
        assert_eq!(
            edge1, edge2,
            "DisjointWith edges should be equal regardless of subject/object order"
        );

        // Test that they hash to the same value by inserting into a HashSet
        let mut edge_set = HashSet::new();
        edge_set.insert(edge1.clone());
        edge_set.insert(edge2.clone());

        assert_eq!(
            edge_set.len(),
            1,
            "HashSet should only contain one edge when both are DisjointWith with swapped subject/object"
        );
    }

    #[test]
    fn test_non_symmetric_edge_distinction() {
        // Create two edges with swapped subject and object for a non-symmetric relation
        let x = 1;
        let y = 2;
        let prop1 = 3;
        let edge1 = Edge {
            domain_term_id: x,
            edge_type: ElementType::Owl(OwlType::Edge(OwlEdge::ObjectProperty)),
            range_term_id: y,
            property_term_id: Some(prop1),
        };

        let edge2 = Edge {
            domain_term_id: y,
            edge_type: ElementType::Owl(OwlType::Edge(OwlEdge::ObjectProperty)),
            range_term_id: x,
            property_term_id: Some(prop1),
        };

        // Test that they are NOT equal
        assert_ne!(
            edge1, edge2,
            "Non-symmetric edges should NOT be equal when subject/object are swapped"
        );

        // Test that they both appear in the HashSet
        let mut edge_set = HashSet::new();
        edge_set.insert(edge1.clone());
        edge_set.insert(edge2.clone());

        assert_eq!(
            edge_set.len(),
            2,
            "HashSet should contain both edges when they are non-symmetric"
        );
    }
}

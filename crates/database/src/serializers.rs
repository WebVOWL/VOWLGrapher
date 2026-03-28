use std::{
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
    rc::Rc,
};

use grapher::prelude::{Characteristic, ElementType, GraphDisplayData, OwlEdge, OwlType};
use log::error;
use vowlr_util::prelude::ErrorRecord;

use crate::serializers::{
    index::TermIndex,
    util::{PROPERTY_EDGE_TYPES, SYMMETRIC_EDGE_TYPES},
};

pub mod frontend;
pub mod index;
pub mod util;

#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub struct Triple {
    /// The subject
    id: usize,
    /// The predicate
    element_type: usize,
    /// The object
    target: Option<usize>,
}

impl Display for Triple {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Triple{{ ")?;
        write!(f, "{} - ", self.id)?;
        write!(f, "{} - ", self.element_type)?;
        write!(
            f,
            "{}",
            self.target
                .as_ref()
                .map(|t| t.to_string())
                .unwrap_or_default(),
        )?;
        write!(f, "}}")
    }
}

impl Triple {
    pub fn new(id: usize, element_type: usize, target: Option<usize>) -> Self {
        Self {
            id,
            element_type,
            target,
        }
    }
}

#[derive(Debug, Clone, Eq)]
pub struct Edge {
    /// The subject term
    subject: usize,
    /// The element type
    element_type: ElementType,
    /// The object term
    object: usize,
    /// The property term
    property: Option<usize>,
}

impl PartialEq for Edge {
    fn eq(&self, other: &Self) -> bool {
        // Element type and property must always match
        if self.element_type != other.element_type || self.property != other.property {
            return false;
        }

        // For symmetric relations, treat (A, B) and (B, A) as equal
        if SYMMETRIC_EDGE_TYPES.contains(&self.element_type) {
            (self.subject == other.subject && self.object == other.object)
                || (self.subject == other.object && self.object == other.subject)
        } else {
            self.subject == other.subject && self.object == other.object
        }
    }
}

impl Hash for Edge {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if SYMMETRIC_EDGE_TYPES.contains(&self.element_type) {
            // For symmetric relations, hash the sorted pair
            let (first, second) = if self.subject.to_string() <= self.object.to_string() {
                (&self.subject, &self.object)
            } else {
                (&self.object, &self.subject)
            };

            first.hash(state);
            second.hash(state);
            self.element_type.hash(state);
        } else if PROPERTY_EDGE_TYPES.contains(&self.element_type) {
            self.subject.hash(state);
            self.element_type.hash(state);
            self.object.hash(state);
            self.property.hash(state);
        } else {
            self.subject.hash(state);
            self.element_type.hash(state);
            self.object.hash(state);
        }
    }
}

impl Display for Edge {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Edge{{ {} - {:?} - {} }}",
            self.subject, self.element_type, self.object
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
    pub on_property: Option<Term>,
    pub filler: Option<Term>,
    pub cardinality: Option<(String, Option<String>)>,
    pub self_restriction: bool,
    pub requires_filler: bool,
    pub render_mode: RestrictionRenderMode,
}

pub struct SerializationDataBuffer {
    /// Maps terms to integer ids and vice-versa.
    ///
    /// Reduces memory usage and allocations.
    term_index: TermIndex,
    /// Stores all resolved node elements.
    ///
    /// These elements may mutate during serialization
    /// if new information regarding them is found.
    /// This also means an element can be completely removed!
    ///
    /// - Key = The subject IRI of a triple.
    /// - Value = The ElementType of `Key`.
    node_element_buffer: HashMap<usize, ElementType>,
    /// Stores all resolved edge elements.
    ///
    /// These elements may mutate during serialization
    /// if new information regarding them is found.
    /// This also means an element can be completely removed!
    ///
    /// - Key = The subject IRI of a triple.
    /// - Value = The ElementType of `Key`.
    edge_element_buffer: HashMap<usize, ElementType>,
    /// Keeps track of edges that should point to a node different
    /// from their definition.
    ///
    /// Key
    /// ---
    /// The object IRI of an edge triple.
    ///
    /// The object is also called:
    /// - the target of an edge.
    /// - the range of an edge.
    ///
    /// Value
    /// -----
    /// The subject IRI of an edge triple.
    ///
    /// The subject is also called:
    /// - the source of an edge.
    /// - the domain of an edge.
    ///
    /// Example
    /// -------
    /// Consider the triples:
    /// ```sparql
    ///     ex:Mother owl:equivalentClass ex:blanknode1
    ///
    ///     ex:blanknode1 rdf:type owl:Class
    ///     ex:blanknode1 owl:intersectionOf ex:blanknode2
    /// ```
    /// Here `ex:Mother` is equivalent to `ex:blanknode1`,
    /// which means all edges referencing `ex:blanknode1` should
    /// be redirected to `ex:Mother`.
    ///
    /// Thus, the edges are redirected to:
    /// ```sparql
    ///     ex:Mother owl:intersectionOf ex:blanknode2
    /// ```
    /// In this case, `blanknode1` is effectively omitted from serialization.
    edge_redirection: HashMap<usize, usize>,
    /// Maps from element IRI to a set of the edges that include it.
    ///
    /// Used to remap when nodes are merges.
    edges_include_map: HashMap<usize, HashSet<Rc<Edge>>>,
    /// Canonical synthesized Thing node per resolved domain.
    ///
    /// This lets structurally-defined ranges like complement/union expressions
    /// collapse to the same Thing node that direct owl:Thing ranges use.
    anchor_thing_map: HashMap<usize, usize>,
    /// Partially assembled restriction metadata keyed by the restriction node.
    restriction_buffer: HashMap<Term, RestrictionState>,
    /// Final display cardinalities keyed by the concrete edge that will be emitted.
    edge_cardinality_buffer: HashMap<Edge, (String, Option<String>)>,
    /// Stores the edges of a property.
    ///
    /// - Key = The property IRI.
    /// - Value = The edges of the property.
    property_edge_map: HashMap<usize, Rc<Edge>>,
    /// Stores the domains of a property.
    ///
    /// - Key = The property IRI.
    /// - Value = The domains of the property.
    property_domain_map: HashMap<usize, HashSet<usize>>,
    /// Stores the ranges of a property.
    ///
    /// - Key = The property IRI.
    /// - Value = The ranges of the property.
    property_range_map: HashMap<usize, HashSet<usize>>,
    /// Stores labels of subject/object.
    ///
    /// - Key = The IRI the label belongs to.
    /// - Value = The label.
    label_buffer: HashMap<usize, String>,
    /// Stores labels of edges.
    ///
    /// - Key = The edge.
    /// - Value = The label.
    edge_label_buffer: HashMap<Rc<Edge>, String>,
    /// Edges in graph, to avoid duplicates
    edge_buffer: HashSet<Rc<Edge>>,
    /// Maps from edge to its characteristic.
    edge_characteristics: HashMap<Rc<Edge>, HashSet<Characteristic>>,
    /// Maps from node iri to its characteristics.
    node_characteristics: HashMap<usize, HashSet<Characteristic>>,
    /// Maps from node iri to number of individuals
    individual_count_buffer: HashMap<Term, u32>,
    /// Stores unresolved triples.
    ///
    /// - Key = The unresolved IRI of the triple
    ///   can be either the subject, object or both (in this case, subject is used)
    /// - Value = The unresolved triples.
    unknown_buffer: HashMap<usize, HashSet<Rc<Triple>>>,
    /// Stores errors encountered during serialization.
    failed_buffer: Vec<ErrorRecord>,
    /// The base IRI of the document.
    ///
    /// For instance: `http://purl.obolibrary.org/obo/envo.owl`
    document_base: Option<String>,
}
impl SerializationDataBuffer {
    pub fn new() -> Self {
        Self {
            term_index: TermIndex::new(),
            node_element_buffer: HashMap::new(),
            edge_element_buffer: HashMap::new(),
            edge_redirection: HashMap::new(),
            edges_include_map: HashMap::new(),
            anchor_thing_map: HashMap::new(),
            restriction_buffer: HashMap::new(),
            edge_cardinality_buffer: HashMap::new(),
            label_buffer: HashMap::new(),
            edge_label_buffer: HashMap::new(),
            edge_buffer: HashSet::new(),
            property_edge_map: HashMap::new(),
            property_domain_map: HashMap::new(),
            property_range_map: HashMap::new(),
            unknown_buffer: HashMap::new(),
            failed_buffer: Vec::new(),
            document_base: None,
            edge_characteristics: HashMap::new(),
            node_characteristics: HashMap::new(),
            individual_count_buffer: HashMap::new(),
        }
    }
}
impl SerializationDataBuffer {
    pub fn add_property_edge(&mut self, property_iri: usize, edge: Rc<Edge>) {
        self.property_edge_map.insert(property_iri, edge);
    }
    pub fn add_property_domain(&mut self, property_iri: usize, domain: usize) {
        self.property_domain_map
            .entry(property_iri)
            .or_default()
            .insert(domain);
    }
    pub fn add_property_range(&mut self, property_iri: usize, range: usize) {
        self.property_range_map
            .entry(property_iri)
            .or_default()
            .insert(range);
    }
    pub fn restriction_mut(&mut self, restriction: &Term) -> &mut RestrictionState {
        self.restriction_buffer
            .entry(restriction.clone())
            .or_default()
    }
}

impl Default for SerializationDataBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl From<SerializationDataBuffer> for GraphDisplayData {
    fn from(mut val: SerializationDataBuffer) -> Self {
        let mut display_data = GraphDisplayData::new();

        // Maps an RDF term's corresponding ID to a [`GraphDisplayData`] index.
        let mut iricache: HashMap<usize, usize> = HashMap::new();

        // Maps an RDF term's corresponding ID to a [`GraphDisplayData`] index.
        let mut inverse_edge_indices: HashMap<usize, usize> = HashMap::new();

        for (term_id, element) in val.node_element_buffer.into_iter() {
            let label = val.label_buffer.remove(&term_id);
            if label.is_none() {
                error!("Label not found for iri: {}, using None", term_id);
            }
            display_data.labels.push(label);
            display_data.elements.push(element);
            iricache.insert(term_id, display_data.elements.len() - 1);
        }

        for edge in val.edge_buffer.iter() {
            let subject_idx = iricache.get(&edge.subject);
            let object_idx = iricache.get(&edge.object);
            let maybe_label = val.edge_label_buffer.remove(edge);
            let characteristics = val.edge_characteristics.remove(edge);
            let cardinality = val.edge_cardinality_buffer.remove(edge);

            match (subject_idx, object_idx) {
                (Some(subject_idx), Some(object_idx)) => {
                    let edge_idx = if edge.element_type
                        == ElementType::Owl(OwlType::Edge(OwlEdge::InverseOf))
                    {
                        let Some(property_iri) = edge.property.clone() else {
                            error!("InverseOf edge is missing merged property id");
                            continue;
                        };

                        match inverse_edge_indices.get(&property_iri) {
                            Some(existing_idx) => *existing_idx,
                            None => {
                                display_data.elements.push(edge.element_type);
                                display_data.labels.push(maybe_label.clone());
                                let new_idx = display_data.elements.len() - 1;
                                inverse_edge_indices.insert(property_iri, new_idx);
                                new_idx
                            }
                        }
                    } else {
                        display_data.elements.push(edge.element_type);
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
                            .expect("edge index overflow");
                        display_data
                            .cardinalities
                            .push((display_edge_idx, cardinality));
                    }
                }
                (None, _) => {
                    error!("Subject in edge not found in iricache: {}", edge.subject);
                }
                (_, None) => {
                    error!("Object in edge not found in iricache: {}", edge.object);
                }
            }
        }

        for (iri, characteristics) in val.node_characteristics.into_iter() {
            let idx = iricache.get(&iri);
            match idx {
                Some(idx) => {
                    display_data.characteristics.insert(*idx, characteristics);
                }
                None => {
                    error!("Characteristic not found for node in iricache: {}", iri);
                }
            }
        }

        for (iri, count) in val.individual_count_buffer.into_iter() {
            match iricache.get(&iri) {
                Some(idx) => {
                    display_data.individual_counts.insert(*idx, count);
                }
                None => {
                    error!("Individual count not found for node in iricache: {}", iri);
                }
            }
        }

        display_data
    }
}

impl Display for SerializationDataBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "SerializationDataBuffer {{")?;
        writeln!(
            f,
            "\tdocument_base: {}",
            self.document_base.as_ref().unwrap_or(&"".to_string())
        )?;
        writeln!(f, "\tnode_element_buffer:")?;
        for (iri, element) in self.node_element_buffer.iter() {
            writeln!(f, "\t\t{} : {}", iri, element)?;
        }
        writeln!(f, "\tedge_element_buffer (not used by into()):")?;
        for (iri, element) in self.edge_element_buffer.iter() {
            writeln!(f, "\t\t{} : {}", iri, element)?;
        }
        writeln!(f, "\tedge_redirection:")?;
        for (iri, subject) in self.edge_redirection.iter() {
            writeln!(f, "\t\t{} -> {}", iri, subject)?;
        }
        writeln!(f, "\tedges_include_map: ")?;
        for (iri, edges) in self.edges_include_map.iter() {
            writeln!(f, "\t\t{} : {{", iri)?;
            for edge in edges.iter() {
                writeln!(f, "\t\t\t{}", edge)?;
            }
            writeln!(f, "\t\t}}")?;
        }
        writeln!(f, "\tlabel_buffer:")?;
        for (iri, label) in self.label_buffer.iter() {
            writeln!(f, "\t\t{} : {}", iri, label)?;
        }
        writeln!(f, "\tedge_buffer:")?;
        for edge in self.edge_buffer.iter() {
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
        for (iri, triples) in self.unknown_buffer.iter() {
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
    use oxrdf::{BlankNode, NamedNode};
    use std::collections::HashSet;

    #[test]
    fn test_disjoint_with_edge_symmetry() {
        // Create two edges with swapped subject and object
        let x = Term::BlankNode(BlankNode::new("_:x").unwrap());
        let y = Term::BlankNode(BlankNode::new("_:y").unwrap());
        let edge1 = Edge {
            subject: x.clone(),
            element_type: ElementType::Owl(OwlType::Edge(OwlEdge::DisjointWith)),
            object: y.clone(),
            property: None,
        };

        let edge2 = Edge {
            subject: y.clone(),
            element_type: ElementType::Owl(OwlType::Edge(OwlEdge::DisjointWith)),
            object: x.clone(),
            property: None,
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
        let x = Term::BlankNode(BlankNode::new("_:x").unwrap());
        let y = Term::BlankNode(BlankNode::new("_:y").unwrap());
        let prop1 = Term::NamedNode(NamedNode::new("http://example.com/prop1").unwrap());
        let edge1 = Edge {
            subject: x.clone(),
            element_type: ElementType::Owl(OwlType::Edge(OwlEdge::ObjectProperty)),
            object: y.clone(),
            property: Some(prop1.clone()),
        };

        let edge2 = Edge {
            subject: y.clone(),
            element_type: ElementType::Owl(OwlType::Edge(OwlEdge::ObjectProperty)),
            object: x.clone(),
            property: Some(prop1.clone()),
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

pub mod buffers;
pub mod edges;
pub mod entity_creation;
pub mod labels;
pub mod metadata;
pub mod nodes;
pub mod serialize_triple;
pub mod synthetic;

use grapher::prelude::{
    ElementType, OwlEdge, OwlNode, OwlType, RdfEdge, RdfType, RdfsEdge, RdfsNode, RdfsType,
};
use log::warn;
use oxrdf::TermRef;
use vowlgrapher_util::prelude::ErrorRecord;

use crate::{
    datastructures::{
        ArcEdge, ArcTerm, index::TermIndex, serialization_data_buffer::SerializationDataBuffer,
    },
    errors::{SerializationError, SerializationErrorKind},
    serializer_util::synthetic::{
        SYNTH_LITERAL, SYNTH_LITERAL_VALUE, SYNTH_LOCAL_LITERAL, SYNTH_LOCAL_THING, SYNTH_THING,
    },
    vocab::{owl, rdf, rdfs, xsd},
};

pub const SYMMETRIC_EDGE_TYPES: [ElementType; 1] =
    [ElementType::Owl(OwlType::Edge(OwlEdge::DisjointWith))];

pub const PROPERTY_EDGE_TYPES: [ElementType; 6] = [
    ElementType::Owl(OwlType::Edge(OwlEdge::ObjectProperty)),
    ElementType::Owl(OwlType::Edge(OwlEdge::DatatypeProperty)),
    ElementType::Owl(OwlType::Edge(OwlEdge::DeprecatedProperty)),
    ElementType::Owl(OwlType::Edge(OwlEdge::ExternalProperty)),
    ElementType::Owl(OwlType::Edge(OwlEdge::ValuesFrom)),
    ElementType::Rdf(RdfType::Edge(RdfEdge::RdfProperty)),
];

/// Returns true if the term has a synthetic suffix.
///
/// Must contain all consts of [`synthetic`].
pub fn is_synthetic(term: &ArcTerm) -> bool {
    let synths = [
        SYNTH_LITERAL,
        SYNTH_LOCAL_LITERAL,
        SYNTH_LOCAL_THING,
        SYNTH_THING,
        SYNTH_LITERAL_VALUE,
    ];
    let str_term = trim_tag_circumfix(&term.to_string());
    for synth in synths {
        if str_term.ends_with(synth) {
            return true;
        }
    }
    false
}

/// Returns true if the term belongs to the class of ontologies.
pub fn is_ontology(term: &ArcTerm) -> bool {
    match term.as_ref().as_ref() {
        TermRef::NamedNode(named_node_ref) => {
            matches!(named_node_ref, owl::ONTOLOGY)
        }
        _ => false,
    }
}

/// Reserved IRIs should not be overridden by e.g. "external class" [`ElementType`].
pub fn is_reserved(term: &ArcTerm) -> bool {
    match term.as_ref().as_ref() {
        TermRef::NamedNode(named_node_ref) => {
            matches!(
                named_node_ref,
                rdf::XML_LITERAL
                    | rdf::HTML
                    | rdf::PLAIN_LITERAL
                    | rdfs::DOMAIN
                    | rdfs::LITERAL
                    | rdfs::RANGE
                    | rdfs::RESOURCE
                    | rdfs::SUB_CLASS_OF
                    | rdfs::SUB_PROPERTY_OF
                    | owl::ALL_DISJOINT_CLASSES
                    | owl::ALL_DISJOINT_PROPERTIES
                    | owl::COMPLEMENT_OF
                    | owl::DATATYPE_COMPLEMENT_OF
                    | owl::DEPRECATED
                    | owl::DEPRECATED_CLASS
                    | owl::DEPRECATED_PROPERTY
                    | owl::DIFFERENT_FROM
                    | owl::DISJOINT_UNION_OF
                    | owl::DISJOINT_WITH
                    | owl::EQUIVALENT_CLASS
                    | owl::EQUIVALENT_PROPERTY
                    | owl::INTERSECTION_OF
                    | owl::THING
                    | owl::UNION_OF
                    | owl::REAL
                    | owl::RATIONAL
                    | xsd::ANY_URI
                    | xsd::BASE_64_BINARY
                    | xsd::BOOLEAN
                    | xsd::BYTE
                    | xsd::DATE
                    | xsd::DATE_TIME
                    | xsd::DATE_TIME_STAMP
                    | xsd::DAY_TIME_DURATION
                    | xsd::DECIMAL
                    | xsd::DOUBLE
                    | xsd::DURATION
                    | xsd::FLOAT
                    | xsd::G_DAY
                    | xsd::G_MONTH
                    | xsd::G_MONTH_DAY
                    | xsd::G_YEAR
                    | xsd::G_YEAR_MONTH
                    | xsd::HEX_BINARY
                    | xsd::INT
                    | xsd::INTEGER
                    | xsd::LANGUAGE
                    | xsd::LONG
                    | xsd::NAME
                    | xsd::NC_NAME
                    | xsd::NEGATIVE_INTEGER
                    | xsd::NMTOKEN
                    | xsd::NON_NEGATIVE_INTEGER
                    | xsd::NON_POSITIVE_INTEGER
                    | xsd::NORMALIZED_STRING
                    | xsd::POSITIVE_INTEGER
                    | xsd::SHORT
                    | xsd::STRING
                    | xsd::TIME
                    | xsd::TOKEN
                    | xsd::UNSIGNED_BYTE
                    | xsd::UNSIGNED_INT
                    | xsd::UNSIGNED_LONG
                    | xsd::UNSIGNED_SHORT
                    | xsd::YEAR_MONTH_DURATION
            )
        }
        _ => false,
    }
}

/// Returns Some(ElementType) if the `term` is a resolvable, reserved IRI.
///
/// ## Implementation details
/// This function must contain exactly the same `NamedNodeRef`s as [`is_reserved`].
#[expect(
    clippy::match_same_arms,
    reason = "by keeping them it makes changing it easier in the future"
)]
pub fn try_resolve_reserved(term: &ArcTerm) -> Option<ElementType> {
    match term.as_ref().as_ref() {
        TermRef::NamedNode(named_node_ref) => match named_node_ref {
            owl::THING => Some(ElementType::Owl(OwlType::Node(OwlNode::Thing))),
            rdfs::DOMAIN
            | rdfs::LITERAL
            | rdfs::RANGE
            | rdfs::RESOURCE
            | rdfs::SUB_CLASS_OF
            | rdfs::SUB_PROPERTY_OF
            | owl::ALL_DISJOINT_CLASSES
            | owl::ALL_DISJOINT_PROPERTIES
            | owl::COMPLEMENT_OF
            | owl::DATATYPE_COMPLEMENT_OF
            | owl::DEPRECATED
            | owl::DEPRECATED_CLASS
            | owl::DEPRECATED_PROPERTY
            | owl::DIFFERENT_FROM
            | owl::DISJOINT_UNION_OF
            | owl::DISJOINT_WITH
            | owl::EQUIVALENT_CLASS
            | owl::EQUIVALENT_PROPERTY
            | owl::INTERSECTION_OF
            | owl::UNION_OF => None,
            rdf::XML_LITERAL
            | rdf::HTML
            | rdf::PLAIN_LITERAL
            | owl::REAL
            | owl::RATIONAL
            | xsd::ANY_URI
            | xsd::BASE_64_BINARY
            | xsd::BOOLEAN
            | xsd::BYTE
            | xsd::DATE
            | xsd::DATE_TIME
            | xsd::DATE_TIME_STAMP
            | xsd::DAY_TIME_DURATION
            | xsd::DECIMAL
            | xsd::DOUBLE
            | xsd::DURATION
            | xsd::FLOAT
            | xsd::G_DAY
            | xsd::G_MONTH
            | xsd::G_MONTH_DAY
            | xsd::G_YEAR
            | xsd::G_YEAR_MONTH
            | xsd::HEX_BINARY
            | xsd::INT
            | xsd::INTEGER
            | xsd::LANGUAGE
            | xsd::LONG
            | xsd::NAME
            | xsd::NC_NAME
            | xsd::NEGATIVE_INTEGER
            | xsd::NMTOKEN
            | xsd::NON_NEGATIVE_INTEGER
            | xsd::NON_POSITIVE_INTEGER
            | xsd::NORMALIZED_STRING
            | xsd::POSITIVE_INTEGER
            | xsd::SHORT
            | xsd::STRING
            | xsd::TIME
            | xsd::TOKEN
            | xsd::UNSIGNED_BYTE
            | xsd::UNSIGNED_INT
            | xsd::UNSIGNED_LONG
            | xsd::UNSIGNED_SHORT
            | xsd::YEAR_MONTH_DURATION => {
                Some(ElementType::Rdfs(RdfsType::Node(RdfsNode::Datatype)))
            }
            _ => None,
        },
        _ => None,
    }
}

/// Removes prefix "<" and suffix ">" from the input to
/// comply with <https://www.ietf.org/rfc/rfc3987.html> (p. 12)
pub fn trim_tag_circumfix(input: &str) -> String {
    input
        .trim_start_matches('<')
        .trim_end_matches('>')
        .to_string()
}

/// Generate a new IRI based on a current one.
pub fn synthetic_iri(base: &ArcTerm, suffix: &str) -> String {
    let clean = trim_tag_circumfix(&base.to_string());
    format!("{clean}{suffix}")
}

pub const fn is_structural_set_node(element: ElementType) -> bool {
    matches!(
        element,
        ElementType::Owl(OwlType::Node(
            OwlNode::Complement
                | OwlNode::IntersectionOf
                | OwlNode::UnionOf
                | OwlNode::DisjointUnion
        ))
    )
}

pub fn can_upgrade_node_type(old: ElementType, new: ElementType) -> bool {
    if matches!(
        old,
        ElementType::Owl(OwlType::Node(OwlNode::Class | OwlNode::AnonymousClass))
    ) {
        return true;
    }

    old == ElementType::Owl(OwlType::Node(OwlNode::EquivalentClass)) && is_structural_set_node(new)
}

pub fn merge_optional_labels(left: Option<&String>, right: Option<&String>) -> Option<String> {
    match (left, right) {
        (Some(left), Some(right)) if left == right => Some(left.clone()),
        (Some(left), Some(right)) => Some(format!("{left}\n{right}")),
        (Some(label), None) | (None, Some(label)) => Some(label.clone()),
        (None, None) => None,
    }
}

pub fn is_query_fallback_endpoint(term: &ArcTerm) -> bool {
    term.as_ref().as_ref() == owl::THING.into() || term.as_ref().as_ref() == rdfs::LITERAL.into()
}

pub fn is_restriction_owner_edge(edge: &ArcEdge) -> bool {
    edge.edge_type == ElementType::Rdfs(RdfsType::Edge(RdfsEdge::SubclassOf))
        || edge.edge_type == ElementType::NoDraw
}

pub fn iri_matches_document_base(base: &str, iri: &str) -> bool {
    iri == base
        || (!base.ends_with('/') && !base.ends_with('#') && iri.starts_with(&format!("{base}#")))
        || ((base.ends_with('/') || base.ends_with('#')) && iri.starts_with(base))
}

pub fn is_external(
    data_buffer: &SerializationDataBuffer,
    term: &ArcTerm,
) -> Result<bool, SerializationError> {
    if term.is_blank_node() {
        return Ok(false);
    }

    let clean_term = trim_tag_circumfix(&term.to_string());
    match &*data_buffer.document_base.read()? {
        Some(base) => Ok(!(iri_matches_document_base(base.as_ref(), &clean_term)
            || is_reserved(term)
            || is_synthetic(term))),
        None => {
            let has_fired = false; // Pending refactor
            if !has_fired {
                let msg = "Cannot determine externals: Missing document base!";
                let e = SerializationErrorKind::MissingDocumentBase(msg.to_string());
                warn!("{msg}");
                data_buffer
                    .failed_buffer
                    .write()?
                    .push(<SerializationError as Into<ErrorRecord>>::into(e.into()));
            }
            Ok(false)
        }
    }
}

pub fn is_synthetic_property_fallback(
    term_index: &TermIndex,
    edge: &ArcEdge,
) -> Result<bool, SerializationError> {
    let is_property_edge = matches!(
        edge.edge_type,
        ElementType::Owl(OwlType::Edge(
            OwlEdge::ObjectProperty
                | OwlEdge::DatatypeProperty
                | OwlEdge::DeprecatedProperty
                | OwlEdge::ExternalProperty
        )) | ElementType::Rdf(RdfType::Edge(RdfEdge::RdfProperty))
    );

    if !is_property_edge {
        return Ok(false);
    }

    let subject_term = term_index.get(&edge.domain_term_id)?;
    let object_term = term_index.get(&edge.range_term_id)?;
    Ok(is_synthetic(&subject_term) && is_synthetic(&object_term))
}

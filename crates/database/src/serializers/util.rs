use std::collections::HashSet;

use crate::vocab::owl;
use grapher::prelude::{ElementType, OwlEdge, OwlType, RdfEdge, RdfType};
use rdf_fusion::model::vocab::{rdf, rdfs};

pub const SYMMETRIC_EDGE_TYPES: [ElementType; 1] =
    [ElementType::Owl(OwlType::Edge(OwlEdge::DisjointWith))];

pub const PROPERTY_EDGE_TYPES: [ElementType; 7] = [
    ElementType::Owl(OwlType::Edge(OwlEdge::ObjectProperty)),
    ElementType::Owl(OwlType::Edge(OwlEdge::DatatypeProperty)),
    ElementType::Owl(OwlType::Edge(OwlEdge::DeprecatedProperty)),
    ElementType::Owl(OwlType::Edge(OwlEdge::ExternalProperty)),
    ElementType::Owl(OwlType::Edge(OwlEdge::ValuesFrom)),
    ElementType::Owl(OwlType::Edge(OwlEdge::InverseOf)),
    ElementType::Rdf(RdfType::Edge(RdfEdge::RdfProperty)),
];

/// Reserved IRIs should not be overridden by e.g. "external class" ElementType.
pub fn get_reserved_iris() -> HashSet<String> {
    let rdf = vec![rdf::XML_LITERAL];
    let rdfs = vec![
        rdfs::DOMAIN,
        rdfs::LITERAL,
        rdfs::RANGE,
        rdfs::RESOURCE,
        rdfs::SUB_CLASS_OF,
        rdfs::SUB_PROPERTY_OF,
    ];
    let owl = vec![
        owl::ALL_DISJOINT_CLASSES,
        owl::ALL_DISJOINT_PROPERTIES,
        owl::COMPLEMENT_OF,
        owl::DATATYPE_COMPLEMENT_OF,
        owl::DEPRECATED,
        owl::DEPRECATED_CLASS,
        owl::DEPRECATED_PROPERTY,
        owl::DIFFERENT_FROM,
        owl::DISJOINT_UNION_OF,
        owl::DISJOINT_WITH,
        owl::EQUIVALENT_CLASS,
        owl::EQUIVALENT_PROPERTY,
        owl::INTERSECTION_OF,
        owl::THING,
        owl::UNION_OF,
    ];

    let iris = [rdf, rdfs, owl]
        .iter()
        .flatten()
        .map(|elem| trim_tag_circumfix(&elem.to_string()))
        .collect::<Vec<String>>();

    HashSet::from_iter(iris)
}

/// Removes prefix "<" and suffix ">" from the input to
/// comply with https://www.ietf.org/rfc/rfc3987.html (p. 12)
pub fn trim_tag_circumfix(input: &str) -> String {
    input
        .trim_start_matches('<')
        .trim_end_matches('>')
        .to_string()
}

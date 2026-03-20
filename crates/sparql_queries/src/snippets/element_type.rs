use grapher::prelude::{ElementType, GenericType, OwlType, RdfType, RdfsType, XSDType};

use crate::snippets::SparqlSnippet;

impl SparqlSnippet for ElementType {
    fn snippet(self) -> &'static str {
        match self {
            ElementType::NoDraw => "",
            ElementType::Rdf(RdfType::Node(node)) => node.snippet(),
            ElementType::Rdf(RdfType::Edge(edge)) => edge.snippet(),
            ElementType::Rdfs(RdfsType::Node(node)) => node.snippet(),
            ElementType::Rdfs(RdfsType::Edge(edge)) => edge.snippet(),
            ElementType::Owl(OwlType::Node(node)) => node.snippet(),
            ElementType::Owl(OwlType::Edge(edge)) => edge.snippet(),
            ElementType::Generic(GenericType::Node(node)) => node.snippet(),
            ElementType::Generic(GenericType::Edge(edge)) => edge.snippet(),
            ElementType::Xsd(XSDType::Node(node)) => node.snippet(),
        }
    }
}

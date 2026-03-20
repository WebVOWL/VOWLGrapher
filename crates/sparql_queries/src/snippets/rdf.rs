use grapher::prelude::{RdfEdge, RdfNode};

use crate::snippets::SparqlSnippet;

impl SparqlSnippet for RdfEdge {
    fn snippet(self) -> &'static str {
        match self {
            RdfEdge::RdfProperty => {
                r#"{
                ?id rdf:Property ?target
                BIND(rdf:Property AS ?nodeType)
                }"#
            }
        }
    }
}

impl SparqlSnippet for RdfNode {
    fn snippet(self) -> &'static str {
        match self {
            Self::HTML => {
                r#"{
                ?id a rdf:HTML .
                BIND(rdf:HTML AS ?nodeType)
                }"#
            }
            Self::PlainLiteral => {
                r#"{
                ?id a rdf:PlainLiteral .
                BIND(rdf:PlainLiteral AS ?nodeType)
                }"#
            }
            Self::XMLLiteral => {
                r#"{
                ?id a rdf:XMLLiteral .
                BIND(rdf:XMLLiteral AS ?nodeType)
                }"#
            }
        }
    }
}

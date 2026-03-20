use grapher::prelude::{RdfsEdge, RdfsNode};

use crate::snippets::SparqlSnippet;

impl SparqlSnippet for RdfsNode {
    fn snippet(self) -> &'static str {
        match self {
            Self::Class => {
                r#"{
                ?id a rdfs:Class .
                FILTER(?id != owl:Class)
                FILTER NOT EXISTS { ?id a owl:Class }
                BIND(rdfs:Class AS ?nodeType)
                }"#
            }
            Self::Literal => {
                r#"{
                ?id a rdfs:Literal .
                BIND(rdfs:Literal AS ?nodeType)
                }"#
            }
            Self::Resource => {
                r#"{
                ?id a rdfs:Resource .
                FILTER(isIRI(?id) || isBlank(?id))
                BIND(rdfs:Resource AS ?nodeType)
                }"#
            }
            Self::Datatype => {
                r#"{
                ?id a rdfs:Datatype .
                BIND(rdfs:Datatype AS ?nodeType)
                }"#
            }
        }
    }
}

impl SparqlSnippet for RdfsEdge {
    fn snippet(self) -> &'static str {
        match self {
            Self::SubclassOf => {
                r#"{
                ?id rdfs:subClassOf ?target .
                FILTER NOT EXISTS { ?target owl:unionOf ?u }
                FILTER NOT EXISTS { ?target owl:intersectionOf ?i }
                FILTER NOT EXISTS { ?target owl:complementOf ?c }
                FILTER NOT EXISTS { ?target owl:disjointUnionOf ?d }
                BIND(rdfs:subClassOf AS ?nodeType)
                }"#
            }
        }
    }
}

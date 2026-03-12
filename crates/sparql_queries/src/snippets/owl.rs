use grapher::prelude::{OwlEdge, OwlNode};

use crate::snippets::SparqlSnippet;

impl SparqlSnippet for OwlNode {
    fn snippet(self) -> &'static str {
        match self {
            OwlNode::AnonymousClass => {
                r#"{
                ?id a owl:Class
                FILTER(!isIRI(?id))
                BIND("blanknode" AS ?nodeType)
                }"#
            }
            OwlNode::Class => {
                r#"{
                ?id a owl:Class .
                FILTER(isIRI(?id))
                BIND(owl:Class AS ?nodeType)
                }"#
            }
            OwlNode::Complement => {
                r#"{
                ?id owl:complementOf ?target .
                BIND(owl:complementOf AS ?nodeType)
                }"#
            }
            OwlNode::DeprecatedClass => {
                r#"{
                ?id a owl:DeprecatedClass .
                BIND(owl:DeprecatedClass AS ?nodeType)
                }"#
            }
            OwlNode::ExternalClass => {
                // Not handled here as externals uses identical
                // logic across classes and properties.
                ""
            }
            OwlNode::EquivalentClass => {
                r#"{
                ?id owl:equivalentClass ?target
                BIND(owl:equivalentClass AS ?nodeType)
                }"#
            }
            OwlNode::DisjointUnion => {
                r#"{
                ?id owl:disjointUnionOf ?target .
                BIND(owl:disjointUnionOf AS ?nodeType)
                }"#
            }
            OwlNode::IntersectionOf => {
                r#"{
                ?id owl:intersectionOf ?target .
                BIND(owl:intersectionOf AS ?nodeType)
                }"#
            }
            OwlNode::Thing => {
                r#"{
                ?id a owl:Thing .
                BIND(owl:Thing AS ?nodeType)
                }"#
            }
            OwlNode::UnionOf => {
                r#"{
                ?id owl:unionOf ?list .
                ?list rdf:rest*/rdf:first ?target .
                FILTER(?target != rdf:nil)
                BIND(owl:unionOf AS ?nodeType)
                }"#
            }
        }
    }
}

impl SparqlSnippet for OwlEdge {
    fn snippet(self) -> &'static str {
        match self {
            OwlEdge::DatatypeProperty => {
                r#"{
                ?id a owl:DatatypeProperty .
                BIND(owl:DatatypeProperty AS ?nodeType)
                }"#
            }
            OwlEdge::DisjointWith => {
                r#"{
                ?id owl:disjointWith ?target
                BIND(owl:disjointWith AS ?nodeType)
                }"#
            }
            OwlEdge::DeprecatedProperty => {
                r#"{
                ?id a owl:DeprecatedProperty .
                BIND(owl:DeprecatedProperty AS ?nodeType)
                }"#
            }
            OwlEdge::ExternalProperty => {
                // Not handled here as externals uses identical
                // logic across classes and properties.
                ""
            }
            OwlEdge::InverseOf => {
                r#"{
                ?id owl:inverseOf ?target .
                BIND(owl:inverseOf AS ?nodeType)
                }"#
            }
            OwlEdge::ObjectProperty => {
                r#"{
                ?id a owl:ObjectProperty
                BIND(owl:ObjectProperty AS ?nodeType)
                }"#
            }
            OwlEdge::ValuesFrom => {
                r#"{
                {
                    ?id owl:someValuesFrom ?target .
                }
                UNION
                {
                    ?id owl:allValuesFrom ?target .
                }
                BIND("ValuesFrom" AS ?nodeType)
                }"#
            }
        }
    }
}

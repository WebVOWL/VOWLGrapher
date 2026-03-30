use grapher::prelude::{OwlEdge, OwlNode};

use crate::snippets::SparqlSnippet;

impl SparqlSnippet for OwlNode {
    fn snippet(self) -> &'static str {
        match self {
            Self::AnonymousClass => {
                r#"{
                # AnonymousClass
                {
                    ?id a owl:Class .
                    FILTER(!isIRI(?id))
                }
                UNION
                {
                    ?id a owl:Restriction .
                    FILTER(!isIRI(?id))
                }

                FILTER NOT EXISTS {
                    ?named rdfs:subClassOf ?id .
                    FILTER(isIRI(?named))
                    {
                        { ?id owl:unionOf ?u }
                        UNION
                        { ?id owl:intersectionOf ?i }
                        UNION
                        { ?id owl:complementOf ?c }
                        UNION
                        { ?id owl:disjointUnionOf ?d }
                    }
                }

                BIND("blanknode" AS ?nodeType)
                }"#
            }
            Self::Class => {
                r#"{
                # owl:Class
                ?id a owl:Class .
                FILTER(isIRI(?id))
                BIND(owl:Class AS ?nodeType)
                }"#
            }
            Self::Complement => {
                r#"{
                # owl:complementOf
                {
                    ?id owl:complementOf ?target .
                    FILTER NOT EXISTS {
                        ?named rdfs:subClassOf ?id .
                        FILTER(isIRI(?named))
                        FILTER(!isIRI(?id))
                    }
                }
                UNION
                {
                    ?named rdfs:subClassOf ?anon .
                    FILTER(isIRI(?named))
                    FILTER(!isIRI(?anon))
                    ?anon owl:complementOf ?target .
                    BIND(?named AS ?id)
                }
                BIND(owl:complementOf AS ?nodeType)
                }"#
            }
            Self::DeprecatedClass => {
                r#"{
                ?id a owl:DeprecatedClass .
                BIND(owl:DeprecatedClass AS ?nodeType)
                }"#
            }
            Self::ExternalClass => {
                // Not handled here as externals uses identical
                // logic across classes and properties.
                ""
            }
            Self::EquivalentClass => {
                r#"{
                ?id owl:equivalentClass ?target
                BIND(owl:equivalentClass AS ?nodeType)
                }"#
            }
            Self::DisjointUnion => {
                r#"{
                # owl:disjointUnionOf
                {
                    ?id owl:disjointUnionOf/rdf:rest*/rdf:first ?target .
                    FILTER(?target != rdf:nil)
                    FILTER NOT EXISTS {
                        ?named rdfs:subClassOf ?id .
                        FILTER(isIRI(?named))
                        FILTER(!isIRI(?id))
                    }
                }
                UNION
                {
                    ?named rdfs:subClassOf ?anon .
                    FILTER(isIRI(?named))
                    FILTER(!isIRI(?anon))
                    ?anon owl:disjointUnionOf/rdf:rest*/rdf:first ?target .
                    FILTER(?target != rdf:nil)
                    BIND(?named AS ?id)
                }
                BIND(owl:disjointUnionOf AS ?nodeType)
                }"#
            }
            Self::IntersectionOf => {
                r#"{
                # owl:intersectionOf
                {
                    ?id owl:intersectionOf/rdf:rest*/rdf:first ?target .
                    FILTER(?target != rdf:nil)
                    FILTER NOT EXISTS {
                        ?named rdfs:subClassOf ?id .
                        FILTER(isIRI(?named))
                        FILTER(!isIRI(?id))
                    }
                }
                UNION
                {
                    ?named rdfs:subClassOf ?anon .
                    FILTER(isIRI(?named))
                    FILTER(!isIRI(?anon))
                    ?anon owl:intersectionOf/rdf:rest*/rdf:first ?target .
                    FILTER(?target != rdf:nil)
                    BIND(?named AS ?id)
                }
                BIND(owl:intersectionOf AS ?nodeType)
                }"#
            }
            Self::Thing => {
                r#"{
                ?id a owl:Thing .
                BIND(owl:Thing AS ?nodeType)
                }"#
            }
            Self::UnionOf => {
                r#"{
                # owl:unionOf
                {
                    ?id owl:unionOf/rdf:rest*/rdf:first ?target .
                    FILTER(?target != rdf:nil)
                    FILTER NOT EXISTS {
                        ?named rdfs:subClassOf ?id .
                        FILTER(isIRI(?named))
                        FILTER(!isIRI(?id))
                    }
                }
                UNION
                {
                    ?named rdfs:subClassOf ?anon .
                    FILTER(isIRI(?named))
                    FILTER(!isIRI(?anon))
                    ?anon owl:unionOf/rdf:rest*/rdf:first ?target .
                    FILTER(?target != rdf:nil)
                    BIND(?named AS ?id)
                }
                BIND(owl:unionOf AS ?nodeType)
                }"#
            }
            Self::Real => {
                r#"{
                ?id a owl:real .
                BIND(owl:real AS ?nodeType)
                }"#
            }
            Self::Rational => {
                r#"{
                ?id a owl:rational .
                BIND(owl:rational AS ?nodeType)
                }"#
            }
        }
    }
}

impl SparqlSnippet for OwlEdge {
    fn snippet(self) -> &'static str {
        match self {
            Self::DatatypeProperty => {
                r#"{
                ?id a owl:DatatypeProperty .
                BIND(owl:DatatypeProperty AS ?nodeType)
                }"#
            }
            Self::DisjointWith => {
                r#"{
                ?id owl:disjointWith ?target
                BIND(owl:disjointWith AS ?nodeType)
                }"#
            }
            Self::DeprecatedProperty => {
                r#"{
                ?id a owl:DeprecatedProperty .
                BIND(owl:DeprecatedProperty AS ?nodeType)
                }"#
            }
            Self::ExternalProperty => {
                // Not handled here as externals uses identical
                // logic across classes and properties.
                ""
            }
            Self::InverseOf => {
                r#"{
                ?id owl:inverseOf ?target .
                BIND(owl:inverseOf AS ?nodeType)
                }"#
            }
            Self::ObjectProperty => {
                r#"{
                ?id a owl:ObjectProperty
                BIND(owl:ObjectProperty AS ?nodeType)
                }"#
            }
            Self::ValuesFrom => {
                r#"{
                # Cardinalities
                { ?id owl:onProperty ?target . BIND(owl:onProperty AS ?nodeType) }
                UNION
                { ?id owl:someValuesFrom ?target . BIND(owl:someValuesFrom AS ?nodeType) }
                UNION
                { ?id owl:allValuesFrom ?target . BIND(owl:allValuesFrom AS ?nodeType) }
                UNION
                { ?id owl:hasSelf ?target . BIND(owl:hasSelf AS ?nodeType) }
                UNION
                { ?id owl:hasValue ?target . BIND(owl:hasValue AS ?nodeType) }
                UNION
                { ?id owl:minCardinality ?target . BIND(owl:minCardinality AS ?nodeType) }
                UNION
                { ?id owl:maxCardinality ?target . BIND(owl:maxCardinality AS ?nodeType) }
                UNION
                { ?id owl:cardinality ?target . BIND(owl:cardinality AS ?nodeType) }
                UNION
                { ?id owl:minQualifiedCardinality ?target . BIND(owl:minQualifiedCardinality AS ?nodeType) }
                UNION
                { ?id owl:maxQualifiedCardinality ?target . BIND(owl:maxQualifiedCardinality AS ?nodeType) }
                UNION
                { ?id owl:qualifiedCardinality ?target . BIND(owl:qualifiedCardinality AS ?nodeType) }
                UNION
                { ?id owl:onClass ?target . BIND(owl:onClass AS ?nodeType) }
                UNION
                { ?id owl:onDataRange ?target . BIND(owl:onDataRange AS ?nodeType) }
                }"#
            }
        }
    }
}

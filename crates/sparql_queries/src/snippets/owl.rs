use grapher::prelude::{OwlEdge, OwlNode};

use crate::snippets::SparqlSnippet;

impl SparqlSnippet for OwlNode {
    fn snippet(self) -> &'static str {
        match self {
            Self::AnonymousClass => {
                r#"{
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
                ?id a owl:Class .
                FILTER(isIRI(?id))
                BIND(owl:Class AS ?nodeType)
                }"#
            }
            Self::Complement => {
                r#"{
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

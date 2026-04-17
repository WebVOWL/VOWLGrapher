//! Provides SPARQL query snippets for generic querying across vocabularies.

/// Flatten collections.
pub const COLLECTIONS: &str = r#"{
    {
    ?id owl:oneOf ?list .
    }
    UNION
    {
        ?owner ?connector ?id .
        FILTER(?connector IN (
            owl:equivalentClass,
            rdfs:subClassOf,
            rdfs:range
        ))
        FILTER(!isIRI(?id))
        ?id owl:oneOf ?list .
    }

    ?list rdf:rest*/rdf:first ?target .
    FILTER(?target != rdf:nil)

    BIND(owl:oneOf AS ?nodeType)
    }"#;

pub const NAMED_INDIVIDUAL_COUNTS: &str = r#"{
    ?target a ?id .
    FILTER(isIRI(?id))
    FILTER(isIRI(?target))
    FILTER(?id != owl:NamedIndividual)

    BIND(owl:NamedIndividual AS ?nodeType)
    }"#;

/// External classes.
///
/// 1. Elements whose base URI differs from that of the visualized ontology.
///    p. 6 of https://www.semantic-web-journal.net/system/files/swj1114.pdf
/// 2. A base URI is EITHER `xml:base` OR that of the document.
///    https://www.w3.org/TR/rdf-syntax-grammar/#section-Syntax-ID-xml-base
pub const XML_BASE: &str = r#"{
            # Get the base URI of the document.
            ?id xml:base ?base .
            BIND(xml:base AS ?nodeType)
            }"#;

/// External classes.
///
/// Get the base URI of the document.
pub const ONTOLOGY: &str = r#"{
            # Get the base URI of the document.
            ?id a owl:Ontology .
            BIND(owl:Ontology AS ?nodeType)
            }"#;

/// Generic, deprecated OWL elements.
pub const OWL_DEPRECATED: &str = r#"{
            # Generic, deprecated OWL elements.
            ?id owl:deprecated ?target .
            FILTER(?target = "true"^^xsd:boolean || lcase(str(?target)) = "true")
            BIND(owl:deprecated AS ?nodeType)
            }"#;

/// Find labels for elements in the following order:
/// 1. Use rdfs:label, if exists.
///    https://www.w3.org/TR/rdf-schema/#ch_label
/// 2. Use rdf:resource, if exists.
///    https://www.w3.org/TR/rdf-syntax-grammar/#section-Syntax-empty-property-elements
/// 3. Use rdf:ID, if exists.
///    https://www.w3.org/TR/rdf-syntax-grammar/#section-Syntax-ID-xml-base
pub const LABEL: &str = r#"{
            # Find labels for elements.
            OPTIONAL { ?id rdfs:label ?theLabel }
            OPTIONAL { ?id rdf:resource ?resLabel }
            OPTIONAL { ?id rdf:ID ?idLabel }
            BIND (
                COALESCE(
                    IF( BOUND(?theLabel), ?theLabel, 1/0 ),
                    IF( BOUND(?resLabel), ?resLabel, 1/0 ),
                    IF( BOUND(?idLabel), ?idLabel, 1/0 ),
                    ""
                ) AS ?label
            )
            }"#;

/// Find the domain and range of any property, and restructure so they appear as singular triples.
pub const DOMAIN_RANGES: &str = r#"{
        # Find the domain and range of any property, and restructure so they appear as singular triples
        {
            VALUES ?property {
                owl:DeprecatedProperty
                owl:DatatypeProperty
                owl:ObjectProperty
                rdf:Property
                owl:FunctionalProperty
                owl:InverseFunctionalProperty
                owl:ReflexiveProperty
                owl:IrreflexiveProperty
                owl:SymmetricProperty
                owl:AsymmetricProperty
                owl:TransitiveProperty
            }
            ?nodeType a ?property .
            ?nodeType rdfs:range ?target .
            ?nodeType rdfs:domain ?id .
        } UNION {
            VALUES ?property {
                owl:DeprecatedProperty
                owl:DatatypeProperty
                owl:ObjectProperty
                rdf:Property
                owl:FunctionalProperty
                owl:InverseFunctionalProperty
                owl:ReflexiveProperty
                owl:IrreflexiveProperty
                owl:SymmetricProperty
                owl:AsymmetricProperty
                owl:TransitiveProperty
            }
            ?nodeType a ?property .
            ?nodeType rdfs:range ?target .
            FILTER NOT EXISTS { ?nodeType rdfs:domain ?x }
            BIND(IF(?property = owl:DatatypeProperty, rdfs:Literal, owl:Thing) AS ?id)
        } UNION {
            VALUES ?property {
                owl:DeprecatedProperty
                owl:DatatypeProperty
                owl:ObjectProperty
                rdf:Property
                owl:FunctionalProperty
                owl:InverseFunctionalProperty
                owl:ReflexiveProperty
                owl:IrreflexiveProperty
                owl:SymmetricProperty
                owl:AsymmetricProperty
                owl:TransitiveProperty
            }
            ?nodeType a ?property .
            ?nodeType rdfs:domain ?id .
            FILTER NOT EXISTS { ?nodeType rdfs:range ?x }
            BIND(IF(?property = owl:DatatypeProperty, rdfs:Literal, owl:Thing) AS ?target)
        } UNION {
            VALUES ?property {
                owl:DeprecatedProperty
                owl:DatatypeProperty
                owl:ObjectProperty
                rdf:Property
                owl:FunctionalProperty
                owl:InverseFunctionalProperty
                owl:ReflexiveProperty
                owl:IrreflexiveProperty
                owl:SymmetricProperty
                owl:AsymmetricProperty
                owl:TransitiveProperty
            }
            ?nodeType a ?property .
            FILTER NOT EXISTS { ?nodeType rdfs:range ?x }
            FILTER NOT EXISTS { ?nodeType rdfs:domain ?x }
            BIND(owl:Thing AS ?id)
            BIND(IF(?property = owl:DatatypeProperty, rdfs:Literal, owl:Thing) AS ?target)
        }
        }"#;

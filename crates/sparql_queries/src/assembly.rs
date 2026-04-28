use crate::snippets::SparqlSnippet;
use crate::{prelude::GENERAL_SNIPPETS, snippets::void::VOID};
use grapher::prelude::ElementType;
use std::collections::{HashMap, HashSet};
use regex::Regex;
use once_cell::sync::Lazy;

// TODO: Remove when automatic prefix fetching is implemented.
pub const DEFAULT_PREFIXES: [&str; 8] = [
    "vowlgrapher: <http://www.example.com/iri#>",
    "owl: <http://www.w3.org/2002/07/owl#>",
    "rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
    "rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
    "xsd: <http://www.w3.org/2001/XMLSchema#>",
    "xml: <http://www.w3.org/XML/1998/namespace>",
    "dc: <http://purl.org/dc/elements/1.1/>",
    "dcterms: <http://purl.org/dc/terms/>",
];

static VAR_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"[?$]([a-zA-Z_][a-zA-Z0-9_\u00B7\u0300-\u036F\u203F-\u2040]*)").unwrap()
});

/// Compiles snippets of SPARQL code into full-fledged SPARQL queries.
pub struct QueryAssembler;

impl QueryAssembler {
    /// Construct a SPARQL query from URI prefixes and SPARQL snippets.
    ///
    /// `prefixes` is the collection of prefixes to use.
    /// An example of a prefix is: `owl: <http://www.w3.org/2002/07/owl#>`.
    ///
    /// `snippets` is the collection of SPARQL snippets to use.
    pub fn assemble_query(prefixes: &Vec<&str>, snippets: &Vec<&'static str>) -> String {
        format!(
            r"
            {}
            SELECT ?id ?nodeType ?target ?label
            WHERE {{
                GRAPH <{{GRAPH_IRI}}> {{
                    {}
                    BIND(
                        IF(?nodeType = owl:Ontology, 0,
                            IF(?nodeType = owl:Class || ?target = owl:Axiom, 1, 2)
                        )
                        AS ?weight
                    )
                }}
            }}
            ORDER BY ?weight
        ",
            prefixes
                .iter()
                .map(|item| format!("PREFIX {item}"))
                .collect::<Vec<_>>()
                .join("\n"),
            snippets
                .iter()
                .map(std::string::ToString::to_string)
                .filter(|item| !item.is_empty())
                .collect::<Vec<_>>()
                .join(" UNION "),
        )
        .as_str()
        .trim_start()
        .to_string()
    }

    /// Construct a filtered SPARQL query based on the truth value of entries in `element_filter`.
    pub fn assemble_filtered_query(element_filter: &HashMap<ElementType, bool>) -> String {
        let mut snippets = element_filter
            .iter()
            .filter(|&(_, &checked)| checked)
            .map(|(elem, _)| elem.snippet())
            .collect::<Vec<&str>>();

        if snippets.is_empty() {
            snippets.push(VOID);
        } else {
            snippets.extend(GENERAL_SNIPPETS);
        }
        Self::assemble_query(&DEFAULT_PREFIXES.into(), &snippets)
    }

    /// Construct a custom SPARQL query based of query inserted by the user in the UI(query_menu)
    pub fn assemble_custom_query(user_query: &str) -> String {

        let vars: HashSet<&str> = VAR_REGEX
            .find_iter(user_query)
            .map(|m| m.as_str())
            .collect();

        let type_finding = vars
            .iter()
            .map(|v| format!("OPTIONAL {{ {} rdf:type ?{}_type }}", v, v.replace(['?', '$'], "")))
            .collect::<Vec<_>>()
            .join("\n");

        let construct_types = vars
            .iter()
            .map(|v| format!("{} rdf:type ?{}_type .", v, v.replace(['?', '$'], "")))
            .collect::<Vec<_>>()
            .join("\n");

        let filter_var = vars
            .iter()
            .map(|v| format!("?s = {0} || ?p = {0} || ?o = {0}", v))
            .collect::<Vec<_>>()
            .join(" || ");

        let prefixes = DEFAULT_PREFIXES
            .iter()
            .map(|item| format!("PREFIX {item}"))
            .collect::<Vec<_>>()
            .join("\n");

        format!(
            r#"
            {}
            CONSTRUCT {{
                ?s ?p ?o .
                {}
            }}
            WHERE {{
                GRAPH <{{GRAPH_IRI}}> {{
                    {{  
                        ?s a owl:Ontology .
                        ?s ?p ?o .
                        BIND(owl:Ontology AS ?s_type)
                    }}
                    UNION
                    {{
                        {{ {} }}

                        {}
                        
                        ?s ?p ?o .
                        FILTER({})
                        OPTIONAL {{ ?s rdf:type ?s_type }}
                        OPTIONAL {{ FILTER(isIRI(?o)) ?o rdf:type ?o_type }}
                    }}
                }}
            }}
            "#,
            prefixes, construct_types, user_query, type_finding, filter_var
        )
    }
}

use crate::snippets::SparqlSnippet;
use crate::{prelude::GENERAL_SNIPPETS, snippets::void::VOID};
use grapher::prelude::ElementType;
use std::collections::HashMap;

// TODO: Remove when automatic prefix fetching is implemented.
pub const DEFAULT_PREFIXES: [&str; 6] = [
    "owl: <http://www.w3.org/2002/07/owl#>",
    "rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
    "rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
    "xsd: <http://www.w3.org/2001/XMLSchema#>",
    "vowlr: <http://www.example.com/iri#>",
    "xml: <http://www.w3.org/XML/1998/namespace>",
];

/// Compiles snippets of SPARQL code into full-fledged SPARQL queries.
pub struct QueryAssembler;

impl QueryAssembler {
    /// Construct a SPARQL query from URI prefixes and SPARQL snippets.
    ///
    /// `prefixes` is the collection of prefixes to use.
    /// An example of a prefix is: `owl: <http://www.w3.org/2002/07/owl#>`.
    ///
    /// `snippets` is the collection of SPARQL snippets to use.
    pub fn assemble_query(prefixes: Vec<&str>, snippets: Vec<&'static str>) -> String {
        format!(
            r#"
            {}
            SELECT DISTINCT ?id ?nodeType ?target ?label
            WHERE {{
                GRAPH <{{GRAPH_IRI}}> {{
                    {}
                    BIND(
                        IF(?nodeType = owl:Ontology, 0,
                            IF(?nodeType = owl:Class, 1, 2)
                        )
                        AS ?weight
                    )
                }}
            }}
            ORDER BY ?weight
        "#,
            prefixes
                .iter()
                .map(|item| format!("PREFIX {item}"))
                .collect::<Vec<_>>()
                .join("\n"),
            snippets
                .iter()
                .map(|item| item.to_string())
                .filter(|item| !item.is_empty())
                .collect::<Vec<_>>()
                .join(" UNION "),
        )
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
        Self::assemble_query(DEFAULT_PREFIXES.into(), snippets)
    }
}

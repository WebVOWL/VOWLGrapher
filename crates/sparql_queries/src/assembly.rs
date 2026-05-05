mod query_regex;
use crate::snippets::SparqlSnippet;
use crate::{prelude::GENERAL_SNIPPETS, snippets::void::VOID};
use grapher::prelude::ElementType;
use indexmap::IndexSet;
use log::info;
use regex::Regex;
use std::collections::HashMap;
use std::fmt::Write;

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

static VAR_REGEX: &str = r"[?$]([a-zA-Z_][a-zA-Z0-9_\u00B7\u0300-\u036F\u203F-\u2040]*)";
static QUERY_VAR_REGEX: &str = r"(?:SELECT|select) ([?$].*|\*)(?:WHERE|where)?";
// static QUERY_VAR_REGEX: &str = r"(?:SELECT|select) (?<first>[?$].*|\*)(?:WHERE|where)?";

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

    #[expect(clippy::unwrap_used, reason = "Testing. Remove on sight")]
    /// Construct a custom SPARQL query based on the query inserted by the user in the `UI(query_menu)`
    pub fn assemble_user_query(user_query: &str) -> String {
        info!("{user_query}");

        let prefixes = DEFAULT_PREFIXES
            .iter()
            .map(|item| format!("PREFIX {item}"))
            .collect::<Vec<_>>()
            .join("\n");

        let query_var_re = Regex::new(VAR_REGEX).unwrap();

        // NEVER remove elements from the IndexSet. Doing so breaks ordering!
        // The variables to use in CONSTRUCT.
        let query_variables = {
            let mut q_set = IndexSet::new();
            let re = Regex::new(QUERY_VAR_REGEX).unwrap();

            for (_, [c]) in re.captures_iter(user_query).map(|c| c.extract()) {
                query_var_re.find_iter(c).for_each(|m| {
                    q_set.insert(m.as_str());
                });
            }

            q_set
        };

        info!("{query_variables:?}");

        let variable_type_snippet = query_variables
            .iter()
            .map(|v| format!("OPTIONAL {{ {v} rdf:type {v}_type }}"))
            .collect::<Vec<_>>()
            .join("\n");

        let construct_type_snippet = query_variables
            .iter()
            .map(|v| format!("{v} rdf:type {v}_type"))
            .collect::<Vec<_>>()
            .join(". \n");

        let query_variable_snippet =
            query_variables
                .iter()
                .fold(String::new(), |mut buffer, var| {
                    // SAFETY: writing strings is infallible
                    // https://doc.rust-lang.org/stable/src/alloc/string.rs.html#2879
                    let _ = write!(buffer, "{var}");
                    buffer
                });

        format!(
            r"
            {prefixes}
            CONSTRUCT {{
                {query_variable_snippet} .
                {construct_type_snippet}
            }}
            WHERE {{
                GRAPH <{{GRAPH_IRI}}> {{
                    {{  
                        ?s a owl:Ontology .
                        ?s ?p ?o .
                        BIND(owl:Ontology AS ?typeS)
                    }}
                    UNION
                    {{
                        {{ {user_query} }}

                        {query_variable_snippet} .
                        {variable_type_snippet}
                        
                    }}
                }}
            }}
            "
        )
    }
}

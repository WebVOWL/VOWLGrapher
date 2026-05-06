pub mod normalization;
pub mod prefix;
pub mod query_regex;

use crate::assembly::normalization::QueryNormalizer;
use crate::assembly::query_regex::{VAR, VAR_FIRST};
use crate::errors::QueryAssemblyError;
use crate::prelude::GENERAL_SNIPPETS;
use crate::snippets::SparqlSnippet;
use crate::snippets::void::VOID;
use grapher::prelude::ElementType;
use indexmap::IndexSet;
use log::info;
use regex::Regex;
use std::collections::HashMap;

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

/// Compiles snippets of SPARQL code into full-fledged SPARQL queries.
pub struct QueryAssembler;

impl QueryAssembler {
    /// Returns the significant query variables of the query.
    ///
    /// That is, all query variables of the first SELECT clause.
    ///
    /// # Note
    /// Do not remove elements from the returned [`IndexSet`]. Doing so breaks the ordering!
    ///
    /// # Errors
    /// Returns an error if the internal Regex procedure fails.
    pub fn get_significant_variable_count(
        query: &str,
    ) -> Result<IndexSet<String>, QueryAssemblyError> {
        let mut q_set = IndexSet::new();
        let query_var_re = Regex::new(VAR)?;
        let var_first_re = Regex::new(VAR_FIRST)?;

        for (_, [c]) in var_first_re.captures_iter(query).map(|c| c.extract()) {
            query_var_re.find_iter(c).for_each(|m| {
                q_set.insert(m.as_str().to_string());
            });
        }

        Ok(q_set)
    }

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

    /// Construct a serializable, user-defined SPARQL query.
    ///
    /// # Errors
    /// Returns an error if the query could not be assembled.
    pub fn assemble_user_query(
        user_query: &str,
        variable_triple_map: &HashMap<String, [String; 3]>,
    ) -> Result<String, QueryAssemblyError> {
        // PIPELINE:
        // 1. Gather query variables from after SELECT.
        //  [WIP] 1.2. If SELECT *, gather all query variables - in order of appearance.
        // 2. Normalize query variables to triples
        //  2.1. Include SPARQL snippets with relevant type information.
        // 3. Insert query triples into CONSTRUCT graph template.
        // 4. Insert user query into CONSTRUCT pattern.
        //  4.1. Include SPARQL snippets with relevant type information.
        // 5. Load result into DB and query as normal.

        info!("{user_query}");

        let prefixes = DEFAULT_PREFIXES
            .iter()
            .map(|item| format!("PREFIX {item}"))
            .collect::<Vec<_>>()
            .join("\n");

        let query_variables = Self::get_significant_variable_count(user_query)?;

        info!("{query_variables:?}");

        let normalized_query_variables =
            QueryNormalizer::normalize_query_variables(&query_variables, variable_triple_map);

        Ok(format!(
            r"
            {prefixes}
            CONSTRUCT {{
                {normalized_query_variables}
            }}
            WHERE {{
                GRAPH <{{GRAPH_IRI}}> {{
                    {{  
                        {normalized_query_variables}
                    }}
                    UNION
                    {{
                        {user_query}
                    }}
                }}
            }}
            "
        ))
    }
}

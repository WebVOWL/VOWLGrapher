//! Normalizes query variables to triples
//!
//! # Introduction
//!
//! The CONSTRUCT graph template must contain triples (cf. <https://www.w3.org/TR/2013/REC-sparql11-query-20130321/#rConstructQuery>).
//!
//! A user query contains a SELECT clause with 1 or more query variables.
//!
//! During normalization, a query variable may be _independent_ or _dependent_ on other query variables.
//!
//! a) An _independent_ query variable does not form triples with other query variables in the CONSTRUCT graph template.
//!
//! b) A _dependent_ query variable may form triples with other query variables, but which triples it is part of is unknown.
//!    - Consider the query `SELECT ?a ?b ?c ?d`.\
//!      Valid triples are `a - b - c` and `b - c - d`.\
//!      Now, we have these possibilities:
//!         - triple a is valid  (?d is independent)
//!         - triple d is valid  (?a is independent)
//!         - No triple is valid (all query variables are independent)
//!  
//! As such, 1-2 query variables are considered _independent_, since they cannot form a triple.
//!
//! However with >2 query variables, at least one triple can be formed. However, forming semantically correct triples is hard due to:
//! 1. The ordering of the query variables is not quaranteed.
//!    - Consider `SELECT * WHERE {?s ?p ?o}`. A valid solution sequence is `?o ?p ?s`.
//! 2. Mapping query variables to triples is ambiguous, see b).
//!    
//! In order to alleviate 2., the user is required to include triple relationships of the query variables in use.
//!
//!
//! # Procedure
//!
//! For all query variables _A_ in SELECT:
//!   1A. If _A_ is independent, return _A_ with relevant SPARQL snippet type information.
//!   2A. If _A_ is dependent, check user-supplied query variable mapping _Q_\
//!     2.1A If _Q_ contains _A_, form a triple of variables* _A_ - _B_ - _C_. See [Triple formation rules](#triple-formation-rules).\
//!       Otherwise, treat _A_ as independent. See 1A.  
//!
//! \* (ordering dictated by _Q_)
//!
//!
//! ## Triple formation rules
//!
//! When forming a triple _T_ consisting of the variables _A_ - _B_ - _C_, the following applies:
//!
//! 1. Return _T_ as part of the solution.
//! 2. Treat _A_, _B_, and _C_ as independent. See 1A.

use std::collections::HashMap;

use indexmap::IndexSet;
use log::info;
use regex::Regex;

use crate::{
    assembly::query_regex::QUERY_NORMALIZATION, errors::QueryAssemblyError,
    prelude::NORMALIZED_SNIPPETS,
};

/// Provides methods to normalize user-defined SPARQL queries.
pub struct QueryNormalizer;

impl QueryNormalizer {
    /// Returns query variables as a string normalized to a sequence of triples.
    ///
    /// Includes all necessary serialization information.
    pub fn normalize_query_variables(
        query_variables: &IndexSet<String>,
        variable_triple_map: &HashMap<String, [String; 3]>,
    ) -> String {
        let mut triples = Vec::new();
        let mut variable_snippets = Vec::new();
        for qvar in query_variables {
            if let Some(triple) = variable_triple_map.get(qvar) {
                // Dependent
                triples.push(format!(
                    "{{\n\t{} {} {}\n}}",
                    triple[0], triple[1], triple[2]
                ));
                for var in triple {
                    variable_snippets.push(Self::build_snippet_for_variable(var));
                }
            } else {
                // Independent
                variable_snippets.push(Self::build_snippet_for_variable(qvar));
            }
        }

        let mut normalized = String::new();
        normalized.push_str(triples.join(" UNION ").as_str());
        normalized.push_str(variable_snippets.join(" UNION ").as_str());
        info!("Norm VARS: {normalized}");
        normalized
    }

    /// Returns SPARQL snippets which provide necessary serialization information
    /// for the variable.
    fn build_snippet_for_variable(variable: &str) -> String {
        NORMALIZED_SNIPPETS
            .iter()
            .map(|snippet| snippet.replace("?s", variable))
            .collect::<Vec<_>>()
            .join(" UNION ")
    }

    /// Normalizes SPARQL snippets for use in user-defined query assembly.
    ///
    /// # Errors
    /// Returns an error if the snippets could not be normalized.
    pub fn normalize_snippets(snippets: &Vec<&str>) -> Result<Vec<String>, QueryAssemblyError> {
        let norm_re = Regex::new(QUERY_NORMALIZATION)?;
        let normalized = snippets
            .iter()
            .map(|snippet| {
                let norm_v1 = snippet.replace("?id", "?s").replace("?target", "?o");
                norm_re.replace_all(&norm_v1, "").to_string()
            })
            .collect();
        Ok(normalized)
    }
}

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
//!      Example valid triples are: `a - b - c` and `b - c - d` (however, any variable combination is potentially valid) \
//!      Now, for the example, we have these possibilities:
//!         - triple `a - b - c` is valid  (?d is independent)
//!         - triple `b - c - d` is valid  (?a is independent)
//!         - No triple is valid (all query variables are independent)
//!  
//! As such, 1-2 query variables are considered _independent_, since they cannot form a triple.
//!
//! With >2 query variables, at least one triple can be formed. However, forming semantically correct triples is hard due to:
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
use regex::Regex;

use crate::{
    assembly::query_regex::{QUERY_NORMALIZATION, VAR, VAR_FIRST},
    errors::{QueryAssemblyError, QueryAssemblyErrorKind},
    prelude::NORMALIZED_SNIPPETS,
    snippets::assembly,
};

pub type VariableTripleMap = HashMap<String, [String; 3]>;

/// Provides methods to normalize user-defined SPARQL queries.
///
/// Normalization is necessary to prepare the query for serialization.
pub struct QueryNormalizer;

impl QueryNormalizer {
    /// Returns query variables as a string normalized to a sequence of triples.
    ///
    /// The return value should be used in the graph template of a `CONSTRUCT` query.
    ///
    /// # Example
    ///
    /// ```sparql
    /// CONSTRUCT {
    /// # This is the graph template
    /// } WHERE {}
    /// ```
    #[expect(clippy::branches_sharing_code, reason = "testing")]
    pub fn normalize_variables_for_template(
        query_variables: &IndexSet<String>,
        variable_triple_map: &VariableTripleMap,
    ) -> String {
        let mut triples = Vec::new();
        for qvar in query_variables {
            if let Some([triple_0, triple_1, triple_2]) = variable_triple_map.get(qvar) {
                // Dependent
                triples.push(format!("{triple_0} {triple_1} {triple_2}"));
                triples.push(format!(
                    "{} {} {}",
                    qvar,
                    Self::encode_predicate(qvar),
                    Self::encode_object(qvar)
                ));
            } else {
                // Independent
                triples.push(format!(
                    "{} {} {}",
                    qvar,
                    Self::encode_predicate(qvar),
                    Self::encode_object(qvar)
                ));
            }
        }
        triples.push(assembly::ONTOLOGY.replace(['{', '}'], ""));
        triples.join(" .\n")
    }

    /// Returns query variables as a string with all necessary serialization information.
    ///
    /// The return value should be used in the graph pattern of a `CONSTRUCT` query.
    ///
    /// # Example
    ///
    /// ```sparql
    /// CONSTRUCT {} WHERE {
    /// # This is the graph pattern
    /// }
    /// ```
    fn normalize_variables_for_pattern(
        query_variables: &IndexSet<String>,
        variable_triple_map: &VariableTripleMap,
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
                variable_snippets.extend(Self::build_snippet_for_triple(triple));
            } else {
                // Independent
                variable_snippets.push(Self::build_snippet_for_variable(qvar));
            }
        }

        triples.extend(variable_snippets);
        triples.join(" UNION ")
    }

    /// Returns SPARQL snippets which provide necessary serialization information
    /// for the variable.
    fn build_snippet_for_variable(variable: &str) -> String {
        NORMALIZED_SNIPPETS
            .iter()
            .map(|snippet| {
                snippet
                    .replace("?_o", &Self::encode_object(variable))
                    .replace("?_p", &Self::encode_predicate(variable))
                    .replace("?_s", variable)
            })
            .collect::<Vec<_>>()
            .join(" UNION ")
    }

    /// Returns SPARQL snippets which provide necessary serialization information
    /// for the variable.
    fn build_snippet_for_triple(triple: &[String; 3]) -> Vec<String> {
        let mut out = Vec::new();
        for variable in triple {
            let a = NORMALIZED_SNIPPETS
                .iter()
                .map(|snippet| {
                    snippet
                        .replace("?_o", &Self::encode_object(variable))
                        .replace("?_p", &Self::encode_predicate(variable))
                        .replace("?_s", variable)
                        // Special case for domain/range
                        .replace("?t2 ", &triple[2])
                        .replace("?t0 ", &triple[0])
                })
                .collect::<Vec<_>>()
                .join(" UNION ");
            out.push(a);
        }
        out
    }

    /// Normalizes SPARQL snippets for use in user-defined query assembly.
    ///
    /// # Errors
    /// If the snippets could not be normalized.
    pub fn normalize_snippets(snippets: &Vec<&str>) -> Result<Vec<String>, QueryAssemblyError> {
        let query_norm_re = Regex::new(QUERY_NORMALIZATION)?;
        let normalized = snippets
            .iter()
            .map(|snippet| {
                let norm_v1 = snippet
                    .replace("?id", "?_s")
                    .replace("?nodeType", "?_p")
                    .replace("?target", "?_o");
                query_norm_re.replace_all(&norm_v1, "").to_string()
            })
            .collect();
        Ok(normalized)
    }

    fn encode_predicate(subject: &str) -> String {
        format!("{subject}_p")
    }

    fn encode_object(subject: &str) -> String {
        format!("{subject}_o")
    }

    /// Returns the variable map of user-defined query variable triple relationships.
    ///
    /// That is, a mapping from a subject variable to the triple containing it.
    ///
    /// # Errors
    /// Returns an error if the user-defined triples are invalid.
    pub fn create_variable_triple_map(
        triple_decls: &str,
    ) -> Result<VariableTripleMap, QueryAssemblyError> {
        let mut map = HashMap::new();
        if !triple_decls.is_empty() {
            for (line, triple_decl) in triple_decls.split('\n').enumerate() {
                let variables = Self::get_query_variables(triple_decl)?;
                if variables.len() != 3 {
                    let msg = format!(
                        "Error at line {line}: A triple must consist of exactly 3 variables"
                    );
                    return Err(QueryAssemblyErrorKind::InvalidTripleDecl(msg))?;
                }

                let subject = variables.first().cloned().ok_or_else(|| {
                    let msg = "Missing subject in triple declaration".to_string();
                    QueryAssemblyErrorKind::InvalidTripleDecl(msg)
                })?;

                let v = {
                    let a = variables.into_iter().collect::<Vec<_>>();
                    [a[0].clone(), a[1].clone(), a[2].clone()]
                };
                map.insert(subject.clone(), v);
            }
        }

        Ok(map)
    }

    /// Returns the significant query variables of the query.
    ///
    /// That is, all query variables of the first SELECT clause.
    ///
    /// # Warning
    /// Do not remove elements from the returned [`IndexSet`]. Doing so breaks the ordering!
    ///
    /// # Errors
    /// If the internal [`Regex`] procedure fails.
    pub fn get_significant_query_variables(
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

    /// Returns all query variables present in the query.
    ///
    /// # Warning
    /// Do not remove elements from the returned [`IndexSet`]. Doing so breaks the ordering!
    ///
    /// # Errors
    /// If the internal [`Regex`] procedure fails.
    pub fn get_query_variables(query: &str) -> Result<IndexSet<String>, QueryAssemblyError> {
        let mut q_set = IndexSet::new();
        let query_var_re = Regex::new(VAR)?;

        query_var_re.find_iter(query).for_each(|m| {
            q_set.insert(m.as_str().to_string());
        });

        Ok(q_set)
    }
}

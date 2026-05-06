//! Contains Regex patterns used by the query assembler.

// static PREFIX_REGEX: &str =
// r"(PN_CHARS_U | ':' | [0-9] | PLX ) ((PN_CHARS | '.' | ':' | PLX)* (PN_CHARS | ':' | PLX) )?";

// https://www.w3.org/TR/2013/REC-sparql11-query-20130321/#rPLX
// "[A-Z] | [a-z] | [#x00C0-#x00D6] | [#x00D8-#x00F6] | [#x00F8-#x02FF] | [#x0370-#x037D] | [#x037F-#x1FFF] | [#x200C-#x200D] | [#x2070-#x218F] | [#x2C00-#x2FEF] | [#x3001-#xD7FF] | [#xF900-#xFDCF] | [#xFDF0-#xFFFD] | [#x10000-#xEFFFF]"

// OLD: (?:PREFIX|prefix) (?:(\w*) *:).*

/// Pattern matching all SPARQL query variables in the input.
///
/// Defined by: <https://www.w3.org/TR/2013/REC-sparql11-query-20130321/#rVar>
pub static VAR: &str = r"[?$]([a-zA-Z_][a-zA-Z0-9_\u00B7\u0300-\u036F\u203F-\u2040]*)";

/// Pattern matching the query variables of the first SELECT clause encountered in the input.
///
/// This is assumed to be the SELECT clause containing the query variables to include in the
/// assembled CONSTRUCT query.
pub static VAR_FIRST: &str = r"(?:SELECT) ([?$].*|\*)(?:WHERE)?";

/// Pattern matching all SPARQL prefixes in the input.
///
/// Defined by: <https://www.w3.org/TR/2013/REC-sparql11-query-20130321/#rPrefixDecl>
pub static PREFIX_DECL: &str = r"";

/// Pattern matching content of SPARQL constructs:
///
/// - `FILTER`
/// - `BIND`
/// - `#`
pub static QUERY_NORMALIZATION: &str = r"(?:FILTER|BIND|#).*\s";

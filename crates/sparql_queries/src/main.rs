//! Automatically write SPARQL query snippets to a file.

use std::fs;

use vowlr_sparql_queries::prelude::DEFAULT_QUERY;

/// Writes the default query to a file.
fn main() -> std::io::Result<()> {
    fs::write(
        "crates/sparql_queries/src/reference/default.rq",
        DEFAULT_QUERY.as_bytes(),
    )?;
    Ok(())
}

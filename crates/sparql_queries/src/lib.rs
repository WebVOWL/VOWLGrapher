//! Shared SPARQL query strings for `VOWLGrapher`.
//!
//! This crate is intentionally free of vowlgrapher-dependencies such
//! that it may be used both server-side and client-side

mod assembly;
mod snippets;

/// Exports all the core types of the library.
pub mod prelude {
    use grapher::prelude::{
        Characteristic, OwlEdge, OwlNode, RdfEdge, RdfNode, RdfsEdge, RdfsNode,
    };
    use std::sync::LazyLock;

    use crate::assembly::DEFAULT_PREFIXES;
    pub use crate::assembly::QueryAssembler;
    use crate::snippets::general::{
        COLLECTIONS, DOMAIN_RANGES, LABEL, NAMED_INDIVIDUAL_COUNTS, ONTOLOGY, OWL_DEPRECATED,
        XML_BASE,
    };
    use crate::snippets::metadata::dcmi::{dc, dcterms};
    use crate::snippets::metadata::{
        ANNOTATED_PROPERTY, ANNOTATED_SOURCE, ANNOTATED_TARGET, AXIOM, BACKWARD_COMPATIBLE_WITH,
        COMMENT, INCOMPATIBLE_WITH, IS_DEFINED_BY, PRIOR_VERSION, SEE_ALSO, VERSION_INFO,
        VERSION_IRI,
    };
    use crate::snippets::snippets_from_enum;

    /// SPARQL snippets that should generally be included in all queries.
    pub static GENERAL_SNIPPETS: [&str; 7] = [
        ONTOLOGY,
        XML_BASE,
        COLLECTIONS,
        DOMAIN_RANGES,
        OWL_DEPRECATED,
        NAMED_INDIVIDUAL_COUNTS,
        LABEL,
    ];

    /// SPARQL snippets fetching data not included in the graph visualization.
    pub static METADATA_SNIPPETS: [&str; 42] = [
        COMMENT,
        IS_DEFINED_BY,
        SEE_ALSO,
        VERSION_INFO,
        VERSION_IRI,
        PRIOR_VERSION,
        INCOMPATIBLE_WITH,
        BACKWARD_COMPATIBLE_WITH,
        dc::CONTRIBUTOR,
        dc::COVERAGE,
        dc::CREATOR,
        dc::DATE,
        dc::DESCRIPTION,
        dc::FORMAT,
        dc::IDENTIFIER,
        dc::LANGUAGE,
        dc::PUBLISHER,
        dc::RELATION,
        dc::RIGHTS,
        dc::SOURCE,
        dc::SUBJECT,
        dc::TITLE,
        dc::TYPE,
        dcterms::CONTRIBUTOR,
        dcterms::COVERAGE,
        dcterms::CREATOR,
        dcterms::DATE,
        dcterms::DESCRIPTION,
        dcterms::FORMAT,
        dcterms::IDENTIFIER,
        dcterms::LANGUAGE,
        dcterms::PUBLISHER,
        dcterms::RELATION,
        dcterms::RIGHTS,
        dcterms::SOURCE,
        dcterms::SUBJECT,
        dcterms::TITLE,
        dcterms::TYPE,
        AXIOM,
        ANNOTATED_SOURCE,
        ANNOTATED_PROPERTY,
        ANNOTATED_TARGET,
    ];

    // PERF: this could maybe be a thread_local instead?
    /// The default query contains all classes and properties supported by `VOWLGrapher`.
    pub static DEFAULT_QUERY: LazyLock<String> = LazyLock::new(|| {
        let snippets = [
            snippets_from_enum::<OwlNode>(),
            snippets_from_enum::<OwlEdge>(),
            snippets_from_enum::<RdfEdge>(),
            snippets_from_enum::<RdfNode>(),
            snippets_from_enum::<RdfsNode>(),
            snippets_from_enum::<RdfsEdge>(),
            snippets_from_enum::<Characteristic>(),
            GENERAL_SNIPPETS.into(),
            METADATA_SNIPPETS.into(),
        ]
        .concat();

        QueryAssembler::assemble_query(&DEFAULT_PREFIXES.into(), &snippets)
    });
}

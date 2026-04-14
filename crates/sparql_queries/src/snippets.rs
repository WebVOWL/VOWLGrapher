pub mod characteristic;
pub mod element_type;
pub mod general;
pub mod generic;
pub mod metadata;
pub mod owl;
pub mod rdf;
pub mod rdfs;
pub mod void;
pub mod xsd;

use grapher::prelude::strum::IntoEnumIterator;

pub fn snippets_from_enum<T>() -> Vec<&'static str>
where
    T: IntoEnumIterator + SparqlSnippet,
{
    T::iter().map(SparqlSnippet::snippet).collect::<Vec<_>>()
}

pub trait SparqlSnippet {
    /// Get the SPARQL snippet representing `self`.
    fn snippet(self) -> &'static str;
}

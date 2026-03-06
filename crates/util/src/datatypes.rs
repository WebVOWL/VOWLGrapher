use std::path::Path;

use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};
use serde::{Deserialize, Serialize};

/// Supported content types.
#[repr(C)]
#[derive(
    Archive, RDeserialize, RSerialize, Deserialize, Serialize, Debug, Copy, Clone, strum::Display,
)]
#[strum(serialize_all = "UPPERCASE")]
pub enum DataType {
    OWL,
    OFN,
    OWX,
    TTL,
    RDF,
    #[strum(serialize = "N-Triples")]
    NTriples,
    #[strum(serialize = "N-Quads")]
    NQuads,
    #[strum(serialize = "TriG")]
    TriG,
    #[strum(serialize = "JSON-LD")]
    JsonLd,
    N3,
    #[strum(serialize = "SPARQL JSON")]
    SPARQLJSON,
    #[strum(serialize = "SPARQL XML")]
    SPARQLXML,
    #[strum(serialize = "SPARQL CSV")]
    SPARQLCSV,
    #[strum(serialize = "SPARQL TSV")]
    SPARQLTSV,
    /// Fallback when type can't be determined.
    UNKNOWN,
}

impl DataType {
    // Fixed string literals called by reference as to not allocate new memory each time the function is called
    /// Get mime type of the data.
    pub fn mime_type(&self) -> &'static str {
        match self {
            Self::OWL => "application/owl+xml",
            Self::OFN => "text/ofn",
            Self::OWX => "application/owl+xml",
            Self::TTL => "text/turtle",
            Self::RDF => "application/rdf+xml",
            Self::NTriples => "application/n-triples",
            Self::NQuads => "application/n-quads",
            Self::TriG => "application/trig",
            Self::JsonLd => "application/ld+json",
            Self::N3 => "text/n3",
            Self::SPARQLJSON => "application/sparql-results+json",
            Self::SPARQLXML => "application/sparql-results+xml",
            Self::SPARQLCSV => "text/csv",
            Self::SPARQLTSV => "text/tab-separated-values",
            Self::UNKNOWN => "application/octet-stream",
        }
    }

    /// Returns the extension of the data.
    pub fn extension(&self) -> &'static str {
        match self {
            DataType::OWL => "owl",
            DataType::OFN => "ofn",
            DataType::OWX => "owx",
            DataType::TTL => "ttl",
            DataType::RDF => "rdf",
            DataType::NTriples => "nt",
            DataType::NQuads => "nq",
            DataType::TriG => "trig",
            DataType::JsonLd => "jsonld",
            DataType::N3 => "n3",
            DataType::SPARQLJSON => "srj",
            DataType::SPARQLXML => "srx",
            DataType::SPARQLCSV => "src",
            DataType::SPARQLTSV => "tsv",
            DataType::UNKNOWN => "txt",
        }
    }
}

impl From<&Path> for DataType {
    fn from(value: &Path) -> Self {
        match value.extension().and_then(|os| os.to_str()) {
            Some(ext) => ext.into(),
            None => Self::UNKNOWN,
        }
    }
}

impl From<&str> for DataType {
    fn from(value: &str) -> Self {
        match value.to_lowercase().as_str() {
            "owl" => Self::OWL,
            "ofn" => Self::OFN,
            "owx" => Self::OWX,
            "ttl" => Self::TTL,
            "rdf" => Self::RDF,
            "nt" => Self::NTriples,
            "nq" => Self::NQuads,
            "trig" => Self::TriG,
            "jsonld" => Self::JsonLd,
            "n3" => Self::N3,
            "srj" | "json" => Self::SPARQLJSON,
            "srx" | "xml" => Self::SPARQLXML,
            "src" | "csv" => Self::SPARQLCSV,
            "tsv" => Self::SPARQLTSV, //TODO: Figure out file extension for TSV and if the file extension of TSV SPARQL Query Result differs.
            _ => Self::UNKNOWN,
        }
    }
}

impl From<String> for DataType {
    fn from(value: String) -> Self {
        value.to_lowercase().as_str().into()
    }
}

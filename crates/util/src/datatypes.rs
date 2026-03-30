use std::path::Path;

/// Supported content types.
#[repr(C)]
#[derive(
    rkyv::Archive,
    rkyv::Deserialize,
    rkyv::Serialize,
    serde::Deserialize,
    serde::Serialize,
    Debug,
    Copy,
    Clone,
    strum::Display,
)]
#[strum(serialize_all = "UPPERCASE")]
pub enum DataType {
    /// Alternative syntax for [`DataType::OFN`].
    ///
    /// Syntax: [RDF/XML](https://www.w3.org/TR/rdf-syntax-grammar/)
    OWL,
    /// Subset of [`DataType::RDF`].
    ///
    /// Syntax: [OWL Functional](https://www.w3.org/TR/owl2-syntax/)
    OFN,
    /// Alternative syntax for [`DataType::OFN`].
    ///
    /// Syntax: [OWL/XML](https://www.w3.org/TR/2012/REC-owl2-xml-serialization-20121211/)
    OWX,
    /// Alternative syntax for [`DataType::RDF`].
    ///
    /// Syntax: [Turtle](https://www.w3.org/TR/turtle/)
    TTL,
    /// The Resource Description Framework (RDF) is a method to describe and exchange graph data.
    ///
    /// Subset of [`DataType::N3`].
    ///
    /// Syntax: [RDF/XML](https://www.w3.org/TR/rdf-syntax-grammar/)
    RDF,
    /// Subset of [`DataType::TTL`].
    ///
    /// Syntax: [N-Triples](https://www.w3.org/TR/n-triples/)
    #[strum(serialize = "N-Triples")]
    NTriples,
    /// Alternative syntax for [`DataType::RDF`].
    ///
    /// Syntax: [N-Quads](https://www.w3.org/TR/n-quads/)
    #[strum(serialize = "N-Quads")]
    NQuads,
    /// Extension of [`DataType::TTL`].
    ///
    /// Syntax: [TriG](https://www.w3.org/TR/trig/)
    #[strum(serialize = "TriG")]
    TriG,
    /// Linked data encoded in JSON.
    ///
    /// Syntax: [JSON-LD](https://www.w3.org/TR/json-ld11/)
    #[strum(serialize = "JSON-LD")]
    JsonLd,
    /// Superset of [`DataType::RDF`].
    ///
    /// Syntax: [Notation3](https://www.w3.org/TeamSubmission/n3/)
    N3,
    #[strum(serialize = "SPARQL JSON")]
    /// SPARQL query results encoded in JSON.
    ///
    /// Syntax: [SPARQL Results JSON](https://www.w3.org/TR/sparql11-results-json/)
    SPARQLJSON,
    #[strum(serialize = "SPARQL XML")]
    /// SPARQL query results encoded in XML.
    ///
    /// Syntax: [SPARQL Results JSON](https://www.w3.org/TR/rdf-sparql-XMLres/)
    SPARQLXML,
    #[strum(serialize = "SPARQL CSV")]
    /// SPARQL query results encoded in CSV.
    ///
    /// Syntax: [SPARQL Results JSON](https://www.w3.org/TR/sparql11-results-csv-tsv/)
    SPARQLCSV,
    #[strum(serialize = "SPARQL TSV")]
    /// SPARQL query results encoded in TSV.
    ///
    /// Syntax: [SPARQL Results JSON](https://www.w3.org/TR/sparql11-results-csv-tsv/)
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

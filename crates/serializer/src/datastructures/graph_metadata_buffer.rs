use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::{Arc, RwLock},
};

use crate::datastructures::index::TermIndex;

#[derive(Default)]
pub struct GraphMetadataBuffer {
    /// Maps terms to integer ids and vice-versa.
    ///
    /// Reduces memory usage and allocations.
    pub term_index: Arc<TermIndex>,
    /// The term's corresponding id which describes the version of an ontology.
    ///
    /// owl:versionIRI
    pub version_iri: Arc<RwLock<Option<usize>>>,
    /// The term's corresponding id which describes the prior version of an ontology.
    ///
    /// owl:priorVersion
    ///
    /// ## Note
    /// The usage of this annotation property on entities other than ontologies is [discouraged](https://www.w3.org/TR/owl-syntax/#Ontology_Annotations).
    /// As such, we should notify the client if this is violated.
    pub prior_version: Arc<RwLock<Option<usize>>>,
    /// The term's cooresponding id which describes the prior version of the ontology that is incompatible with the current version, i.e., [`Self::version_iri`]-
    ///
    /// owl:incompatibleWith
    ///
    /// ## Note
    /// The usage of this annotation property on entities other than ontologies is [discouraged](https://www.w3.org/TR/owl-syntax/#Ontology_Annotations).
    /// As such, we should notify the client if this is violated.
    pub incompatible_with: Arc<RwLock<Option<usize>>>,
    /// The term's cooresponding id which describes the prior version of the ontology that is compatible with the current version, i.e., [`Self::version_iri`]-
    ///
    /// owl:backwardCompatibleWith
    ///
    /// ## Note
    /// The usage of this annotation property on entities other than ontologies is [discouraged](https://www.w3.org/TR/owl-syntax/#Ontology_Annotations).
    /// As such, we should notify the client if this is violated.
    pub backward_compatible_with: Arc<RwLock<Option<usize>>>,
    /// An entity primarily responsible for making the resource.
    ///
    /// Stores the creators of a term, keyed by the term's corresponding id.
    ///
    /// The stored creators are the corresponding ids of the creators' terms.
    ///
    /// dc:creator | dcterms:creator
    pub creator: Arc<RwLock<HashMap<usize, HashSet<usize>>>>,
    /// An entity responsible for making contributions to the resource.
    ///
    /// Stores the contributors of a term, keyed by the term's corresponding id.
    ///
    /// The stored contributors are the corresponding ids of the contributors' terms.
    ///
    /// dc:contributor | dcterms:contributor
    pub contributor: Arc<RwLock<HashMap<usize, HashSet<usize>>>>,
    /// Stores the coverage of a term, keyed by the term's corresponding id.
    ///
    /// The coverage stored is the corresponding id of a coverage term.
    ///
    /// dc:coverage | dcterms:coverage
    pub coverage: Arc<RwLock<HashMap<usize, usize>>>,
    /// Stores the date of a term, keyed by the term's corresponding id.
    ///
    /// The date stored is the corresponding id of a date term.
    ///
    /// dc:date | dcterms:date
    pub date: Arc<RwLock<HashMap<usize, usize>>>,
    /// Stores the description of a term, keyed by the term's corresponding id.
    ///
    /// The description stored is the corresponding id of a description term.
    ///
    /// dc:description | dcterms:description
    pub description: Arc<RwLock<HashMap<usize, usize>>>,
    /// Stores the format of a term, keyed by the term's corresponding id.
    ///
    /// The format stored is the corresponding id of a format term.
    ///
    /// dc:format | dcterms:format
    pub format: Arc<RwLock<HashMap<usize, usize>>>,
    /// Stores the identifier of a term, keyed by the term's corresponding id.
    ///
    /// The identifier stored is the corresponding id of a identifier term.
    ///
    /// dc:identifier | dcterms:identifier
    pub identifier: Arc<RwLock<HashMap<usize, usize>>>,
    /// Stores the language of a term, keyed by the term's corresponding id.
    ///
    /// The language stored is the corresponding id of a language term.
    ///
    /// dc:language | dcterms:language
    pub language: Arc<RwLock<HashMap<usize, usize>>>,
    /// Stores the publishers of a term, keyed by the term's corresponding id.
    ///
    /// The stored publishers are the corresponding ids of the publishers' terms.
    ///
    /// dc:publisher | dcterms:publisher
    pub publisher: Arc<RwLock<HashMap<usize, HashSet<usize>>>>,
    /// Stores the relations of a term, keyed by the term's corresponding id.
    ///
    /// The stored relations are the corresponding ids of the relation terms.
    ///
    /// dc:relation | dcterms:relation
    pub relation: Arc<RwLock<HashMap<usize, HashSet<usize>>>>,
    /// Stores the rights of a term, keyed by the term's corresponding id.
    ///
    /// The stored rights are the corresponding ids of the rights' terms.
    ///
    /// dc:rights | dcterms:rights
    pub rights: Arc<RwLock<HashMap<usize, HashSet<usize>>>>,
    /// Stores the publishers of a term, keyed by the term's corresponding id.
    ///
    /// The stored sources are the corresponding ids of the sources' terms.
    ///
    /// dc:source | dcterms:source
    pub source: Arc<RwLock<HashMap<usize, HashSet<usize>>>>,
    /// Stores the subject of a term, keyed by the term's corresponding id.
    ///
    /// The subject stored is the corresponding id of a subject term.
    ///
    /// dc:subject | dcterms:subject
    pub subject: Arc<RwLock<HashMap<usize, usize>>>,
    /// Stores the title of a term, keyed by the term's corresponding id.
    ///
    /// The title stored is the corresponding id of a title term.
    ///
    /// dc:title | dcterms:title
    pub title: Arc<RwLock<HashMap<usize, usize>>>,
    /// Stores the type of a term, keyed by the term's corresponding id.
    ///
    /// The type stored is the corresponding id of a type term.
    ///
    /// dc:type | dcterms:type
    pub r#type: Arc<RwLock<HashMap<usize, usize>>>,
    /// Stores comments of terms, keyed by the term's corresponding id.
    ///
    /// rdfs:comment
    pub comment: Arc<RwLock<HashMap<usize, usize>>>,
    /// Maps from a term's cooresponding id to the term's corresponding id which defines it.
    ///
    /// rdfs:isDefinedBy
    pub defined_by: Arc<RwLock<HashMap<usize, usize>>>,
    /// Maps from a term's cooresponding id to the terms' corresponding ids which
    /// provide additional info about it.
    ///
    /// rdfs:seeAlso
    pub see_also: Arc<RwLock<HashMap<usize, HashSet<usize>>>>,
    /// Stores the version of terms, keyed by the term's cooresponding id.
    ///
    /// owl:versionInfo
    pub version_info: Arc<RwLock<HashMap<usize, usize>>>,

    /// Maps from `owl:annotatedSource` to a hashmap, mapping `owl:annotatedProperty` to `owl:annotatedTarget`.
    pub annotations: Arc<RwLock<HashMap<usize, HashMap<usize, usize>>>>,
}

impl GraphMetadataBuffer {
    pub fn new(index: Arc<TermIndex>) -> Self {
        Self {
            term_index: index,
            ..Default::default()
        }
    }

    fn fmt_usize_hashmap(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        map: &Arc<RwLock<HashMap<usize, usize>>>,
    ) -> std::fmt::Result {
        for (term_id, object_term_id) in map
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            writeln!(
                f,
                "\t\t\t{} - {}",
                self.term_index
                    .get(*term_id)
                    .map_or_else(|e| e.to_string(), |term| term.to_string()),
                self.term_index
                    .get(*object_term_id)
                    .map_or_else(|e| e.to_string(), |term| term.to_string())
            )?;
        }
        write!(f, "")
    }

    fn fmt_hashset_hashmap(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        map: &Arc<RwLock<HashMap<usize, HashSet<usize>>>>,
    ) -> std::fmt::Result {
        for (term_id, object_term_ids) in map
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            writeln!(
                f,
                "\t\t\t{}",
                self.term_index
                    .get(*term_id)
                    .map_or_else(|e| e.to_string(), |term| term.to_string()),
            )?;
            for object_term_id in object_term_ids {
                writeln!(
                    f,
                    "\t\t\t\t{}",
                    self.term_index
                        .get(*object_term_id)
                        .map_or_else(|e| e.to_string(), |term| term.to_string())
                )?;
            }
        }
        write!(f, "")
    }

    fn fmt_option_usize(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        title: &str,
        value: &Arc<RwLock<Option<usize>>>,
    ) -> std::fmt::Result {
        writeln!(
            f,
            "\t\t\t{title}: {}",
            value
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .map_or_else(String::new, |term_id| {
                    self.term_index
                        .get(term_id)
                        .map_or_else(|e| e.to_string(), |term| term.to_string())
                })
        )
    }
}

impl Display for GraphMetadataBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "\tGraphMetadataBuffer {{")?;
        writeln!(f, "\t\tOntology")?;
        self.fmt_option_usize(f, "version_iri", &self.version_iri)?;
        self.fmt_option_usize(f, "prior_version", &self.prior_version)?;
        self.fmt_option_usize(f, "incompatible_with", &self.incompatible_with)?;
        self.fmt_option_usize(f, "backward_compatible_wit", &self.backward_compatible_with)?;
        writeln!(f, "\t\tCreator:")?;
        self.fmt_hashset_hashmap(f, &self.creator)?;
        writeln!(f, "\t\tContributor:")?;
        self.fmt_hashset_hashmap(f, &self.contributor)?;
        writeln!(f, "\t\tCoverage:")?;
        self.fmt_usize_hashmap(f, &self.coverage)?;
        writeln!(f, "\t\tDate:")?;
        self.fmt_usize_hashmap(f, &self.date)?;
        writeln!(f, "\t\tDescription:")?;
        self.fmt_usize_hashmap(f, &self.description)?;
        writeln!(f, "\t\tFormat:")?;
        self.fmt_usize_hashmap(f, &self.format)?;
        writeln!(f, "\t\tIdentifier:")?;
        self.fmt_usize_hashmap(f, &self.identifier)?;
        writeln!(f, "\t\tLanguage:")?;
        self.fmt_usize_hashmap(f, &self.language)?;
        writeln!(f, "\t\tPublisher:")?;
        self.fmt_hashset_hashmap(f, &self.publisher)?;
        writeln!(f, "\t\tRelation:")?;
        self.fmt_hashset_hashmap(f, &self.relation)?;
        writeln!(f, "\t\tRights:")?;
        self.fmt_hashset_hashmap(f, &self.rights)?;
        writeln!(f, "\t\tSource:")?;
        self.fmt_hashset_hashmap(f, &self.source)?;
        writeln!(f, "\t\tSubject:")?;
        self.fmt_usize_hashmap(f, &self.subject)?;
        writeln!(f, "\t\tTitle:")?;
        self.fmt_usize_hashmap(f, &self.title)?;
        writeln!(f, "\t\tType:")?;
        self.fmt_usize_hashmap(f, &self.r#type)?;
        writeln!(f, "\t\tComment:")?;
        self.fmt_usize_hashmap(f, &self.comment)?;
        writeln!(f, "\t\tDefined By:")?;
        self.fmt_usize_hashmap(f, &self.defined_by)?;
        writeln!(f, "\t\tSee Also:")?;
        self.fmt_hashset_hashmap(f, &self.see_also)?;
        writeln!(f, "\t\tVersion Info:")?;
        self.fmt_usize_hashmap(f, &self.version_info)?;
        writeln!(f, "\t\tAnnotations:")?;
        writeln!(f, "\t\t\t{:?}", self.annotations)?;
        writeln!(f, "\t}}")
    }
}

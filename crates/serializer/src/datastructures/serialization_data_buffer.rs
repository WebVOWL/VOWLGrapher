use crate::{
    datastructures::{
        ArcEdge, ArcLockRestrictionState, ArcTerm, ArcTriple, DocumentBase, TermID,
        graph_metadata_buffer::GraphMetadataBuffer, index::TermIndex,
    },
    errors::{SerializationError, SerializationErrorKind},
    serializer_util::trim_tag_circumfix,
    vocab::dcmi::{dc, dcterms},
};
use grapher::prelude::{
    Characteristic, ElementType, GraphDisplayData, GraphMetadata, OwlEdge, OwlType,
};
use log::debug;
use oxrdf::Term;
use std::{
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter},
    mem::take,
    sync::{Arc, RwLock},
};
use vowlgrapher_util::prelude::{ErrorRecord, VOWLGrapherError};

/// An intermediate container for serialization data.
///
/// This data may mutate during serialization
/// if new information regarding them is found.
/// This also means an element can be completely removed!
#[derive(Default)]
pub struct SerializationDataBuffer {
    /// Maps terms to integer ids and vice-versa.
    ///
    /// Reduces memory usage and allocations.
    pub term_index: Arc<TermIndex>,
    /// Stores all resolved node elements.
    ///
    /// The key is a term's corresponding id.
    ///
    /// The value is a term's type, e.g., "Owl Class".
    pub node_element_buffer: Arc<RwLock<HashMap<TermID, ElementType>>>,
    /// Stores all resolved edge elements.
    ///
    /// The key is a term's corresponding id.
    ///
    /// The value is a term's type, e.g., "Owl Class".
    pub edge_element_buffer: Arc<RwLock<HashMap<TermID, ElementType>>>,
    /// Keeps track of edges that should point to a node different
    /// from their definition.
    ///
    /// This can happen if, e.g., two nodes are merged.
    ///
    /// The key is the range term of an edge triple, translated to that term's corresponding id.
    ///
    /// The value is the domain term of an edge triple, translated to that term's corresponding id.
    pub edge_redirection: Arc<RwLock<HashMap<TermID, TermID>>>,
    /// Maps a term's corresponding id to the set of edges that include it.
    ///
    /// Used to remap edges when nodes are merged.
    pub edges_include_map: Arc<RwLock<HashMap<TermID, HashSet<ArcEdge>>>>,
    /// Canonical synthesized owl:Thing node per resolved domain.
    ///
    /// This lets structurally-defined ranges like complement/union expressions
    /// collapse to the same owl:Thing node that direct owl:Thing ranges use.
    pub anchor_thing_map: Arc<RwLock<HashMap<TermID, TermID>>>,
    /// Partially assembled restriction metadata keyed by the restriction node.
    pub restriction_buffer: Arc<RwLock<HashMap<TermID, ArcLockRestrictionState>>>,
    #[expect(clippy::type_complexity)]
    /// Final display cardinalities keyed by the concrete edge that will be emitted.
    pub edge_cardinality_buffer: Arc<RwLock<HashMap<ArcEdge, (String, Option<String>)>>>,
    /// Stores the edges of a property, keyed by the property's corresponding id.
    pub property_edge_map: Arc<RwLock<HashMap<TermID, ArcEdge>>>,
    /// Stores the domains of a property, keyed by the property's corresponding id.
    pub property_domain_map: Arc<RwLock<HashMap<TermID, HashSet<TermID>>>>,
    /// Stores the ranges of a property, keyed by the property's corresponding id.
    pub property_range_map: Arc<RwLock<HashMap<TermID, HashSet<TermID>>>>,
    /// Stores declared domains of a property, keyed by the property's corresponding id.
    ///
    /// This is used by owl:inverseOf resolution and should contain only query-level
    /// domain/range evidence, never endpoints inferred from rendered property edges.
    pub declared_property_domain_map: Arc<RwLock<HashMap<TermID, HashSet<TermID>>>>,
    /// Stores declared ranges of a property, keyed by the property's corresponding id.
    ///
    /// This is used by owl:inverseOf resolution and should contain only query-level
    /// domain/range evidence, never endpoints inferred from rendered property edges.
    pub declared_property_range_map: Arc<RwLock<HashMap<TermID, HashSet<TermID>>>>,
    /// Stores labels of terms, keyed by the term's corresponding id.
    pub label_buffer: Arc<RwLock<HashMap<TermID, Option<String>>>>,
    /// Stores labels of edges, keyed by the edge it belongs to.
    pub edge_label_buffer: Arc<RwLock<HashMap<ArcEdge, Option<String>>>>,
    /// Edges in graph, to avoid duplicates
    pub edge_buffer: Arc<RwLock<HashSet<ArcEdge>>>,
    /// Maps from an edge to its characteristic.
    pub edge_characteristics: Arc<RwLock<HashMap<ArcEdge, HashSet<Characteristic>>>>,
    /// Maps from a node term's corresponding id to its characteristics.
    pub node_characteristics: Arc<RwLock<HashMap<TermID, HashSet<Characteristic>>>>,
    /// Maps from node term's corresponding id to its number of individuals.
    pub individual_count_buffer: Arc<RwLock<HashMap<TermID, u32>>>,
    /// Maps from a class term id to the set of canonical individual term ids already counted for it.
    pub counted_individual_members: Arc<RwLock<HashMap<TermID, HashSet<TermID>>>>,
    /// Stores unresolved triples.
    ///
    /// This is a mapping of a term's corresponding id to the set of triples referencing it.
    pub unknown_buffer: Arc<RwLock<HashMap<TermID, HashSet<ArcTriple>>>>,
    /// Stores errors encountered during serialization.
    pub failed_buffer: Arc<RwLock<Vec<ErrorRecord>>>,
    /// The base IRI of the document.
    ///
    /// For instance: `http://purl.obolibrary.org/obo/envo.owl`
    pub document_base: Arc<RwLock<Option<DocumentBase>>>,
    /// Data not visualized in the graph.
    pub metadata: GraphMetadataBuffer,
}
impl SerializationDataBuffer {
    pub fn new() -> Self {
        let term_index: Arc<TermIndex> = TermIndex::new().into();
        Self {
            metadata: GraphMetadataBuffer::new(term_index.clone()),
            term_index,
            ..Default::default()
        }
    }

    /// Unpack the predicate term id of the triple.
    ///
    /// Returns an error if the term id is None.
    pub fn get_predicate(&self, triple: &ArcTriple) -> Result<TermID, SerializationError> {
        match triple.predicate_term_id {
            Some(predicate_term_id) => Ok(predicate_term_id),
            None => Err(SerializationErrorKind::MissingPredicate(
                self.term_index.display_triple(triple)?,
                "Cannot serialize a triple with a missing predicate".to_string(),
            ))?,
        }
    }

    /// Unpack the object term id of the triple.
    ///
    /// Returns an error if the term id is None.
    pub fn get_object(&self, triple: &ArcTriple) -> Result<usize, SerializationError> {
        match triple.object_term_id {
            Some(object_term_id) => Ok(object_term_id),
            None => Err(SerializationErrorKind::MissingObject(
                self.term_index.display_triple(triple)?,
                "Cannot serialize a triple with a missing object".to_string(),
            ))?,
        }
    }

    /// Converts [`self`] into [`GraphDisplayData`].
    ///
    /// Works like [`TryFrom`] except it also returns non-critical errors in [`Result::Ok`].
    pub fn convert_into(
        self,
    ) -> Result<(GraphDisplayData, Option<VOWLGrapherError>), SerializationError> {
        let mut display_data = GraphDisplayData::new();
        let mut failed: Vec<ErrorRecord> = Vec::new();

        // Maps an RDF term's corresponding id to a [`GraphDisplayData`] index.
        let mut iricache: HashMap<usize, usize> = HashMap::new();

        // Maps an RDF term's corresponding id to a [`GraphDisplayData`] index.
        let mut inverse_edge_indices: HashMap<usize, usize> = HashMap::new();

        self.convert_graph_data(
            &mut display_data,
            &mut failed,
            &mut iricache,
            &mut inverse_edge_indices,
        )?;
        self.convert_metadata(&mut display_data, &mut failed, &iricache)?;

        if failed.is_empty() {
            Ok((display_data, None))
        } else {
            Ok((display_data, Some(failed.into())))
        }
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "this method runs single-threaded"
    )]
    fn convert_graph_data(
        &self,
        display_data: &mut GraphDisplayData,
        failed: &mut Vec<ErrorRecord>,
        iricache: &mut HashMap<usize, usize>,
        inverse_edge_indices: &mut HashMap<usize, usize>,
    ) -> Result<(), SerializationError> {
        let mut label_buffer = self.label_buffer.write()?;
        let mut node_element_buffer = self.node_element_buffer.write()?;
        for (term_id, element) in take(&mut *node_element_buffer) {
            let label = label_buffer.remove(&term_id);
            if label.is_none() && !self.term_index.is_blank_node(term_id)? {
                let msg = match self.term_index.get(term_id) {
                    Ok(term) => {
                        format!("Label not found for term '{term}'. Using None")
                    }
                    Err(e) => {
                        format!("Label not found for term '{e}'. Using None")
                    }
                };
                debug!("{msg}");
            }
            iricache.insert(term_id, display_data.elements.len());
            display_data.labels.push(label.flatten());
            display_data.elements.push(element);
        }

        let mut edge_label_buffer = self.edge_label_buffer.write()?;
        let mut edge_characteristics = self.edge_characteristics.write()?;
        let mut edge_cardinality_buffer = self.edge_cardinality_buffer.write()?;
        for edge in self.edge_buffer.read()?.iter() {
            let subject_idx = iricache.get(&edge.domain_term_id);
            let object_idx = iricache.get(&edge.range_term_id);
            let maybe_label = edge_label_buffer.remove(edge).flatten();
            let characteristics = edge_characteristics.remove(edge);
            let cardinality = edge_cardinality_buffer.remove(edge);

            match (subject_idx, object_idx) {
                (Some(subject_idx), Some(object_idx)) => {
                    let edge_idx =
                        if edge.edge_type == ElementType::Owl(OwlType::Edge(OwlEdge::InverseOf)) {
                            let Some(property_id) = edge.property_term_id else {
                                let msg = format!("Edge is missing merged property id\n{edge}");
                                failed.push(<SerializationError as Into<ErrorRecord>>::into(
                                    SerializationErrorKind::MissingProperty(msg).into(),
                                ));
                                continue;
                            };

                            if let Some(existing_idx) = inverse_edge_indices.get(&property_id) {
                                *existing_idx
                            } else {
                                display_data.elements.push(edge.edge_type);
                                display_data.labels.push(maybe_label.clone());
                                let new_idx = display_data.elements.len() - 1;
                                inverse_edge_indices.insert(property_id, new_idx);
                                new_idx
                            }
                        } else {
                            display_data.elements.push(edge.edge_type);
                            display_data.labels.push(maybe_label.clone());
                            display_data.elements.len() - 1
                        };

                    display_data
                        .edges
                        .push([*subject_idx, edge_idx, *object_idx]);

                    if let Some(characteristics) = characteristics {
                        display_data
                            .characteristics
                            .insert(edge_idx, characteristics);
                    }

                    if let Some(cardinality) = cardinality {
                        let display_edge_idx = u32::try_from(display_data.edges.len() - 1)
                            .map_err(|_| {
                                SerializationErrorKind::SerializationFailed(format!(
                                    "Cardinality edge index overflow ({}/{})",
                                    display_data.edges.len() - 1,
                                    u32::MAX
                                ))
                            })?;
                        display_data
                            .cardinalities
                            .push((display_edge_idx, cardinality));
                    }
                }
                (None, _) => {
                    let msg = "Domain of edge not found in iricache".to_string();
                    failed.push(<SerializationError as Into<ErrorRecord>>::into(
                        SerializationErrorKind::MissingDomain(
                            self.term_index.display_edge(edge)?,
                            msg,
                        )
                        .into(),
                    ));
                }
                (_, None) => {
                    let msg = "Range of edge not found in iricache".to_string();
                    failed.push(<SerializationError as Into<ErrorRecord>>::into(
                        SerializationErrorKind::MissingRange(
                            self.term_index.display_edge(edge)?,
                            msg,
                        )
                        .into(),
                    ));
                }
            }
        }

        let mut node_characteristics = self.node_characteristics.write()?;
        for (term_id, characteristics) in take(&mut *node_characteristics) {
            let idx = iricache.get(&term_id);
            if let Some(idx) = idx {
                display_data.characteristics.insert(*idx, characteristics);
            } else {
                let msg = match self.term_index.get(term_id) {
                    Ok(term) => {
                        format!("Characteristic not found for term '{term}' in iricache")
                    }
                    Err(e) => {
                        format!("Characteristic not found for term '{e}' in iricache")
                    }
                };
                debug!("{msg}");
            }
        }

        let mut individual_count_buffer = self.individual_count_buffer.write()?;
        for (term_id, count) in take(&mut *individual_count_buffer) {
            if let Some(idx) = iricache.get(&term_id) {
                display_data.individual_counts.insert(*idx, count);
            }
        }
        Ok(())
    }

    #[expect(
        clippy::significant_drop_in_scrutinee,
        reason = "this method runs single-threaded"
    )]
    fn convert_metadata(
        &self,
        display_data: &mut GraphDisplayData,
        failed: &mut Vec<ErrorRecord>,
        iricache: &HashMap<usize, usize>,
    ) -> Result<(), SerializationError> {
        let mut metadata_buffer = GraphMetadata::new();

        metadata_buffer.graph_header.document_base = self
            .document_base
            .read()?
            .clone()
            .map_or_else(String::new, |docbase| docbase.base);
        metadata_buffer.graph_header.title = {
            let value = self.document_base.read()?.clone();
            if let Some(docbase) = value {
                if let Ok(base_term_id) = self.term_index.get_id(&docbase.base_term) {
                    // Try getting title from title buffer first
                    let maybe_title = match self
                        .metadata
                        .element_metadata
                        .write()?
                        .remove(&base_term_id)
                    {
                        Some(metadata_type) => {
                            if let Ok(metadata_term_id) =
                                self.term_index
                                    .get_id(&<Term as Into<ArcTerm>>::into(dcterms::TITLE.into()))
                            {
                                metadata_type.get(&metadata_term_id).map(|tagged_metadata| {
                                    HashMap::from_iter(tagged_metadata.iter().map(
                                        |(lang_tag, content)| {
                                            (
                                                lang_tag.to_string(),
                                                self.translate_metadata_content(content),
                                            )
                                        },
                                    ))
                                })
                            } else if let Ok(metadata_term_id) = self
                                .term_index
                                .get_id(&<Term as Into<ArcTerm>>::into(dc::TITLE.into()))
                            {
                                // metadata_type.get(&metadata_term_id)
                                None
                            } else {
                                None
                            }
                        }
                        None => None,
                    };

                    match maybe_title {
                        Some(title) => title,
                        None => {
                            // Try getting title from label buffer
                            match self.label_buffer.read()?.get(&base_term_id) {
                                Some(Some(label)) => {
                                    let mut map = HashMap::new();
                                    map.insert("".to_string(), vec![label.clone()]);
                                    map
                                }
                                Some(None) => {
                                    // No label declared
                                    let msg = "Ontology title not found in ontology.".to_string();

                                    debug!("{msg}");
                                    failed.push(
                                        SerializationErrorKind::SerializationWarning(msg).into(),
                                    );
                                    let mut map = HashMap::new();
                                    map.insert("".to_string(), vec![msg]);
                                    map
                                }
                                None => {
                                    // No label found in buffer
                                    let msg =
                                        "Ontology title not found in label buffer".to_string();
                                    debug!("{msg}");
                                    failed.push(
                                        SerializationErrorKind::SerializationWarning(msg.clone())
                                            .into(),
                                    );
                                    let mut map = HashMap::new();
                                    map.insert("".to_string(), vec![msg]);
                                    map
                                }
                            }
                        }
                    }
                } else {
                    let msg = format!(
                        "Failed to create ontology title: Term id for document base '{}' not found in term index",
                        docbase.base_term
                    );
                    debug!("{msg}");
                    failed.push(SerializationErrorKind::TermIndexError(msg.clone()).into());
                    let mut map = HashMap::new();
                    map.insert("".to_string(), vec![msg]);
                    map
                }
            } else {
                let mut map = HashMap::new();
                map.insert("".to_string(), vec![String::new()]);
                map
            }
        };
        metadata_buffer.graph_header.description = {
            let value = self.document_base.read()?.clone();
            if let Some(docbase) = value {
                if let Ok(base_term_id) = self.term_index.get_id(&docbase.base_term) {
                    // Try getting description from description buffer first
                    match self.metadata.description.write()?.remove(&base_term_id) {
                        Some(descriptions) => descriptions
                            .into_iter()
                            .map(|desc_term_id| {
                                self.term_index
                                    .get(desc_term_id)
                                    .map_or_else(|e| e.to_string(), |term| term.to_string())
                            })
                            .collect::<Vec<_>>(),
                        None => {
                            // Try getting description from comment buffer
                            self.metadata
                                .comment
                                .write()?
                                .remove(&base_term_id)
                                .map_or_else(Vec::new, |comments| {
                                    comments
                                        .into_iter()
                                        .map(|comment_term_id| {
                                            self.term_index.get(comment_term_id).map_or_else(
                                                |e| e.to_string(),
                                                |term| term.to_string(),
                                            )
                                        })
                                        .collect::<Vec<_>>()
                                })
                        }
                    }
                } else {
                    let msg = format!(
                        "Failed to create ontology description: Term id for document base '{}' not found in term index",
                        docbase.base_term
                    );
                    debug!("{msg}");
                    failed.push(SerializationErrorKind::TermIndexError(msg.clone()).into());
                    Vec::from([msg])
                }
            } else {
                Vec::new()
            }
        };
        metadata_buffer.graph_header.creator = {
            let maybe_docbase = self.document_base.read()?.clone();
            match maybe_docbase {
                Some(docbase) => {
                    let base_term_id = self.term_index.get_id(&docbase.base_term)?;
                    self.metadata
                        .creator
                        .write()?
                        .remove(&base_term_id)
                        .map_or_else(Vec::new, |creators| {
                            creators
                                .iter()
                                .map(|creator_term_id| {
                                    self.term_index
                                        .get(*creator_term_id)
                                        .map_or_else(|e| e.to_string(), |term| term.to_string())
                                })
                                .collect::<Vec<_>>()
                        })
                }
                None => Vec::new(),
            }
        };
        metadata_buffer.graph_header.contributor = {
            let maybe_docbase = self.document_base.read()?.clone();
            match maybe_docbase {
                Some(docbase) => {
                    let base_term_id = self.term_index.get_id(&docbase.base_term)?;
                    self.metadata
                        .contributor
                        .write()?
                        .remove(&base_term_id)
                        .map_or_else(Vec::new, |contributors| {
                            contributors
                                .iter()
                                .map(|contributor_term_id| {
                                    self.term_index
                                        .get(*contributor_term_id)
                                        .map_or_else(|e| e.to_string(), |term| term.to_string())
                                })
                                .collect::<Vec<_>>()
                        })
                }
                None => Vec::new(),
            }
        };
        metadata_buffer.graph_header.version_iri = {
            /// Try getting version iri from version iri buffer
            fn get_version_iri(
                self_metadata: &GraphMetadataBuffer,
                self_term_index: &TermIndex,
            ) -> Result<Option<String>, SerializationError> {
                let version_iri = Some(self_metadata.version_iri.read()?.map_or_else(
                    String::new,
                    |version_term_id| {
                        self_term_index
                            .get(version_term_id)
                            .map_or_else(|e| e.to_string(), |term| term.to_string())
                    },
                ));
                Ok(version_iri)
            }

            let value = self.document_base.read()?.clone();
            match value {
                Some(docbase) => match self.term_index.get_id(&docbase.base_term) {
                    Ok(base_term_id) => {
                        // Try getting version iri from version info buffer first
                        match self.metadata.version_info.write()?.remove(&base_term_id) {
                            Some(version_infos) => {
                                let version_info = version_infos
                                    .into_iter()
                                    .map(|version_info_term_id| {
                                        self.term_index
                                            .get(version_info_term_id)
                                            .map_or_else(|e| e.to_string(), |term| term.to_string())
                                    })
                                    .collect::<Vec<_>>()
                                    .join("\n");
                                Some(version_info)
                            }
                            None => get_version_iri(&self.metadata, &self.term_index)?,
                        }
                    }
                    Err(e) => {
                        let msg = format!(
                            "Failed to create ontology version_iri from version_info using document base '{}': {e}",
                            docbase.base_term
                        );
                        debug!("{msg}");
                        get_version_iri(&self.metadata, &self.term_index)?
                    }
                },
                None => get_version_iri(&self.metadata, &self.term_index)?,
            }
        };
        metadata_buffer.graph_header.prior_version = Some(
            self.metadata
                .prior_version
                .read()?
                .map_or_else(String::new, |prior_version_term_id| {
                    self.translate_term_with_fallback(prior_version_term_id)
                }),
        );
        metadata_buffer.graph_header.incompatible_with =
            Some(self.metadata.incompatible_with.read()?.map_or_else(
                String::new,
                |incompatible_with_term_id| {
                    self.translate_term_with_fallback(incompatible_with_term_id)
                },
            ));
        metadata_buffer.graph_header.backward_compatible_with =
            Some(self.metadata.backward_compatible_with.read()?.map_or_else(
                String::new,
                |backward_compatible_with_term_id| {
                    self.translate_term_with_fallback(backward_compatible_with_term_id)
                },
            ));
        self.convert_element_metadata(&mut metadata_buffer, iricache, failed)?;
        display_data.graph_metadata = metadata_buffer;
        Ok(())
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "this method runs single-threaded"
    )]
    fn convert_hashset_hashmap(
        &self,
        seralization_buffer: &Arc<RwLock<HashMap<usize, HashSet<usize>>>>,
        data_buffer: &mut HashMap<usize, Vec<String>>,
        iricache: &HashMap<usize, usize>,
        failed: &mut Vec<ErrorRecord>,
        data_type: &str,
    ) -> Result<(), SerializationError> {
        let mut buffer = seralization_buffer.write()?;
        for (term_id, term_id_set) in take(&mut *buffer) {
            if let Some(term_idx) = iricache.get(&term_id) {
                data_buffer.insert(
                    *term_idx,
                    term_id_set
                        .into_iter()
                        .map(|term_id_in_set| self.translate_term_with_fallback(term_id_in_set))
                        .collect(),
                );
            } else {
                let msg = self.term_index.get(term_id).map_or_else(
                    |e| {
                        format!(
                            "Failed to map {data_type}: Subject term '{e}' not found in iricache"
                        )
                    },
                    |term| {
                        format!(
                            "Failed to map {data_type}: Subject term '{term}' not found in iricache"
                        )
                    },
                );
                debug!("{msg}");
                failed.push(SerializationErrorKind::SerializationWarning(msg).into());
            }
        }
        Ok(())
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "this method runs single-threaded"
    )]
    fn convert_element_metadata(
        &self,
        metadata_buffer: &mut GraphMetadata,
        iricache: &HashMap<usize, usize>,
        failed: &mut Vec<ErrorRecord>,
    ) -> Result<(), SerializationError> {
        let mut term_cache: HashMap<ArcTerm, Arc<String>> = HashMap::new();
        let mut get_term_string = |term: ArcTerm| {
            term_cache
                .entry(term)
                .or_insert_with_key(|key| trim_tag_circumfix(&key.to_string()).into())
                .clone()
        };

        let mut buffer = self.metadata.element_metadata.write()?;
        for (term_id, metadata_types) in take(&mut *buffer) {
            if let Some(term_idx) = iricache.get(&term_id) {
                for (metadata_term_id, tagged_metadata) in metadata_types {
                    match self.term_index.get(metadata_term_id) {
                        Ok(metadata_term) => {
                            let tagged_metadata_entry = metadata_buffer
                                .metadata_type
                                .entry(*term_idx)
                                .or_default()
                                .entry(get_term_string(metadata_term))
                                .or_default();
                            for (lang_tag, content) in tagged_metadata {
                                tagged_metadata_entry
                                    .entry(lang_tag)
                                    .or_insert_with(|| self.translate_metadata_content(&content));
                            }
                        }
                        Err(e) => failed.push(e.into()),
                    }
                }
            } else {
                for metadata_term_id in metadata_types.keys() {
                    let metadata_term = self.translate_term_with_fallback(*metadata_term_id);
                    let msg = self.term_index.get(term_id).map_or_else(
                    |e| {
                        format!(
                            "Failed to map metadata term '{metadata_term}': Subject term '{e}' not found in iricache"
                        )
                    },
                    |term| {
                        format!(
                            "Failed to map metadata term '{metadata_term}': Subject term '{term}' not found in iricache"
                        )
                    },
                );
                    debug!("{msg}");
                    failed.push(SerializationErrorKind::SerializationWarning(msg).into());
                }
            }
        }
        Ok(())
    }

    fn translate_metadata_content(&self, content: &HashSet<TermID>) -> Vec<String> {
        content
            .iter()
            .map(|content_term_id| self.translate_term_with_fallback(*content_term_id))
            .collect()
    }

    fn translate_term_with_fallback(&self, term_id: usize) -> String {
        self.term_index
            .get(term_id)
            .map_or_else(|e| e.to_string(), |term| term.to_string())
    }
}

impl Display for SerializationDataBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "SerializationDataBuffer {{")?;

        writeln!(
            f,
            "\tdocument_base: {}",
            self.document_base
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
                .map_or_else(String::new, |docbase| docbase.base)
        )?;
        writeln!(f, "\tnode_element_buffer:")?;
        for (term_id, element) in self
            .node_element_buffer
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            let term = self
                .term_index
                .get(*term_id)
                .map_or_else(|e| e.to_string(), |term| term.to_string());
            writeln!(f, "\t\t{term} : {element}")?;
        }
        writeln!(f, "\tedge_element_buffer:")?;
        for (term_id, element) in self
            .edge_element_buffer
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            let term = self
                .term_index
                .get(*term_id)
                .map_or_else(|e| e.to_string(), |term| term.to_string());
            writeln!(f, "\t\t{term} : {element}")?;
        }
        writeln!(f, "\tedge_redirection:")?;
        for (term_id, subject_term_id) in self
            .edge_redirection
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            let term = self
                .term_index
                .get(*term_id)
                .map_or_else(|e| e.to_string(), |term| term.to_string());
            let subject_term = self
                .term_index
                .get(*subject_term_id)
                .map_or_else(|e| e.to_string(), |term| term.to_string());
            writeln!(f, "\t\t{term} -> {subject_term}")?;
        }
        writeln!(f, "\tedges_include_map: ")?;
        for (term_id, edges) in self
            .edges_include_map
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            let term = self
                .term_index
                .get(*term_id)
                .map_or_else(|e| e.to_string(), |term| term.to_string());
            writeln!(f, "\t\t{term} : {{")?;
            for edge in edges {
                let display_edge = self
                    .term_index
                    .display_edge(edge)
                    .unwrap_or_else(|e| e.to_string());

                writeln!(f, "\t\t\t{display_edge}")?;
            }
            writeln!(f, "\t\t}}")?;
        }
        writeln!(f, "\tlabel_buffer:")?;
        for (term_id, label) in self
            .label_buffer
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            let term = self
                .term_index
                .get(*term_id)
                .map_or_else(|e| e.to_string(), |term| term.to_string());
            writeln!(f, "\t\t{term} : {label:?}")?;
        }
        writeln!(f, "\tedge_buffer:")?;
        for edge in self
            .edge_buffer
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            let display_edge = self
                .term_index
                .display_edge(edge)
                .unwrap_or_else(|e| e.to_string());
            writeln!(f, "\t\t{display_edge}")?;
        }

        writeln!(f, "\tedge_characteristics:")?;
        for (edge, characteristics) in self
            .edge_characteristics
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            let display_edge = self
                .term_index
                .display_edge(edge)
                .unwrap_or_else(|e| e.to_string());
            writeln!(f, "\t\t{display_edge}\n\t\t\t{characteristics:?}")?;
        }

        writeln!(f, "\tnode_characteristics:")?;
        for (term_id, characteristics) in self
            .node_characteristics
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            let term = self
                .term_index
                .get(*term_id)
                .map_or_else(|e| e.to_string(), |term| term.to_string());
            writeln!(f, "\t\t{term}\n\t\t\t{characteristics:?}")?;
        }

        writeln!(f, "\tindividual_count_buffer:")?;
        for (term_id, individual_count) in self
            .individual_count_buffer
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            let term = self
                .term_index
                .get(*term_id)
                .map_or_else(|e| e.to_string(), |term| term.to_string());
            writeln!(
                f,
                "\t\t{} : {} individual{}",
                term,
                individual_count,
                if *individual_count == 1 { "" } else { "s" }
            )?;
        }

        writeln!(f, "\tunknown_buffer:")?;
        for (term_id, triples) in self
            .unknown_buffer
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            let term = self
                .term_index
                .get(*term_id)
                .map_or_else(|e| e.to_string(), |term| term.to_string());
            write!(f, "\t\t{term} : ")?;
            for triple in triples {
                let display_triple = self
                    .term_index
                    .display_triple(triple)
                    .unwrap_or_else(|e| e.to_string());
                writeln!(f, "\t\t\t{display_triple}")?;
            }
        }
        // Not needed as it's displayed by the serializer
        // writeln!(f, "\tfailed_buffer:")?;
        // writeln!(f, "{}", ErrorRecord::format_records(&self.failed_buffer))?;
        write!(f, "{}", self.metadata)?;
        writeln!(f, "}}")
    }
}

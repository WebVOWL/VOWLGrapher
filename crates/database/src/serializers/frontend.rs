use std::{
    collections::{HashMap, HashSet},
    mem::take,
    num::NonZero,
    sync::{Arc, RwLock},
    thread::available_parallelism,
    time::Instant,
};

use super::{Edge, RestrictionRenderMode, SerializationDataBuffer, Triple};
use crate::{
    errors::{SerializationError, SerializationErrorKind},
    serializers::{
        ArcEdge, ArcTerm, ArcTriple, RestrictionState,
        index::TermIndex,
        util::{
            PROPERTY_EDGE_TYPES, can_upgrade_node_type, is_query_fallback_endpoint, is_reserved,
            is_restriction_owner_edge, is_structural_set_node, is_synthetic, merge_optional_labels,
            synthetic::{
                SYNTH_LITERAL, SYNTH_LITERAL_VALUE, SYNTH_LOCAL_LITERAL, SYNTH_LOCAL_THING,
                SYNTH_THING,
            },
            synthetic_iri, trim_tag_circumfix, try_resolve_reserved,
        },
    },
    vocab::{owl, rdf, rdfs, xsd},
};
use fluent_uri::Iri;
use futures::StreamExt;
use grapher::prelude::{
    Characteristic, ElementType, GraphDisplayData, OwlEdge, OwlNode, OwlType, RdfEdge, RdfType,
    RdfsEdge, RdfsNode, RdfsType,
};
use log::{debug, error, info, trace, warn};
use oxrdf::Literal;
use rayon::ThreadPoolBuilder;
use rdf_fusion::{
    execution::results::{QuerySolution, QuerySolutionStream},
    model::{BlankNode, NamedNode, Term},
};

use unescape_zero_copy::unescape_default;
use vowlgrapher_parser::errors::VOWLGrapherStoreError;
use vowlgrapher_util::prelude::{ErrorRecord, VOWLGrapherError};

pub enum SerializationStatus {
    Serialized,
    Deferred,
}

#[derive(Default)]
pub struct GraphDisplayDataSolutionSerializer {
    document_base_warning_fired: Arc<RwLock<bool>>,
}

impl GraphDisplayDataSolutionSerializer {
    /// Create a new [`GraphDisplayDataSolutionSerializer`]
    pub fn new() -> Self {
        Self::default()
    }

    #[expect(unused, reason = "performance currently less than single-threaded")]
    /// Serializes a query solution stream into the data buffer using all available threads.
    ///
    /// This method tries to continue serializing despite errors.
    /// As such, the `Ok` value contains non-fatal errors encountered during
    /// serialization. The `Err` value contains fatal errors, preventing serialization.
    pub async fn par_serialize_solution_stream(
        &self,
        data: &mut GraphDisplayData,
        mut solution_stream: QuerySolutionStream,
    ) -> Result<Option<VOWLGrapherError>, VOWLGrapherError> {
        let thread_count = available_parallelism()
            .unwrap_or(NonZero::new(1).unwrap())
            .into();

        info!("Serializing query solution stream using {thread_count} threads...");

        // TODO: Make a global threadpool instead of making a new one for each call to this method.
        // Should prolly work together with PR #223.
        let pool = ThreadPoolBuilder::new()
            .num_threads(thread_count)
            .build()
            .unwrap();

        let mut count: u64 = 0;
        let mut data_buffer = SerializationDataBuffer::new();
        let mut query_time = None;
        let start_time = Instant::now();

        while let Some(maybe_solution) = solution_stream.next().await {
            if query_time.is_none() {
                query_time = Some(Instant::now());
            }

            let solution = match maybe_solution {
                Ok(solution) => solution,
                Err(e) => {
                    data_buffer
                        .failed_buffer
                        .write()
                        .map_err(|pe| {
                            <SerializationError as Into<VOWLGrapherError>>::into(pe.into())
                        })?
                        .push(<VOWLGrapherStoreError as Into<ErrorRecord>>::into(e.into()));
                    continue;
                }
            };

            pool.install(|| self.serialize_solution(solution, &mut data_buffer))?;

            count += 1;
        }

        // TODO: Parallelize this
        self.check_all_unknowns(&mut data_buffer).or_else(|e| {
            data_buffer
                .failed_buffer
                .write()
                .map_err(|pe| <SerializationError as Into<VOWLGrapherError>>::into(pe.into()))?
                .push(e.into());
            Ok::<(), VOWLGrapherError>(())
        })?;

        // Catch permanently unresolved triples
        for (term_id, triples) in data_buffer
            .unknown_buffer
            .write()
            .map_err(|pe| <SerializationError as Into<VOWLGrapherError>>::into(pe.into()))?
            .drain()
        {
            for triple in triples {
                let e: SerializationError = SerializationErrorKind::SerializationFailedTriple(
                    data_buffer.term_index.display_triple(&triple)?,
                    format!("Unresolved reference: could not map '{}'", term_id),
                )
                .into();
                data_buffer
                    .failed_buffer
                    .write()
                    .map_err(|pe| <SerializationError as Into<VOWLGrapherError>>::into(pe.into()))?
                    .push(e.into());
            }
        }

        let all_errors = self
            .post_serialization_cleanup(data, &mut data_buffer, start_time, query_time, count)
            .map_err(<SerializationError as Into<VOWLGrapherError>>::into)?;

        Ok(all_errors)
    }

    /// Serializes a query solution stream into the data buffer.
    ///
    /// This method tries to continue serializing despite errors.
    /// As such, the `Ok` value contains non-fatal errors encountered during
    /// serialization. The `Err` value contains fatal errors, preventing serialization.
    pub async fn serialize_solution_stream(
        &self,
        data: &mut GraphDisplayData,
        mut solution_stream: QuerySolutionStream,
    ) -> Result<Option<VOWLGrapherError>, VOWLGrapherError> {
        info!("Serializing query solution stream...");
        let mut count: u64 = 0;
        let mut data_buffer = SerializationDataBuffer::new();
        let mut query_time = None;
        let start_time = Instant::now();

        while let Some(maybe_solution) = solution_stream.next().await {
            if query_time.is_none() {
                query_time = Some(Instant::now());
            }

            let solution = match maybe_solution {
                Ok(solution) => solution,
                Err(e) => {
                    data_buffer
                        .failed_buffer
                        .write()
                        .map_err(|pe| {
                            <SerializationError as Into<VOWLGrapherError>>::into(pe.into())
                        })?
                        .push(<VOWLGrapherStoreError as Into<ErrorRecord>>::into(e.into()));
                    continue;
                }
            };

            self.serialize_solution(solution, &mut data_buffer)?;

            count += 1;
        }

        self.check_all_unknowns(&mut data_buffer).or_else(|e| {
            data_buffer
                .failed_buffer
                .write()
                .map_err(|pe| <SerializationError as Into<VOWLGrapherError>>::into(pe.into()))?
                .push(e.into());
            Ok::<(), VOWLGrapherError>(())
        })?;

        // Catch permanently unresolved triples
        for (term_id, triples) in data_buffer
            .unknown_buffer
            .write()
            .map_err(|pe| <SerializationError as Into<VOWLGrapherError>>::into(pe.into()))?
            .drain()
        {
            for triple in triples {
                let e: SerializationError = SerializationErrorKind::SerializationFailedTriple(
                    data_buffer.term_index.display_triple(&triple)?,
                    format!(
                        "Unresolved reference: could not map '{}'",
                        data_buffer.term_index.get(&term_id)?
                    ),
                )
                .into();
                data_buffer
                    .failed_buffer
                    .write()
                    .map_err(|pe| <SerializationError as Into<VOWLGrapherError>>::into(pe.into()))?
                    .push(e.into());
            }
        }

        let all_errors = self
            .post_serialization_cleanup(data, &mut data_buffer, start_time, query_time, count)
            .map_err(<SerializationError as Into<VOWLGrapherError>>::into)?;

        Ok(all_errors)
    }

    /// Serializes one solution into the data buffer.
    fn serialize_solution(
        &self,
        solution: QuerySolution,
        data_buffer: &mut SerializationDataBuffer,
    ) -> Result<(), SerializationError> {
        let Some(subject_term) = solution.get("id") else {
            return Ok(());
        };

        // Label must be extracted between getting id and nodeType from solutions due to "continue" in the else clause.
        let subject_term_id = data_buffer.term_index.insert(subject_term.to_owned())?;
        self.extract_label(
            data_buffer,
            solution.get("label"),
            subject_term,
            &subject_term_id,
        )?;

        let Some(node_type_term) = solution.get("nodeType") else {
            return Ok(());
        };

        let predicate_term_id = data_buffer.term_index.insert(node_type_term.to_owned())?;
        let object_term_id = match solution.get("target") {
            Some(term) => Some(data_buffer.term_index.insert(term.to_owned())?),
            None => None,
        };

        let triple = self.create_triple_from_id(
            &data_buffer.term_index,
            subject_term_id,
            Some(predicate_term_id),
            object_term_id,
        )?;

        self.write_node_triple(data_buffer, triple).or_else(|e| {
            data_buffer.failed_buffer.write()?.push(e.into());
            Ok::<SerializationStatus, SerializationError>(SerializationStatus::Serialized)
        })?;
        Ok(())
    }

    /// Performs post-serialization cleanup.
    ///
    /// Must be called exactly once in any serialization implementation.
    fn post_serialization_cleanup(
        &self,
        data: &mut GraphDisplayData,
        data_buffer: &mut SerializationDataBuffer,
        start_time: Instant,
        query_time: Option<Instant>,
        count: u64,
    ) -> Result<Option<VOWLGrapherError>, SerializationError> {
        let (element_count, edge_count, label_count, cardinality_count, characteristics_count) = {
            (
                data_buffer.node_element_buffer.read()?.len(),
                data_buffer.edge_buffer.read()?.len(),
                data_buffer.label_buffer.read()?.len(),
                data_buffer.edge_cardinality_buffer.read()?.len(),
                data_buffer.edge_characteristics.read()?.len()
                    + data_buffer.node_characteristics.read()?.len(),
            )
        };

        debug!("{}", data_buffer);
        let serializer_errors = if !data_buffer.failed_buffer.read()?.is_empty() {
            let mut failed_buffer = data_buffer.failed_buffer.write()?;
            let total = failed_buffer.len();
            let err: VOWLGrapherError = take(&mut *failed_buffer).into();
            error!(
                "Failed to serialize {} triple{}:\n{}",
                total,
                if total != 1 { "s" } else { "" },
                err
            );
            Some(err)
        } else {
            None
        };
        let (converted, convert_errors) = data_buffer.convert_into()?;
        *data = converted;
        debug!("{}", data);

        let all_errors = match (serializer_errors, convert_errors) {
            (Some(mut e), Some(mut ce)) => {
                let ue = take(&mut e.records)
                    .into_iter()
                    .chain(take(&mut ce.records))
                    .collect::<Vec<_>>();
                Some(<Vec<ErrorRecord> as Into<VOWLGrapherError>>::into(ue))
            }
            (Some(e), None) => Some(e),
            (None, Some(ce)) => Some(ce),
            (None, None) => None,
        };

        let finish_time = Instant::now()
            .checked_duration_since(start_time)
            .unwrap_or_default()
            .as_secs_f32();
        let query_finish_time = if let Some(qtime) = query_time {
            qtime
                .checked_duration_since(start_time)
                .unwrap_or_default()
                .as_secs_f32()
        } else {
            0.0
        };
        info!(
            "Serialization completed\n \
            \tQuery execution time: {:.5} s\n \
            \tSerialization time  : {:.5} s\n \
            \tTotal solutions     : {count}\n \
            \tElements            : {}\n \
            \tEdges               : {}\n \
            \tLabels              : {}\n \
            \tCardinalities       : {}\n \
            \tCharacteristics     : {}\n\n \
        ",
            query_finish_time,
            finish_time - query_finish_time,
            element_count,
            edge_count,
            label_count,
            cardinality_count,
            characteristics_count
        );

        Ok(all_errors)
    }

    /// Extract label info from the query solution and store until
    /// they can be mapped to their ElementType.
    fn extract_label(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        maybe_label: Option<&Term>,
        term: &Term,
        term_id: &usize,
    ) -> Result<(), SerializationError> {
        // Prevent overriding labels
        if data_buffer.label_buffer.read()?.contains_key(term_id) {
            return Ok(());
        }

        match maybe_label {
            // Case 1: Label is a rdfs:label OR rdfs:Resource OR rdf:ID
            Some(label) => {
                let str_label = label.to_string();

                // Handle cases where label is: "Some Label"@en or contains "
                let split_label = str_label.split_inclusive("\"").collect::<Vec<_>>();
                let clean_label = if split_label.len() > 2 {
                    let joined_label = split_label[0..split_label.len() - 1].join("");
                    let stripped_label = joined_label
                        .strip_prefix("\"")
                        .and_then(|sub_str| sub_str.strip_suffix("\""))
                        .unwrap_or_else(|| &joined_label);

                    // Unescape string sequences like "\"" into """
                    unescape_default(stripped_label)
                        .unwrap_or_default()
                        .to_string()
                } else {
                    str_label
                };

                if !clean_label.is_empty() {
                    trace!("Inserting label '{clean_label}' for term '{}'", term);
                    data_buffer
                        .label_buffer
                        .write()?
                        .insert(*term_id, Some(clean_label));
                } else {
                    debug!("Empty label detected for term '{}'", term);
                }
            }
            // Case 2: Try parsing the term
            None => {
                let iri = term.to_string();
                match Iri::parse(trim_tag_circumfix(&iri)) {
                    // Case 2.1: Look for fragments in the iri
                    Ok(parsed_iri) => match parsed_iri.fragment() {
                        Some(frag) => {
                            trace!("Inserting fragment '{frag}' as label for iri '{}'", term);
                            data_buffer
                                .label_buffer
                                .write()?
                                .insert(*term_id, Some(frag.to_string()));
                        }
                        // Case 2.2: Look for path in iri
                        None => {
                            debug!("No fragment found in iri '{iri}'");
                            match parsed_iri.path().rsplit_once('/') {
                                Some(path) => {
                                    trace!(
                                        "Inserting path '{}' as label for iri '{}'",
                                        path.1, term
                                    );
                                    data_buffer
                                        .label_buffer
                                        .write()?
                                        .insert(*term_id, Some(path.1.to_string()));
                                }
                                None => {
                                    debug!("No path found in iri '{iri}'");
                                }
                            }
                        }
                    },
                    Err(e) => {
                        // Do not make a 'warn!'. A parse error is allowed to happen (e.g. on blank nodes).
                        trace!("Failed to parse iri '{}':\n{:?}", iri, e);
                    }
                }
            }
        };
        Ok(())
    }

    /// Returns the term if its element type is known.
    fn resolve(
        &self,
        data_buffer: &SerializationDataBuffer,
        term_id: usize,
    ) -> Result<Option<usize>, SerializationError> {
        let resolved = self.follow_redirection(data_buffer, term_id)?;

        if let Some(elem) = data_buffer.node_element_buffer.read()?.get(&resolved) {
            trace!(
                "Resolved: {}: {}",
                data_buffer.term_index.get(&resolved)?,
                elem
            );
            return Ok(Some(resolved));
        }

        if let Some(elem) = data_buffer.edge_element_buffer.read()?.get(&resolved) {
            trace!(
                "Resolved: {}: {}",
                data_buffer.term_index.get(&resolved)?,
                elem
            );
            return Ok(Some(resolved));
        }
        Ok(None)
    }

    /// Returns the subject and object of the triple if their element type is known.
    fn resolve_so(
        &self,
        data_buffer: &SerializationDataBuffer,
        triple: &ArcTriple,
    ) -> Result<(Option<usize>, Option<usize>), SerializationError> {
        let resolved_subject = self.resolve(data_buffer, triple.subject_term_id)?;
        let resolved_object = match &triple.object_term_id {
            Some(target) => self.resolve(data_buffer, *target)?,
            None => {
                debug!(
                    "Cannot resolve object of triple:\n {}",
                    data_buffer.term_index.display_triple(triple)?
                );
                None
            }
        };
        Ok((resolved_subject, resolved_object))
    }

    /// Add subject of triple to the element buffer.
    ///
    /// In the future, this function will handle cases where an element
    /// identifies itself as multiple elements. E.g. an element is both an rdfs:Class and a owl:class.
    fn add_triple_to_element_buffer(
        &self,
        term_index: &TermIndex,
        element_buffer: &mut Arc<RwLock<HashMap<usize, ElementType>>>,
        triple: &ArcTriple,
        element_type: ElementType,
    ) -> Result<(), SerializationError> {
        self.add_term_to_element_buffer(
            term_index,
            element_buffer,
            triple.subject_term_id,
            element_type,
        )
    }

    /// Add a term id to a node/edge element buffer.
    fn add_term_to_element_buffer(
        &self,
        term_index: &TermIndex,
        element_buffer: &mut Arc<RwLock<HashMap<usize, ElementType>>>,
        term_id: usize,
        element_type: ElementType,
    ) -> Result<(), SerializationError> {
        if let Some(element) = element_buffer.write()?.insert(term_id, element_type) {
            warn!(
                "Registered '{}' to subject '{}' already registered as '{}'",
                element_type,
                term_index.get(&term_id)?,
                element
            );
        } else {
            trace!(
                "Adding to element buffer: {}: {}",
                term_index.get(&term_id)?,
                element_type
            );
        }
        Ok(())
    }

    /// Add an IRI to the unresolved, unknown buffer.
    fn add_to_unknown_buffer(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        term_id: usize,
        triple: ArcTriple,
    ) -> Result<(), SerializationError> {
        trace!(
            "Adding to unknown buffer: {}: {}",
            data_buffer.term_index.get(&term_id)?,
            data_buffer.term_index.display_triple(&triple)?
        );

        data_buffer
            .unknown_buffer
            .write()?
            .entry(term_id)
            .or_default()
            .insert(triple);

        Ok(())
    }

    /// Insert an edge into the element's edge set.
    fn insert_edge_include(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        term_id: usize,
        edge: ArcEdge,
    ) -> Result<(), SerializationError> {
        data_buffer
            .edges_include_map
            .write()?
            .entry(term_id)
            .or_default()
            .insert(edge);
        Ok(())
    }

    fn redirect_iri(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        old_term_id: usize,
        new_term_id: usize,
    ) -> Result<(), SerializationError> {
        debug!(
            "Redirecting '{}' to '{}'",
            data_buffer.term_index.get(&old_term_id)?,
            data_buffer.term_index.get(&new_term_id)?
        );
        {
            data_buffer
                .edge_redirection
                .write()?
                .insert(old_term_id, new_term_id);
        }
        self.check_unknown_buffer(data_buffer, &old_term_id)?;
        Ok(())
    }

    fn follow_redirection(
        &self,
        data_buffer: &SerializationDataBuffer,
        term_id: usize,
    ) -> Result<usize, SerializationError> {
        let mut current = term_id;

        let edge_redirection = data_buffer.edge_redirection.read()?;
        while let Some(next) = edge_redirection.get(&current) {
            if *next == current {
                break;
            }
            current = *next;
        }

        Ok(current)
    }

    fn check_unknown_buffer(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        term_id: &usize,
    ) -> Result<(), SerializationError> {
        let maybe_triples = { data_buffer.unknown_buffer.write()?.remove(term_id) };

        if let Some(triples) = maybe_triples {
            for triple in triples {
                self.write_node_triple(data_buffer, triple)?;
            }
        }
        Ok(())
    }

    fn insert_node(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        triple: ArcTriple,
        node_type: ElementType,
    ) -> Result<(), SerializationError> {
        if data_buffer
            .edge_redirection
            .read()?
            .contains_key(&triple.subject_term_id)
        {
            debug!(
                "Skipping insert_node for '{}': already redirected",
                data_buffer.term_index.get(&triple.subject_term_id)?
            );
            return Ok(());
        }

        let new_type = if self.is_external(
            data_buffer,
            &data_buffer.term_index.get(&triple.subject_term_id)?,
        )? {
            ElementType::Owl(OwlType::Node(OwlNode::ExternalClass))
        } else {
            node_type
        };

        self.add_triple_to_element_buffer(
            &data_buffer.term_index,
            &mut data_buffer.node_element_buffer,
            &triple,
            new_type,
        )?;
        self.check_unknown_buffer(data_buffer, &triple.subject_term_id)?;

        Ok(())
    }

    /// Inserts an edge triple into the serialization buffer,
    /// where subject and object are both nodes.
    ///
    /// Note that tuples or any triple where the subject is an edge iri,
    /// not present in the element buffer, will NEVER be resolved!
    fn insert_edge(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        triple: ArcTriple,
        edge_type: ElementType,
        label: Option<String>,
    ) -> Result<Option<ArcEdge>, SerializationError> {
        let predicate_term_id = data_buffer.get_predicate(&triple)?;

        let external_probe = if PROPERTY_EDGE_TYPES.contains(&edge_type) {
            &predicate_term_id
        } else {
            &triple.subject_term_id
        };

        // Skip external check for NoDraw and SubClassOf edges - they should always retain their type
        let new_type = if !matches!(
            edge_type,
            ElementType::NoDraw | ElementType::Rdfs(RdfsType::Edge(RdfsEdge::SubclassOf))
        ) && self
            .is_external(data_buffer, &data_buffer.term_index.get(external_probe)?)?
        {
            ElementType::Owl(OwlType::Edge(OwlEdge::ExternalProperty))
        } else {
            edge_type
        };

        match self.resolve_so(data_buffer, &triple)? {
            (Some(subject_term_id), Some(object_term_id)) => {
                let should_hash_property = [
                    ElementType::Owl(OwlType::Edge(OwlEdge::ObjectProperty)),
                    ElementType::Owl(OwlType::Edge(OwlEdge::DatatypeProperty)),
                    ElementType::Owl(OwlType::Edge(OwlEdge::DeprecatedProperty)),
                    ElementType::Owl(OwlType::Edge(OwlEdge::ExternalProperty)),
                ];
                let property_term_id = if should_hash_property.contains(&new_type) {
                    Some(predicate_term_id)
                } else {
                    None
                };
                let edge = self.create_edge_from_id(
                    &data_buffer.term_index,
                    subject_term_id,
                    new_type,
                    object_term_id,
                    property_term_id,
                )?;
                trace!("Inserting: {}", data_buffer.term_index.display_edge(&edge)?);

                data_buffer
                    .edge_element_buffer
                    .write()?
                    .insert(predicate_term_id, edge.edge_type);

                data_buffer.edge_buffer.write()?.insert(edge.clone());
                self.insert_edge_include(data_buffer, subject_term_id, edge.clone())?;
                self.insert_edge_include(data_buffer, object_term_id, edge.clone())?;

                data_buffer
                    .edge_label_buffer
                    .write()?
                    .insert(edge.clone(), label);
                return Ok(Some(edge));
            }
            (None, Some(_)) => {
                debug!(
                    "Cannot resolve subject of triple:\n {}",
                    data_buffer.term_index.display_triple(&triple)?
                );
                self.add_to_unknown_buffer(data_buffer, triple.subject_term_id, triple)?;
            }
            (Some(_), None) => {
                if let Some(object_term_id) = &triple.object_term_id {
                    // resolve_so already warns about unresolved object. No need to repeat it here.
                    self.add_to_unknown_buffer(data_buffer, *object_term_id, triple)?;
                }
            }
            _ => {
                debug!(
                    "Cannot resolve subject and object of triple:\n {}",
                    data_buffer.term_index.display_triple(&triple)?
                );
                self.add_to_unknown_buffer(data_buffer, triple.subject_term_id, triple)?;
            }
        }
        Ok(None)
    }

    fn iri_matches_document_base(base: &str, iri: &str) -> bool {
        iri == base
            || (!base.ends_with('/')
                && !base.ends_with('#')
                && iri.starts_with(&format!("{base}#")))
            || ((base.ends_with('/') || base.ends_with('#')) && iri.starts_with(base))
    }

    fn is_external(
        &self,
        data_buffer: &SerializationDataBuffer,
        term: &ArcTerm,
    ) -> Result<bool, SerializationError> {
        if term.is_blank_node() {
            return Ok(false);
        }

        let clean_term = trim_tag_circumfix(&term.to_string());
        match &*data_buffer.document_base.read()? {
            Some(base) => Ok(
                !(Self::iri_matches_document_base(base.as_ref(), &clean_term)
                    || is_reserved(term)
                    || is_synthetic(term)),
            ),
            None => {
                let has_fired = { *self.document_base_warning_fired.read()? };
                if !has_fired {
                    let msg = "Cannot determine externals: Missing document base!";
                    let e = SerializationErrorKind::MissingDocumentBase(msg.to_string());
                    warn!("{msg}");
                    data_buffer
                        .failed_buffer
                        .write()?
                        .push(<SerializationError as Into<ErrorRecord>>::into(e.into()));
                    *self.document_base_warning_fired.write()? = true;
                }
                Ok(false)
            }
        }
    }

    fn merge_nodes(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        old_term_id: usize,
        new_term_id: usize,
    ) -> Result<(), SerializationError> {
        if old_term_id == new_term_id {
            return Ok(());
        }

        debug!(
            "Merging node '{}' into '{}'",
            data_buffer.term_index.get(&old_term_id)?,
            data_buffer.term_index.get(&new_term_id)?
        );
        self.merge_restriction_state(data_buffer, old_term_id, new_term_id)?;
        {
            data_buffer
                .node_element_buffer
                .write()?
                .remove(&old_term_id);
        }
        self.update_edges(data_buffer, old_term_id, new_term_id)?;
        self.merge_individual_counts(data_buffer, &old_term_id, new_term_id)?;
        self.redirect_iri(data_buffer, old_term_id, new_term_id)?;
        Ok(())
    }

    fn merge_restriction_state(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        old_term_id: usize,
        new_term_id: usize,
    ) -> Result<(), SerializationError> {
        let mut restriction_buffer = data_buffer.restriction_buffer.write()?;

        let Some(old_state) = restriction_buffer.remove(&old_term_id) else {
            return Ok(());
        };

        let RestrictionState {
            on_property,
            filler,
            cardinality,
            self_restriction,
            requires_filler,
            render_mode,
        } = &*old_state.read()?;

        let mut new_state = restriction_buffer.entry(new_term_id).or_default().write()?;

        if new_state.on_property.is_none() {
            new_state.on_property = *on_property;
        }
        if new_state.filler.is_none() {
            new_state.filler = *filler;
        }
        if new_state.cardinality.is_none() {
            new_state.cardinality = cardinality.clone();
        }

        new_state.self_restriction |= self_restriction;
        new_state.requires_filler |= requires_filler;

        if render_mode.priority() > new_state.render_mode.priority() {
            new_state.render_mode = *render_mode;
        }
        Ok(())
    }

    fn update_edges(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        old_term_id: usize,
        new_term_id: usize,
    ) -> Result<(), SerializationError> {
        let old_edges = { data_buffer.edges_include_map.write()?.remove(&old_term_id) };
        if let Some(old_edges) = old_edges {
            debug!(
                "Updating edges from '{}' to '{}'",
                data_buffer.term_index.get(&old_term_id)?,
                data_buffer.term_index.get(&new_term_id)?
            );
            for old_edge in old_edges {
                let label = { data_buffer.edge_label_buffer.write()?.remove(&old_edge) };
                let cardinality = {
                    data_buffer
                        .edge_cardinality_buffer
                        .write()?
                        .remove(&old_edge)
                };
                let characteristics =
                    { data_buffer.edge_characteristics.write()?.remove(&old_edge) };

                {
                    data_buffer.edge_buffer.write()?.remove(&old_edge);
                }

                if old_edge.domain_term_id != old_term_id {
                    self.remove_edge_include(data_buffer, &old_edge.domain_term_id, &old_edge)?;
                }
                if old_edge.range_term_id != old_term_id {
                    self.remove_edge_include(data_buffer, &old_edge.range_term_id, &old_edge)?;
                }

                let is_degenerate_structural_edge = old_edge.domain_term_id
                    == old_edge.range_term_id
                    && matches!(
                        old_edge.edge_type,
                        ElementType::NoDraw
                            | ElementType::Rdfs(RdfsType::Edge(RdfsEdge::SubclassOf))
                    );

                if is_degenerate_structural_edge {
                    debug!(
                        "Dropping degenerate structural self-edge: {}",
                        data_buffer.term_index.display_edge(&old_edge)?
                    );
                    continue;
                }

                let new_domain_term_id = if old_edge.domain_term_id == old_term_id {
                    new_term_id
                } else {
                    old_edge.domain_term_id
                };
                let new_range_term_id = if old_edge.range_term_id == old_term_id {
                    new_term_id
                } else {
                    old_edge.range_term_id
                };
                let new_edge = self.create_edge_from_id(
                    &data_buffer.term_index,
                    new_domain_term_id,
                    old_edge.edge_type,
                    new_range_term_id,
                    old_edge.property_term_id,
                )?;

                {
                    data_buffer.edge_buffer.write()?.insert(new_edge.clone());
                }
                self.insert_edge_include(data_buffer, new_term_id, new_edge.clone())?;
                if let Some(label) = label {
                    data_buffer
                        .edge_label_buffer
                        .write()?
                        .insert(new_edge.clone(), label);
                }
                if let Some(cardinality) = cardinality {
                    data_buffer
                        .edge_cardinality_buffer
                        .write()?
                        .insert(new_edge.clone(), cardinality);
                }
                if let Some(characteristics) = characteristics {
                    data_buffer
                        .edge_characteristics
                        .write()?
                        .insert(new_edge.clone(), characteristics);
                }

                {
                    let mut property_edge_map = data_buffer.property_edge_map.write()?;
                    for mapped_edge in property_edge_map.values_mut() {
                        if *mapped_edge == old_edge {
                            *mapped_edge = new_edge.clone();
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn upgrade_node_type(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        term_id: usize,
        new_element: ElementType,
    ) -> Result<(), SerializationError> {
        let maybe_old_element_type = {
            data_buffer
                .node_element_buffer
                .read()?
                .get(&term_id)
                .copied()
        };
        match maybe_old_element_type {
            Some(old_elem) => {
                if can_upgrade_node_type(old_elem, new_element) {
                    self.add_term_to_element_buffer(
                        &data_buffer.term_index,
                        &mut data_buffer.node_element_buffer,
                        term_id,
                        new_element,
                    )?;
                }
                debug!(
                    "Upgraded subject '{}' from {} to {}",
                    data_buffer.term_index.get(&term_id)?,
                    old_elem,
                    new_element
                )
            }
            None => {
                let msg = format!(
                    "Upgraded unresolved subject '{}' to {}",
                    data_buffer.term_index.get(&term_id)?,
                    new_element
                );
                debug!("{msg}");
            }
        }
        Ok(())
    }

    fn has_named_equivalent_aliases(
        data_buffer: &SerializationDataBuffer,
        term_id: &usize,
    ) -> Result<bool, SerializationError> {
        for (alias, target) in data_buffer.edge_redirection.read()?.iter() {
            if target == term_id && data_buffer.term_index.is_named_node(alias)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn upgrade_deprecated_node_type(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        term_id: &usize,
    ) -> Result<(), SerializationError> {
        let old_elem_opt = {
            data_buffer
                .node_element_buffer
                .read()?
                .get(term_id)
                .copied()
        };
        match old_elem_opt {
            Some(old_elem)
                if matches!(
                    old_elem,
                    ElementType::Owl(OwlType::Node(
                        OwlNode::Class
                            | OwlNode::AnonymousClass
                            | OwlNode::DeprecatedClass
                            | OwlNode::ExternalClass
                    )) | ElementType::Rdfs(RdfsType::Node(RdfsNode::Class))
                ) =>
            {
                let new_element = ElementType::Owl(OwlType::Node(OwlNode::DeprecatedClass));
                self.add_term_to_element_buffer(
                    &data_buffer.term_index,
                    &mut data_buffer.node_element_buffer,
                    *term_id,
                    new_element,
                )?;
                debug!(
                    "Upgraded deprecated class '{}' from {} to {}",
                    data_buffer.term_index.get(term_id)?,
                    old_elem,
                    new_element
                );
            }
            Some(old_elem) => {
                let msg = format!(
                    "Skipping owl:Deprecated node upgrade for '{}': {} is not a class",
                    data_buffer.term_index.get(term_id)?,
                    old_elem
                );
                debug!("{msg}");
            }
            None => {
                let msg = format!(
                    "Cannot upgrade unresolved subject '{}' to DeprecatedClass",
                    data_buffer.term_index.get(term_id)?
                );
                debug!("{msg}");
            }
        }
        Ok(())
    }

    fn upgrade_property_type(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        property_term_id: &usize,
        new_element: ElementType,
    ) -> Result<(), SerializationError> {
        let old_elem_opt = {
            data_buffer
                .edge_element_buffer
                .read()?
                .get(property_term_id)
                .copied()
        };
        let Some(old_elem) = old_elem_opt else {
            let msg = format!(
                "Cannot upgrade unresolved property '{}' to {}",
                data_buffer.term_index.get(property_term_id)?,
                new_element
            );
            debug!("{msg}");

            return Ok(());
        };

        if !matches!(
            old_elem,
            ElementType::Owl(OwlType::Edge(
                OwlEdge::ObjectProperty
                    | OwlEdge::DatatypeProperty
                    | OwlEdge::DeprecatedProperty
                    | OwlEdge::ExternalProperty
            )) | ElementType::Rdf(RdfType::Edge(RdfEdge::RdfProperty))
        ) {
            let msg = format!(
                "Skipping owl:Deprecated property upgrade for '{}': {} is not a property",
                data_buffer.term_index.get(property_term_id)?,
                old_elem
            );
            debug!("{msg}");

            return Ok(());
        }

        self.add_term_to_element_buffer(
            &data_buffer.term_index,
            &mut data_buffer.edge_element_buffer,
            *property_term_id,
            new_element,
        )?;

        let maybe_old_edge = {
            data_buffer
                .property_edge_map
                .read()?
                .get(property_term_id)
                .cloned()
        };
        let Some(old_edge) = maybe_old_edge else {
            debug!(
                "Upgraded property '{}' from {} to {} before edge materialization",
                data_buffer.term_index.get(property_term_id)?,
                old_elem,
                new_element
            );
            return Ok(());
        };

        if old_edge.edge_type == ElementType::Owl(OwlType::Edge(OwlEdge::InverseOf)) {
            debug!(
                "Keeping merged inverse edge for '{}' as {} instead of downgrading it to {}",
                data_buffer.term_index.get(property_term_id)?,
                old_edge.edge_type,
                new_element
            );
            return Ok(());
        }

        let new_edge = self.create_edge_from_id(
            &data_buffer.term_index,
            old_edge.domain_term_id,
            new_element,
            old_edge.range_term_id,
            old_edge.property_term_id,
        )?;

        {
            let mut edge_buffer = data_buffer.edge_buffer.write()?;
            edge_buffer.remove(&old_edge);
            edge_buffer.insert(new_edge.clone());
        }

        {
            let mut edge_label_buffer = data_buffer.edge_label_buffer.write()?;
            let label = data_buffer
                .label_buffer
                .read()?
                .get(property_term_id)
                .cloned()
                .or_else(|| edge_label_buffer.remove(&old_edge));

            edge_label_buffer.insert(new_edge.clone(), label.flatten());
        }

        {
            let mut edge_characteristics = data_buffer.edge_characteristics.write()?;
            if let Some(characteristics) = edge_characteristics.remove(&old_edge) {
                edge_characteristics.insert(new_edge.clone(), characteristics);
            }
        }

        {
            let mut edges_include_map = data_buffer.edges_include_map.write()?;
            if let Some(edges) = edges_include_map.get_mut(&old_edge.domain_term_id) {
                edges.remove(&old_edge);
                edges.insert(new_edge.clone());
            }
            if let Some(edges) = edges_include_map.get_mut(&old_edge.range_term_id) {
                edges.remove(&old_edge);
                edges.insert(new_edge.clone());
            }
        }

        {
            data_buffer
                .property_edge_map
                .write()?
                .insert(*property_term_id, new_edge);
        }
        debug!(
            "Upgraded deprecated property '{}' from {} to {}",
            data_buffer.term_index.get(property_term_id)?,
            old_elem,
            new_element
        );
        Ok(())
    }

    fn remove_edge_include(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        element_id: &usize,
        edge: &ArcEdge,
    ) -> Result<(), SerializationError> {
        if let Some(edges) = data_buffer.edges_include_map.write()?.get_mut(element_id) {
            edges.remove(edge);
        }
        Ok(())
    }

    fn merge_properties(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        old_term_id: &usize,
        new_term_id: &usize,
    ) -> Result<(), SerializationError> {
        if old_term_id == new_term_id {
            return Ok(());
        }

        debug!(
            "Merging property '{}' into '{}'",
            data_buffer.term_index.get(old_term_id)?,
            data_buffer.term_index.get(new_term_id)?
        );

        {
            data_buffer.edge_element_buffer.write()?.remove(old_term_id);
        }

        // Remove stale node placeholders for property aliases.
        {
            data_buffer.node_element_buffer.write()?.remove(old_term_id);
        }
        {
            data_buffer.label_buffer.write()?.remove(old_term_id);
        }
        {
            data_buffer
                .node_characteristics
                .write()?
                .remove(old_term_id);
        }

        {
            let mut property_domain_map = data_buffer.property_domain_map.write()?;
            if let Some(domains) = property_domain_map.remove(old_term_id) {
                property_domain_map
                    .entry(*new_term_id)
                    .or_default()
                    .extend(domains);
            }
        }

        {
            let mut property_range_map = data_buffer.property_range_map.write()?;
            if let Some(ranges) = property_range_map.remove(old_term_id) {
                property_range_map
                    .entry(*new_term_id)
                    .or_default()
                    .extend(ranges);
            }
        }

        self.redirect_iri(data_buffer, *old_term_id, *new_term_id)?;
        Ok(())
    }

    fn normalize_inverse_endpoint(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        endpoint_term_id: &usize,
        opposite_term_id: &usize,
    ) -> Result<usize, SerializationError> {
        let Some(element_type) = ({
            data_buffer
                .node_element_buffer
                .read()?
                .get(endpoint_term_id)
                .copied()
        }) else {
            return Ok(*endpoint_term_id);
        };

        match element_type {
            ElementType::Owl(OwlType::Node(
                OwlNode::Complement
                | OwlNode::IntersectionOf
                | OwlNode::UnionOf
                | OwlNode::DisjointUnion
                | OwlNode::EquivalentClass,
            )) => self.get_or_create_anchor_thing(data_buffer, opposite_term_id),
            _ => Ok(*endpoint_term_id),
        }
    }

    fn inverse_edge_endpoints(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        property_term_id: &usize,
    ) -> Result<Option<(usize, usize)>, SerializationError> {
        let domain = {
            data_buffer
                .property_domain_map
                .read()?
                .get(property_term_id)
                .and_then(|domains| domains.iter().next())
                .copied()
        };
        let range = {
            data_buffer
                .property_range_map
                .read()?
                .get(property_term_id)
                .and_then(|ranges| ranges.iter().next())
                .copied()
        };

        match (&domain, &range) {
            (Some(domain), Some(range)) => {
                let subject = self.normalize_inverse_endpoint(data_buffer, domain, range)?;
                let object = self.normalize_inverse_endpoint(data_buffer, range, domain)?;
                Ok(Some((subject, object)))
            }
            _ => Ok(None),
        }
    }

    fn insert_inverse_of(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        triple: ArcTriple,
    ) -> Result<SerializationStatus, SerializationError> {
        let left_property_raw = triple.subject_term_id;
        let Some(right_property_raw) = triple.object_term_id else {
            let msg = format!(
                "owl:inverseOf triple is missing a target: {}",
                data_buffer.term_index.display_triple(&triple)?
            );
            let e = SerializationErrorKind::SerializationWarning(msg.to_string());
            warn!("{msg}");
            data_buffer
                .failed_buffer
                .write()?
                .push(<SerializationError as Into<ErrorRecord>>::into(e.into()));

            return Ok(SerializationStatus::Serialized);
        };

        let left_is_blank = data_buffer.term_index.is_blank_node(&left_property_raw)?;
        let right_is_blank = data_buffer.term_index.is_blank_node(&right_property_raw)?;

        match (left_is_blank, right_is_blank) {
            (true, false) => {
                self.ensure_object_property_registration(data_buffer, right_property_raw)?;
                self.merge_properties(data_buffer, &left_property_raw, &right_property_raw)?;
                return Ok(SerializationStatus::Serialized);
            }
            (false, true) => {
                self.ensure_object_property_registration(data_buffer, left_property_raw)?;
                self.merge_properties(data_buffer, &right_property_raw, &left_property_raw)?;
                return Ok(SerializationStatus::Serialized);
            }
            (true, true) => {
                self.add_to_unknown_buffer(data_buffer, left_property_raw, triple)?;
                return Ok(SerializationStatus::Deferred);
            }
            (false, false) => {}
        }

        self.ensure_object_property_registration(data_buffer, left_property_raw)?;
        self.ensure_object_property_registration(data_buffer, right_property_raw)?;

        let Some(left_property) = self.resolve(data_buffer, left_property_raw)? else {
            self.add_to_unknown_buffer(data_buffer, left_property_raw, triple)?;
            return Ok(SerializationStatus::Deferred);
        };

        let Some(right_property) = self.resolve(data_buffer, right_property_raw)? else {
            self.add_to_unknown_buffer(data_buffer, right_property_raw, triple)?;
            return Ok(SerializationStatus::Deferred);
        };

        if left_property == right_property {
            return Ok(SerializationStatus::Serialized);
        }

        let (left_subject, left_object) =
            match self.inverse_edge_endpoints(data_buffer, &left_property)? {
                Some(endpoints) => endpoints,
                None => {
                    self.add_to_unknown_buffer(data_buffer, left_property, triple)?;
                    return Ok(SerializationStatus::Deferred);
                }
            };

        let (right_subject, right_object) =
            match self.inverse_edge_endpoints(data_buffer, &right_property)? {
                Some(endpoints) => endpoints,
                None => {
                    self.add_to_unknown_buffer(data_buffer, right_property, triple)?;
                    return Ok(SerializationStatus::Deferred);
                }
            };

        let compatible = left_subject == right_object && left_object == right_subject;
        if !compatible {
            let msg = format!(
                "Cannot merge owl:inverseOf '{}'<->'{}': normalized edges do not align ({} -> {}, {} -> {})",
                data_buffer.term_index.get(&left_property)?,
                data_buffer.term_index.get(&right_property)?,
                data_buffer.term_index.get(&left_subject)?,
                data_buffer.term_index.get(&left_object)?,
                data_buffer.term_index.get(&right_subject)?,
                data_buffer.term_index.get(&right_object)?
            );
            let e = SerializationErrorKind::SerializationWarning(msg.to_string());
            warn!("{msg}");
            data_buffer
                .failed_buffer
                .write()?
                .push(<SerializationError as Into<ErrorRecord>>::into(e.into()));

            return Ok(SerializationStatus::Serialized);
        }

        let (merged_label, merged_characteristics) = {
            let left_edge = {
                data_buffer
                    .property_edge_map
                    .read()?
                    .get(&left_property)
                    .cloned()
            };
            let right_edge = {
                data_buffer
                    .property_edge_map
                    .read()?
                    .get(&right_property)
                    .cloned()
            };

            let merged_label = {
                let edge_label_buffer = data_buffer.edge_label_buffer.read()?;
                let label_buffer = data_buffer.label_buffer.read()?;
                let left_label = left_edge
                    .as_ref()
                    .and_then(|edge| edge_label_buffer.get(edge))
                    .or_else(|| label_buffer.get(&left_property))
                    .and_then(Option::as_ref);

                let right_label = right_edge
                    .as_ref()
                    .and_then(|edge| edge_label_buffer.get(edge))
                    .or_else(|| label_buffer.get(&right_property))
                    .and_then(Option::as_ref);
                merge_optional_labels(left_label, right_label)
            };

            self.merge_properties(data_buffer, &right_property, &left_property)?;

            if let Some(ref left_edge) = left_edge {
                self.remove_edge_include(data_buffer, &left_edge.domain_term_id, left_edge)?;
                self.remove_edge_include(data_buffer, &left_edge.range_term_id, left_edge)?;
                data_buffer.edge_buffer.write()?.remove(left_edge);
                data_buffer.edge_label_buffer.write()?.remove(left_edge);
            }

            if let Some(ref right_edge) = right_edge {
                self.remove_edge_include(data_buffer, &right_edge.domain_term_id, right_edge)?;
                self.remove_edge_include(data_buffer, &right_edge.range_term_id, right_edge)?;
                data_buffer.edge_buffer.write()?.remove(right_edge);
                data_buffer.edge_label_buffer.write()?.remove(right_edge);
            }

            let merged_characteristics = {
                let mut edge_characteristics = data_buffer.edge_characteristics.write()?;
                let mut merged_characteristics = left_edge
                    .and_then(|edge| edge_characteristics.remove(&edge))
                    .unwrap_or_default();

                if let Some(right_characteristics) =
                    right_edge.and_then(|edge| edge_characteristics.remove(&edge))
                {
                    merged_characteristics.extend(right_characteristics);
                }
                merged_characteristics
            };
            (merged_label, merged_characteristics)
        };

        let inverse_property = Some(left_property);
        let edge_type = ElementType::Owl(OwlType::Edge(OwlEdge::InverseOf));
        let inverse_edges = [
            self.create_edge_from_id(
                &data_buffer.term_index,
                left_subject,
                edge_type,
                left_object,
                inverse_property,
            )?,
            self.create_edge_from_id(
                &data_buffer.term_index,
                left_object,
                edge_type,
                left_subject,
                inverse_property,
            )?,
        ];

        let canonical_edge = inverse_edges[0].clone();

        for edge in inverse_edges {
            {
                data_buffer.edge_buffer.write()?.insert(edge.clone());
            }
            self.insert_edge_include(data_buffer, edge.domain_term_id, edge.clone())?;
            self.insert_edge_include(data_buffer, edge.range_term_id, edge.clone())?;
            if let Some(ref label) = merged_label {
                data_buffer
                    .edge_label_buffer
                    .write()?
                    .insert(edge.clone(), Some(label.clone()));
            }

            if !merged_characteristics.is_empty() {
                data_buffer
                    .edge_characteristics
                    .write()?
                    .insert(edge, merged_characteristics.clone());
            }
        }

        let mut property_edge_map = data_buffer.property_edge_map.write()?;
        property_edge_map.insert(left_property, canonical_edge);
        property_edge_map.remove(&right_property);

        Ok(SerializationStatus::Serialized)
    }

    /// Appends a string to an element's label.
    fn extend_element_label(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        element_id: &usize,
        label_to_append: String,
    ) -> Result<(), SerializationError> {
        debug!(
            "Extending element '{}' with label '{}'",
            data_buffer.term_index.get(element_id)?,
            label_to_append
        );
        let mut label_buffer = data_buffer.label_buffer.write()?;
        if let Some(Some(label)) = label_buffer.get_mut(element_id) {
            label.push_str(format!("\n{}", label_to_append).as_str());
        } else {
            label_buffer.insert(*element_id, Some(label_to_append));
        }
        Ok(())
    }

    /// Creates a named node from an IRI.
    fn create_named_node(&self, iri: &String) -> Result<NamedNode, SerializationError> {
        Ok(NamedNode::new(iri)
            .map_err(|e| SerializationErrorKind::IriParseError(iri.clone(), Box::new(e)))?)
    }

    /// Creates a blank node from a blank node ID.
    fn create_blank_node(&self, id: &String) -> Result<BlankNode, SerializationError> {
        Ok(BlankNode::new(id)
            .map_err(|e| SerializationErrorKind::BlankNodeParseError(id.clone(), Box::new(e)))?)
    }

    /// Creates a term from a string, automatically handling named/blank nodes.
    fn create_term(&self, term: &String) -> Result<Term, SerializationError> {
        match self.create_named_node(term) {
            Ok(named_node) => Ok(Term::NamedNode(named_node)),
            Err(_) => Ok(Term::BlankNode(self.create_blank_node(term)?)),
        }
    }

    /// Creates a triple of subject-predicate-object terms, automatically handling named/blank nodes.
    ///
    /// The new terms are automatically registered in the term index.
    fn create_triple_from_iri(
        &self,
        term_index: &mut TermIndex,
        subject_iri: &String,
        predicate_iri: &String,
        object_iri: Option<&String>,
    ) -> Result<ArcTriple, SerializationError> {
        let subject_term_id = {
            let subject_term = self.create_term(subject_iri)?;
            term_index.insert(subject_term)?
        };

        let predicate_term_id = term_index.insert(self.create_term(predicate_iri)?)?;

        let object_term_id = match object_iri {
            Some(iri) => Some(term_index.insert(self.create_term(iri)?)?),
            None => None,
        };

        self.create_triple_from_id(
            term_index,
            subject_term_id,
            Some(predicate_term_id),
            object_term_id,
        )
    }

    /// Creates a triple of subject-predicate-object term IDs.
    fn create_triple_from_id(
        &self,
        term_index: &TermIndex,
        subject_term_id: usize,
        predicate_term_id: Option<usize>,
        object_term_id: Option<usize>,
    ) -> Result<ArcTriple, SerializationError> {
        let triple = Triple::new(subject_term_id, predicate_term_id, object_term_id).into();
        debug!(
            "Created new triple: {}",
            term_index.display_triple(&triple)?
        );
        Ok(triple)
    }

    /// Creates an edge from term IDs.
    fn create_edge_from_id(
        &self,
        term_index: &TermIndex,
        domain_term_id: usize,
        edge_type: ElementType,
        range_term_id: usize,
        property_term_id: Option<usize>,
    ) -> Result<ArcEdge, SerializationError> {
        let edge = Edge::new(domain_term_id, edge_type, range_term_id, property_term_id).into();
        debug!("Created new edge: {}", term_index.display_edge(&edge)?);
        Ok(edge)
    }

    /// Try to serialize the triples of all unknown terms until a fixpoint is reached (i.e. trying again doesn't change the outcome).
    fn check_all_unknowns(
        &self,
        data_buffer: &mut SerializationDataBuffer,
    ) -> Result<(), SerializationError> {
        self.retry_restrictions(data_buffer)?;

        let mut pending = {
            let mut unknown_buffer = data_buffer.unknown_buffer.write()?;
            take(&mut *unknown_buffer)
        };
        let mut pass: usize = 0;
        let max_passes: usize = 4;

        while !pending.is_empty() && pass < max_passes {
            pass += 1;

            let pending_before: usize = pending.values().map(|set| set.len()).sum();
            info!(
                "Unknown resolution pass {} ({} triples pending)",
                pass, pending_before
            );

            self.retry_restrictions(data_buffer)?;
            let current = pending;

            for (term_id, triples) in current {
                let term = data_buffer.term_index.get(&term_id)?;

                if !data_buffer.label_buffer.read()?.contains_key(&term_id) {
                    self.extract_label(data_buffer, None, &term, &term_id)?;
                }

                if self.is_external(data_buffer, &term)? {
                    let external_triple =
                        self.create_triple_from_id(&data_buffer.term_index, term_id, None, None)?;

                    self.insert_node(
                        data_buffer,
                        external_triple,
                        ElementType::Owl(OwlType::Node(OwlNode::ExternalClass)),
                    )?;
                } else if let Some(element_type) = try_resolve_reserved(&term) {
                    let reserved_triple =
                        self.create_triple_from_id(&data_buffer.term_index, term_id, None, None)?;

                    self.insert_node(data_buffer, reserved_triple, element_type)?;
                } else if term.is_blank_node() {
                    let anonymous_triple =
                        self.create_triple_from_id(&data_buffer.term_index, term_id, None, None)?;

                    self.insert_node(
                        data_buffer,
                        anonymous_triple,
                        ElementType::Owl(OwlType::Node(OwlNode::AnonymousClass)),
                    )?;
                }

                for triple in triples {
                    match self.write_node_triple(data_buffer, triple) {
                        Ok(SerializationStatus::Serialized) => {}
                        Ok(SerializationStatus::Deferred) => {}
                        Err(e) => {
                            data_buffer.failed_buffer.write()?.push(e.into());
                        }
                    }
                }
            }

            // Collect newly deferred triples produced during this pass.
            pending = {
                let mut unknown_buffer = data_buffer.unknown_buffer.write()?;
                take(&mut *unknown_buffer)
            };
            let pending_after: usize = pending.values().map(|set| set.len()).sum();

            if pending_after >= pending_before || pending_after == 0 {
                info!(
                    "Unknown resolution reached fixpoint after pass {} ({} triples still pending)",
                    pass, pending_after
                );
                break;
            }
        }

        *data_buffer.unknown_buffer.write()? = pending;
        Ok(())
    }

    /// Serialize a triple to `data_buffer`.
    fn write_node_triple(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        triple: ArcTriple,
    ) -> Result<SerializationStatus, SerializationError> {
        let predicate_term_id = data_buffer.get_predicate(&triple)?;
        let predicate_term = data_buffer.term_index.get(&predicate_term_id)?;

        match predicate_term.as_ref() {
            Term::BlankNode(bnode) => {
                // The query must never put blank nodes in the ?nodeType variable
                let msg = format!("Illegal blank node during serialization: '{bnode}'");
                return Err(SerializationErrorKind::SerializationFailedTriple(
                    data_buffer.term_index.display_triple(&triple)?,
                    msg,
                )
                .into());
            }
            Term::Literal(literal) => match literal.value() {
                "blanknode" => {
                    self.insert_node(
                        data_buffer,
                        triple,
                        ElementType::Owl(OwlType::Node(OwlNode::AnonymousClass)),
                    )?;
                }
                other => {
                    let msg = format!("Visualization of literal '{other}' is not supported");
                    let e = SerializationErrorKind::SerializationWarning(msg.to_string());
                    warn!("{msg}");
                    data_buffer
                        .failed_buffer
                        .write()?
                        .push(<SerializationError as Into<ErrorRecord>>::into(e.into()));
                }
            },
            Term::NamedNode(uri) => {
                // NOTE: Only supports RDF 1.1
                match uri.as_ref() {
                    // ----------- RDF ----------- //

                    // rdf::ALT => {}
                    // rdf::BAG => {}
                    // rdf::FIRST => {}
                    rdf::HTML => {
                        self.insert_node(
                            data_buffer,
                            triple,
                            ElementType::Rdfs(RdfsType::Node(RdfsNode::Datatype)),
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }
                    // rdf::LANG_STRING => {}
                    // rdf::LIST => {}
                    // rdf::NIL => {}
                    // rdf::OBJECT => {}
                    // rdf::PREDICATE => {}
                    rdf::PROPERTY => {
                        match self.insert_edge(
                            data_buffer,
                            triple,
                            ElementType::Rdf(RdfType::Edge(RdfEdge::RdfProperty)),
                            None,
                        )? {
                            Some(_) => {
                                return Ok(SerializationStatus::Serialized);
                            }
                            None => {
                                return Ok(SerializationStatus::Deferred);
                            }
                        }
                    }
                    // rdf::REST => {}
                    // rdf::SEQ => {}
                    // rdf::STATEMENT => {}
                    // rdf::SUBJECT => {}
                    // rdf::TYPE => {}
                    // rdf::VALUE => {}
                    rdf::XML_LITERAL => {
                        self.insert_node(
                            data_buffer,
                            triple,
                            ElementType::Rdfs(RdfsType::Node(RdfsNode::Datatype)),
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }
                    rdf::PLAIN_LITERAL => {
                        self.insert_node(
                            data_buffer,
                            triple,
                            ElementType::Rdfs(RdfsType::Node(RdfsNode::Datatype)),
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }
                    // rdf::COMPOUND_LITERAL => {}
                    // rdf::DIRECTION => {}

                    // ----------- RDFS ----------- //
                    rdfs::CLASS => {
                        self.insert_node(
                            data_buffer,
                            triple,
                            ElementType::Rdfs(RdfsType::Node(RdfsNode::Class)),
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }

                    //TODO: OWL1
                    // rdfs::COMMENT => {}

                    // rdfs::CONTAINER => {}
                    // rdfs::CONTAINER_MEMBERSHIP_PROPERTY => {}
                    rdfs::DATATYPE => {
                        self.insert_node(
                            data_buffer,
                            triple,
                            ElementType::Rdfs(RdfsType::Node(RdfsNode::Datatype)),
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }
                    rdfs::DOMAIN => {
                        return Err(SerializationErrorKind::SerializationFailedTriple(
                            data_buffer.term_index.display_triple(&triple)?,
                            "SPARQL query should not have rdfs:domain triples".to_string(),
                        )
                        .into());
                    }

                    // rdfs::IS_DEFINED_BY => {}

                    // rdfs::LABEL => {}
                    rdfs::LITERAL => {
                        self.insert_node(
                            data_buffer,
                            triple,
                            ElementType::Rdfs(RdfsType::Node(RdfsNode::Literal)),
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }
                    // rdfs::MEMBER => {}
                    rdfs::RANGE => {
                        return Err(SerializationErrorKind::SerializationFailedTriple(
                            data_buffer.term_index.display_triple(&triple)?,
                            "SPARQL query should not have rdfs:range triples".to_string(),
                        )
                        .into());
                    }
                    rdfs::RESOURCE => {
                        self.insert_node(
                            data_buffer,
                            triple,
                            ElementType::Rdfs(RdfsType::Node(RdfsNode::Resource)),
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }

                    //TODO: OWL1
                    // rdfs::SEE_ALSO => {}
                    rdfs::SUB_CLASS_OF => {
                        // TODO: Some cases of owl:Thing self-subclass triple are not handled here.
                        // Particularly if we haven't seen subject in the element buffer.
                        if let Some(target) = triple.object_term_id
                            && target == triple.subject_term_id
                            && is_synthetic(&data_buffer.term_index.get(&triple.subject_term_id)?)
                            && data_buffer
                                .node_element_buffer
                                .read()?
                                .get(&triple.subject_term_id)
                                == Some(&ElementType::Owl(OwlType::Node(OwlNode::Thing)))
                        {
                            debug!("Skipping synthetic owl:Thing self-subclass triple");
                            return Ok(SerializationStatus::Serialized);
                        }

                        match self.insert_edge(
                            data_buffer,
                            triple.clone(),
                            ElementType::Rdfs(RdfsType::Edge(RdfsEdge::SubclassOf)),
                            None,
                        )? {
                            Some(_) => {
                                if let Some(restriction_term_id) = triple.object_term_id.as_ref() {
                                    self.try_materialize_restriction(
                                        data_buffer,
                                        restriction_term_id,
                                    )?;
                                }
                                return Ok(SerializationStatus::Serialized);
                            }
                            None => {
                                return Ok(SerializationStatus::Deferred);
                            }
                        }
                    }
                    //TODO: OWL1
                    //rdfs::SUB_PROPERTY_OF => {},

                    // ----------- OWL 2 ----------- //

                    //TODO: OWL1
                    // owl::ALL_DIFFERENT => {},

                    // owl::ALL_DISJOINT_CLASSES => {},
                    // owl::ALL_DISJOINT_PROPERTIES => {},
                    owl::ALL_VALUES_FROM => {
                        {
                            let mut restriction_buffer = data_buffer.restriction_buffer.write()?;
                            let mut state = restriction_buffer
                                .entry(triple.subject_term_id)
                                .or_default()
                                .write()?;
                            state.filler = triple.object_term_id;
                            state.cardinality = Some(("∀".to_string(), None));
                            state.requires_filler = true;
                            state.render_mode = RestrictionRenderMode::ValuesFrom;
                        }

                        return self
                            .try_materialize_restriction(data_buffer, &triple.subject_term_id);
                    }

                    // owl::ANNOTATED_PROPERTY => {},
                    // owl::ANNOTATED_SOURCE => {},
                    // owl::ANNOTATED_TARGET => {},
                    // owl::ANNOTATION => {},

                    //TODO: OWL1
                    // owl::ANNOTATION_PROPERTY => {},

                    // owl::ASSERTION_PROPERTY => {},
                    owl::ASYMMETRIC_PROPERTY => {
                        return self.insert_characteristic(
                            data_buffer,
                            triple,
                            Characteristic::AsymmetricProperty,
                        );
                    }

                    // owl::AXIOM => {},
                    // owl::BACKWARD_COMPATIBLE_WITH => {},
                    // owl::BOTTOM_DATA_PROPERTY => {},
                    // owl::BOTTOM_OBJECT_PROPERTY => {},
                    owl::CARDINALITY => {
                        let exact = Self::cardinality_literal(data_buffer, &triple)?;
                        {
                            let mut restriction_buffer = data_buffer.restriction_buffer.write()?;
                            let mut state = restriction_buffer
                                .entry(triple.subject_term_id)
                                .or_default()
                                .write()?;
                            state.cardinality = Some((exact.clone(), None));
                        }

                        return self
                            .try_materialize_restriction(data_buffer, &triple.subject_term_id);
                    }
                    owl::QUALIFIED_CARDINALITY => {
                        let exact = Self::cardinality_literal(data_buffer, &triple)?;
                        {
                            let mut restriction_buffer = data_buffer.restriction_buffer.write()?;
                            let mut state = restriction_buffer
                                .entry(triple.subject_term_id)
                                .or_default()
                                .write()?;
                            state.cardinality = Some((exact.clone(), Some(exact)));
                            state.requires_filler = true;
                        }

                        return self
                            .try_materialize_restriction(data_buffer, &triple.subject_term_id);
                    }
                    owl::CLASS => {
                        self.insert_node(
                            data_buffer,
                            triple,
                            ElementType::Owl(OwlType::Node(OwlNode::Class)),
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }
                    owl::COMPLEMENT_OF => {
                        if let Some(target) = triple.object_term_id.as_ref()
                            && self.should_skip_structural_operand(
                                data_buffer,
                                &triple.subject_term_id,
                                target,
                                "owl:complementOf",
                            )?
                        {
                            return Ok(SerializationStatus::Serialized);
                        }

                        let edge = self.insert_edge(
                            data_buffer,
                            triple.clone(),
                            ElementType::NoDraw,
                            None,
                        )?;

                        if triple.object_term_id.is_some()
                            && let Some(index) =
                                self.resolve(data_buffer, triple.subject_term_id)?
                            && !Self::has_named_equivalent_aliases(data_buffer, &index)?
                        {
                            self.upgrade_node_type(
                                data_buffer,
                                index,
                                ElementType::Owl(OwlType::Node(OwlNode::Complement)),
                            )?;
                        }

                        if edge.is_some() {
                            return Ok(SerializationStatus::Serialized);
                        } else {
                            return Ok(SerializationStatus::Deferred);
                        }
                    }

                    //TODO: OWL1
                    //owl::DATATYPE_COMPLEMENT_OF => {}
                    owl::DATATYPE_PROPERTY => {
                        let e = ElementType::Owl(OwlType::Edge(OwlEdge::DatatypeProperty));
                        self.add_triple_to_element_buffer(
                            &data_buffer.term_index,
                            &mut data_buffer.edge_element_buffer,
                            &triple,
                            e,
                        )?;
                        self.check_unknown_buffer(data_buffer, &triple.subject_term_id)?;
                        return Ok(SerializationStatus::Serialized);
                    }

                    //TODO: OWL1 (deprecated in OWL2, replaced by rdfs:datatype)
                    // owl::DATA_RANGE => {}
                    owl::DEPRECATED => {
                        let Some(resolved_term_id) =
                            self.resolve(data_buffer, triple.subject_term_id)?
                        else {
                            debug!(
                                "Deferring owl:Deprecated for '{}': subject type unresolved",
                                data_buffer.term_index.get(&triple.subject_term_id)?
                            );
                            self.add_to_unknown_buffer(
                                data_buffer,
                                triple.subject_term_id,
                                triple,
                            )?;
                            return Ok(SerializationStatus::Deferred);
                        };

                        if data_buffer
                            .node_element_buffer
                            .read()?
                            .contains_key(&resolved_term_id)
                        {
                            self.upgrade_deprecated_node_type(data_buffer, &resolved_term_id)?;
                            return Ok(SerializationStatus::Serialized);
                        }

                        if data_buffer
                            .edge_element_buffer
                            .read()?
                            .contains_key(&resolved_term_id)
                        {
                            self.upgrade_property_type(
                                data_buffer,
                                &resolved_term_id,
                                ElementType::Owl(OwlType::Edge(OwlEdge::DeprecatedProperty)),
                            )?;
                            self.check_unknown_buffer(data_buffer, &resolved_term_id)?;
                            return Ok(SerializationStatus::Serialized);
                        }

                        debug!(
                            "Skipping owl:Deprecated for '{}': resolved subject has no node/edge entry",
                            data_buffer.term_index.get(&resolved_term_id)?
                        );
                        return Ok(SerializationStatus::Deferred);
                    }

                    owl::DEPRECATED_CLASS => {
                        self.insert_node(
                            data_buffer,
                            triple,
                            ElementType::Owl(OwlType::Node(OwlNode::DeprecatedClass)),
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }
                    owl::DEPRECATED_PROPERTY => {
                        match self.insert_edge(
                            data_buffer,
                            triple,
                            ElementType::Owl(OwlType::Edge(OwlEdge::DeprecatedProperty)),
                            None,
                        )? {
                            Some(_) => {
                                return Ok(SerializationStatus::Serialized);
                            }
                            None => {
                                return Ok(SerializationStatus::Deferred);
                            }
                        }
                    }

                    //TODO: OWL1
                    // owl::DIFFERENT_FROM => {}
                    owl::DISJOINT_UNION_OF => {
                        if let Some(target) = triple.object_term_id.as_ref()
                            && self.should_skip_structural_operand(
                                data_buffer,
                                &triple.subject_term_id,
                                target,
                                "owl:disjointUnionOf",
                            )?
                        {
                            return Ok(SerializationStatus::Serialized);
                        }

                        match self.insert_edge(data_buffer, triple, ElementType::NoDraw, None)? {
                            Some(edge) => {
                                if !Self::has_named_equivalent_aliases(
                                    data_buffer,
                                    &edge.domain_term_id,
                                )? {
                                    self.upgrade_node_type(
                                        data_buffer,
                                        edge.domain_term_id,
                                        ElementType::Owl(OwlType::Node(OwlNode::DisjointUnion)),
                                    )?;
                                }
                                return Ok(SerializationStatus::Serialized);
                            }
                            None => {
                                return Ok(SerializationStatus::Deferred);
                            }
                        }
                    }
                    owl::DISJOINT_WITH => {
                        match self.insert_edge(
                            data_buffer,
                            triple,
                            ElementType::Owl(OwlType::Edge(OwlEdge::DisjointWith)),
                            None,
                        )? {
                            Some(_) => {
                                return Ok(SerializationStatus::Serialized);
                            }
                            None => {
                                return Ok(SerializationStatus::Deferred);
                            }
                        }
                    }

                    //TODO: OWL1
                    // owl::DISTINCT_MEMBERS => {}
                    owl::EQUIVALENT_CLASS => match self.resolve_so(data_buffer, &triple)? {
                        (Some(resolved_subject_term_id), Some(resolved_object_term_id)) => {
                            self.merge_nodes(
                                data_buffer,
                                resolved_object_term_id,
                                resolved_subject_term_id,
                            )?;

                            let resolved_subject_element = {
                                match data_buffer
                                    .node_element_buffer
                                    .read()?
                                    .get(&resolved_subject_term_id)
                                    .copied()
                                {
                                    Some(elem) => elem,
                                    None => {
                                        let msg = "subject not present in node_element_buffer"
                                            .to_string();
                                        return Err(
                                            SerializationErrorKind::SerializationFailedTriple(
                                                data_buffer.term_index.display_triple(&triple)?,
                                                msg,
                                            ),
                                        )?;
                                    }
                                }
                            };

                            if resolved_subject_element
                                != ElementType::Owl(OwlType::Node(OwlNode::AnonymousClass))
                            {
                                let object_was_anonymous_expr = {
                                    match triple.object_term_id {
                                        Some(object_term_id) => {
                                            data_buffer.term_index.is_blank_node(&object_term_id)?
                                        }
                                        None => false,
                                    }
                                };
                                let keep_structural_type = object_was_anonymous_expr
                                    && !Self::has_named_equivalent_aliases(
                                        data_buffer,
                                        &resolved_subject_term_id,
                                    )?;

                                let upgraded_element = if keep_structural_type {
                                    let resolved_object_element = data_buffer
                                        .node_element_buffer
                                        .read()?
                                        .get(&resolved_object_term_id)
                                        .copied();

                                    match resolved_object_element {
                                        Some(object_element)
                                            if is_structural_set_node(object_element) =>
                                        {
                                            object_element
                                        }
                                        _ => ElementType::Owl(OwlType::Node(
                                            OwlNode::EquivalentClass,
                                        )),
                                    }
                                } else {
                                    ElementType::Owl(OwlType::Node(OwlNode::EquivalentClass))
                                };

                                self.upgrade_node_type(
                                    data_buffer,
                                    resolved_subject_term_id,
                                    upgraded_element,
                                )?;

                                let maybe_label = data_buffer
                                    .label_buffer
                                    .read()?
                                    .get(&resolved_object_term_id)
                                    .cloned()
                                    .flatten();
                                if let Some(label) = maybe_label {
                                    self.extend_element_label(
                                        data_buffer,
                                        &resolved_subject_term_id,
                                        label,
                                    )?;
                                }
                            }
                        }
                        (Some(_), None) => match triple.object_term_id {
                            Some(target) => {
                                self.add_to_unknown_buffer(data_buffer, target, triple)?;
                                return Ok(SerializationStatus::Deferred);
                            }
                            None => {
                                let msg = "Failed to merge object of equivalence relation into subject: object not found".to_string();
                                return Err(SerializationErrorKind::MissingObject(
                                    data_buffer.term_index.display_triple(&triple)?,
                                    msg,
                                )
                                .into());
                            }
                        },
                        (None, Some(resolved_object_term_id)) => {
                            self.add_to_unknown_buffer(
                                data_buffer,
                                resolved_object_term_id,
                                triple,
                            )?;
                            return Ok(SerializationStatus::Deferred);
                        }
                        (None, None) => {
                            self.add_to_unknown_buffer(
                                data_buffer,
                                triple.subject_term_id,
                                triple,
                            )?;
                            return Ok(SerializationStatus::Deferred);
                        }
                    },
                    // owl::EQUIVALENT_PROPERTY => {}
                    owl::FUNCTIONAL_PROPERTY => {
                        return self.insert_characteristic(
                            data_buffer,
                            triple,
                            Characteristic::FunctionalProperty,
                        );
                    }

                    // owl::HAS_KEY => {}
                    owl::HAS_SELF => {
                        let truthy = {
                            match &triple.object_term_id {
                                Some(object_term_id) => {
                                    data_buffer.term_index.is_literal_truthy(object_term_id)?
                                }
                                None => false,
                            }
                        };

                        if truthy {
                            {
                                let mut restriction_buffer =
                                    data_buffer.restriction_buffer.write()?;
                                let mut state = restriction_buffer
                                    .entry(triple.subject_term_id)
                                    .or_default()
                                    .write()?;
                                state.self_restriction = true;
                                state.cardinality = Some(("self".to_string(), None));
                            }
                        }

                        return self
                            .try_materialize_restriction(data_buffer, &triple.subject_term_id);
                    }

                    owl::HAS_VALUE => {
                        {
                            let mut restriction_buffer = data_buffer.restriction_buffer.write()?;
                            let mut state = restriction_buffer
                                .entry(triple.subject_term_id)
                                .or_default()
                                .write()?;
                            state.filler = triple.object_term_id;
                            state.cardinality = Some(("value".to_string(), None));
                            state.render_mode = RestrictionRenderMode::ExistingProperty;
                        }

                        return self
                            .try_materialize_restriction(data_buffer, &triple.subject_term_id);
                    }

                    // owl::IMPORTS => {}
                    // owl::INCOMPATIBLE_WITH => {}
                    owl::INTERSECTION_OF => {
                        if let Some(target) = triple.object_term_id.as_ref()
                            && self.should_skip_structural_operand(
                                data_buffer,
                                &triple.subject_term_id,
                                target,
                                "owl:intersectionOf",
                            )?
                        {
                            return Ok(SerializationStatus::Serialized);
                        }

                        match self.insert_edge(data_buffer, triple, ElementType::NoDraw, None)? {
                            Some(edge) => {
                                if !Self::has_named_equivalent_aliases(
                                    data_buffer,
                                    &edge.domain_term_id,
                                )? {
                                    self.upgrade_node_type(
                                        data_buffer,
                                        edge.domain_term_id,
                                        ElementType::Owl(OwlType::Node(OwlNode::IntersectionOf)),
                                    )?;
                                }
                                return Ok(SerializationStatus::Serialized);
                            }
                            None => {
                                return Ok(SerializationStatus::Deferred);
                            }
                        }
                    }
                    owl::INVERSE_FUNCTIONAL_PROPERTY => {
                        return self.insert_characteristic(
                            data_buffer,
                            triple,
                            Characteristic::InverseFunctionalProperty,
                        );
                    }

                    owl::INVERSE_OF => {
                        return self.insert_inverse_of(data_buffer, triple);
                    }

                    owl::IRREFLEXIVE_PROPERTY => {
                        return self.insert_characteristic(
                            data_buffer,
                            triple,
                            Characteristic::IrreflexiveProperty,
                        );
                    }

                    owl::MAX_CARDINALITY => {
                        let max = Self::cardinality_literal(data_buffer, &triple)?;
                        {
                            let mut restriction_buffer = data_buffer.restriction_buffer.write()?;
                            let mut state = restriction_buffer
                                .entry(triple.subject_term_id)
                                .or_default()
                                .write()?;
                            state.cardinality = Some((String::new(), Some(max)));
                        }

                        return self
                            .try_materialize_restriction(data_buffer, &triple.subject_term_id);
                    }

                    owl::MAX_QUALIFIED_CARDINALITY => {
                        {
                            let mut restriction_buffer = data_buffer.restriction_buffer.write()?;
                            let mut state = restriction_buffer
                                .entry(triple.subject_term_id)
                                .or_default()
                                .write()?;
                            state.cardinality = Some((
                                String::new(),
                                Some(Self::cardinality_literal(data_buffer, &triple)?),
                            ));
                            state.requires_filler = true;
                        }

                        return self
                            .try_materialize_restriction(data_buffer, &triple.subject_term_id);
                    }
                    // owl::MEMBERS => {}
                    owl::MIN_CARDINALITY => {
                        let min = Self::cardinality_literal(data_buffer, &triple)?;
                        {
                            let mut restriction_buffer = data_buffer.restriction_buffer.write()?;
                            let mut state = restriction_buffer
                                .entry(triple.subject_term_id)
                                .or_default()
                                .write()?;
                            state.cardinality = Some((min, Some("*".to_string())));
                        }

                        return self
                            .try_materialize_restriction(data_buffer, &triple.subject_term_id);
                    }
                    owl::MIN_QUALIFIED_CARDINALITY => {
                        {
                            let mut restriction_buffer = data_buffer.restriction_buffer.write()?;
                            let mut state = restriction_buffer
                                .entry(triple.subject_term_id)
                                .or_default()
                                .write()?;
                            state.cardinality = Some((
                                Self::cardinality_literal(data_buffer, &triple)?,
                                Some("*".to_string()),
                            ));
                            state.requires_filler = true;
                        }

                        return self
                            .try_materialize_restriction(data_buffer, &triple.subject_term_id);
                    }
                    owl::NAMED_INDIVIDUAL => {
                        let count = Self::individual_count_literal(data_buffer, &triple)?;
                        self.increment_individual_count(
                            data_buffer,
                            triple.subject_term_id,
                            count,
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }
                    // owl::NEGATIVE_PROPERTY_ASSERTION => {}

                    //TODO: OWL1
                    //owl::NOTHING => {}
                    owl::OBJECT_PROPERTY => {
                        self.add_triple_to_element_buffer(
                            &data_buffer.term_index,
                            &mut data_buffer.edge_element_buffer,
                            &triple,
                            ElementType::Owl(OwlType::Edge(OwlEdge::ObjectProperty)),
                        )?;
                        self.check_unknown_buffer(data_buffer, &triple.subject_term_id)?;
                        return Ok(SerializationStatus::Serialized);
                    }
                    owl::ONE_OF => {
                        let Some(raw_target) = triple.object_term_id else {
                            return Err(SerializationErrorKind::MissingObject(
                                data_buffer.term_index.display_triple(&triple)?,
                                "owl:oneOf triple is missing a target".to_string(),
                            )
                            .into());
                        };

                        let should_count_member = matches!(
                            data_buffer.term_index.get(&raw_target)?.as_ref(),
                            Term::NamedNode(_) | Term::BlankNode(_)
                        );

                        let materialized_target = self.materialize_one_of_target(
                            data_buffer,
                            &triple.subject_term_id,
                            &raw_target,
                        )?;

                        let member_already_present = if should_count_member {
                            self.has_enumeration_member_edge(
                                data_buffer,
                                triple.subject_term_id,
                                materialized_target,
                            )?
                        } else {
                            false
                        };

                        let edge_triple = self.create_triple_from_id(
                            &data_buffer.term_index,
                            triple.subject_term_id,
                            triple.predicate_term_id,
                            Some(materialized_target),
                        )?;

                        match self.insert_edge(
                            data_buffer,
                            edge_triple,
                            ElementType::NoDraw,
                            None,
                        )? {
                            Some(_) => {
                                if should_count_member && !member_already_present {
                                    self.increment_individual_count(
                                        data_buffer,
                                        triple.subject_term_id,
                                        1,
                                    )?;
                                }
                                return Ok(SerializationStatus::Serialized);
                            }
                            None => return Ok(SerializationStatus::Deferred),
                        }
                    }
                    owl::ONTOLOGY => {
                        let mut document_base = data_buffer.document_base.write()?;
                        let base_term = data_buffer.term_index.get(&triple.subject_term_id)?;
                        let base = trim_tag_circumfix(&base_term.to_string());
                        if let Some(base) = &*document_base {
                            let msg = format!(
                                "Attempting to override document base '{base}' with new base '{}'. Skipping",
                                base
                            );
                            let e = SerializationErrorKind::SerializationWarning(msg.to_string());
                            warn!("{msg}");
                            data_buffer
                                .failed_buffer
                                .write()?
                                .push(<SerializationError as Into<ErrorRecord>>::into(e.into()));
                        } else {
                            info!("Using document base: '{}'", base);
                            *document_base = Some(base.into());
                        }
                    }

                    //TODO: OWL1
                    // owl::ONTOLOGY_PROPERTY => {}
                    owl::ON_CLASS | owl::ON_DATARANGE => {
                        {
                            let mut restriction_buffer = data_buffer.restriction_buffer.write()?;
                            let mut state = restriction_buffer
                                .entry(triple.subject_term_id)
                                .or_default()
                                .write()?;
                            state.filler = triple.object_term_id;
                            state.requires_filler = true;
                        }

                        return self
                            .try_materialize_restriction(data_buffer, &triple.subject_term_id);
                    }
                    // owl::ON_DATATYPE => {}
                    // owl::ON_PROPERTIES => {}
                    owl::ON_PROPERTY => {
                        let Some(target) = triple.object_term_id else {
                            return Err(SerializationErrorKind::MissingObject(
                                data_buffer.term_index.display_triple(&triple)?,
                                "owl:onProperty triple is missing a target".to_string(),
                            )
                            .into());
                        };

                        {
                            let mut restriction_buffer = data_buffer.restriction_buffer.write()?;
                            let mut state = restriction_buffer
                                .entry(triple.subject_term_id)
                                .or_default()
                                .write()?;
                            state.on_property = Some(target);
                        }

                        return self
                            .try_materialize_restriction(data_buffer, &triple.subject_term_id);
                    }

                    // owl::PRIOR_VERSION => {}
                    // owl::PROPERTY_CHAIN_AXIOM => {}
                    // owl::PROPERTY_DISJOINT_WITH => {}
                    // owl::QUALIFIED_CARDINALITY => {}
                    owl::REFLEXIVE_PROPERTY => {
                        return self.insert_characteristic(
                            data_buffer,
                            triple,
                            Characteristic::ReflexiveProperty,
                        );
                    }

                    //TODO: OWL1
                    // owl::RESTRICTION => {}

                    //TODO: OWL1
                    // owl::SAME_AS => {}
                    owl::SOME_VALUES_FROM => {
                        {
                            let mut restriction_buffer = data_buffer.restriction_buffer.write()?;
                            let mut state = restriction_buffer
                                .entry(triple.subject_term_id)
                                .or_default()
                                .write()?;
                            state.filler = triple.object_term_id;
                            state.cardinality = Some(("∃".to_string(), None));
                            state.requires_filler = true;
                            state.render_mode = RestrictionRenderMode::ValuesFrom;
                        }

                        return self
                            .try_materialize_restriction(data_buffer, &triple.subject_term_id);
                    }
                    // owl::SOURCE_INDIVIDUAL => {}
                    owl::SYMMETRIC_PROPERTY => {
                        return self.insert_characteristic(
                            data_buffer,
                            triple,
                            Characteristic::SymmetricProperty,
                        );
                    }
                    // owl::TARGET_INDIVIDUAL => {}
                    // owl::TARGET_VALUE => {}
                    owl::THING => {
                        self.insert_node(
                            data_buffer,
                            triple,
                            ElementType::Owl(OwlType::Node(OwlNode::Thing)),
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }
                    // owl::TOP_DATA_PROPERTY => {}
                    // owl::TOP_OBJECT_PROPERTY => {}
                    owl::TRANSITIVE_PROPERTY => {
                        return self.insert_characteristic(
                            data_buffer,
                            triple,
                            Characteristic::TransitiveProperty,
                        );
                    }
                    owl::UNION_OF => {
                        if let Some(target) = triple.object_term_id.as_ref()
                            && self.should_skip_structural_operand(
                                data_buffer,
                                &triple.subject_term_id,
                                target,
                                "owl:unionOf",
                            )?
                        {
                            return Ok(SerializationStatus::Serialized);
                        }

                        match self.insert_edge(data_buffer, triple, ElementType::NoDraw, None)? {
                            Some(edge) => {
                                if !Self::has_named_equivalent_aliases(
                                    data_buffer,
                                    &edge.domain_term_id,
                                )? {
                                    self.upgrade_node_type(
                                        data_buffer,
                                        edge.domain_term_id,
                                        ElementType::Owl(OwlType::Node(OwlNode::UnionOf)),
                                    )?;
                                }
                                return Ok(SerializationStatus::Serialized);
                            }
                            None => {
                                return Ok(SerializationStatus::Deferred);
                            }
                        }
                    }
                    // owl::VERSION_INFO => {}
                    // owl::VERSION_IRI => {}
                    // owl::WITH_RESTRICTIONS => {}
                    owl::REAL => {
                        self.insert_node(
                            data_buffer,
                            triple,
                            ElementType::Rdfs(RdfsType::Node(RdfsNode::Datatype)),
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }
                    owl::RATIONAL => {
                        self.insert_node(
                            data_buffer,
                            triple,
                            ElementType::Rdfs(RdfsType::Node(RdfsNode::Datatype)),
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }

                    // ----------- XSD ----------- //
                    xsd::ANY_URI
                    | xsd::BASE_64_BINARY
                    | xsd::BOOLEAN
                    | xsd::BYTE
                    | xsd::DATE
                    | xsd::DATE_TIME
                    | xsd::DATE_TIME_STAMP
                    | xsd::DAY_TIME_DURATION
                    | xsd::DECIMAL
                    | xsd::DOUBLE
                    | xsd::DURATION
                    | xsd::FLOAT
                    | xsd::G_DAY
                    | xsd::G_MONTH
                    | xsd::G_MONTH_DAY
                    | xsd::G_YEAR
                    | xsd::G_YEAR_MONTH
                    | xsd::HEX_BINARY
                    | xsd::INT
                    | xsd::INTEGER
                    | xsd::LANGUAGE
                    | xsd::LONG
                    | xsd::NAME
                    | xsd::NC_NAME
                    | xsd::NEGATIVE_INTEGER
                    | xsd::NMTOKEN
                    | xsd::NON_NEGATIVE_INTEGER
                    | xsd::NON_POSITIVE_INTEGER
                    | xsd::NORMALIZED_STRING
                    | xsd::POSITIVE_INTEGER
                    | xsd::SHORT
                    | xsd::STRING
                    | xsd::TIME
                    | xsd::TOKEN
                    | xsd::UNSIGNED_BYTE
                    | xsd::UNSIGNED_INT
                    | xsd::UNSIGNED_LONG
                    | xsd::UNSIGNED_SHORT
                    | xsd::YEAR_MONTH_DURATION => {
                        self.insert_node(
                            data_buffer,
                            triple,
                            ElementType::Rdfs(RdfsType::Node(RdfsNode::Datatype)),
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }
                    _ => {
                        match triple.object_term_id {
                            Some(object_term_id) => {
                                let (maybe_node_triples, edge_triple): (
                                    Option<Vec<ArcTriple>>,
                                    Option<ArcTriple>,
                                ) = match (
                                    self.resolve(data_buffer, triple.subject_term_id)?,
                                    self.resolve(data_buffer, predicate_term_id)?,
                                    self.resolve(data_buffer, object_term_id)?,
                                ) {
                                    (
                                        Some(domain_term_id),
                                        Some(property_term_id),
                                        Some(range_term_id),
                                    ) => {
                                        trace!(
                                            "Resolving object property: range: {}, property: {}, domain: {}",
                                            data_buffer.term_index.get(&range_term_id)?,
                                            data_buffer.term_index.get(&property_term_id)?,
                                            data_buffer.term_index.get(&domain_term_id)?
                                        );

                                        (
                                            None,
                                            Some(self.create_triple_from_id(
                                                &data_buffer.term_index,
                                                domain_term_id,
                                                Some(property_term_id),
                                                Some(range_term_id),
                                            )?),
                                        )
                                    }
                                    (Some(domain_term_id), Some(property_term_id), None) => {
                                        trace!(
                                            "Missing range: {}",
                                            data_buffer.term_index.display_triple(&triple)?
                                        );

                                        let object_term =
                                            data_buffer.term_index.get(&object_term_id)?;
                                        if *object_term == owl::THING.into() {
                                            let thing_term_id = self.get_or_create_domain_thing(
                                                data_buffer,
                                                &domain_term_id,
                                            )?;

                                            (
                                                None,
                                                Some(self.create_triple_from_id(
                                                    &data_buffer.term_index,
                                                    triple.subject_term_id,
                                                    triple.predicate_term_id,
                                                    Some(thing_term_id),
                                                )?),
                                            )
                                        } else if *object_term == rdfs::LITERAL.into() {
                                            let property_term =
                                                data_buffer.term_index.get(&property_term_id)?;
                                            let target_iri =
                                                synthetic_iri(&property_term, SYNTH_LITERAL);
                                            let node = self.create_triple_from_iri(
                                                &mut data_buffer.term_index,
                                                &target_iri,
                                                &rdfs::LITERAL.as_str().to_string(),
                                                None,
                                            )?;

                                            (
                                                Some(vec![node.clone()]),
                                                Some(self.create_triple_from_id(
                                                    &data_buffer.term_index,
                                                    triple.subject_term_id,
                                                    triple.predicate_term_id,
                                                    Some(node.subject_term_id),
                                                )?),
                                            )
                                        } else {
                                            // Register the property itself as an element so it can be resolved by characteristics
                                            let predicate_term =
                                                data_buffer.term_index.get(&property_term_id)?;
                                            if *predicate_term == owl::OBJECT_PROPERTY.into() {
                                                self.add_triple_to_element_buffer(
                                                    &data_buffer.term_index,
                                                    &mut data_buffer.edge_element_buffer,
                                                    &triple,
                                                    ElementType::Owl(OwlType::Edge(
                                                        OwlEdge::ObjectProperty,
                                                    )),
                                                )?;
                                                self.check_unknown_buffer(
                                                    data_buffer,
                                                    &triple.subject_term_id,
                                                )?;
                                                return Ok(SerializationStatus::Serialized);
                                            } else if *predicate_term
                                                == owl::DATATYPE_PROPERTY.into()
                                            {
                                                self.add_triple_to_element_buffer(
                                                    &data_buffer.term_index,
                                                    &mut data_buffer.edge_element_buffer,
                                                    &triple,
                                                    ElementType::Owl(OwlType::Edge(
                                                        OwlEdge::DatatypeProperty,
                                                    )),
                                                )?;
                                                self.check_unknown_buffer(
                                                    data_buffer,
                                                    &triple.subject_term_id,
                                                )?;
                                                return Ok(SerializationStatus::Serialized);
                                            }

                                            self.add_to_unknown_buffer(
                                                data_buffer,
                                                object_term_id,
                                                triple.clone(),
                                            )?;
                                            return Ok(SerializationStatus::Deferred);
                                        }
                                    }
                                    (None, Some(property_term_id), Some(range_term_id)) => {
                                        trace!(
                                            "Missing domain: {}",
                                            data_buffer.term_index.display_triple(&triple)?
                                        );

                                        let subject_term =
                                            data_buffer.term_index.get(&triple.subject_term_id)?;
                                        if *subject_term == owl::THING.into() {
                                            let thing_term_id = self.get_or_create_anchor_thing(
                                                data_buffer,
                                                &range_term_id,
                                            )?;

                                            (
                                                None,
                                                Some(self.create_triple_from_id(
                                                    &data_buffer.term_index,
                                                    thing_term_id,
                                                    Some(property_term_id),
                                                    Some(range_term_id),
                                                )?),
                                            )
                                        } else if *subject_term == rdfs::LITERAL.into() {
                                            let range_term =
                                                data_buffer.term_index.get(&range_term_id)?;
                                            let target_iri =
                                                synthetic_iri(&range_term, SYNTH_LITERAL);
                                            let node = self.create_triple_from_iri(
                                                &mut data_buffer.term_index,
                                                &target_iri,
                                                &rdfs::LITERAL.as_str().to_string(),
                                                None,
                                            )?;

                                            (
                                                Some(vec![node.clone()]),
                                                Some(self.create_triple_from_id(
                                                    &data_buffer.term_index,
                                                    node.subject_term_id,
                                                    Some(property_term_id),
                                                    triple.object_term_id,
                                                )?),
                                            )
                                        } else {
                                            self.add_to_unknown_buffer(
                                                data_buffer,
                                                object_term_id,
                                                triple,
                                            )?;
                                            return Ok(SerializationStatus::Deferred);
                                        }
                                    }
                                    (None, Some(property_term_id), None) => {
                                        trace!(
                                            "Missing domain and range: {}",
                                            data_buffer.term_index.display_triple(&triple)?
                                        );

                                        if self.has_non_fallback_property_edge(
                                            data_buffer,
                                            &property_term_id,
                                        )? {
                                            debug!(
                                                "Skipping structural fallback for '{}': property already has a concrete edge",
                                                data_buffer.term_index.get(&property_term_id)?
                                            );
                                            return Ok(SerializationStatus::Serialized);
                                        }

                                        let is_full_query_fallback = {
                                            if let Some(object_term_id) = triple.object_term_id {
                                                is_query_fallback_endpoint(
                                                    &data_buffer
                                                        .term_index
                                                        .get(&triple.subject_term_id)?,
                                                ) && is_query_fallback_endpoint(
                                                    &data_buffer.term_index.get(&object_term_id)?,
                                                )
                                            } else {
                                                false
                                            }
                                        };

                                        if !is_full_query_fallback {
                                            trace!(
                                                "Deferring property triple with unresolved structural domain/range: {}",
                                                data_buffer.term_index.display_triple(&triple)?
                                            );
                                            self.add_to_unknown_buffer(
                                                data_buffer,
                                                triple.subject_term_id,
                                                triple,
                                            )?;
                                            return Ok(SerializationStatus::Deferred);
                                        }

                                        let property_element_type = {
                                            data_buffer
                                                .edge_element_buffer
                                                .read()?
                                                .get(&property_term_id)
                                                .copied()
                                        };
                                        match property_element_type {
                                            Some(ElementType::Owl(OwlType::Edge(
                                                OwlEdge::DatatypeProperty,
                                            ))) => {
                                                let property_term = data_buffer
                                                    .term_index
                                                    .get(&property_term_id)?;

                                                let local_literal_iri = synthetic_iri(
                                                    &property_term,
                                                    SYNTH_LOCAL_LITERAL,
                                                );
                                                let literal_triple = self.create_triple_from_iri(
                                                    &mut data_buffer.term_index,
                                                    &local_literal_iri,
                                                    &rdfs::LITERAL.as_str().to_string(),
                                                    None,
                                                )?;

                                                let local_thing_iri = synthetic_iri(
                                                    &property_term,
                                                    SYNTH_LOCAL_THING,
                                                );
                                                let thing_triple = self.create_triple_from_iri(
                                                    &mut data_buffer.term_index,
                                                    &local_thing_iri,
                                                    &owl::THING.as_str().to_string(),
                                                    None,
                                                )?;

                                                (
                                                    Some(vec![
                                                        literal_triple.clone(),
                                                        thing_triple.clone(),
                                                    ]),
                                                    Some(self.create_triple_from_id(
                                                        &data_buffer.term_index,
                                                        thing_triple.subject_term_id,
                                                        Some(property_term_id),
                                                        Some(literal_triple.subject_term_id),
                                                    )?),
                                                )
                                            }
                                            Some(ElementType::Owl(OwlType::Edge(
                                                OwlEdge::ObjectProperty,
                                            ))) => {
                                                let thing_anchor_term_id = {
                                                    data_buffer
                                                        .term_index
                                                        .insert(owl::THING.into())?
                                                };
                                                let thing_term_id = self
                                                    .get_or_create_anchor_thing(
                                                        data_buffer,
                                                        &thing_anchor_term_id,
                                                    )?;

                                                (
                                                    None,
                                                    Some(self.create_triple_from_id(
                                                        &data_buffer.term_index,
                                                        thing_term_id,
                                                        Some(property_term_id),
                                                        Some(thing_term_id),
                                                    )?),
                                                )
                                            }
                                            _ => {
                                                debug!(
                                                    "Property triple ignored: Subject or Object not in display buffer."
                                                );
                                                return Ok(SerializationStatus::Deferred);
                                            }
                                        }
                                    }

                                    (Some(_), None, Some(_)) => {
                                        self.add_to_unknown_buffer(
                                            data_buffer,
                                            predicate_term_id,
                                            triple,
                                        )?;
                                        return Ok(SerializationStatus::Deferred);
                                    }
                                    _ => {
                                        self.add_to_unknown_buffer(
                                            data_buffer,
                                            triple.subject_term_id,
                                            triple,
                                        )?;
                                        return Ok(SerializationStatus::Deferred);
                                    }
                                };

                                match maybe_node_triples {
                                    Some(node_triples) => {
                                        for node_triple in node_triples {
                                            let predicate_term_id =
                                                data_buffer.get_predicate(&node_triple)?;

                                            let predicate_term =
                                                data_buffer.term_index.get(&predicate_term_id)?;
                                            if *predicate_term == owl::THING.into() {
                                                self.insert_node(
                                                    data_buffer,
                                                    node_triple,
                                                    ElementType::Owl(OwlType::Node(OwlNode::Thing)),
                                                )?;
                                            } else if *predicate_term == rdfs::LITERAL.into() {
                                                self.insert_node(
                                                    data_buffer,
                                                    node_triple,
                                                    ElementType::Rdfs(RdfsType::Node(
                                                        RdfsNode::Literal,
                                                    )),
                                                )?;
                                            }
                                        }
                                    }
                                    None => {
                                        // When subject/property/object are already resolved, no synthetic node is needed
                                    }
                                }

                                match edge_triple {
                                    Some(edge_triple) => {
                                        let edge_triple_predicate_term_id =
                                            data_buffer.get_predicate(&edge_triple)?;
                                        let property = {
                                            match data_buffer
                                                .edge_element_buffer
                                                .read()?
                                                .get(&edge_triple_predicate_term_id)
                                                .copied()
                                            {
                                                Some(prop) => prop,
                                                None => {
                                                    let msg = "Edge triple not present in edge_element_buffer".to_string();
                                                    let display_edge = data_buffer
                                                        .term_index
                                                        .display_triple(&edge_triple)?;
                                                    return Err(SerializationErrorKind::SerializationFailedTriple(display_edge, msg))?;
                                                }
                                            }
                                        };

                                        let label = {
                                            data_buffer
                                                .label_buffer
                                                .read()?
                                                .get(&edge_triple_predicate_term_id)
                                                .cloned()
                                        };
                                        let maybe_edge = self.insert_edge(
                                            data_buffer,
                                            edge_triple.clone(),
                                            property,
                                            label.flatten(),
                                        )?;

                                        if let Some(edge) = maybe_edge {
                                            let should_replace = !self
                                                .has_non_fallback_property_edge(
                                                    data_buffer,
                                                    &edge_triple_predicate_term_id,
                                                )?;

                                            if should_replace {
                                                data_buffer
                                                    .property_edge_map
                                                    .write()?
                                                    .insert(edge_triple_predicate_term_id, edge);
                                            }

                                            data_buffer
                                                .property_domain_map
                                                .write()?
                                                .entry(edge_triple_predicate_term_id)
                                                .or_default()
                                                .insert(edge_triple.subject_term_id);

                                            let object_term_id =
                                                edge_triple.object_term_id.ok_or_else(|| {
                                                    SerializationErrorKind::MissingObject(
                                                        data_buffer
                                                            .term_index
                                                            .display_triple(&edge_triple)
                                                            .unwrap_or_default(),
                                                        "Failed to update range for edge"
                                                            .to_string(),
                                                    )
                                                })?;

                                            data_buffer
                                                .property_range_map
                                                .write()?
                                                .entry(edge_triple_predicate_term_id)
                                                .or_default()
                                                .insert(object_term_id);
                                        }
                                    }
                                    None => {
                                        return Err(
                                            SerializationErrorKind::SerializationFailedTriple(
                                                data_buffer.term_index.display_triple(&triple)?,
                                                "Error creating edge".to_string(),
                                            )
                                            .into(),
                                        );
                                    }
                                }
                            }
                            None => {
                                return Err(SerializationErrorKind::SerializationFailedTriple(
                                    data_buffer.term_index.display_triple(&triple)?,
                                    "Object property triples should have a target".to_string(),
                                )
                                .into());
                            }
                        }
                    }
                }
            }
        }
        Ok(SerializationStatus::Serialized)
    }

    fn merge_individual_counts(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        old_term_id: &usize,
        new_term_id: usize,
    ) -> Result<(), SerializationError> {
        let mut individual_count_buffer = data_buffer.individual_count_buffer.write()?;
        if let Some(old_count) = individual_count_buffer.remove(old_term_id) {
            *individual_count_buffer.entry(new_term_id).or_default() += old_count;
        };
        Ok(())
    }

    fn get_or_create_domain_thing(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        domain_term_id: &usize,
    ) -> Result<usize, SerializationError> {
        {
            if let Some(existing) = data_buffer.anchor_thing_map.read()?.get(domain_term_id) {
                return Ok(*existing);
            }
        }

        let domain_term = data_buffer.term_index.get(domain_term_id)?;
        let thing_iri = synthetic_iri(&domain_term, SYNTH_THING);
        let thing_triple = self.create_triple_from_iri(
            &mut data_buffer.term_index,
            &thing_iri,
            &owl::THING.as_str().to_string(),
            None,
        )?;
        let thing_element = ElementType::Owl(OwlType::Node(OwlNode::Thing));

        self.insert_node(data_buffer, thing_triple.clone(), thing_element)?;

        {
            data_buffer
                .label_buffer
                .write()?
                .insert(thing_triple.subject_term_id, None);
        }
        {
            data_buffer
                .anchor_thing_map
                .write()?
                .insert(*domain_term_id, thing_triple.subject_term_id);
        }
        Ok(thing_triple.subject_term_id)
    }

    fn get_or_create_anchor_thing(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        anchor_term_id: &usize,
    ) -> Result<usize, SerializationError> {
        {
            if let Some(existing) = data_buffer.anchor_thing_map.read()?.get(anchor_term_id) {
                return Ok(*existing);
            }
        }

        let anchor_term = data_buffer.term_index.get(anchor_term_id)?;
        let thing_iri = synthetic_iri(&anchor_term, SYNTH_THING);
        let thing_triple = self.create_triple_from_iri(
            &mut data_buffer.term_index,
            &thing_iri,
            &owl::THING.as_str().to_string(),
            None,
        )?;
        let thing_element = ElementType::Owl(OwlType::Node(OwlNode::Thing));

        self.insert_node(data_buffer, thing_triple.clone(), thing_element)?;
        {
            data_buffer
                .label_buffer
                .write()?
                .insert(thing_triple.subject_term_id, None);
        }
        {
            data_buffer
                .anchor_thing_map
                .write()?
                .insert(*anchor_term_id, thing_triple.subject_term_id);
        }
        Ok(thing_triple.subject_term_id)
    }

    fn ensure_object_property_registration(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        property_term_id: usize,
    ) -> Result<(), SerializationError> {
        let already_registered = {
            data_buffer
                .edge_element_buffer
                .read()?
                .contains_key(&property_term_id)
        };
        if already_registered {
            return Ok(());
        }

        let property_iri = data_buffer.term_index.get(&property_term_id)?;
        if is_reserved(&property_iri) {
            return Ok(());
        }

        self.add_term_to_element_buffer(
            &data_buffer.term_index,
            &mut data_buffer.edge_element_buffer,
            property_term_id,
            ElementType::Owl(OwlType::Edge(OwlEdge::ObjectProperty)),
        )?;

        self.check_unknown_buffer(data_buffer, &property_term_id)?;
        Ok(())
    }

    fn insert_characteristic(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        triple: ArcTriple,
        characteristic: Characteristic,
    ) -> Result<SerializationStatus, SerializationError> {
        self.ensure_object_property_registration(data_buffer, triple.subject_term_id)?;

        let Some(resolved_property_term_id) = self.resolve(data_buffer, triple.subject_term_id)?
        else {
            let property_iri = data_buffer.term_index.get(&triple.subject_term_id)?;
            if is_reserved(&property_iri) {
                debug!(
                    "Skipping characteristic '{}' for reserved built-in '{}'",
                    characteristic, property_iri
                );
                return Ok(SerializationStatus::Serialized);
            }

            debug!(
                "Deferring characteristic '{}' for '{}': property unresolved",
                characteristic,
                data_buffer.term_index.get(&triple.subject_term_id)?
            );
            self.add_to_unknown_buffer(data_buffer, triple.subject_term_id, triple)?;
            return Ok(SerializationStatus::Deferred);
        };

        // Characteristic can attach only after a concrete edge exists
        let maybe_edge = {
            data_buffer
                .property_edge_map
                .read()?
                .get(&resolved_property_term_id)
                .cloned()
        };
        if let Some(edge) = maybe_edge {
            debug!(
                "Inserting edge characteristic: {} -> {}",
                data_buffer.term_index.get(&resolved_property_term_id)?,
                characteristic
            );

            let target_edges = if edge.edge_type
                == ElementType::Owl(OwlType::Edge(OwlEdge::InverseOf))
            {
                data_buffer
                    .edge_buffer
                    .read()?
                    .iter()
                    .filter(|candidate| {
                        candidate.edge_type == ElementType::Owl(OwlType::Edge(OwlEdge::InverseOf))
                            && candidate.property_term_id.as_ref()
                                == Some(&resolved_property_term_id)
                    })
                    .cloned()
                    .collect()
            } else {
                vec![edge]
            };

            let mut edge_characteristics = data_buffer.edge_characteristics.write()?;
            for target_edge in target_edges {
                edge_characteristics
                    .entry(target_edge)
                    .or_default()
                    .insert(characteristic);
            }
            return Ok(SerializationStatus::Serialized);
        }

        // Property is known, but edge not materialized yet
        let property_is_known = {
            data_buffer
                .edge_element_buffer
                .read()?
                .contains_key(&resolved_property_term_id)
        };
        if property_is_known {
            debug!(
                "Deferring characteristic '{}' for '{}': property known, edge not materialized yet",
                characteristic,
                data_buffer.term_index.get(&resolved_property_term_id)?
            );
            self.add_to_unknown_buffer(data_buffer, resolved_property_term_id, triple)?;
            return Ok(SerializationStatus::Deferred);
        }

        let resolved_iri = data_buffer.term_index.get(&resolved_property_term_id)?;
        if is_reserved(&resolved_iri) {
            debug!(
                "Skipping characteristic '{}' for reserved built-in '{}'",
                characteristic, resolved_iri
            );
            return Ok(SerializationStatus::Serialized);
        }

        // No attach point yet
        debug!(
            "Deferring characteristic '{}' for '{}': no attach point available yet",
            characteristic,
            data_buffer.term_index.get(&resolved_property_term_id)?
        );
        self.add_to_unknown_buffer(data_buffer, resolved_property_term_id, triple)?;
        Ok(SerializationStatus::Deferred)
    }

    fn should_skip_structural_operand(
        &self,
        data_buffer: &SerializationDataBuffer,
        subject_term_id: &usize,
        object_term_id: &usize,
        operator: &str,
    ) -> Result<bool, SerializationError> {
        if Self::is_consumed_restriction(data_buffer, object_term_id)? {
            debug!(
                "Skipping {} operand '{}': restriction already materialized",
                operator,
                data_buffer.term_index.get(object_term_id)?
            );
            return Ok(true);
        }

        if let (Some(resolved_subject), Some(resolved_target)) = (
            self.resolve(data_buffer, *subject_term_id)?,
            self.resolve(data_buffer, *object_term_id)?,
        ) && resolved_subject == resolved_target
        {
            debug!(
                "Skipping {} self-loop after restriction redirection: {} -> {}",
                operator,
                data_buffer.term_index.get(&resolved_subject)?,
                data_buffer.term_index.get(&resolved_target)?
            );
            return Ok(true);
        }

        Ok(false)
    }

    fn cardinality_literal(
        data_buffer: &SerializationDataBuffer,
        triple: &ArcTriple,
    ) -> Result<String, SerializationError> {
        let Some(object_term_id) = triple.object_term_id else {
            return Err(SerializationErrorKind::MissingObject(
                data_buffer.term_index.display_triple(triple)?,
                "Restriction cardinality triple is missing a target".to_string(),
            )
            .into());
        };

        let object_term = data_buffer.term_index.get(&object_term_id)?;
        match object_term.as_ref() {
            Term::Literal(literal) => Ok(literal.value().to_string()),
            other => Err(SerializationErrorKind::SerializationFailedTriple(
                data_buffer.term_index.display_triple(triple)?,
                format!("Expected cardinality literal, got '{other}'"),
            )
            .into()),
        }
    }

    fn canonical_count_term_id(
        &self,
        data_buffer: &SerializationDataBuffer,
        term_id: usize,
    ) -> Result<usize, SerializationError> {
        self.follow_redirection(data_buffer, term_id)
    }

    fn increment_individual_count(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        term_id: usize,
        delta: u32,
    ) -> Result<(), SerializationError> {
        let canonical_term_id = self.canonical_count_term_id(data_buffer, term_id)?;
        *data_buffer
            .individual_count_buffer
            .write()?
            .entry(canonical_term_id)
            .or_default() += delta;
        Ok(())
    }

    fn has_enumeration_member_edge(
        &self,
        data_buffer: &SerializationDataBuffer,
        subject_term_id: usize,
        object_term_id: usize,
    ) -> Result<bool, SerializationError> {
        let canonical_subject_term_id =
            self.canonical_count_term_id(data_buffer, subject_term_id)?;
        let canonical_object_term_id = self.canonical_count_term_id(data_buffer, object_term_id)?;

        let candidate = self.create_edge_from_id(
            &data_buffer.term_index,
            canonical_subject_term_id,
            ElementType::NoDraw,
            canonical_object_term_id,
            None,
        )?;

        Ok(data_buffer.edge_buffer.read()?.contains(&candidate))
    }

    fn individual_count_literal(
        data_buffer: &SerializationDataBuffer,
        triple: &ArcTriple,
    ) -> Result<u32, SerializationError> {
        let Some(object_term_id) = triple.object_term_id else {
            return Err(SerializationErrorKind::MissingObject(
                data_buffer.term_index.display_triple(triple)?,
                "NamedIndividual count triple is missing a target".to_string(),
            )
            .into());
        };

        let object_term = data_buffer.term_index.get(&object_term_id)?;
        match object_term.as_ref() {
            Term::Literal(literal) => match literal.value().parse::<u32>() {
                Ok(val) => Ok(val),
                Err(e) => Err(SerializationErrorKind::SerializationFailedTriple(
                    data_buffer.term_index.display_triple(triple)?,
                    format!(
                        "Expected individual count literal, got '{}': {}",
                        literal.value(),
                        e
                    ),
                ))?,
            },
            other => Err(SerializationErrorKind::SerializationFailedTriple(
                data_buffer.term_index.display_triple(triple)?,
                format!("Expected individual count literal, got '{other}'"),
            )
            .into()),
        }
    }

    fn is_consumed_restriction(
        data_buffer: &SerializationDataBuffer,
        restriction_term_id: &usize,
    ) -> Result<bool, SerializationError> {
        let result = data_buffer
            .edge_redirection
            .read()?
            .contains_key(restriction_term_id)
            && !data_buffer
                .node_element_buffer
                .read()?
                .contains_key(restriction_term_id)
            && !data_buffer
                .restriction_buffer
                .read()?
                .contains_key(restriction_term_id);
        Ok(result)
    }

    fn is_ephemeral_restriction_node(
        data_buffer: &SerializationDataBuffer,
        restriction_term_id: &usize,
    ) -> Result<bool, SerializationError> {
        let restriction = data_buffer.term_index.get(restriction_term_id)?;
        Ok(restriction.is_blank_node()
            || matches!(
                data_buffer
                    .node_element_buffer
                    .read()?
                    .get(restriction_term_id),
                Some(ElementType::Owl(OwlType::Node(OwlNode::AnonymousClass)))
            ))
    }

    fn restriction_owner(
        &self,
        data_buffer: &SerializationDataBuffer,
        restriction_term_id: &usize,
    ) -> Result<Option<usize>, SerializationError> {
        // After an owl:equivalentClass merge, restriction state can live on the
        // named class IRI itself. In that case, the class is the owner and must
        // not be inferred from incoming subclass edges.
        if !Self::is_ephemeral_restriction_node(data_buffer, restriction_term_id)? {
            return Ok(Some(*restriction_term_id));
        }

        let result = data_buffer
            .edges_include_map
            .read()?
            .get(restriction_term_id)
            .and_then(|edges| {
                edges.iter().find_map(|edge| {
                    (edge.range_term_id == *restriction_term_id && is_restriction_owner_edge(edge))
                        .then(|| edge.domain_term_id)
                })
            });
        Ok(result)
    }

    fn default_restriction_target(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        owner_term_id: &usize,
        property_term_id: &usize,
    ) -> Result<usize, SerializationError> {
        let property_edge_type = {
            data_buffer
                .edge_element_buffer
                .read()?
                .get(property_term_id)
                .copied()
        };

        match property_edge_type {
            Some(ElementType::Owl(OwlType::Edge(OwlEdge::DatatypeProperty))) => {
                let preferred_range_term_id = {
                    data_buffer
                        .property_range_map
                        .read()?
                        .get(property_term_id)
                        .and_then(|ranges| ranges.iter().next())
                        .copied()
                };

                if let Some(range_term_id) = preferred_range_term_id {
                    let range_term = data_buffer.term_index.get(&range_term_id)?;

                    if !is_query_fallback_endpoint(&range_term) {
                        if let Some(resolved_range_term_id) =
                            self.resolve(data_buffer, range_term_id)?
                        {
                            return Ok(resolved_range_term_id);
                        }

                        let node_exists = {
                            data_buffer
                                .node_element_buffer
                                .read()?
                                .contains_key(&range_term_id)
                        };

                        if !node_exists {
                            let predicate_term_id = {
                                if let Some(element_type) = try_resolve_reserved(&range_term) {
                                    let predicate = match element_type {
                                        ElementType::Rdfs(RdfsType::Node(RdfsNode::Datatype)) => {
                                            data_buffer.term_index.insert(rdfs::DATATYPE.into())?
                                        }
                                        _ => {
                                            data_buffer.term_index.insert(rdfs::RESOURCE.into())?
                                        }
                                    };

                                    let range_triple = self.create_triple_from_id(
                                        &data_buffer.term_index,
                                        range_term_id,
                                        Some(predicate),
                                        None,
                                    )?;

                                    self.insert_node(data_buffer, range_triple, element_type)?;
                                    return Ok(range_term_id);
                                }

                                data_buffer.term_index.insert(rdfs::DATATYPE.into())?
                            };

                            let range_triple = self.create_triple_from_id(
                                &data_buffer.term_index,
                                range_term_id,
                                Some(predicate_term_id),
                                None,
                            )?;

                            self.insert_node(
                                data_buffer,
                                range_triple,
                                ElementType::Rdfs(RdfsType::Node(RdfsNode::Datatype)),
                            )?;
                        }

                        return Ok(range_term_id);
                    }
                }

                let property_term = data_buffer.term_index.get(property_term_id)?;
                let literal_iri = synthetic_iri(&property_term, SYNTH_LITERAL);
                let literal_triple = self.create_triple_from_iri(
                    &mut data_buffer.term_index,
                    &literal_iri,
                    &rdfs::LITERAL.as_str().to_string(),
                    None,
                )?;
                let element_type = ElementType::Rdfs(RdfsType::Node(RdfsNode::Literal));

                let node_exists = {
                    data_buffer
                        .node_element_buffer
                        .read()?
                        .contains_key(&literal_triple.subject_term_id)
                };
                if !node_exists {
                    self.insert_node(data_buffer, literal_triple.clone(), element_type)?;
                }

                {
                    data_buffer.label_buffer.write()?.insert(
                        literal_triple.subject_term_id,
                        Some(element_type.to_string()),
                    );
                }

                Ok(literal_triple.subject_term_id)
            }
            _ => self.get_or_create_domain_thing(data_buffer, owner_term_id),
        }
    }

    fn materialize_one_of_target(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        owner_term_id: &usize,
        target_term_id: &usize,
    ) -> Result<usize, SerializationError> {
        let target_term = data_buffer.term_index.get(target_term_id)?;
        match target_term.as_ref() {
            Term::Literal(literal) => {
                self.materialize_literal_value_target(data_buffer, owner_term_id, literal)
            }
            Term::NamedNode(_) | Term::BlankNode(_) => {
                if let Some(resolved) = self.resolve(data_buffer, *target_term_id)? {
                    return Ok(resolved);
                }

                if !data_buffer
                    .label_buffer
                    .read()?
                    .contains_key(target_term_id)
                {
                    self.extract_label(data_buffer, None, &target_term, target_term_id)?;
                }

                let node_exists = {
                    data_buffer
                        .node_element_buffer
                        .read()?
                        .contains_key(target_term_id)
                };
                if !node_exists {
                    let predicate_term_id =
                        { data_buffer.term_index.insert(rdfs::RESOURCE.into())? };

                    let resource_triple = self.create_triple_from_id(
                        &data_buffer.term_index,
                        *target_term_id,
                        Some(predicate_term_id),
                        None,
                    )?;

                    self.insert_node(
                        data_buffer,
                        resource_triple,
                        ElementType::Rdfs(RdfsType::Node(RdfsNode::Resource)),
                    )?;
                }

                Ok(*target_term_id)
            }
        }
    }

    fn materialize_literal_value_target(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        restriction_term_id: &usize,
        literal: &Literal,
    ) -> Result<usize, SerializationError> {
        let subject_term_id = {
            let literal_iri = synthetic_iri(
                &data_buffer.term_index.get(restriction_term_id)?,
                SYNTH_LITERAL_VALUE,
            );
            data_buffer
                .term_index
                .insert(self.create_term(&literal_iri)?)?
        };

        let node_exists = {
            data_buffer
                .node_element_buffer
                .read()?
                .contains_key(&subject_term_id)
        };

        if !node_exists {
            let predicate_term_id = { data_buffer.term_index.insert(rdfs::LITERAL.into())? };
            let literal_triple = self.create_triple_from_id(
                &data_buffer.term_index,
                subject_term_id,
                Some(predicate_term_id),
                None,
            )?;

            self.insert_node(
                data_buffer,
                literal_triple,
                ElementType::Rdfs(RdfsType::Node(RdfsNode::Literal)),
            )?;
        }

        data_buffer
            .label_buffer
            .write()?
            .insert(subject_term_id, Some(literal.value().to_string()));

        Ok(subject_term_id)
    }

    fn register_property_endpoints(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        property_term_id: usize,
        edge: &ArcEdge,
    ) -> Result<(), SerializationError> {
        data_buffer
            .property_domain_map
            .write()?
            .entry(property_term_id)
            .or_default()
            .insert(edge.domain_term_id);

        data_buffer
            .property_range_map
            .write()?
            .entry(property_term_id)
            .or_default()
            .insert(edge.range_term_id);

        Ok(())
    }

    #[expect(clippy::too_many_arguments)]
    fn insert_restriction_edge(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        subject_term_id: usize,
        property_term_id: usize,
        object_term_id: usize,
        edge_type: ElementType,
        label: String,
        cardinality: Option<(String, Option<String>)>,
    ) -> Result<ArcEdge, SerializationError> {
        let edge = self.create_edge_from_id(
            &data_buffer.term_index,
            subject_term_id,
            edge_type,
            object_term_id,
            Some(property_term_id),
        )?;

        data_buffer.edge_buffer.write()?.insert(edge.clone());

        self.insert_edge_include(data_buffer, subject_term_id, edge.clone())?;
        self.insert_edge_include(data_buffer, object_term_id, edge.clone())?;

        {
            data_buffer
                .edge_label_buffer
                .write()?
                .insert(edge.clone(), Some(label));
        }
        if let Some(cardinality) = cardinality {
            data_buffer
                .edge_cardinality_buffer
                .write()?
                .insert(edge.clone(), cardinality);
        }

        Ok(edge)
    }

    fn restriction_edge_type(
        &self,
        data_buffer: &SerializationDataBuffer,
        property_term_id: &usize,
        render_mode: RestrictionRenderMode,
    ) -> Result<ElementType, SerializationError> {
        if render_mode == RestrictionRenderMode::ValuesFrom {
            return Ok(ElementType::Owl(OwlType::Edge(OwlEdge::ValuesFrom)));
        }

        match data_buffer
            .edge_element_buffer
            .read()?
            .get(property_term_id)
            .copied()
        {
            Some(
                edge_type @ ElementType::Owl(OwlType::Edge(
                    OwlEdge::ObjectProperty
                    | OwlEdge::DatatypeProperty
                    | OwlEdge::DeprecatedProperty
                    | OwlEdge::ExternalProperty,
                )),
            )
            | Some(edge_type @ ElementType::Rdf(RdfType::Edge(RdfEdge::RdfProperty))) => {
                Ok(edge_type)
            }
            Some(_) | None => Ok(ElementType::Owl(OwlType::Edge(OwlEdge::ObjectProperty))),
        }
    }

    fn is_numeric_cardinality(cardinality: &(String, Option<String>)) -> bool {
        let (min, max) = cardinality;

        let min_ok = min.is_empty() || min.chars().all(|c| c.is_ascii_digit());
        let max_ok = max
            .as_ref()
            .is_none_or(|value| value.is_empty() || value.chars().all(|c| c.is_ascii_digit()));

        min_ok && max_ok
    }

    fn try_materialize_restriction(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        restriction_term_id: &usize,
    ) -> Result<SerializationStatus, SerializationError> {
        let maybe_state_lock = {
            data_buffer
                .restriction_buffer
                .read()?
                .get(restriction_term_id)
                .cloned()
        };
        let Some(state_lock) = maybe_state_lock else {
            debug!(
                "Deferring restriction for term '{}': restriction metadata not available",
                data_buffer.term_index.get(restriction_term_id)?
            );
            return Ok(SerializationStatus::Deferred);
        };

        let state = state_lock.read()?;

        let Some(raw_property_term_id) = state.on_property else {
            debug!(
                "Deferring restriction for term '{}': restriction property not available",
                data_buffer.term_index.get(restriction_term_id)?
            );
            return Ok(SerializationStatus::Deferred);
        };
        let Some(property_term_id) = self.resolve(data_buffer, raw_property_term_id)? else {
            debug!(
                "Deferring restriction for term '{}': cannot resolve restriction property",
                data_buffer.term_index.get(restriction_term_id)?
            );
            return Ok(SerializationStatus::Deferred);
        };
        let Some(raw_subject_term_id) = self.restriction_owner(data_buffer, restriction_term_id)?
        else {
            debug!(
                "Deferring restriction for term '{}': cannot determine restriction owner",
                data_buffer.term_index.get(restriction_term_id)?
            );
            return Ok(SerializationStatus::Deferred);
        };

        let has_restriction_payload =
            state.self_restriction || state.filler.is_some() || state.cardinality.is_some();

        if !has_restriction_payload {
            debug!(
                "Deferring restriction for term '{}': only owl:onProperty is available so far",
                data_buffer.term_index.get(restriction_term_id)?
            );
            return Ok(SerializationStatus::Deferred);
        }
        if state.requires_filler && !state.self_restriction && state.filler.is_none() {
            debug!(
                "Deferring restriction for term '{}': filler is required, but not available",
                data_buffer.term_index.get(restriction_term_id)?
            );
            return Ok(SerializationStatus::Deferred);
        }

        let subject_term_id = match self.resolve(data_buffer, raw_subject_term_id)? {
            Some(term_id) => term_id,
            None => self.follow_redirection(data_buffer, raw_subject_term_id)?,
        };

        let restriction_edge_type =
            self.restriction_edge_type(data_buffer, &property_term_id, state.render_mode)?;

        let restriction_label = {
            let label_buffer = data_buffer.label_buffer.read()?;
            let property_edge_map = data_buffer.property_edge_map.read()?;
            let edge_label_buffer = data_buffer.edge_label_buffer.read()?;

            label_buffer
                .get(&raw_property_term_id)
                .cloned()
                .or_else(|| label_buffer.get(&property_term_id).cloned())
                .or_else(|| {
                    property_edge_map
                        .get(&property_term_id)
                        .and_then(|edge| edge_label_buffer.get(edge).cloned())
                })
                .flatten()
                .unwrap_or_else(|| restriction_edge_type.to_string())
        };

        if state.render_mode == RestrictionRenderMode::ExistingProperty {
            let Some(existing_edge) = data_buffer
                .property_edge_map
                .read()?
                .get(&property_term_id)
                .cloned()
            else {
                debug!(
                    "Deferring restriction for term '{}': edge not yet created",
                    data_buffer.term_index.get(restriction_term_id)?
                );
                return Ok(SerializationStatus::Deferred);
            };

            let object_term_id = if let Some(filler_id) = state.filler.as_ref() {
                let filler_term = data_buffer.term_index.get(filler_id)?;
                match &*filler_term {
                    Term::Literal(literal) => {
                        data_buffer.label_buffer.write()?.insert(
                            existing_edge.range_term_id,
                            Some(literal.value().to_string()),
                        );
                        existing_edge.range_term_id
                    }
                    _ => match self.resolve(data_buffer, *filler_id)? {
                        Some(resolved) => resolved,
                        None => self.materialize_named_value_target(
                            data_buffer,
                            &property_term_id,
                            filler_id,
                        )?,
                    },
                }
            } else {
                existing_edge.range_term_id
            };

            let edge = {
                let rewritten = self.rewrite_property_edge(
                    data_buffer,
                    &property_term_id,
                    subject_term_id,
                    object_term_id,
                )?;

                match rewritten {
                    Some(edge) => edge,
                    None => {
                        let triple = self.create_triple_from_id(
                            &data_buffer.term_index,
                            subject_term_id,
                            Some(property_term_id),
                            None,
                        )?;
                        let display_triple = data_buffer.term_index.display_triple(&triple)?;
                        return Err(SerializationErrorKind::SerializationFailedTriple(
                            display_triple,
                            "Failed to rewrite canonical property edge for hasValue restriction"
                                .to_string(),
                        ))?;
                    }
                }
            };

            data_buffer
                .edge_label_buffer
                .write()?
                .insert(edge.clone(), Some(restriction_label));

            if let Some(cardinality) = &state.cardinality {
                data_buffer
                    .edge_cardinality_buffer
                    .write()?
                    .insert(edge, cardinality.clone());
            }

            self.remove_restriction_stub(data_buffer, restriction_term_id)?;
            self.remove_restriction_node(data_buffer, restriction_term_id)?;

            if subject_term_id != *restriction_term_id {
                self.redirect_iri(data_buffer, *restriction_term_id, subject_term_id)?;
            }

            trace!(
                "Succesfully materialized restriction '{}'",
                data_buffer.term_index.get(restriction_term_id)?
            );
            return Ok(SerializationStatus::Serialized);
        }

        let object_term_id = if state.self_restriction {
            subject_term_id
        } else if let Some(filler_id) = state.filler {
            let filler_term = data_buffer.term_index.get(&filler_id)?;
            match &*filler_term {
                Term::Literal(literal) => self.materialize_literal_value_target(
                    data_buffer,
                    restriction_term_id,
                    literal,
                )?,
                _ => match self.resolve(data_buffer, filler_id)? {
                    Some(resolved) => resolved,
                    None => self.materialize_named_value_target(
                        data_buffer,
                        &property_term_id,
                        &filler_id,
                    )?,
                },
            }
        } else {
            self.default_restriction_target(data_buffer, &subject_term_id, &property_term_id)?
        };

        let maybe_numeric_cardinality = state
            .cardinality
            .as_ref()
            .filter(|cardinality| Self::is_numeric_cardinality(cardinality))
            .cloned();

        if let Some(cardinality) = maybe_numeric_cardinality {
            let maybe_existing_edge = {
                data_buffer
                    .property_edge_map
                    .read()?
                    .get(&property_term_id)
                    .cloned()
            };

            if let Some(existing_edge) = maybe_existing_edge {
                self.remove_property_fallback_edge(data_buffer, &property_term_id)?;

                let edge = match self.rewrite_property_edge(
                    data_buffer,
                    &property_term_id,
                    subject_term_id,
                    object_term_id,
                )? {
                    Some(edge) => edge,
                    None => existing_edge,
                };

                self.register_property_endpoints(data_buffer, property_term_id, &edge)?;

                {
                    data_buffer
                        .edge_cardinality_buffer
                        .write()?
                        .insert(edge, cardinality);
                }

                self.remove_restriction_stub(data_buffer, restriction_term_id)?;
                self.remove_restriction_node(data_buffer, restriction_term_id)?;

                if subject_term_id != *restriction_term_id {
                    self.redirect_iri(data_buffer, *restriction_term_id, subject_term_id)?;
                }

                trace!(
                    "Successfully materialized numeric cardinality restriction '{}' on existing property edge",
                    data_buffer.term_index.get(restriction_term_id)?
                );
                return Ok(SerializationStatus::Serialized);
            }
        }

        self.remove_property_fallback_edge(data_buffer, &property_term_id)?;

        let edge = self.insert_restriction_edge(
            data_buffer,
            subject_term_id,
            property_term_id,
            object_term_id,
            restriction_edge_type,
            restriction_label,
            state.cardinality.clone(),
        )?;

        self.register_property_endpoints(data_buffer, property_term_id, &edge)?;

        {
            data_buffer
                .property_edge_map
                .write()?
                .insert(property_term_id, edge);
        }
        self.remove_restriction_stub(data_buffer, restriction_term_id)?;
        self.remove_restriction_node(data_buffer, restriction_term_id)?;

        if subject_term_id != *restriction_term_id {
            self.redirect_iri(data_buffer, *restriction_term_id, subject_term_id)?;
        }

        trace!(
            "Succesfully materialized restriction '{}'",
            data_buffer.term_index.get(restriction_term_id)?
        );
        Ok(SerializationStatus::Serialized)
    }

    fn remove_restriction_stub(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        restriction_term_id: &usize,
    ) -> Result<(), SerializationError> {
        if !Self::is_ephemeral_restriction_node(data_buffer, restriction_term_id)? {
            return Ok(());
        }

        if let Some(edges) = {
            data_buffer
                .edges_include_map
                .read()?
                .get(restriction_term_id)
                .cloned()
        } {
            for edge in edges {
                if edge.range_term_id == *restriction_term_id && is_restriction_owner_edge(&edge) {
                    self.remove_edge_include(data_buffer, &edge.domain_term_id, &edge)?;
                    self.remove_edge_include(data_buffer, &edge.range_term_id, &edge)?;
                    {
                        data_buffer.edge_buffer.write()?.remove(&edge);
                    }
                    {
                        data_buffer.edge_label_buffer.write()?.remove(&edge);
                    }
                    {
                        data_buffer.edge_cardinality_buffer.write()?.remove(&edge);
                    }
                    {
                        data_buffer.edge_characteristics.write()?.remove(&edge);
                    }
                }
            }
        }
        Ok(())
    }

    fn materialize_named_value_target(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        property_term_id: &usize,
        target_term_id: &usize,
    ) -> Result<usize, SerializationError> {
        let property_element_type = {
            data_buffer
                .edge_element_buffer
                .read()?
                .get(property_term_id)
                .copied()
        };
        match property_element_type {
            Some(ElementType::Owl(OwlType::Edge(OwlEdge::ObjectProperty)))
            | Some(ElementType::Owl(OwlType::Edge(OwlEdge::ExternalProperty)))
            | Some(ElementType::Owl(OwlType::Edge(OwlEdge::DeprecatedProperty)))
            | Some(ElementType::Rdf(RdfType::Edge(RdfEdge::RdfProperty))) => {
                let target_has_label = {
                    data_buffer
                        .label_buffer
                        .read()?
                        .contains_key(target_term_id)
                };
                if !target_has_label {
                    let target_term = data_buffer.term_index.get(target_term_id)?;
                    self.extract_label(data_buffer, None, &target_term, target_term_id)?;
                }

                let node_exists = {
                    data_buffer
                        .node_element_buffer
                        .read()?
                        .contains_key(target_term_id)
                };
                if !node_exists {
                    let predicate_term_id = data_buffer.term_index.insert(rdfs::RESOURCE.into())?;
                    let resource_triple = self.create_triple_from_id(
                        &data_buffer.term_index,
                        *target_term_id,
                        Some(predicate_term_id),
                        None,
                    )?;

                    self.insert_node(
                        data_buffer,
                        resource_triple,
                        ElementType::Rdfs(RdfsType::Node(RdfsNode::Resource)),
                    )?;
                }

                Ok(*target_term_id)
            }

            Some(ElementType::Owl(OwlType::Edge(OwlEdge::DatatypeProperty))) => {
                let target_has_label = {
                    data_buffer
                        .label_buffer
                        .read()?
                        .contains_key(target_term_id)
                };
                if !target_has_label {
                    let target_term = data_buffer.term_index.get(target_term_id)?;
                    self.extract_label(data_buffer, None, &target_term, target_term_id)?;
                }

                let node_exists = {
                    data_buffer
                        .node_element_buffer
                        .read()?
                        .contains_key(target_term_id)
                };
                if !node_exists {
                    let target_term = data_buffer.term_index.get(target_term_id)?;

                    let predicate_term_id =
                        if let Some(element_type) = try_resolve_reserved(&target_term) {
                            let predicate = match element_type {
                                ElementType::Rdfs(RdfsType::Node(RdfsNode::Datatype)) => {
                                    data_buffer.term_index.insert(rdfs::DATATYPE.into())?
                                }
                                _ => data_buffer.term_index.insert(rdfs::RESOURCE.into())?,
                            };

                            let datatype_triple = self.create_triple_from_id(
                                &data_buffer.term_index,
                                *target_term_id,
                                Some(predicate),
                                None,
                            )?;

                            self.insert_node(data_buffer, datatype_triple, element_type)?;
                            return Ok(*target_term_id);
                        } else {
                            data_buffer.term_index.insert(rdfs::DATATYPE.into())?
                        };

                    let datatype_triple = self.create_triple_from_id(
                        &data_buffer.term_index,
                        *target_term_id,
                        Some(predicate_term_id),
                        None,
                    )?;

                    self.insert_node(
                        data_buffer,
                        datatype_triple,
                        ElementType::Rdfs(RdfsType::Node(RdfsNode::Datatype)),
                    )?;
                }

                Ok(*target_term_id)
            }

            _ => {
                let triple = self.create_triple_from_id(
                    &data_buffer.term_index,
                    *target_term_id,
                    Some(*property_term_id),
                    None,
                )?;
                let display_triple = data_buffer.term_index.display_triple(&triple)?;
                Err(SerializationErrorKind::SerializationFailedTriple(
                    display_triple,
                    format!(
                        "Cannot materialize named value target '{}' for non-object restriction",
                        data_buffer.term_index.get(target_term_id)?
                    ),
                )
                .into())
            }
        }
    }

    fn retry_restrictions(
        &self,
        data_buffer: &mut SerializationDataBuffer,
    ) -> Result<(), SerializationError> {
        let restrictions = {
            data_buffer
                .restriction_buffer
                .read()?
                .keys()
                .cloned()
                .collect::<Vec<_>>()
        };

        for restriction in restrictions {
            self.try_materialize_restriction(data_buffer, &restriction)?;
        }

        Ok(())
    }

    fn remove_restriction_node(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        restriction_term_id: &usize,
    ) -> Result<(), SerializationError> {
        // Named classes can temporarily carry restriction state after merging
        // an anonymous equivalentClass expression. Clear only the restriction state.
        if !Self::is_ephemeral_restriction_node(data_buffer, restriction_term_id)? {
            data_buffer
                .restriction_buffer
                .write()?
                .remove(restriction_term_id);
            return Ok(());
        }

        {
            data_buffer
                .node_element_buffer
                .write()?
                .remove(restriction_term_id);
        }
        {
            data_buffer
                .label_buffer
                .write()?
                .remove(restriction_term_id);
        }
        {
            data_buffer
                .node_characteristics
                .write()?
                .remove(restriction_term_id);
        }
        {
            data_buffer
                .edges_include_map
                .write()?
                .remove(restriction_term_id);
        }
        {
            data_buffer
                .restriction_buffer
                .write()?
                .remove(restriction_term_id);
        }
        {
            data_buffer
                .individual_count_buffer
                .write()?
                .remove(restriction_term_id);
        }
        Ok(())
    }

    fn is_synthetic_property_fallback(
        term_index: &TermIndex,
        edge: &Edge,
    ) -> Result<bool, SerializationError> {
        let is_property_edge = matches!(
            edge.edge_type,
            ElementType::Owl(OwlType::Edge(
                OwlEdge::ObjectProperty
                    | OwlEdge::DatatypeProperty
                    | OwlEdge::DeprecatedProperty
                    | OwlEdge::ExternalProperty
            )) | ElementType::Rdf(RdfType::Edge(RdfEdge::RdfProperty))
        );

        if !is_property_edge {
            return Ok(false);
        }

        let subject_term = term_index.get(&edge.domain_term_id)?;
        let object_term = term_index.get(&edge.range_term_id)?;
        Ok(is_synthetic(&subject_term) && is_synthetic(&object_term))
    }

    fn remove_orphan_synthetic_node(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        term_id: &usize,
    ) -> Result<(), SerializationError> {
        let term = data_buffer.term_index.get(term_id)?;

        if !is_synthetic(&term) {
            return Ok(());
        }

        let still_used = data_buffer
            .edges_include_map
            .read()?
            .get(term_id)
            .is_some_and(|edges| !edges.is_empty());

        if still_used {
            return Ok(());
        }

        {
            data_buffer.edges_include_map.write()?.remove(term_id);
        }
        {
            data_buffer.node_element_buffer.write()?.remove(term_id);
        }
        {
            data_buffer.label_buffer.write()?.remove(term_id);
        }
        {
            data_buffer.node_characteristics.write()?.remove(term_id);
        }
        {
            data_buffer
                .anchor_thing_map
                .write()?
                .retain(|_, value| value != term_id);
        }
        {
            data_buffer.individual_count_buffer.write()?.remove(term_id);
        }
        Ok(())
    }

    fn has_non_fallback_property_edge(
        &self,
        data_buffer: &SerializationDataBuffer,
        property_term_id: &usize,
    ) -> Result<bool, SerializationError> {
        let edge = {
            data_buffer
                .property_edge_map
                .read()?
                .get(property_term_id)
                .cloned()
        };

        let Some(edge) = edge else {
            return Ok(false);
        };

        Ok(!Self::is_synthetic_property_fallback(
            &data_buffer.term_index,
            &edge,
        )?)
    }

    fn remove_property_fallback_edge(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        property_term_id: &usize,
    ) -> Result<(), SerializationError> {
        let edge = {
            data_buffer
                .property_edge_map
                .read()?
                .get(property_term_id)
                .cloned()
        };
        let Some(edge) = edge else {
            return Ok(());
        };

        if !Self::is_synthetic_property_fallback(&data_buffer.term_index, &edge)? {
            return Ok(());
        }

        self.remove_edge_include(data_buffer, &edge.domain_term_id, &edge)?;
        self.remove_edge_include(data_buffer, &edge.range_term_id, &edge)?;

        {
            data_buffer.edge_buffer.write()?.remove(&edge);
        }
        {
            data_buffer.edge_label_buffer.write()?.remove(&edge);
        }
        {
            data_buffer.edge_cardinality_buffer.write()?.remove(&edge);
        }
        {
            data_buffer.edge_characteristics.write()?.remove(&edge);
        }
        {
            data_buffer
                .property_edge_map
                .write()?
                .remove(property_term_id);
        }
        {
            data_buffer
                .property_domain_map
                .write()?
                .remove(property_term_id);
        }
        {
            data_buffer
                .property_range_map
                .write()?
                .remove(property_term_id);
        }
        self.remove_orphan_synthetic_node(data_buffer, &edge.domain_term_id)?;
        self.remove_orphan_synthetic_node(data_buffer, &edge.range_term_id)?;
        Ok(())
    }

    fn rewrite_property_edge(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        property_term_id: &usize,
        new_subject_term_id: usize,
        new_object_term_id: usize,
    ) -> Result<Option<ArcEdge>, SerializationError> {
        let Some(old_edge) = ({
            data_buffer
                .property_edge_map
                .read()?
                .get(property_term_id)
                .cloned()
        }) else {
            return Ok(None);
        };

        if old_edge.domain_term_id == new_subject_term_id
            && old_edge.range_term_id == new_object_term_id
        {
            return Ok(Some(old_edge));
        }

        let new_edge = self.create_edge_from_id(
            &data_buffer.term_index,
            new_subject_term_id,
            old_edge.edge_type,
            new_object_term_id,
            old_edge.property_term_id,
        )?;

        let label = { data_buffer.edge_label_buffer.write()?.remove(&old_edge) };
        let characteristics = { data_buffer.edge_characteristics.write()?.remove(&old_edge) };
        let cardinality = {
            data_buffer
                .edge_cardinality_buffer
                .write()?
                .remove(&old_edge)
        };

        self.remove_edge_include(data_buffer, &old_edge.domain_term_id, &old_edge)?;
        self.remove_edge_include(data_buffer, &old_edge.range_term_id, &old_edge)?;
        {
            let mut edge_buffer = data_buffer.edge_buffer.write()?;
            edge_buffer.remove(&old_edge);

            edge_buffer.insert(new_edge.clone());
        }
        self.insert_edge_include(data_buffer, new_edge.domain_term_id, new_edge.clone())?;
        self.insert_edge_include(data_buffer, new_edge.range_term_id, new_edge.clone())?;

        if let Some(label) = label {
            data_buffer
                .edge_label_buffer
                .write()?
                .insert(new_edge.clone(), label);
        }
        if let Some(characteristics) = characteristics {
            data_buffer
                .edge_characteristics
                .write()?
                .insert(new_edge.clone(), characteristics);
        }
        if let Some(cardinality) = cardinality {
            data_buffer
                .edge_cardinality_buffer
                .write()?
                .insert(new_edge.clone(), cardinality);
        }

        {
            data_buffer
                .property_edge_map
                .write()?
                .insert(*property_term_id, new_edge.clone());
        }
        {
            data_buffer
                .property_domain_map
                .write()?
                .insert(*property_term_id, HashSet::from([new_subject_term_id]));
        }
        {
            data_buffer
                .property_range_map
                .write()?
                .insert(*property_term_id, HashSet::from([new_object_term_id]));
        }

        self.remove_orphan_synthetic_node(data_buffer, &old_edge.domain_term_id)?;
        self.remove_orphan_synthetic_node(data_buffer, &old_edge.range_term_id)?;

        Ok(Some(new_edge))
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    // use super::*;
    // use oxrdf::{BlankNode, Literal, NamedNode};

    #[ignore = "Not refactored yet"]
    #[test]
    fn test_replace_node() {
        // let _ = env_logger::builder().is_test(true).try_init();
        // let serializer = GraphDisplayDataSolutionSerializer::new();
        // let mut data_buffer = SerializationDataBuffer::new();

        // let example_com = Term::NamedNode(NamedNode::new("http://example.com#").unwrap());
        // let owl_ontology =
        //     Term::NamedNode(NamedNode::new("http://www.w3.org/2002/07/owl#Ontology").unwrap());
        // let example_parent = Term::NamedNode(NamedNode::new("http://example.com#Parent").unwrap());
        // let owl_class =
        //     Term::NamedNode(NamedNode::new("http://www.w3.org/2002/07/owl#Class").unwrap());
        // let example_mother = Term::NamedNode(NamedNode::new("http://example.com#Mother").unwrap());
        // let example_guardian =
        //     Term::NamedNode(NamedNode::new("http://example.com#Guardian").unwrap());
        // let example_warden = Term::NamedNode(NamedNode::new("http://example.com#Warden").unwrap());
        // let example_warden1 =
        //     Term::NamedNode(NamedNode::new("http://example.com#Warden1").unwrap());
        // let rdfs_subclass_of = Term::NamedNode(
        //     NamedNode::new("http://www.w3.org/2000/01/rdf-schema#subClassOf").unwrap(),
        // );
        // let blanknode1 =
        //     Term::BlankNode(BlankNode::new("e1013e66f734c508511575854b0c9396").unwrap());

        // let t1 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)
        // let t2 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)
        // let t3 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)
        // let t4 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)
        // let t5 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)
        // let t6 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)
        // let t7 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)
        // let t8 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)
        // let t9 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)
        // let t10 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)
        // let t11 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)
        // let t12 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)

        // serializer.write_node_triple(
        //     &mut data_buffer,
        //     Triple {
        //         subject_term_id: example_com.clone(),
        //         predicate_term_id: owl_ontology.clone(),
        //         object_term_id: None,
        //     },
        // );
        // serializer.write_node_triple(
        //     &mut data_buffer,
        //     Triple {
        //         subject_term_id: example_parent.clone(),
        //         predicate_term_id: owl_class.clone(),
        //         object_term_id: None,
        //     },
        // );
        // serializer.write_node_triple(
        //     &mut data_buffer,
        //     Triple {
        //         subject_term_id: example_mother.clone(),
        //         predicate_term_id: owl_class.clone(),
        //         object_term_id: None,
        //     },
        // );
        // serializer.write_node_triple(
        //     &mut data_buffer,
        //     Triple {
        //         subject_term_id: example_guardian.clone(),
        //         predicate_term_id: owl_class.clone(),
        //         object_term_id: None,
        //     },
        // );
        // serializer.write_node_triple(
        //     &mut data_buffer,
        //     Triple {
        //         subject_term_id: example_warden.clone(),
        //         predicate_term_id: owl_class.clone(),
        //         object_term_id: None,
        //     },
        // );
        // serializer.write_node_triple(
        //     &mut data_buffer,
        //     Triple {
        //         subject_term_id: example_warden1.clone(),
        //         predicate_term_id: owl_class.clone(),
        //         object_term_id: None,
        //     },
        // );
        // serializer.write_node_triple(
        //     &mut data_buffer,
        //     Triple {
        //         subject_term_id: example_warden.clone(),
        //         predicate_term_id: rdfs_subclass_of.clone(),
        //         object_term_id: Some(example_guardian.clone()),
        //     },
        // );
        // serializer.write_node_triple(
        //     &mut data_buffer,
        //     Triple {
        //         subject_term_id: example_mother.clone(),
        //         predicate_term_id: rdfs_subclass_of.clone(),
        //         object_term_id: Some(example_parent.clone()),
        //     },
        // );
        // serializer.write_node_triple(
        //     &mut data_buffer,
        //     Triple {
        //         subject_term_id: blanknode1.clone(),
        //         predicate_term_id: Term::Literal(Literal::new_simple_literal(
        //             "blanknode".to_string(),
        //         )),
        //         object_term_id: None,
        //     },
        // );
        // serializer.write_node_triple(
        //     &mut data_buffer,
        //     Triple {
        //         subject_term_id: example_warden1.clone(),
        //         predicate_term_id: Term::NamedNode(
        //             NamedNode::new("http://www.w3.org/2002/07/owl#unionOf").unwrap(),
        //         ),
        //         object_term_id: Some(example_warden.clone()),
        //     },
        // );

        // print_graph_display_data(&data_buffer);
        // println!("--------------------------------");

        // let triple = Triple {
        //     subject_term_id: example_guardian.clone(),
        //     predicate_term_id: Term::NamedNode(
        //         NamedNode::new("http://www.w3.org/2002/07/owl#equivalentClass").unwrap(),
        //     ),
        //     object_term_id: Some(example_warden.clone()),
        // };
        // serializer.write_node_triple(&mut data_buffer, triple);
        // for (k, v) in data_buffer.node_element_buffer.iter() {
        //     println!("element_buffer: {} -> {}", k, v);
        // }
        // for (k, v) in data_buffer.edges_include_map.iter() {
        //     println!("edges_include_map: {} -> {:?}", k, v);
        // }
        // for (k, v) in data_buffer.edge_redirection.iter() {
        //     println!("edge_redirection: {} -> {}", k, v);
        // }
        // assert!(
        //     data_buffer
        //         .node_element_buffer
        //         .contains_key(&example_guardian.clone())
        // );
        // assert!(
        //     !data_buffer
        //         .node_element_buffer
        //         .contains_key(&example_warden)
        // );
        // assert!(
        //     data_buffer
        //         .node_element_buffer
        //         .contains_key(&example_warden1)
        // );
        // assert!(data_buffer.edges_include_map.contains_key(&example_warden1));

        // assert!(data_buffer.edge_buffer.contains(&Edge {
        //     domain_term_id: example_warden1,
        //     edge_type: ElementType::NoDraw,
        //     range_term_id: example_guardian.clone(),
        //     property_term_id: None
        // }));
        // assert!(data_buffer.edge_redirection.contains_key(&example_warden));
        // assert_eq!(
        //     data_buffer
        //         .edge_redirection
        //         .get(&example_warden)
        //         .unwrap()
        //         .clone(),
        //     example_guardian
        // );
        // serializer.write_node_triple(
        //     &mut data_buffer,
        //     Triple {
        //         subject_term_id: Term::NamedNode(
        //             NamedNode::new("http://example.com#Guardian").unwrap(),
        //         ),
        //         predicate_term_id: Term::NamedNode(
        //             NamedNode::new("http://www.w3.org/2002/07/owl#equivalentClass").unwrap(),
        //         ),
        //         object_term_id: Some(blanknode1.clone()),
        //     },
        // );
        // let s = serializer.resolve(&data_buffer, blanknode1.clone());
        // assert!(s.is_some());
        // for (k, v) in data_buffer.node_element_buffer.iter() {
        //     println!("element_buffer: {} -> {}", k, v);
        // }
        // for (k, v) in data_buffer.edge_redirection.iter() {
        //     println!("edge_redirection: {} -> {}", k, v);
        // }
        // assert!(s.unwrap() == example_guardian);
        // assert!(!data_buffer.edges_include_map.contains_key(&blanknode1));
        // assert!(!data_buffer.edges_include_map.contains_key(&example_warden));
    }
}

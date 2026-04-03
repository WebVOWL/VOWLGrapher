use std::{
    collections::{HashMap, HashSet}, mem::take, rc::Rc, sync::{Arc, RwLock}, time::{Duration, Instant}, usize
};

use super::{Edge, RestrictionRenderMode, SerializationDataBuffer, Triple};
use crate::{
    errors::{SerializationError, SerializationErrorKind},
    serializers::{
        ArcEdge, ArcTriple, RestrictionState, index::TermIndex, util::{
            PROPERTY_EDGE_TYPES, is_reserved, is_synthetic,
            synthetic::{SYNTH_LITERAL, SYNTH_LOCAL_LITERAL, SYNTH_LOCAL_THING, SYNTH_THING},
            synthetic_iri, trim_tag_circumfix, try_resolve_reserved,
        }
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
use rdf_fusion::{
    execution::results::QuerySolutionStream,
    model::{BlankNode, NamedNode, Term},
};

use unescape_zero_copy::unescape_default;
use vowlr_parser::errors::VOWLRStoreError;
use vowlr_util::prelude::{ErrorRecord, VOWLRError};

// use rayon::iter::ParallelBridge;
// TODO:
// - https://morestina.net/1432/parallel-stream-processing-with-rayon
// - https://docs.rs/rayon/latest/rayon/iter/trait.ParallelBridge.html
// - https://users.rust-lang.org/t/parallel-work-collected-sequentially/13504/2
// - https://docs.rs/parallel-stream/latest/parallel_stream/
// - https://doc.rust-lang.org/nightly/std/sync/index.html

pub struct GraphDisplayDataSolutionSerializer {
    /// Maps terms to integer ids and vice-versa.
    ///
    /// Reduces memory usage and allocations.
    term_index: TermIndex,
}

pub enum SerializationStatus {
    Serialized,
    Deferred,
}

impl GraphDisplayDataSolutionSerializer {
    pub fn new() -> Self {
        Self {
            term_index: TermIndex::new(),
        }
    }

    /// Serializes a query solution stream into the data buffer.
    ///
    /// This method tries to continue serializing despite errors.
    /// As such, the `Ok` value contains non-fatal errors encountered during
    /// serialization. The `Err` value contains fatal errors, preventing serialization.
    pub async fn serialize_nodes_stream(
        &self,
        data: &mut GraphDisplayData,
        mut solution_stream: QuerySolutionStream,
    ) -> Result<Option<VOWLRError>, VOWLRError> {
        let mut count: u64 = 0;
        info!("Serializing query solution stream...");
        let start_time = Instant::now();
        let mut data_buffer = SerializationDataBuffer::new();

        // TODO: solution_stream.next() should return owned values.
        while let Some(maybe_solution) = solution_stream.next().await {
            let solution = match maybe_solution {
                Ok(solution) => solution,
                Err(e) => {
                    data_buffer
                        .failed_buffer.write().map_err(|pe| <SerializationError as Into<VOWLRError>>::into(pe.into()))?
                        .push(<VOWLRStoreError as Into<ErrorRecord>>::into(e.into()));
                    continue;
                }
            };
            let Some(subject_term) = solution.get("id") else {
                continue;
            };

            // Label must be extracted between getting id and nodeType from solutions due to "continue" in the else clause.
            let subject_term_id = data_buffer.term_index.insert(subject_term.to_owned())?;
            self.extract_label(
                &mut data_buffer,
                solution.get("label"),
                subject_term,
                &subject_term_id,
            );

            let Some(node_type_term) = solution.get("nodeType") else {
                continue;
            };

            let object_term_id = match solution
                    .get("target"){
                        Some(term) => {Some( data_buffer.term_index.insert(term.to_owned())?)}
                        None => None
                    };

            let triple = self.create_triple_from_id(subject_term_id, data_buffer.term_index.insert(node_type_term.to_owned())?, object_term_id) ;

            self.write_node_triple(&mut data_buffer, triple)
                .or_else(|e| {
                    data_buffer.failed_buffer.write().map_err(|pe| <SerializationError as Into<VOWLRError>>::into(pe.into()))?.push(e.into());
                    Ok::<SerializationStatus, VOWLRError>(SerializationStatus::Serialized)
                })?;
            count += 1;
        }
        self.check_all_unknowns(&mut data_buffer).or_else(|e| {
            data_buffer.failed_buffer.write().map_err(|pe| <SerializationError as Into<VOWLRError>>::into(pe.into()))?.push(e.into());
            Ok::<(), VOWLRError>(())
        })?;

        // Catch permanently unresolved triples
        for (term_id, triples) in data_buffer.unknown_buffer.write().map_err(|pe| <SerializationError as Into<VOWLRError>>::into(pe.into()))?.drain() {
            for triple in triples {
                let e: SerializationError = SerializationErrorKind::SerializationFailedTriple(
                    triple,
                    format!("Unresolved reference: could not map '{}'", term_id),
                )
                .into();
                data_buffer.failed_buffer.write().map_err(|pe| <SerializationError as Into<VOWLRError>>::into(pe.into()))?.push(e.into());
            }
        }

        let finish_time = Instant::now()
            .checked_duration_since(start_time)
            .unwrap_or(Duration::new(0, 0))
            .as_secs_f32();
        info!(
            "Serialization completed in {} s\n \
            \tTotal solutions: {count}\n \
            \tElements       : {}\n \
            \tEdges          : {}\n \
            \tLabels         : {}\n \
            \tCardinalities  : {}\n \
            \tCharacteristics: {}\n\n \
        ",
            finish_time,
            data_buffer.node_element_buffer.read().map_err(|pe| <SerializationError as Into<VOWLRError>>::into(pe.into()))?.len(),
            data_buffer.edge_buffer.read().map_err(|pe| <SerializationError as Into<VOWLRError>>::into(pe.into()))?.len(),
            data_buffer.label_buffer.read().map_err(|pe| <SerializationError as Into<VOWLRError>>::into(pe.into()))?.len(),
            data_buffer.edge_cardinality_buffer.read().map_err(|pe| <SerializationError as Into<VOWLRError>>::into(pe.into()))?.len(),
            data_buffer.edge_characteristics.read().map_err(|pe| <SerializationError as Into<VOWLRError>>::into(pe.into()))?.len() + data_buffer.node_characteristics.read().map_err(|pe| <SerializationError as Into<VOWLRError>>::into(pe.into()))?.len(),
        );
        debug!("{}", data_buffer);
        let errors = if !data_buffer.failed_buffer.read().map_err(|pe| <SerializationError as Into<VOWLRError>>::into(pe.into()))?.is_empty() {
            let mut failed_buffer = data_buffer.failed_buffer.write().map_err(|pe| <SerializationError as Into<VOWLRError>>::into(pe.into()))?;
            let total = failed_buffer.len();
            let err: VOWLRError = take(&mut *failed_buffer).into();
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
   
        let all_errors = match (errors, convert_errors) {
            (Some(mut e), Some(mut ce)) => {
                let ue = take(&mut e.records).into_iter().chain(take(&mut ce.records).into_iter()).collect::<Vec<_>>();
                Some(ue.into())
            }
            (Some(e), None) => {Some(e)}
            (None, Some(ce)) => {Some(ce)}
            (None, None) => {None}

        };
   
   
   
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
                    data_buffer.label_buffer.write()?.insert(*term_id, clean_label);
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
                            data_buffer.label_buffer.write()?.insert(*term_id, frag.to_string());
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
                                        .label_buffer.write()?
                                        .insert(*term_id, path.1.to_string());
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
    fn resolve(&self, data_buffer: &SerializationDataBuffer, term_id: usize) -> Result<Option<usize>, SerializationError> {
        let resolved = self.follow_redirection(data_buffer, term_id)?;

        if let Some(elem) = data_buffer.node_element_buffer.read()?.get(&resolved) {
            debug!("Resolved: {}: {}", resolved, elem);
            return Ok(Some(resolved));
        }

        if let Some(elem) = data_buffer.edge_element_buffer.read()?.get(&resolved) {
            debug!("Resolved: {}: {}", resolved, elem);
            return Ok(Some(resolved));
        }
        Ok(None)
    }

    /// Returns the subject and object of the triple if their element type is known.
    fn resolve_so(
        &self,
        data_buffer: &SerializationDataBuffer,
        triple: &Triple,
    ) -> Result<(Option<usize>, Option<usize>), SerializationError> {
        let resolved_subject = self.resolve(data_buffer, triple.subject_id)?;
        let resolved_object = match &triple.object_id {
            Some(target) => self.resolve(data_buffer, *target)?,
            None => {
                debug!("Cannot resolve object of triple:\n {}", triple);
                None
            }
        };
        Ok((resolved_subject, resolved_object))
    }

    /// Add subject of triple to the element buffer.
    ///
    /// In the future, this function will handle cases where an element
    /// identifies itself as multiple elements. E.g. an element is both an rdfs:Class and a owl:class.
    fn add_to_element_buffer(
        &self,
        element_buffer: &mut Arc<RwLock<HashMap<usize, ElementType>>>,
        triple: &ArcTriple,
        element_type: ElementType,
    ) -> Result<(), SerializationError> {
        // TODO: Check for potential deadlock with read and write (read must be out of scope before write)
        if let Some(element) = element_buffer.read()?.get(&triple.subject_id) {
            warn!(
                "Attempted to register '{}' to subject '{}' already registered as '{}'. Skipping",
                element_type, triple.subject_id, element
            );
        } else {
            trace!("Adding to element buffer: {}: {}", triple.subject_id, element_type);
            element_buffer.write()?.insert(triple.subject_id, element_type);
        }
        Ok(())
    }

    /// Add an IRI to the unresolved, unknown buffer.
    fn add_to_unknown_buffer(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        element_iri: usize,
        triple: ArcTriple,
    ) -> Result<(), SerializationError> {
        trace!("Adding to unknown buffer: {}: {}", element_iri, triple);

        data_buffer
            .unknown_buffer.write()?
            .entry(element_iri)
            .or_default()
            .insert(triple);



        Ok(())
    }

    /// Insert an edge into the element's edge set.
    fn insert_edge_include(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        element_id: usize,
        edge: ArcEdge,
    ) -> Result<(), SerializationError> {
        data_buffer
            .edges_include_map.write()?
            .entry(element_id)
            .or_default()
            .insert(edge);
        Ok(())
    }

    pub fn redirect_iri(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        old_id: usize,
        new_id: usize,
    ) -> Result<(), SerializationError> {
        debug!("Redirecting '{}' to '{}'", old_id, new_id);
        {
        data_buffer.edge_redirection.write()?.insert(old_id, new_id);
        }self.check_unknown_buffer(data_buffer, &old_id)?;
        Ok(())
    }

    fn follow_redirection(&self, data_buffer: &SerializationDataBuffer, term_id: usize) -> Result<usize, SerializationError> {
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

    pub fn check_unknown_buffer(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        term_id: &usize,
    ) -> Result<(), SerializationError> {
        let maybe_triples = {
            data_buffer.unknown_buffer.write()?.remove(term_id)
        };
        
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
        self.insert_node_impl(data_buffer, triple, node_type, true)
    }

    fn insert_node_impl(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        triple: ArcTriple,
        node_type: ElementType,
        retry_restrictions: bool,
    ) -> Result<(), SerializationError> {
        if data_buffer.edge_redirection.read()?.contains_key(&triple.subject_id) {
            debug!(
                "Skipping insert_node for '{}': already redirected",
                triple.subject_id
            );
            return Ok(());
        }

        let new_type = if self.is_external(data_buffer, &triple.subject_id)? {
            ElementType::Owl(OwlType::Node(OwlNode::ExternalClass))
        } else {
            node_type
        };

        self.add_to_element_buffer(&mut data_buffer.node_element_buffer, &triple, new_type);
        self.check_unknown_buffer(data_buffer, &triple.subject_id)?;

        if retry_restrictions {
            self.retry_restrictions(data_buffer)?;
        }

        Ok(())
    }

    fn insert_node_without_retry(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        triple: ArcTriple,
        node_type: ElementType,
    ) -> Result<(), SerializationError> {
        self.insert_node_impl(data_buffer, triple, node_type, false)
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
        let external_probe = if PROPERTY_EDGE_TYPES.contains(&edge_type) {
            &triple.predicate_id
        } else {
            &triple.subject_id
        };

        // Skip external check for NoDraw edges - they should always retain their type
        let new_type =
            if edge_type != ElementType::NoDraw && self.is_external(data_buffer, external_probe)? {
                ElementType::Owl(OwlType::Edge(OwlEdge::ExternalProperty))
            } else {
                edge_type
            };

        match self.resolve_so(data_buffer, &triple)? {
            (Some(subject_id), Some(object_id)) => {
                let should_hash_property = [
                    ElementType::Owl(OwlType::Edge(OwlEdge::ObjectProperty)),
                    ElementType::Owl(OwlType::Edge(OwlEdge::DatatypeProperty)),
                    ElementType::Owl(OwlType::Edge(OwlEdge::DeprecatedProperty)),
                    ElementType::Owl(OwlType::Edge(OwlEdge::ExternalProperty)),
                ];
                let property_id = if should_hash_property.contains(&new_type) {
                    Some(triple.predicate_id)
                } else {
                    None
                };
                let edge = self.create_edge_from_id(subject_id, new_type, object_id, property_id);
                trace!(
                    "Inserting edge: {} -> {} -> {}",
                    edge.domain_id, edge.edge_type, edge.range_id
                );

                data_buffer
                    .edge_element_buffer.write()?
                    .insert(triple.predicate_id, edge.edge_type);

                data_buffer.edge_buffer.write()?.insert(edge.clone());
                self.insert_edge_include(data_buffer, subject_id, edge.clone())?;
                self.insert_edge_include(data_buffer, object_id, edge.clone())?;

                data_buffer
                    .edge_label_buffer.write()?
                    .insert(edge.clone(), label.unwrap_or(new_type.to_string()));
                return Ok(Some(edge));
            }
            (None, Some(_)) => {
                debug!("Cannot resolve subject of triple:\n {}", triple);
                self.add_to_unknown_buffer(data_buffer, triple.subject_id, triple)?;
            }
            (Some(_), None) => {
                if let Some(obj_iri) = &triple.object_id {
                    // resolve_so already warns about unresolved object. No need to repeat it here.
                    self.add_to_unknown_buffer(data_buffer, obj_iri.clone(), triple.clone())?;
                }
            }
            _ => {
                debug!("Cannot resolve subject and object of triple:\n {}", triple);
                self.add_to_unknown_buffer(data_buffer, triple.subject_id.clone(), triple.clone())?;
            }
        }
        Ok(None)
    }

    fn is_external(&self, data_buffer: &SerializationDataBuffer, term_id: &usize) -> Result<bool, SerializationError> {
        let term =data_buffer.term_index.get(term_id)?;

            if term.is_blank_node() {
                return Ok(false);
            }
            let clean_term = trim_tag_circumfix(&term.to_string());
            match *data_buffer.document_base.read()? {
                Some(base) => {
                    Ok(!(clean_term.contains(&base)
                        || is_reserved(&term)
                        || is_synthetic(&term)))
                }
                None => {
                    warn!("Cannot determine externals: Missing document base!");
                    Ok(false)
                }
            }
        }

    fn merge_nodes(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        old: usize,
        new: usize,
    ) -> Result<(), SerializationError> {
        if old == new {
            return Ok(());
        }

        debug!("Merging node '{old}' into '{new}'");
        self.merge_restriction_state(data_buffer, old, new)?;
        {
        data_buffer.node_element_buffer.write()?.remove(&old);
        }self.update_edges(data_buffer, old, new);
        self.merge_individual_counts(data_buffer, old, new);
        self.redirect_iri(data_buffer, old, new)?;
        self.retry_restrictions(data_buffer)?;
        Ok(())
    }

    fn merge_restriction_state(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        old_id: usize,
        new_id: usize,
    ) -> Result<(), SerializationError> {
        let restriction_buffer = data_buffer.restriction_buffer.write()?;

        let Some(old_state) = restriction_buffer.remove(&old_id) else {
            return Ok(());
        };

        let RestrictionState {
            on_property,
            filler,
            cardinality,
            self_restriction,
            requires_filler,
            render_mode,
        } = *old_state.read()?;

        let new_state = restriction_buffer.entry(new_id).or_default().write()?;

        if new_state.on_property.is_none() {
            new_state.on_property = on_property;
        }
        if new_state.filler.is_none() {
            new_state.filler = filler;
        }
        if new_state.cardinality.is_none() {
            new_state.cardinality = cardinality;
        }

        new_state.self_restriction |= self_restriction;
        new_state.requires_filler |= requires_filler;

        if new_state.render_mode == RestrictionRenderMode::ValuesFromEdge {
            new_state.render_mode = render_mode;
        }
        Ok(())
    }

    fn update_edges(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        old_id: usize,
        new_id: usize,
    ) -> Result<(), SerializationError> {
        let old_edges = data_buffer.edges_include_map.remove(old_id);
        if let Some(old_edges) = old_edges {
            debug!("Updating edges from '{}' to '{}'", old_id, new_id);
            for mut edge in old_edges {
                let old_edge = edge.clone();
                let label = data_buffer.edge_label_buffer.remove(&old_edge);
                let cardinality = data_buffer.edge_cardinality_buffer.remove(&old_edge);
                let characteristics = data_buffer.edge_characteristics.remove(&old_edge);

                data_buffer.edge_buffer.remove(&old_edge);

                if old_edge.subject != *old_id {
                    self.remove_edge_include(data_buffer, &old_edge.subject, &old_edge);
                }
                if old_edge.range_id != *old_id {
                    self.remove_edge_include(data_buffer, &old_edge.range_id, &old_edge);
                }

                if edge.object == *old_id {
                    edge.object = new_id;
                }
                if edge.subject == *old_id {
                    edge.subject = new_id;
                }

                let is_degenerate_structural_edge = edge.subject == edge.object
                    && matches!(
                        edge.element_type,
                        ElementType::NoDraw
                            | ElementType::Rdfs(RdfsType::Edge(RdfsEdge::SubclassOf))
                    );

                if is_degenerate_structural_edge {
                    debug!("Dropping degenerate structural self-edge: {}", edge);
                    continue;
                }

                data_buffer.edge_buffer.insert(edge.clone());
                self.insert_edge_include(data_buffer, new_id, edge.clone());
                if let Some(label) = label {
                    data_buffer.edge_label_buffer.insert(edge.clone(), label);
                }
                if let Some(cardinality) = cardinality {
                    data_buffer
                        .edge_cardinality_buffer
                        .insert(edge.clone(), cardinality);
                }
                if let Some(characteristics) = characteristics {
                    data_buffer
                        .edge_characteristics
                        .insert(edge.clone(), characteristics);
                }

                for mapped_edge in data_buffer.property_edge_map.values_mut() {
                    if *mapped_edge == old_edge {
                        *mapped_edge = edge.clone();
                    }
                }
            }
        }
        Ok(())
    }

    fn upgrade_node_type(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        term_id: &usize,
        new_element: ElementType,
    ) {
        let old_elem_opt = data_buffer.node_element_buffer.get(term_id).cloned();
        match old_elem_opt {
            Some(old_elem) => {
                if Self::can_upgrade_node_type(old_elem, new_element) {
                    data_buffer
                        .node_element_buffer
                        .insert(*term_id, new_element);
                }
                debug!(
                    "Upgraded subject '{}' from {} to {}",
                    term_id, old_elem, new_element
                )
            }
            None => {
                warn!(
                    "Upgraded unresolved subject '{}' to {}",
                    term_id, new_element
                )
            }
        }
    }

    fn is_structural_set_node(element: ElementType) -> bool {
        matches!(
            element,
            ElementType::Owl(OwlType::Node(
                OwlNode::Complement
                    | OwlNode::IntersectionOf
                    | OwlNode::UnionOf
                    | OwlNode::DisjointUnion
            ))
        )
    }

    fn has_named_equivalent_aliases(
        data_buffer: &SerializationDataBuffer,
        term_id: &usize,
    ) -> bool {
        data_buffer
            .edge_redirection
            .iter()
            .any(|(alias, target)| target == term_id && data_buffer.term_index.is_named_node(alias))
    }

    fn can_upgrade_node_type(old: ElementType, new: ElementType) -> bool {
        if matches!(
            old,
            ElementType::Owl(OwlType::Node(OwlNode::Class | OwlNode::AnonymousClass))
        ) {
            return true;
        }

        old == ElementType::Owl(OwlType::Node(OwlNode::EquivalentClass))
            && Self::is_structural_set_node(new)
    }

    fn upgrade_deprecated_node_type(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        term_id: &usize,
    ) {
        let old_elem_opt = data_buffer.node_element_buffer.get(term_id).copied();
        match old_elem_opt {
            Some(old_elem)
                if matches!(
                    old_elem,
                    ElementType::Owl(OwlType::Node(OwlNode::Class | OwlNode::AnonymousClass))
                        | ElementType::Rdfs(RdfsType::Node(RdfsNode::Class))
                        | ElementType::Owl(OwlType::Node(OwlNode::DeprecatedClass))
                ) =>
            {
                let new_element = ElementType::Owl(OwlType::Node(OwlNode::DeprecatedClass));
                data_buffer
                    .node_element_buffer
                    .insert(*term_id, new_element);
                debug!(
                    "Upgraded deprecated class '{}' from {} to {}",
                    term_id, old_elem, new_element
                );
            }
            Some(old_elem) => {
                warn!(
                    "Skipping owl:Deprecated node upgrade for '{}': {} is not a class",
                    term_id, old_elem
                );
            }
            None => {
                warn!(
                    "Cannot upgrade unresolved subject '{}' to DeprecatedClass",
                    term_id
                );
            }
        }
    }

    fn upgrade_property_type(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        property_id: &usize,
        new_element: ElementType,
    ) {
        let old_elem_opt = data_buffer.edge_element_buffer.get(property_id).copied();
        let Some(old_elem) = old_elem_opt else {
            warn!(
                "Cannot upgrade unresolved property '{}' to {}",
                property_id, new_element
            );
            return;
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
            warn!(
                "Skipping owl:Deprecated property upgrade for '{}': {} is not a property",
                property_id, old_elem
            );
            return;
        }

        data_buffer
            .edge_element_buffer
            .insert(*property_id, new_element);

        let Some(old_edge) = data_buffer.property_edge_map.get(property_id).cloned() else {
            debug!(
                "Upgraded property '{}' from {} to {} before edge materialization",
                property_id, old_elem, new_element
            );
            return;
        };

        if old_edge.edge_type == ElementType::Owl(OwlType::Edge(OwlEdge::InverseOf)) {
            debug!(
                "Keeping merged inverse edge for '{}' as {} instead of downgrading it to {}",
                property_id, old_edge.edge_type, new_element
            );
            return;
        }

        // TODO: Check if can be replaced with mutation on old_edge
        let mut new_edge = old_edge.clone();
        new_edge.edge_type = new_element;

        data_buffer.edge_buffer.remove(&old_edge);
        data_buffer.edge_buffer.insert(new_edge.clone());

        let label = data_buffer
            .label_buffer
            .get(property_id)
            .cloned()
            .or_else(|| data_buffer.edge_label_buffer.remove(&old_edge))
            .unwrap_or_else(|| new_element.to_string());
        data_buffer
            .edge_label_buffer
            .insert(new_edge.clone(), label);

        if let Some(characteristics) = data_buffer.edge_characteristics.remove(&old_edge) {
            data_buffer
                .edge_characteristics
                .insert(new_edge.clone(), characteristics);
        }

        if let Some(edges) = data_buffer.edges_include_map.get_mut(&old_edge.domain) {
            edges.remove(&old_edge);
            edges.insert(new_edge.clone());
        }
        if let Some(edges) = data_buffer.edges_include_map.get_mut(&old_edge.range) {
            edges.remove(&old_edge);
            edges.insert(new_edge.clone());
        }

        data_buffer
            .property_edge_map
            .insert(property_id.clone(), new_edge);

        debug!(
            "Upgraded deprecated property '{}' from {} to {}",
            property_id, old_elem, new_element
        );
    }

    fn remove_edge_include(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        element_id: &usize,
        edge: &Rc<Edge>,
    ) {
        if let Some(edges) = data_buffer.edges_include_map.get_mut(element_id) {
            edges.remove(edge);
        }
    }

    fn merge_properties(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        old_id: &usize,
        new_id: &usize,
    ) -> Result<(), SerializationError> {
        if old_id == new_id {
            return Ok(());
        }

        debug!("Merging property '{old_id}' into '{new_id}'");

        data_buffer.edge_element_buffer.remove(old_id);

        // Remove stale node placeholders for property aliases.
        data_buffer.node_element_buffer.remove(old_id);
        data_buffer.label_buffer.remove(old_id);
        data_buffer.node_characteristics.remove(old_id);

        if let Some(domains) = data_buffer.property_domain_map.remove(old_id) {
            data_buffer
                .property_domain_map
                .entry(*new_id)
                .or_default()
                .extend(domains);
        }

        if let Some(ranges) = data_buffer.property_range_map.remove(old_id) {
            data_buffer
                .property_range_map
                .entry(*new_id)
                .or_default()
                .extend(ranges);
        }

        self.redirect_iri(data_buffer, *old_id, *new_id)?;
        Ok(())
    }

    fn normalize_inverse_endpoint(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        endpoint_id: &usize,
        opposite_id: &usize,
    ) -> Result<usize, SerializationError> {
        let Some(element_type) = data_buffer.node_element_buffer.get(endpoint_id) else {
            return Ok(*endpoint_id);
        };

        match element_type {
            ElementType::Owl(OwlType::Node(
                OwlNode::Complement
                | OwlNode::IntersectionOf
                | OwlNode::UnionOf
                | OwlNode::DisjointUnion
                | OwlNode::EquivalentClass,
            )) => self.get_or_create_anchor_thing(data_buffer, opposite_id),
            _ => Ok(*endpoint_id),
        }
    }

    fn inverse_edge_endpoints(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        property_id: &usize,
    ) -> Result<Option<(usize, usize)>, SerializationError> {
        let domain = data_buffer
            .property_domain_map
            .get(property_id)
            .and_then(|domains| domains.iter().next())
            .cloned();
        let range = data_buffer
            .property_range_map
            .get(property_id)
            .and_then(|ranges| ranges.iter().next())
            .cloned();

        match (domain, range) {
            (Some(domain), Some(range)) => {
                let subject = self.normalize_inverse_endpoint(data_buffer, &domain, &range)?;
                let object = self.normalize_inverse_endpoint(data_buffer, &range, &domain)?;
                Ok(Some((*subject, *object)))
            }
            _ => Ok(None),
        }
    }

    fn insert_inverse_of(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        triple: Rc<Triple>,
    ) -> SerializationStatus {
        let left_property_raw = triple.subject_id;
        let Some(right_property_raw) = triple.object_id else {
            warn!("owl:inverseOf triple is missing a target: {}", triple);
            return SerializationStatus::Serialized;
        };

        let Some(left_property) = self.resolve(data_buffer, left_property_raw) else {
            self.add_to_unknown_buffer(data_buffer, left_property_raw, triple);
            return SerializationStatus::Deferred;
        };

        let Some(right_property) = self.resolve(data_buffer, right_property_raw) else {
            self.add_to_unknown_buffer(data_buffer, right_property_raw, triple);
            return SerializationStatus::Deferred;
        };

        if left_property == right_property {
            return SerializationStatus::Serialized;
        }

        let (left_subject, left_object) =
            match self.inverse_edge_endpoints(data_buffer, &left_property) {
                Ok(Some(endpoints)) => endpoints,
                Ok(None) => {
                    self.add_to_unknown_buffer(data_buffer, left_property, triple);
                    return SerializationStatus::Deferred;
                }
                Err(err) => {
                    error!(
                        "Failed to resolve inverse endpoints for '{}': {}",
                        left_property, err
                    );
                    return SerializationStatus::Deferred;
                }
            };

        let (right_subject, right_object) =
            match self.inverse_edge_endpoints(data_buffer, &right_property) {
                Ok(Some(endpoints)) => endpoints,
                Ok(None) => {
                    self.add_to_unknown_buffer(data_buffer, right_property, triple);
                    return SerializationStatus::Deferred;
                }
                Err(err) => {
                    error!(
                        "Failed to resolve inverse endpoints for '{}': {}",
                        right_property, err
                    );
                    return SerializationStatus::Deferred;
                }
            };

        let compatible = left_subject == right_object && left_object == right_subject;
        if !compatible {
            warn!(
                "Cannot merge owl:inverseOf '{}'<->'{}': normalized edges do not align ({} -> {}, {} -> {})",
                left_property,
                right_property,
                left_subject,
                left_object,
                right_subject,
                right_object
            );
            return SerializationStatus::Serialized;
        }

        let left_edge = data_buffer.property_edge_map.get(&left_property).cloned();
        let right_edge = data_buffer.property_edge_map.get(&right_property).cloned();

        let left_label = left_edge
            .as_ref()
            .and_then(|edge| data_buffer.edge_label_buffer.get(edge).cloned())
            .or_else(|| data_buffer.label_buffer.get(&left_property).cloned());

        let right_label = right_edge
            .as_ref()
            .and_then(|edge| data_buffer.edge_label_buffer.get(edge).cloned())
            .or_else(|| data_buffer.label_buffer.get(&right_property).cloned());

        let merged_label = Self::merge_optional_labels(left_label, right_label);

        if let Err(err) = self.merge_properties(data_buffer, &right_property, &left_property) {
            error!(
                "Failed to merge inverse properties '{}' and '{}': {}",
                left_property, right_property, err
            );
            return SerializationStatus::Deferred;
        }

        if let Some(left_edge) = left_edge.as_ref() {
            self.remove_edge_include(data_buffer, &left_edge.domain, left_edge);
            self.remove_edge_include(data_buffer, &left_edge.range_id, left_edge);
            data_buffer.edge_buffer.remove(left_edge);
            data_buffer.edge_label_buffer.remove(left_edge);
        }

        if let Some(right_edge) = right_edge.as_ref() {
            self.remove_edge_include(data_buffer, &right_edge.domain, right_edge);
            self.remove_edge_include(data_buffer, &right_edge.range_id, right_edge);
            data_buffer.edge_buffer.remove(right_edge);
            data_buffer.edge_label_buffer.remove(right_edge);
        }

        let mut merged_characteristics = left_edge
            .as_ref()
            .and_then(|edge| data_buffer.edge_characteristics.remove(edge))
            .unwrap_or_default();

        if let Some(right_characteristics) = right_edge
            .as_ref()
            .and_then(|edge| data_buffer.edge_characteristics.remove(edge))
        {
            merged_characteristics.extend(right_characteristics);
        }

        let inverse_property = Some(left_property);
        let inverse_edges = [
            Rc::new(Edge {
                domain_id: left_subject,
                edge_type: ElementType::Owl(OwlType::Edge(OwlEdge::InverseOf)),
                range_id: left_object,
                property_id: inverse_property,
            }),
            Rc::new(Edge {
                domain_id: left_object,
                edge_type: ElementType::Owl(OwlType::Edge(OwlEdge::InverseOf)),
                range_id: left_subject,
                property_id: inverse_property,
            }),
        ];

        let canonical_edge = inverse_edges[0].clone();

        for edge in inverse_edges {
            data_buffer.edge_buffer.insert(edge.clone());
            self.insert_edge_include(data_buffer, edge.domain_id, edge.clone());
            self.insert_edge_include(data_buffer, edge.range_id, edge.clone());
            if let Some(label) = merged_label.clone() {
                data_buffer.edge_label_buffer.insert(edge.clone(), label);
            }

            if !merged_characteristics.is_empty() {
                data_buffer
                    .edge_characteristics
                    .insert(edge, merged_characteristics.clone());
            }
        }

        data_buffer
            .property_edge_map
            .insert(left_property, canonical_edge);
        data_buffer.property_edge_map.remove(&right_property);

        SerializationStatus::Serialized
    }

    /// Appends a string to an element's label.
    fn extend_element_label(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        element_id: &usize,
        label_to_append: String,
    ) {
        debug!(
            "Extending element '{}' with label '{}'",
            element_id, label_to_append
        );
        if let Some(label) = data_buffer.label_buffer.get_mut(element_id) {
            label.push_str(format!("\n{}", label_to_append).as_str());
        } else {
            data_buffer
                .label_buffer
                .insert(*element_id, label_to_append);
        }
    }

    fn create_named_node(&self, iri: &String) -> Result<NamedNode, SerializationError> {
        Ok(NamedNode::new(iri)
            .map_err(|e| SerializationErrorKind::IriParseError(iri.clone(), Box::new(e)))?)
    }

    fn create_blank_node(&self, id: &String) -> Result<BlankNode, SerializationError> {
        Ok(BlankNode::new(id)
            .map_err(|e| SerializationErrorKind::BlankNodeParseError(id.clone(), Box::new(e)))?)
    }

    fn create_triple_from_iri(
        &self,
        term_index: &mut TermIndex,
        subject_iri: &String,
        predicate_iri: &String,
        object_iri: Option<&String>,
    ) -> Result<ArcTriple, SerializationError> {
        let subject_id = {
            let subject_term = match NamedNode::new(subject_iri) {
                Ok(node) => Term::NamedNode(node),
                Err(_) => Term::BlankNode(self.create_blank_node(subject_iri)?),
            };
            term_index.insert(subject_term)?
        };

        let predicate_id =
            term_index.insert(Term::NamedNode(self.create_named_node(predicate_iri)?))?;

        let object_id = match object_iri {
            Some(iri) => Some(term_index.insert(Term::NamedNode(self.create_named_node(iri)?))?),
            None => None,
        };

        let triple = Triple::new(subject_id, predicate_id, object_id).into();
        debug!("Created new triple: {}", triple);
        Ok(triple)
    }

    fn create_triple_from_id(&self,
        subject_id: usize,
        predicate_id: usize,
        object_id: Option<usize>,
    ) -> ArcTriple {
        let triple = Triple::new(subject_id, predicate_id, object_id).into();
        debug!("Created new triple: {}", triple);
        triple
    }

    fn create_edge_from_id(&self,         domain_id: usize,
        edge_type: ElementType,
        range_id: usize,property_id: Option<usize>) -> ArcEdge {
            let edge = Edge::new(domain_id, edge_type, range_id, property_id).into();
              debug!("Created new edge: {}", edge);
        edge
        }

    fn check_all_unknowns(
        &self,
        data_buffer: &mut SerializationDataBuffer,
    ) -> Result<(), SerializationError> {
        let mut pending = take(&mut data_buffer.unknown_buffer);
        let mut pass: usize = 0;
        let max_passes: usize = 4;

        while !pending.is_empty() && pass < max_passes {
            pass += 1;

            let pending_before: usize = pending.values().map(|set| set.len()).sum();
            info!(
                "Unknown resolution pass {} ({} triples pending)",
                pass, pending_before
            );

            let current = pending;

            for (term_id, triples) in current {
                let term = data_buffer.term_index.get(&term_id).ok_or_else(|| {
                    SerializationErrorKind::TermIndexError(format!(
                        "Failed to find term {} in the term index",
                        term_id
                    ))
                })?;

                if !data_buffer.label_buffer.contains_key(&term_id) {
                    self.extract_label(data_buffer, None, &term, &term_id);
                }

                if self.is_external(data_buffer, &term_id) {
                    // Dummy triple, only subject matters.
                    let external_triple = Triple::new(term_id, usize::MAX, None);

                    self.insert_node(
                        data_buffer,
                        &external_triple,
                        ElementType::Owl(OwlType::Node(OwlNode::ExternalClass)),
                    )?;
                } else if let Some(element_type) = try_resolve_reserved(&term) {
                    // Dummy triple, only subject matters.
                    let reserved_triple = Triple::new(term_id, usize::MAX, None);

                    self.insert_node(data_buffer, &reserved_triple, element_type)?;
                }

                for triple in triples {
                    match self.write_node_triple(data_buffer, triple) {
                        Ok(SerializationStatus::Serialized) => {}
                        Ok(SerializationStatus::Deferred) => {}
                        Err(e) => {
                            data_buffer.failed_buffer.push(e.into());
                        }
                    }
                }
            }

            // Collect newly deferred triples produced during this pass.
            pending = take(&mut data_buffer.unknown_buffer);
            let pending_after: usize = pending.values().map(|set| set.len()).sum();

            if pending_after >= pending_before {
                info!(
                    "Unknown resolution reached fixpoint after pass {} ({} triples still pending)",
                    pass, pending_after
                );
                break;
            }
        }

        data_buffer.unknown_buffer = pending;
        Ok(())
    }

    /// Serialize a triple to `data_buffer`.
    fn write_node_triple(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        triple: Arc<Triple>,
    ) -> Result<SerializationStatus, SerializationError> {
        let predicate_term = data_buffer
            .term_index
            .get(&triple.predicate_id)?;


        match *predicate_term {
            Term::BlankNode(bnode) => {
                // The query must never put blank nodes in the ?nodeType variable
                let msg = format!("Illegal blank node during serialization: '{bnode}'");
                return Err(SerializationErrorKind::SerializationFailedTriple(triple, msg).into());
            }
            Term::Literal(literal) => match literal.value() {
                "blanknode" => {
                    self.insert_node(
                        data_buffer,
                        &triple,
                        ElementType::Owl(OwlType::Node(OwlNode::AnonymousClass)),
                    )?;
                }
                other => {
                    warn!("Visualization of literal '{other}' is not supported");
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
                            &triple,
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
                        ) {
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
                            &triple,
                            ElementType::Rdfs(RdfsType::Node(RdfsNode::Datatype)),
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }
                    rdf::PLAIN_LITERAL => {
                        self.insert_node(
                            data_buffer,
                            &triple,
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
                            &triple,
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
                            &triple,
                            ElementType::Rdfs(RdfsType::Node(RdfsNode::Datatype)),
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }
                    rdfs::DOMAIN => {
                        return Err(SerializationErrorKind::SerializationFailedTriple(
                            triple,
                            "SPARQL query should not have rdfs:domain triples".to_string(),
                        )
                        .into());
                    }

                    // rdfs::IS_DEFINED_BY => {}

                    // rdfs::LABEL => {}
                    rdfs::LITERAL => {
                        self.insert_node(
                            data_buffer,
                            &triple,
                            ElementType::Rdfs(RdfsType::Node(RdfsNode::Literal)),
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }
                    // rdfs::MEMBER => {}
                    rdfs::RANGE => {
                        return Err(SerializationErrorKind::SerializationFailedTriple(
                            triple,
                            "SPARQL query should not have rdfs:range triples".to_string(),
                        )
                        .into());
                    }
                    rdfs::RESOURCE => {
                        self.insert_node(
                            data_buffer,
                            &triple,
                            ElementType::Rdfs(RdfsType::Node(RdfsNode::Resource)),
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }

                    //TODO: OWL1
                    // rdfs::SEE_ALSO => {}
                    rdfs::SUB_CLASS_OF => {
                        // TODO: Some cases of owl:Thing self-subclass triple are not handled here.
                        // Particularly if we haven't seen subject in the element buffer.
                        if triple.object_id.as_ref().is_some_and(|target| {
                            target == &triple.subject_id
                                && is_synthetic({
                                    let a = match data_buffer.term_index.get(&triple.subject_id).ok_or_else(
                                        || {
                                            SerializationErrorKind::TermIndexError(format!(
                                                "Failed to find term {} in the term index",
                                                triple.subject_id
                                            ))
                                        },
                                    ) {
                                        Ok(a) => a,
                                        Err(e) => panic!("Refactor this please ({e:#?})"),
                                    };
                                    &a
                                })
                                && data_buffer.node_element_buffer.get(&triple.subject_id).copied()
                                    == Some(ElementType::Owl(OwlType::Node(OwlNode::Thing)))
                        }) {
                            debug!("Skipping synthetic owl:Thing self-subclass triple");
                            return Ok(SerializationStatus::Serialized);
                        }

                        match self.insert_edge(
                            data_buffer,
                            triple,
                            ElementType::Rdfs(RdfsType::Edge(RdfsEdge::SubclassOf)),
                            None,
                        ) {
                            Some(_) => {
                                if let Some(restriction) = triple.object_id.as_ref() {
                                    self.try_materialize_restriction(data_buffer, restriction)?;
                                } else {
                                    self.retry_restrictions(data_buffer)?;
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
                        let state = data_buffer.restriction_mut(&triple.subject_id);
                        state.filler = triple.object_id.clone();
                        state.cardinality = Some(("∀".to_string(), None));
                        return self.try_materialize_restriction(data_buffer, &triple.subject_id);
                    }

                    // owl::ANNOTATED_PROPERTY => {},
                    // owl::ANNOTATED_SOURCE => {},
                    // owl::ANNOTATED_TARGET => {},
                    // owl::ANNOTATION => {},

                    //TODO: OWL1
                    // owl::ANNOTATION_PROPERTY => {},

                    // owl::ASSERTION_PROPERTY => {},
                    owl::ASYMMETRIC_PROPERTY => {
                        return Ok(self.insert_characteristic(
                            data_buffer,
                            triple,
                            Characteristic::AsymmetricProperty,
                        ));
                    }

                    // owl::AXIOM => {},
                    // owl::BACKWARD_COMPATIBLE_WITH => {},
                    // owl::BOTTOM_DATA_PROPERTY => {},
                    // owl::BOTTOM_OBJECT_PROPERTY => {},
                    owl::CARDINALITY => {
                        let exact = Self::cardinality_literal(&triple)?;
                        data_buffer.restriction_mut(&triple.subject_id).cardinality =
                            Some((exact.clone(), Some(exact)));
                        return self.try_materialize_restriction(data_buffer, &triple.subject_id);
                    }
                    owl::QUALIFIED_CARDINALITY => {
                        let exact = Self::cardinality_literal(&triple)?;
                        let state = data_buffer.restriction_mut(&triple.subject_id);
                        state.cardinality = Some((exact.clone(), Some(exact)));
                        state.requires_filler = true;
                        return self.try_materialize_restriction(data_buffer, &triple.subject_id);
                    }
                    owl::CLASS => {
                        self.insert_node(
                            data_buffer,
                            &triple,
                            ElementType::Owl(OwlType::Node(OwlNode::Class)),
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }
                    owl::COMPLEMENT_OF => {
                        if let Some(target) = triple.object_id.as_ref()
                            && self.should_skip_structural_operand(
                                data_buffer,
                                &triple.subject_id,
                                target,
                                "owl:complementOf",
                            )
                        {
                            return Ok(SerializationStatus::Serialized);
                        }

                        let edge =
                            self.insert_edge(data_buffer, &triple, ElementType::NoDraw, None);

                        if triple.object_id.is_some()
                            && let Some(index) = self.resolve(data_buffer, triple.subject_id.clone())
                            && !Self::has_named_equivalent_aliases(data_buffer, &index)
                        {
                            self.upgrade_node_type(
                                data_buffer,
                                &index,
                                ElementType::Owl(OwlType::Node(OwlNode::Complement)),
                            );
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
                        self.add_to_element_buffer(
                            &mut data_buffer.edge_element_buffer,
                            &triple,
                            e,
                        );
                        self.check_unknown_buffer(data_buffer, &triple.subject_id)?;
                        self.retry_restrictions(data_buffer)?;
                        return Ok(SerializationStatus::Serialized);
                    }

                    //TODO: OWL1 (deprecated in OWL2, replaced by rdfs:datatype)
                    // owl::DATA_RANGE => {}
                    owl::DEPRECATED => {
                        let Some(resolved_iri) = self.resolve(data_buffer, triple.subject_id.clone())
                        else {
                            debug!(
                                "Deferring owl:Deprecated for '{}': subject type unresolved",
                                triple.subject_id
                            );
                            self.add_to_unknown_buffer(data_buffer, triple.subject_id.clone(), triple);
                            return Ok(SerializationStatus::Deferred);
                        };

                        if data_buffer.node_element_buffer.contains_key(&resolved_iri) {
                            self.upgrade_deprecated_node_type(data_buffer, &resolved_iri);
                            return Ok(SerializationStatus::Serialized);
                        }

                        if data_buffer.edge_element_buffer.contains_key(&resolved_iri) {
                            self.upgrade_property_type(
                                data_buffer,
                                &resolved_iri,
                                ElementType::Owl(OwlType::Edge(OwlEdge::DeprecatedProperty)),
                            );
                            self.check_unknown_buffer(data_buffer, &resolved_iri)?;
                            return Ok(SerializationStatus::Serialized);
                        }

                        warn!(
                            "Skipping owl:Deprecated for '{}': resolved subject has no node/edge entry",
                            resolved_iri
                        );
                        return Ok(SerializationStatus::Deferred);
                    }

                    owl::DEPRECATED_CLASS => {
                        self.insert_node(
                            data_buffer,
                            &triple,
                            ElementType::Owl(OwlType::Node(OwlNode::DeprecatedClass)),
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }
                    owl::DEPRECATED_PROPERTY => {
                        match self.insert_edge(
                            data_buffer,
                            &triple,
                            ElementType::Owl(OwlType::Edge(OwlEdge::DeprecatedProperty)),
                            None,
                        ) {
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
                        if let Some(target) = triple.object_id.as_ref()
                            && self.should_skip_structural_operand(
                                data_buffer,
                                &triple.subject_id,
                                target,
                                "owl:disjointUnionOf",
                            )
                        {
                            return Ok(SerializationStatus::Serialized);
                        }

                        match self.insert_edge(data_buffer, &triple, ElementType::NoDraw, None) {
                            Some(edge) => {
                                if !Self::has_named_equivalent_aliases(data_buffer, &edge.domain) {
                                    self.upgrade_node_type(
                                        data_buffer,
                                        &edge.domain,
                                        ElementType::Owl(OwlType::Node(OwlNode::DisjointUnion)),
                                    );
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
                            &triple,
                            ElementType::Owl(OwlType::Edge(OwlEdge::DisjointWith)),
                            None,
                        ) {
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
                    owl::EQUIVALENT_CLASS => {
                        let (index_s, index_o) = self.resolve_so(data_buffer, &triple);
                        match (index_s, index_o) {
                            (Some(index_s), Some(index_o)) => {
                                let target_element =
                                    data_buffer.node_element_buffer.get(&index_o).copied();
                                let target_was_anonymous_expr =
                                    matches!(triple.object_id.as_ref(), Some(Term::BlankNode(_)));

                                self.merge_nodes(data_buffer, &index_o, &index_s)?;

                                let index_s_element = data_buffer
                                    .node_element_buffer
                                    .get(&index_s)
                                    .ok_or_else(|| {
                                        let msg = "subject not present in node_element_buffer"
                                            .to_string();
                                        SerializationErrorKind::SerializationFailedTriple(triple, msg)
                                    })?;

                                if *index_s_element
                                    != ElementType::Owl(OwlType::Node(OwlNode::AnonymousClass))
                                {
                                    let keep_structural_type = target_was_anonymous_expr
                                        && !Self::has_named_equivalent_aliases(
                                            data_buffer,
                                            &index_s,
                                        );

                                    let upgraded_type = if keep_structural_type {
                                        match target_element {
                                            Some(element)
                                                if Self::is_structural_set_node(element) =>
                                            {
                                                element
                                            }
                                            _ => ElementType::Owl(OwlType::Node(
                                                OwlNode::EquivalentClass,
                                            )),
                                        }
                                    } else {
                                        ElementType::Owl(OwlType::Node(OwlNode::EquivalentClass))
                                    };

                                    self.upgrade_node_type(data_buffer, &index_s, upgraded_type);

                                    if let Some(label) = data_buffer.label_buffer.get(&index_o) {
                                        self.extend_element_label(
                                            data_buffer,
                                            &index_s,
                                            label.clone(),
                                        );
                                    }
                                }
                            }
                            (Some(_), None) => match triple.object_id.clone() {
                                Some(target) => {
                                    self.add_to_unknown_buffer(data_buffer, target, triple.clone());
                                    return Ok(SerializationStatus::Deferred);
                                }
                                None => {
                                    let msg = "Failed to merge object of equivalence relation into subject: object not found".to_string();
                                    return Err(
                                        SerializationErrorKind::MissingObject(triple, msg).into()
                                    );
                                }
                            },
                            (None, Some(index_o)) => {
                                self.add_to_unknown_buffer(data_buffer, index_o, triple.clone());
                                return Ok(SerializationStatus::Deferred);
                            }
                            (None, None) => {
                                self.add_to_unknown_buffer(
                                    data_buffer,
                                    triple.subject_id.clone(),
                                    triple.clone(),
                                );
                                return Ok(SerializationStatus::Deferred);
                            }
                        }
                    }
                    // owl::EQUIVALENT_PROPERTY => {}
                    owl::FUNCTIONAL_PROPERTY => {
                        return Ok(self.insert_characteristic(
                            data_buffer,
                            triple,
                            Characteristic::FunctionalProperty,
                        ));
                    }

                    // owl::HAS_KEY => {}
                    owl::HAS_SELF => {
                        let truthy = matches!(
                            triple.object_id.as_ref(),
                            Some(Term::Literal(lit)) if lit.value() == "true"
                        );

                        if truthy {
                            let state = data_buffer.restriction_mut(&triple.subject_id);
                            state.self_restriction = true;
                            state.cardinality = Some(("self".to_string(), None));
                        }

                        return self.try_materialize_restriction(data_buffer, &triple.subject_id);
                    }

                    owl::HAS_VALUE => {
                        let state = data_buffer.restriction_mut(&triple.subject_id);
                        state.filler = triple.object_id.clone();
                        state.cardinality = Some(("value".to_string(), None));
                        state.render_mode = RestrictionRenderMode::ExistingPropertyEdge;
                        return self.try_materialize_restriction(data_buffer, &triple.subject_id);
                    }

                    // owl::IMPORTS => {}
                    // owl::INCOMPATIBLE_WITH => {}
                    owl::INTERSECTION_OF => {
                        if let Some(target) = triple.object_id.as_ref()
                            && self.should_skip_structural_operand(
                                data_buffer,
                                &triple.subject_id,
                                target,
                                "owl:intersectionOf",
                            )
                        {
                            return Ok(SerializationStatus::Serialized);
                        }

                        match self.insert_edge(data_buffer, &triple, ElementType::NoDraw, None) {
                            Some(edge) => {
                                if !Self::has_named_equivalent_aliases(data_buffer, &edge.domain) {
                                    self.upgrade_node_type(
                                        data_buffer,
                                        &edge.domain,
                                        ElementType::Owl(OwlType::Node(OwlNode::IntersectionOf)),
                                    );
                                }
                                return Ok(SerializationStatus::Serialized);
                            }
                            None => {
                                return Ok(SerializationStatus::Deferred);
                            }
                        }
                    }
                    owl::INVERSE_FUNCTIONAL_PROPERTY => {
                        return Ok(self.insert_characteristic(
                            data_buffer,
                            triple,
                            Characteristic::InverseFunctionalProperty,
                        ));
                    }

                    owl::INVERSE_OF => {
                        return Ok(self.insert_inverse_of(data_buffer, triple));
                    }

                    owl::IRREFLEXIVE_PROPERTY => {
                        return Ok(self.insert_characteristic(
                            data_buffer,
                            triple,
                            Characteristic::IrreflexiveProperty,
                        ));
                    }

                    owl::MAX_CARDINALITY => {
                        let max = Self::cardinality_literal(&triple)?;
                        data_buffer.restriction_mut(&triple.subject_id).cardinality =
                            Some((String::new(), Some(max)));
                        return self.try_materialize_restriction(data_buffer, &triple.subject_id);
                    }

                    owl::MAX_QUALIFIED_CARDINALITY => {
                        let state = data_buffer.restriction_mut(&triple.subject_id);
                        state.cardinality =
                            Some((String::new(), Some(Self::cardinality_literal(&triple)?)));
                        state.requires_filler = true;
                        return self.try_materialize_restriction(data_buffer, &triple.subject_id);
                    }
                    // owl::MEMBERS => {}
                    owl::MIN_CARDINALITY => {
                        let min = Self::cardinality_literal(&triple)?;
                        data_buffer.restriction_mut(&triple.subject_id).cardinality = Some((min, None));
                        return self.try_materialize_restriction(data_buffer, &triple.subject_id);
                    }
                    owl::MIN_QUALIFIED_CARDINALITY => {
                        let state = data_buffer.restriction_mut(&triple.subject_id);
                        state.cardinality = Some((Self::cardinality_literal(&triple)?, None));
                        state.requires_filler = true;
                        return self.try_materialize_restriction(data_buffer, &triple.subject_id);
                    }
                    owl::NAMED_INDIVIDUAL => {
                        let count = Self::individual_count_literal(&triple)?;
                        *data_buffer
                            .individual_count_buffer
                            .entry(triple.subject_id.clone())
                            .or_default() += count;
                        return Ok(SerializationStatus::Serialized);
                    }
                    // owl::NEGATIVE_PROPERTY_ASSERTION => {}

                    //TODO: OWL1
                    //owl::NOTHING => {}
                    owl::OBJECT_PROPERTY => {
                        let e = ElementType::Owl(OwlType::Edge(OwlEdge::ObjectProperty));
                        self.add_to_element_buffer(
                            &mut data_buffer.edge_element_buffer,
                            &triple,
                            e,
                        );
                        self.check_unknown_buffer(data_buffer, &triple.subject_id)?;
                        self.retry_restrictions(data_buffer)?;
                        return Ok(SerializationStatus::Serialized);
                    }
                    owl::ONE_OF => {
                        let Some(raw_target) = triple.object_id.clone() else {
                            return Err(SerializationErrorKind::MissingObject(
                                triple,
                                "owl:oneOf triple is missing a target".to_string(),
                            )
                            .into());
                        };

                        let materialized_target =
                            self.materialize_one_of_target(data_buffer, &triple.subject_id, &raw_target)?;

                        let edge_triple = Triple {
                            subject_id: triple.subject_id.clone(),
                            predicate_id: triple.predicate_id.clone(),
                            object_id: Some(materialized_target),
                        };

                        match self.insert_edge(data_buffer, &edge_triple, ElementType::NoDraw, None)
                        {
                            Some(_) => return Ok(SerializationStatus::Serialized),
                            None => return Ok(SerializationStatus::Deferred),
                        }
                    }
                    owl::ONTOLOGY => {
                        if let Some(base) = &data_buffer.document_base {
                            warn!(
                                "Attempting to override document base '{base}' with new base '{}'. Skipping",
                                triple.subject_id
                            );
                        } else {
                            let base = trim_tag_circumfix(&triple.subject_id.to_string());
                            info!("Using document base: '{}'", base);
                            data_buffer.document_base = Some(base);
                        }
                    }

                    //TODO: OWL1
                    // owl::ONTOLOGY_PROPERTY => {}
                    owl::ON_CLASS | owl::ON_DATARANGE => {
                        let state = data_buffer.restriction_mut(&triple.subject_id);
                        state.filler = triple.object_id.clone();
                        state.requires_filler = true;
                        return self.try_materialize_restriction(data_buffer, &triple.subject_id);
                    }
                    // owl::ON_DATATYPE => {}
                    // owl::ON_PROPERTIES => {}
                    owl::ON_PROPERTY => {
                        let Some(target) = triple.object_id.clone() else {
                            return Err(SerializationErrorKind::MissingObject(
                                triple,
                                "owl:onProperty triple is missing a target".to_string(),
                            )
                            .into());
                        };

                        data_buffer.restriction_mut(&triple.subject_id).on_property = Some(target);
                        return self.try_materialize_restriction(data_buffer, &triple.subject_id);
                    }

                    // owl::PRIOR_VERSION => {}
                    // owl::PROPERTY_CHAIN_AXIOM => {}
                    // owl::PROPERTY_DISJOINT_WITH => {}
                    // owl::QUALIFIED_CARDINALITY => {}
                    owl::REFLEXIVE_PROPERTY => {
                        return Ok(self.insert_characteristic(
                            data_buffer,
                            triple,
                            Characteristic::ReflexiveProperty,
                        ));
                    }

                    //TODO: OWL1
                    // owl::RESTRICTION => {}

                    //TODO: OWL1
                    // owl::SAME_AS => {}
                    owl::SOME_VALUES_FROM => {
                        let state = data_buffer.restriction_mut(&triple.subject_id);
                        state.filler = triple.object_id.clone();
                        state.cardinality = Some(("∃".to_string(), None));
                        return self.try_materialize_restriction(data_buffer, &triple.subject_id);
                    }
                    // owl::SOURCE_INDIVIDUAL => {}
                    owl::SYMMETRIC_PROPERTY => {
                        return Ok(self.insert_characteristic(
                            data_buffer,
                            triple,
                            Characteristic::SymmetricProperty,
                        ));
                    }
                    // owl::TARGET_INDIVIDUAL => {}
                    // owl::TARGET_VALUE => {}
                    owl::THING => {
                        self.insert_node(
                            data_buffer,
                            &triple,
                            ElementType::Owl(OwlType::Node(OwlNode::Thing)),
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }
                    // owl::TOP_DATA_PROPERTY => {}
                    // owl::TOP_OBJECT_PROPERTY => {}
                    owl::TRANSITIVE_PROPERTY => {
                        return Ok(self.insert_characteristic(
                            data_buffer,
                            triple,
                            Characteristic::TransitiveProperty,
                        ));
                    }
                    owl::UNION_OF => {
                        if let Some(target) = triple.object_id.as_ref()
                            && self.should_skip_structural_operand(
                                data_buffer,
                                &triple.subject_id,
                                target,
                                "owl:unionOf",
                            )
                        {
                            return Ok(SerializationStatus::Serialized);
                        }

                        match self.insert_edge(data_buffer, &triple, ElementType::NoDraw, None) {
                            Some(edge) => {
                                if !Self::has_named_equivalent_aliases(data_buffer, &edge.domain) {
                                    self.upgrade_node_type(
                                        data_buffer,
                                        &edge.domain,
                                        ElementType::Owl(OwlType::Node(OwlNode::UnionOf)),
                                    );
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
                            &triple,
                            ElementType::Rdfs(RdfsType::Node(RdfsNode::Datatype)),
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }
                    owl::RATIONAL => {
                        self.insert_node(
                            data_buffer,
                            &triple,
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
                            &triple,
                            ElementType::Rdfs(RdfsType::Node(RdfsNode::Datatype)),
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }
                    _ => {
                        match triple.object_id.clone() {
                            Some(target) => {
                                let (node_triple, edge_triple): (
                                    Option<Vec<Triple>>,
                                    Option<Triple>,
                                ) = match (
                                    self.resolve(data_buffer, triple.subject_id.clone()),
                                    self.resolve(data_buffer, triple.predicate_id.clone()),
                                    self.resolve(data_buffer, target.clone()),
                                ) {
                                    (Some(domain), Some(property), Some(range)) => {
                                        trace!(
                                            "Resolving object property: range: {}, property: {}, domain: {}",
                                            range, property, domain
                                        );

                                        (
                                            None,
                                            Some(Triple {
                                                subject_id: domain,
                                                predicate_id: property,
                                                object_id: Some(range),
                                            }),
                                        )
                                    }
                                    (Some(domain), Some(property), None) => {
                                        trace!("Missing range: {}", triple);

                                        if target == owl::THING.into() {
                                            let thing = self
                                                .get_or_create_domain_thing(data_buffer, &domain)?;

                                            (
                                                None,
                                                Some(Triple {
                                                    subject_id: triple.subject_id.clone(),
                                                    predicate_id: triple.predicate_id.clone(),
                                                    object_id: Some(thing),
                                                }),
                                            )
                                        } else if target == rdfs::LITERAL.into() {
                                            let target_iri =
                                                synthetic_iri(&property, SYNTH_LITERAL);
                                            info!("Creating literal node: {}", target_iri);
                                            let node = self.create_triple_from_iri(
                                                target_iri.clone(),
                                                rdfs::LITERAL.into(),
                                                None,
                                            )?;

                                            (
                                                Some(vec![node.clone()]),
                                                Some(Triple {
                                                    subject_id: triple.subject_id.clone(),
                                                    predicate_id: triple.predicate_id.clone(),
                                                    object_id: Some(node.subject_id),
                                                }),
                                            )
                                        } else {
                                            // Register the property itself as an element so it can be resolved by characteristics
                                            if triple.predicate_id == owl::OBJECT_PROPERTY.into() {
                                                self.add_to_element_buffer(
                                                    &mut data_buffer.edge_element_buffer,
                                                    &triple,
                                                    ElementType::Owl(OwlType::Edge(
                                                        OwlEdge::ObjectProperty,
                                                    )),
                                                );
                                                self.check_unknown_buffer(data_buffer, &triple.subject_id)?;
                                                return Ok(SerializationStatus::Serialized);
                                            } else if triple.predicate_id
                                                == owl::DATATYPE_PROPERTY.into()
                                            {
                                                self.add_to_element_buffer(
                                                    &mut data_buffer.edge_element_buffer,
                                                    &triple,
                                                    ElementType::Owl(OwlType::Edge(
                                                        OwlEdge::DatatypeProperty,
                                                    )),
                                                );
                                                self.check_unknown_buffer(data_buffer, &triple.subject_id)?;
                                                return Ok(SerializationStatus::Serialized);
                                            }

                                            self.add_to_unknown_buffer(
                                                data_buffer,
                                                target,
                                                triple.clone(),
                                            );
                                            return Ok(SerializationStatus::Deferred);
                                        }
                                    }
                                    (None, Some(property), Some(range)) => {
                                        trace!("Missing domain: {}", triple);

                                        if triple.subject_id == owl::THING.into() {
                                            let thing_anchor = range.clone();

                                            let thing = self.get_or_create_anchor_thing(
                                                data_buffer,
                                                &thing_anchor,
                                            )?;

                                            (
                                                None,
                                                Some(Triple {
                                                    subject_id: thing,
                                                    predicate_id: property,
                                                    object_id: Some(range),
                                                }),
                                            )
                                        } else if triple.subject_id == rdfs::LITERAL.into() {
                                            let target_iri = synthetic_iri(&range, SYNTH_LITERAL);
                                            let node = self.create_triple_from_iri(
                                                target_iri,
                                                rdfs::LITERAL.into(),
                                                None,
                                            )?;

                                            (
                                                Some(vec![node.clone()]),
                                                Some(Triple {
                                                    subject_id: node.subject_id,
                                                    predicate_id: property,
                                                    object_id: triple.object_id.clone(),
                                                }),
                                            )
                                        } else {
                                            self.add_to_unknown_buffer(
                                                data_buffer,
                                                target,
                                                triple.clone(),
                                            );
                                            return Ok(SerializationStatus::Deferred);
                                        }
                                    }
                                    (None, Some(property), None) => {
                                        trace!("Missing domain and range: {}", triple);

                                        let is_full_query_fallback =
                                            Self::is_query_fallback_endpoint(&triple.subject_id)
                                                && triple
                                                    .object_id
                                                    .as_ref()
                                                    .is_some_and(Self::is_query_fallback_endpoint);

                                        if !is_full_query_fallback {
                                            trace!(
                                                "Deferring property triple with unresolved structural domain/range: {}",
                                                triple
                                            );
                                            self.add_to_unknown_buffer(
                                                data_buffer,
                                                triple.subject_id.clone(),
                                                triple.clone(),
                                            );
                                            return Ok(SerializationStatus::Deferred);
                                        }

                                        match data_buffer
                                            .edge_element_buffer
                                            .get(&property)
                                            .copied()
                                        {
                                            Some(ElementType::Owl(OwlType::Edge(
                                                OwlEdge::DatatypeProperty,
                                            ))) => {
                                                let local_literal_iri =
                                                    synthetic_iri(&property, SYNTH_LOCAL_LITERAL);
                                                let literal_triple = self.create_triple_from_iri(
                                                    local_literal_iri.clone(),
                                                    rdfs::LITERAL.into(),
                                                    None,
                                                )?;
                                                debug!(
                                                    "Creating literal node: {}",
                                                    local_literal_iri
                                                );

                                                let local_thing_iri =
                                                    synthetic_iri(&property, SYNTH_LOCAL_THING);
                                                let thing_triple = self.create_triple_from_iri(
                                                    local_thing_iri.clone(),
                                                    owl::THING.into(),
                                                    None,
                                                )?;
                                                debug!("Creating thing node: {}", local_thing_iri);

                                                (
                                                    Some(vec![
                                                        literal_triple.clone(),
                                                        thing_triple.clone(),
                                                    ]),
                                                    Some(Triple {
                                                        subject_id: thing_triple.subject_id.clone(),
                                                        predicate_id: property.clone(),
                                                        object_id: Some(literal_triple.subject_id),
                                                    }),
                                                )
                                            }
                                            Some(ElementType::Owl(OwlType::Edge(
                                                OwlEdge::ObjectProperty,
                                            ))) => {
                                                let thing_anchor: Term = owl::THING.into();
                                                let thing = self.get_or_create_anchor_thing(
                                                    data_buffer,
                                                    &thing_anchor,
                                                )?;

                                                (
                                                    None,
                                                    Some(Triple {
                                                        subject_id: thing.clone(),
                                                        predicate_id: property.clone(),
                                                        object_id: Some(thing),
                                                    }),
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
                                            triple.predicate_id.clone(),
                                            triple.clone(),
                                        );
                                        return Ok(SerializationStatus::Deferred);
                                    }
                                    _ => {
                                        self.add_to_unknown_buffer(
                                            data_buffer,
                                            triple.subject_id.clone(),
                                            triple.clone(),
                                        );
                                        return Ok(SerializationStatus::Deferred);
                                    }
                                };
                                match node_triple {
                                    Some(node_triples) => {
                                        for node_triple in node_triples {
                                            if node_triple.predicate_id == owl::THING.into() {
                                                self.insert_node(
                                                    data_buffer,
                                                    &node_triple,
                                                    ElementType::Owl(OwlType::Node(OwlNode::Thing)),
                                                )?;
                                            } else if node_triple.predicate_id
                                                == rdfs::LITERAL.into()
                                            {
                                                self.insert_node(
                                                    data_buffer,
                                                    &node_triple,
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
                                        // Dummy variable
                                        // TODO: Refactor clones away in all of serializer
                                        let element_type = edge_triple.predicate_id.clone();
                                        let dummy = || edge_triple.clone();

                                        let property= data_buffer
                                        .edge_element_buffer
                                        .get(&element_type).ok_or_else(|| {
                                            let msg = "Edge triple not present in edge_element_buffer".to_string();
                                            SerializationErrorKind::SerializationFailedTriple(dummy(), msg)})?;
                                        let edge = self.insert_edge(
                                            data_buffer,
                                            &edge_triple,
                                            *property,
                                            data_buffer
                                                .label_buffer
                                                .get(&edge_triple.predicate_id)
                                                .cloned(),
                                        );
                                        if let Some(edge) = edge {
                                            // Clone the property IRI before it gets consumed below
                                            let prop_iri = edge_triple.predicate_id.clone();

                                            data_buffer.add_property_edge(
                                                edge_triple.predicate_id.clone(),
                                                edge,
                                            );
                                            data_buffer.add_property_domain(
                                                edge_triple.predicate_id.clone(),
                                                edge_triple.subject_id.clone(),
                                            );
                                            data_buffer.add_property_range(
                                                edge_triple.predicate_id.clone(),
                                                edge_triple.object_id.clone().ok_or_else(|| {
                                                    SerializationErrorKind::SerializationFailedTriple(
                                                        dummy(),
                                                        "target should be a string".to_string(),
                                                    )
                                                })?,
                                            );

                                            // Re-evaluate any characteristics waiting for this edge to exist
                                            self.check_unknown_buffer(data_buffer, &prop_iri)?;
                                        }
                                    }
                                    None => {
                                        return Err(SerializationErrorKind::SerializationFailedTriple(
                                            triple,
                                            "Error creating edge".to_string(),
                                        )
                                        .into());
                                    }
                                }
                            }
                            None => {
                                return Err(SerializationErrorKind::SerializationFailedTriple(
                                    triple,
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

    fn merge_optional_labels(left: Option<String>, right: Option<String>) -> Option<String> {
        match (left, right) {
            (Some(left), Some(right)) if left == right => Some(left),
            (Some(left), Some(right)) => Some(format!("{left}\n{right}")),
            (Some(label), None) | (None, Some(label)) => Some(label),
            (None, None) => None,
        }
    }

    fn merge_individual_counts(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        old: &Term,
        new: &Term,
    ) {
        if let Some(old_count) = data_buffer.individual_count_buffer.remove(old) {
            *data_buffer
                .individual_count_buffer
                .entry(new.clone())
                .or_default() += old_count;
        };
    }

    fn get_or_create_domain_thing(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        domain: &Term,
    ) -> Result<Term, SerializationError> {
        if let Some(existing) = data_buffer.anchor_thing_map.get(domain) {
            return Ok(existing.clone());
        }

        let thing_iri = synthetic_iri(domain, SYNTH_THING);
        let thing_triple = self.create_triple_from_iri(thing_iri, owl::THING.into(), None)?;
        let thing_id = thing_triple.subject_id.clone();

        self.insert_node_without_retry(
            data_buffer,
            &thing_triple,
            ElementType::Owl(OwlType::Node(OwlNode::Thing)),
        )?;

        data_buffer
            .label_buffer
            .insert(thing_id.clone(), "Thing".to_string());

        data_buffer
            .anchor_thing_map
            .insert(domain.clone(), thing_id.clone());

        Ok(thing_id)
    }

    fn get_or_create_anchor_thing(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        anchor_id: &usize,
    ) -> Result<usize, SerializationError> {
        if let Some(existing) = data_buffer.anchor_thing_map.get(anchor_id) {
            return Ok(*existing);
        }

        let anchor_term = data_buffer.term_index.get(anchor_id).ok_or_else(|| {
            SerializationErrorKind::TermIndexError(format!(
                "Failed to find anchor term '{anchor_id}' in the term index"
            ))
        })?;
        let thing_iri = synthetic_iri(&anchor_term, SYNTH_THING);
        let thing_triple = self.create_triple_from_iri(
            &mut data_buffer.term_index,
            &thing_iri,
            &owl::THING.as_str().to_string(),
            None,
        )?;
        let thing_id = thing_triple.subject_id;

        self.insert_node_without_retry(
            data_buffer,
            &thing_triple,
            ElementType::Owl(OwlType::Node(OwlNode::Thing)),
        )?;

        data_buffer
            .label_buffer
            .insert(thing_id, "Thing".to_string());

        data_buffer.anchor_thing_map.insert(*anchor_id, thing_id);

        Ok(thing_id)
    }

    fn is_query_fallback_endpoint(term: &Term) -> bool {
        *term == owl::THING.into() || *term == rdfs::LITERAL.into()
    }

    fn insert_characteristic(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        triple: Triple,
        characteristic: Characteristic,
    ) -> SerializationStatus {
        let property_iri = triple.subject_id.clone();

        let Some(resolved_iri) = self.resolve(data_buffer, property_iri.clone()) else {
            debug!(
                "Deferring characteristic '{}' for '{}': property unresolved",
                characteristic, property_iri
            );
            self.add_to_unknown_buffer(data_buffer, property_iri, triple);
            return SerializationStatus::Deferred;
        };

        // Characteristic can attach only after a concrete edge exists
        if let Some(edge) = data_buffer.property_edge_map.get(&resolved_iri).cloned() {
            debug!(
                "Inserting edge characteristic: {} -> {}",
                resolved_iri, characteristic
            );

            let target_edges: Vec<Edge> =
                if edge.edge_type == ElementType::Owl(OwlType::Edge(OwlEdge::InverseOf)) {
                    data_buffer
                        .edge_buffer
                        .iter()
                        .filter(|candidate| {
                            candidate.edge_type
                                == ElementType::Owl(OwlType::Edge(OwlEdge::InverseOf))
                                && candidate.property_id.as_ref() == Some(&resolved_iri)
                        })
                        .cloned()
                        .collect()
                } else {
                    vec![edge]
                };

            for target_edge in target_edges {
                data_buffer
                    .edge_characteristics
                    .entry(target_edge)
                    .or_default()
                    .insert(characteristic);
            }
            return SerializationStatus::Serialized;
        }

        // Property is known, but edge not materialized yet
        if data_buffer.edge_element_buffer.contains_key(&resolved_iri) {
            debug!(
                "Deferring characteristic '{}' for '{}': property known, edge not materialized yet",
                characteristic, resolved_iri
            );
            self.add_to_unknown_buffer(data_buffer, resolved_iri, triple);
            return SerializationStatus::Deferred;
        }

        // No attach point yet
        debug!(
            "Deferring characteristic '{}' for '{}': no attach point available yet",
            characteristic, resolved_iri
        );
        self.add_to_unknown_buffer(data_buffer, resolved_iri, triple);
        SerializationStatus::Deferred
    }

    fn should_skip_structural_operand(
        &self,
        data_buffer: &SerializationDataBuffer,
        subject: &Term,
        target: &Term,
        operator: &str,
    ) -> bool {
        if Self::is_consumed_restriction(data_buffer, target) {
            debug!(
                "Skipping {} operand '{}': restriction already materialized",
                operator, target
            );
            return true;
        }

        if let (Some(resolved_subject), Some(resolved_target)) = (
            self.resolve(data_buffer, subject.clone()),
            self.resolve(data_buffer, target.clone()),
        ) && resolved_subject == resolved_target
        {
            debug!(
                "Skipping {} self-loop after restriction redirection: {} -> {}",
                operator, resolved_subject, resolved_target
            );
            return true;
        }

        false
    }

    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
    fn cardinality_literal(triple: &Triple) -> Result<String, SerializationError> {
        match triple.object_id.as_ref() {
            Some(Term::Literal(literal)) => Ok(literal.value().to_string()),
            Some(other) => Err(SerializationErrorKind::SerializationFailedTriple(
                triple.clone(),
                format!("Expected cardinality literal, got '{other}'"),
            )
            .into()),
            None => Err(SerializationErrorKind::MissingObject(
                triple.clone(),
                "Restriction cardinality triple is missing a target".to_string(),
            )
            .into()),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
    fn individual_count_literal(triple: &Triple) -> Result<u32, SerializationError> {
        match triple.object_id.as_ref() {
            Some(Term::Literal(literal)) => literal.value().parse::<u32>().map_err(|e| {
                SerializationErrorKind::SerializationFailedTriple(
                    triple.clone(),
                    format!(
                        "Expected individual count literal, got '{}': {}",
                        literal.value(),
                        e
                    ),
                )
                .into()
            }),
            Some(other) => Err(SerializationErrorKind::SerializationFailedTriple(
                triple.clone(),
                format!("Expected individual count literal, got '{other}'"),
            )
            .into()),
            None => Err(SerializationErrorKind::MissingObject(
                triple.clone(),
                "NamedIndividual count triple is missing a target".to_string(),
            )
            .into()),
        }
    }

    fn is_restriction_owner_edge(edge: &Edge) -> bool {
        edge.edge_type == ElementType::Rdfs(RdfsType::Edge(RdfsEdge::SubclassOf))
            || edge.edge_type == ElementType::NoDraw
    }

    fn is_consumed_restriction(
        data_buffer: &SerializationDataBuffer,
        restriction_id: &usize,
    ) -> bool {
        data_buffer.edge_redirection.contains_key(restriction_id)
            && !data_buffer.node_element_buffer.contains_key(restriction_id)
            && !data_buffer.restriction_buffer.contains_key(restriction_id)
    }

    fn restriction_owner(
        &self,
        data_buffer: &SerializationDataBuffer,
        restriction_id: &usize,
    ) -> Option<usize> {
        data_buffer
            .edges_include_map
            .get(restriction_id)
            .and_then(|edges| {
                edges.iter().find_map(|edge| {
                    (edge.object == *restriction_id && Self::is_restriction_owner_edge(edge))
                        .then(|| edge.domain_id)
                })
            })
    }

    fn default_restriction_target(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        owner_id: &usize,
        property_id: &usize,
    ) -> Result<usize, SerializationError> {
        match data_buffer.edge_element_buffer.get(property_id) {
            Some(ElementType::Owl(OwlType::Edge(OwlEdge::DatatypeProperty))) => {
                let literal_iri = synthetic_iri(property_id, "_literal");
                let literal_triple = self.create_triple_from_iri(literal_iri, rdfs::LITERAL.into(), None)?;
                let literal_id = literal_triple.subject_id.clone();

                if !data_buffer.node_element_buffer.contains_key(&literal_id) {
                    self.insert_node_without_retry(
                        data_buffer,
                        &literal_triple,
                        ElementType::Rdfs(RdfsType::Node(RdfsNode::Literal)),
                    )?;
                }

                data_buffer
                    .label_buffer
                    .insert(literal_id.clone(), "Literal".to_string());

                Ok(literal_id)
            }
            _ => self.get_or_create_domain_thing(data_buffer, owner_id),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
    fn materialize_one_of_target(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        owner: &Term,
        target: &Term,
    ) -> Result<Term, SerializationError> {
        match target {
            Term::Literal(literal) => {
                self.materialize_literal_value_target(data_buffer, owner, literal)
            }
            Term::NamedNode(_) | Term::BlankNode(_) => {
                if let Some(resolved) = self.resolve(data_buffer, target.clone()) {
                    return Ok(resolved);
                }

                if !data_buffer.label_buffer.contains_key(target) {
                    self.extract_label(data_buffer, None, target);
                }

                let resource_triple = Triple::new(target.clone(), rdfs::RESOURCE.into(), None);

                if !data_buffer.node_element_buffer.contains_key(target) {
                    self.insert_node_without_retry(
                        data_buffer,
                        &resource_triple,
                        ElementType::Rdfs(RdfsType::Node(RdfsNode::Resource)),
                    )?;
                }

                Ok(target.clone())
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
    fn materialize_literal_value_target(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        restriction_id: &usize,
        literal: &Literal,
    ) -> Result<usize, SerializationError> {
        let literal_iri = synthetic_iri(restriction_id, "_value");
        let literal_triple = self.create_triple_from_iri(literal_iri, rdfs::LITERAL.into(), None)?;
        let literal_id = literal_triple.subject_id.clone();

        if !data_buffer.node_element_buffer.contains_key(&literal_id) {
            self.insert_node_without_retry(
                data_buffer,
                literal_triple,
                ElementType::Rdfs(RdfsType::Node(RdfsNode::Literal)),
            )?;
        }

        data_buffer
            .label_buffer
            .insert(literal_id.clone(), literal.value().to_string());

        Ok(literal_id)
    }

    fn insert_restriction_edge(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        subject_id: usize,
        property_id: usize,
        object_id: usize,
        label: String,
        cardinality: Option<(String, Option<String>)>,
    ) -> Rc<Edge> {
        let edge = Edge {
            domain_id: subject_id,
            edge_type: ElementType::Owl(OwlType::Edge(OwlEdge::ValuesFrom)),
            range_id: object_id,
            property_id: Some(property_id),
        }.into();

        data_buffer.edge_buffer.insert(edge.clone());
        self.insert_edge_include(data_buffer, subject_id, edge.clone());
        self.insert_edge_include(data_buffer, object_id, edge.clone());

        data_buffer.edge_label_buffer.insert(edge.clone(), label);

        if let Some(cardinality) = cardinality {
            data_buffer
                .edge_cardinality_buffer
                .insert(edge.clone(), cardinality);
        }

        edge
    }

    fn try_materialize_restriction(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        restriction_id: &usize,
    ) -> Result<SerializationStatus, SerializationError> {
        let Some(state) = data_buffer.restriction_buffer.get(restriction_id) else {
            return Ok(SerializationStatus::Deferred);
        };

        let Some(raw_property_id) = state.on_property else {
            return Ok(SerializationStatus::Deferred);
        };
        let Some(property_id) = self.resolve(data_buffer, raw_property_id) else {
            return Ok(SerializationStatus::Deferred);
        };
        let Some(raw_subject_id) = self.restriction_owner(data_buffer, restriction_id) else {
            return Ok(SerializationStatus::Deferred);
        };
        let subject_id = self
            .resolve(data_buffer, raw_subject_id)
            .unwrap_or_else(|| self.follow_redirection(data_buffer, raw_subject_id));

        let restriction_label = data_buffer
            .label_buffer
            .get(&raw_property_id)
            .or_else(|| data_buffer.label_buffer.get(&property_id))
            .or_else(|| {
                data_buffer
                    .property_edge_map
                    .get(&property_id)
                    .and_then(|edge| data_buffer.edge_label_buffer.get(edge))
            }).cloned()
            .unwrap_or_else(|| OwlEdge::ValuesFrom.to_string());

        if state.requires_filler && !state.self_restriction && state.filler.is_none() {
            return Ok(SerializationStatus::Deferred);
        }

        if state.render_mode == RestrictionRenderMode::ExistingPropertyEdge {
            let Some(existing_edge) = data_buffer.property_edge_map.get(&property_id) else {
                return Ok(SerializationStatus::Deferred);
            };

            let object = if let Some(filler_id) = state.filler.as_ref() {
                let filler_term = data_buffer.term_index.get(filler_id).ok_or_else(|| {
                    SerializationErrorKind::TermIndexError(format!(
                        "Failed to find filler term {} in the term index",
                        filler_id
                    ))
                })?;
                match &*filler_term {
                    Term::Literal(literal) => {
                        data_buffer
                            .label_buffer
                            .insert(existing_edge.range, literal.value().to_string());
                        existing_edge.range
                    }
                    _ => match self.resolve(data_buffer, *filler_id) {
                        Some(resolved) => resolved,
                        None => return Ok(SerializationStatus::Deferred),
                    },
                }
            } else {
                existing_edge.range
            };

            let edge = self
                .rewrite_property_edge(data_buffer, &property_id, subject_id, object)
                .ok_or_else(|| {
                    SerializationErrorKind::SerializationFailedTriple(
                        Triple::new(subject_id, property_id, None).into(),
                        "Failed to rewrite canonical property edge for hasValue restriction"
                            .to_string(),
                    )
                })?;

            data_buffer
                .edge_label_buffer
                .insert(edge.clone(), restriction_label.clone());

            if let Some(cardinality) = state.cardinality {
                data_buffer
                    .edge_cardinality_buffer
                    .insert(edge, cardinality);
            }

            self.remove_restriction_stub(data_buffer, restriction_id);
            self.remove_restriction_node(data_buffer, restriction_id);

            if subject_id != *restriction_id {
                self.redirect_iri(data_buffer, *restriction_id, subject_id)?;
            }

            return Ok(SerializationStatus::Serialized);
        }

        let object_id = if state.self_restriction {
            subject_id
        } else if let Some(filler_id) = state.filler {
            let filler_term = data_buffer.term_index.get(&filler_id).ok_or_else(|| {SerializationErrorKind::TermIndexError(format!("Failed to get term {filler_id} in the term index"))})?;
            match *filler_term {
                Term::Literal(literal) => {
                    self.materialize_literal_value_target(data_buffer, restriction_id, &literal)?
                }
                other => match self.resolve(data_buffer, filler_id) {
                    Some(resolved) => resolved,
                    None => {
                        self.materialize_named_value_target(data_buffer, &property_id, &filler_id)?
                    }
                },
            }
        } else {
            self.default_restriction_target(data_buffer, &subject_id, &property_id)?
        };

        self.remove_property_fallback_edge(data_buffer, &property_id);

        let edge = self.insert_restriction_edge(
            data_buffer,
            subject_id,
            property_id,
            object_id,
            restriction_label,
            state.cardinality,
        );

        data_buffer
            .property_edge_map
            .insert(property_id.clone(), edge);

        self.remove_restriction_stub(data_buffer, restriction_id);
        self.remove_restriction_node(data_buffer, restriction_id);

        if subject_id != *restriction_id {
            self.redirect_iri(data_buffer, *restriction_id, subject_id)?;
        }

        Ok(SerializationStatus::Serialized)
    }

    fn remove_restriction_stub(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        restriction_id: &usize,
    ) {
        if let Some(edges) = data_buffer.edges_include_map.get(restriction_id).cloned() {
            for edge in edges {
                if edge.object == *restriction_id && Self::is_restriction_owner_edge(&edge) {
                    self.remove_edge_include(data_buffer, &edge.domain_id, &edge);
                    self.remove_edge_include(data_buffer, &edge.range_id, &edge);
                    data_buffer.edge_buffer.remove(&edge);
                    data_buffer.edge_label_buffer.remove(&edge);
                    data_buffer.edge_cardinality_buffer.remove(&edge);
                    data_buffer.edge_characteristics.remove(&edge);
                }
            }
        }
    }

    fn materialize_named_value_target(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        property_id: &usize,
        target_id: &usize,
    ) -> Result<usize, SerializationError> {
        match data_buffer.edge_element_buffer.get(property_id) {
            Some(ElementType::Owl(OwlType::Edge(OwlEdge::ObjectProperty)))
            | Some(ElementType::Owl(OwlType::Edge(OwlEdge::ExternalProperty)))
            | Some(ElementType::Owl(OwlType::Edge(OwlEdge::DeprecatedProperty)))
            | Some(ElementType::Rdf(RdfType::Edge(RdfEdge::RdfProperty))) => {
                if !data_buffer.label_buffer.contains_key(target_id) {
                    let target_term = data_buffer.term_index.get(target_id).ok_or_else(|| {
                        SerializationErrorKind::TermIndexError(format!(
                            "Failed to get term {target_id} from the term index"
                        ))
                    })?;
                    self.extract_label(data_buffer, None, &target_term, target_id);
                }

                let resource_triple = self.create_triple_from_iri(target_id, subject_iri, predicate_iri, object_iri)
                let resource_triple = Triple::new(target_id, rdfs::RESOURCE.into(), None);

                if !data_buffer.node_element_buffer.contains_key(target_id) {
                    self.insert_node_without_retry(
                        data_buffer,
                        &resource_triple,
                        ElementType::Rdfs(RdfsType::Node(RdfsNode::Resource)),
                    )?;
                }

                Ok(target_id.clone())
            }
            _ => Err(SerializationErrorKind::SerializationFailedTriple(
                Triple::new(target_id.clone(), property_id.clone(), None),
                format!(
                    "Cannot materialize named value target '{target_id}' for non-object restriction"
                ),
            )
            .into()),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
    fn retry_restrictions(
        &self,
        data_buffer: &mut SerializationDataBuffer,
    ) -> Result<(), SerializationError> {
        let restrictions = data_buffer
            .restriction_buffer
            .keys()
            .cloned()
            .collect::<Vec<_>>();

        for restriction in restrictions {
            self.try_materialize_restriction(data_buffer, &restriction)?;
        }

        Ok(())
    }

    fn remove_restriction_node(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        restriction_id: &usize,
    ) {
        data_buffer.node_element_buffer.remove(restriction_id);
        data_buffer.label_buffer.remove(restriction_id);
        data_buffer.node_characteristics.remove(restriction_id);
        data_buffer.edges_include_map.remove(restriction_id);
        data_buffer.restriction_buffer.remove(restriction_id);
        data_buffer.individual_count_buffer.remove(restriction_id);
    }

    fn is_synthetic_property_fallback(edge: &Edge) -> bool {
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
            return false;
        }

        let subject = trim_tag_circumfix(&edge.domain_id.to_string());
        let object = trim_tag_circumfix(&edge.range_id.to_string());

        let synthetic_subject =
            subject.ends_with(SYNTH_THING) || subject.ends_with(SYNTH_LOCAL_THING);
        let synthetic_object = object.ends_with(SYNTH_THING)
            || object.ends_with(SYNTH_LOCAL_THING)
            || object.ends_with(SYNTH_LITERAL)
            || object.ends_with(SYNTH_LOCAL_LITERAL);

        synthetic_subject && synthetic_object
    }

    fn remove_orphan_synthetic_node(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        term_id: &usize,
    ) -> Result<(), SerializationError> {
        let term = data_buffer.term_index.get(term_id).ok_or_else(|| {
            SerializationErrorKind::TermIndexError(format!(
                "Failed to find term {} in the term index",
                term_id
            ))
        })?;

        if !is_synthetic(&term) {
            return Ok(());
        }

        let still_used = data_buffer
            .edges_include_map
            .get(term_id)
            .is_some_and(|edges| !edges.is_empty());

        if still_used {
            return Ok(());
        }

        data_buffer.edges_include_map.remove(term_id);
        data_buffer.node_element_buffer.remove(term_id);
        data_buffer.label_buffer.remove(term_id);
        data_buffer.node_characteristics.remove(term_id);
        data_buffer
            .anchor_thing_map
            .retain(|_, value| value != term_id);
        data_buffer.individual_count_buffer.remove(term_id);
        Ok(())
    }

    fn remove_property_fallback_edge(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        property_id: &usize,
    ) {
        let Some(edge) = data_buffer.property_edge_map.get(property_id).cloned() else {
            return;
        };

        if !Self::is_synthetic_property_fallback(&edge) {
            return;
        }

        self.remove_edge_include(data_buffer, &edge.domain_id, &edge);
        self.remove_edge_include(data_buffer, &edge.range_id, &edge);

        data_buffer.edge_buffer.remove(&edge);
        data_buffer.edge_label_buffer.remove(&edge);
        data_buffer.edge_cardinality_buffer.remove(&edge);
        data_buffer.edge_characteristics.remove(&edge);

        data_buffer.property_edge_map.remove(property_id);
        data_buffer.property_domain_map.remove(property_id);
        data_buffer.property_range_map.remove(property_id);

        self.remove_orphan_synthetic_node(data_buffer, &edge.domain_id);
        self.remove_orphan_synthetic_node(data_buffer, &edge.range_id);
    }

    fn rewrite_property_edge(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        property_id: &usize,
        new_subject_id: usize,
        new_object_id: usize,
    ) -> Option<Rc<Edge>> {
        let old_edge = data_buffer.property_edge_map.get(property_id)?;

        if old_edge.domain == new_subject_id && old_edge.range == new_object_id {
            return Some(*old_edge);
        }

        // TODO: Check if can be replaced with mutation on old_edge
        let mut new_edge = old_edge.clone();
        new_edge.domain = new_subject_id;
        new_edge.range = new_object_id;

        let label = data_buffer.edge_label_buffer.remove(old_edge);
        let characteristics = data_buffer.edge_characteristics.remove(old_edge);
        let cardinality = data_buffer.edge_cardinality_buffer.remove(old_edge);

        self.remove_edge_include(data_buffer, &old_edge.domain, &old_edge);
        self.remove_edge_include(data_buffer, &old_edge.range_id, &old_edge);
        data_buffer.edge_buffer.remove(old_edge);

        data_buffer.edge_buffer.insert(new_edge.clone());
        self.insert_edge_include(data_buffer, &new_edge.domain, new_edge.clone());
        self.insert_edge_include(data_buffer, &new_edge.range, new_edge.clone());

        if let Some(label) = label {
            data_buffer
                .edge_label_buffer
                .insert(new_edge.clone(), label);
        }
        if let Some(characteristics) = characteristics {
            data_buffer
                .edge_characteristics
                .insert(new_edge.clone(), characteristics);
        }
        if let Some(cardinality) = cardinality {
            data_buffer
                .edge_cardinality_buffer
                .insert(new_edge.clone(), cardinality);
        }

        data_buffer
            .property_edge_map
            .insert(property_id.clone(), new_edge.clone());

        data_buffer
            .property_domain_map
            .insert(property_id.clone(), HashSet::from([new_subject_id.clone()]));
        data_buffer
            .property_range_map
            .insert(property_id.clone(), HashSet::from([new_object_id.clone()]));

        self.remove_orphan_synthetic_node(data_buffer, &old_edge.domain_id);
        self.remove_orphan_synthetic_node(data_buffer, &old_edge.range_id);

        Some(new_edge)
    }
}

impl Default for GraphDisplayDataSolutionSerializer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use oxrdf::{BlankNode, Literal, NamedNode};

    #[test]
    fn test_replace_node() {
        let _ = env_logger::builder().is_test(true).try_init();
        let serializer = GraphDisplayDataSolutionSerializer::new();
        let mut data_buffer = SerializationDataBuffer::new();

        let example_com = Term::NamedNode(NamedNode::new("http://example.com#").unwrap());
        let owl_ontology =
            Term::NamedNode(NamedNode::new("http://www.w3.org/2002/07/owl#Ontology").unwrap());
        let example_parent = Term::NamedNode(NamedNode::new("http://example.com#Parent").unwrap());
        let owl_class =
            Term::NamedNode(NamedNode::new("http://www.w3.org/2002/07/owl#Class").unwrap());
        let example_mother = Term::NamedNode(NamedNode::new("http://example.com#Mother").unwrap());
        let example_guardian =
            Term::NamedNode(NamedNode::new("http://example.com#Guardian").unwrap());
        let example_warden = Term::NamedNode(NamedNode::new("http://example.com#Warden").unwrap());
        let example_warden1 =
            Term::NamedNode(NamedNode::new("http://example.com#Warden1").unwrap());
        let rdfs_subclass_of = Term::NamedNode(
            NamedNode::new("http://www.w3.org/2000/01/rdf-schema#subClassOf").unwrap(),
        );
        let blanknode1 =
            Term::BlankNode(BlankNode::new("e1013e66f734c508511575854b0c9396").unwrap());

        serializer.write_node_triple(
            &mut data_buffer,
            Triple {
                subject_id: example_com.clone(),
                predicate_id: owl_ontology.clone(),
                object_id: None,
            },
        );
        serializer.write_node_triple(
            &mut data_buffer,
            Triple {
                subject_id: example_parent.clone(),
                predicate_id: owl_class.clone(),
                object_id: None,
            },
        );
        serializer.write_node_triple(
            &mut data_buffer,
            Triple {
                subject_id: example_mother.clone(),
                predicate_id: owl_class.clone(),
                object_id: None,
            },
        );
        serializer.write_node_triple(
            &mut data_buffer,
            Triple {
                subject_id: example_guardian.clone(),
                predicate_id: owl_class.clone(),
                object_id: None,
            },
        );
        serializer.write_node_triple(
            &mut data_buffer,
            Triple {
                subject_id: example_warden.clone(),
                predicate_id: owl_class.clone(),
                object_id: None,
            },
        );
        serializer.write_node_triple(
            &mut data_buffer,
            Triple {
                subject_id: example_warden1.clone(),
                predicate_id: owl_class.clone(),
                object_id: None,
            },
        );
        serializer.write_node_triple(
            &mut data_buffer,
            Triple {
                subject_id: example_warden.clone(),
                predicate_id: rdfs_subclass_of.clone(),
                object_id: Some(example_guardian.clone()),
            },
        );
        serializer.write_node_triple(
            &mut data_buffer,
            Triple {
                subject_id: example_mother.clone(),
                predicate_id: rdfs_subclass_of.clone(),
                object_id: Some(example_parent.clone()),
            },
        );
        serializer.write_node_triple(
            &mut data_buffer,
            Triple {
                subject_id: blanknode1.clone(),
                predicate_id: Term::Literal(Literal::new_simple_literal("blanknode".to_string())),
                object_id: None,
            },
        );
        serializer.write_node_triple(
            &mut data_buffer,
            Triple {
                subject_id: example_warden1.clone(),
                predicate_id: Term::NamedNode(
                    NamedNode::new("http://www.w3.org/2002/07/owl#unionOf").unwrap(),
                ),
                object_id: Some(example_warden.clone()),
            },
        );

        print_graph_display_data(&data_buffer);
        println!("--------------------------------");

        let triple = Triple {
            subject_id: example_guardian.clone(),
            predicate_id: Term::NamedNode(
                NamedNode::new("http://www.w3.org/2002/07/owl#equivalentClass").unwrap(),
            ),
            object_id: Some(example_warden.clone()),
        };
        serializer.write_node_triple(&mut data_buffer, triple);
        for (k, v) in data_buffer.node_element_buffer.iter() {
            println!("element_buffer: {} -> {}", k, v);
        }
        for (k, v) in data_buffer.edges_include_map.iter() {
            println!("edges_include_map: {} -> {:?}", k, v);
        }
        for (k, v) in data_buffer.edge_redirection.iter() {
            println!("edge_redirection: {} -> {}", k, v);
        }
        assert!(
            data_buffer
                .node_element_buffer
                .contains_key(&example_guardian.clone())
        );
        assert!(
            !data_buffer
                .node_element_buffer
                .contains_key(&example_warden)
        );
        assert!(
            data_buffer
                .node_element_buffer
                .contains_key(&example_warden1)
        );
        assert!(data_buffer.edges_include_map.contains_key(&example_warden1));

        assert!(data_buffer.edge_buffer.contains(&Edge {
            domain_id: example_warden1,
            edge_type: ElementType::NoDraw,
            range_id: example_guardian.clone(),
            property_id: None
        }));
        assert!(data_buffer.edge_redirection.contains_key(&example_warden));
        assert_eq!(
            data_buffer
                .edge_redirection
                .get(&example_warden)
                .unwrap()
                .clone(),
            example_guardian
        );
        serializer.write_node_triple(
            &mut data_buffer,
            Triple {
                subject_id: Term::NamedNode(NamedNode::new("http://example.com#Guardian").unwrap()),
                predicate_id: Term::NamedNode(
                    NamedNode::new("http://www.w3.org/2002/07/owl#equivalentClass").unwrap(),
                ),
                object_id: Some(blanknode1.clone()),
            },
        );
        let s = serializer.resolve(&data_buffer, blanknode1.clone());
        assert!(s.is_some());
        for (k, v) in data_buffer.node_element_buffer.iter() {
            println!("element_buffer: {} -> {}", k, v);
        }
        for (k, v) in data_buffer.edge_redirection.iter() {
            println!("edge_redirection: {} -> {}", k, v);
        }
        assert!(s.unwrap() == example_guardian);
        assert!(!data_buffer.edges_include_map.contains_key(&blanknode1));
        assert!(!data_buffer.edges_include_map.contains_key(&example_warden));
        print_graph_display_data(&data_buffer);
        println!("data_buffer: {}", data_buffer);
    }

    pub fn print_graph_display_data(data_buffer: &SerializationDataBuffer) {
        for (index, (element, label)) in data_buffer.node_element_buffer.iter().enumerate() {
            println!("{index}: {label} -> {element:?}");
        }
        for edge in data_buffer.edge_buffer.iter() {
            println!(
                "{} -> {:?} -> {}",
                edge.domain, edge.edge_type, edge.range
            );
        }
    }
}

use std::{
    collections::{HashMap, HashSet},
    mem::take,
    rc::Rc,
    time::{Duration, Instant},
};

use super::{Edge, RestrictionRenderMode, SerializationDataBuffer, Triple};
use crate::{
    errors::{SerializationError, SerializationErrorKind},
    serializers::{
        index::TermIndex,
        util::{
            PROPERTY_EDGE_TYPES, is_reserved, is_synthetic,
            synthetic::{SYNTH_LITERAL, SYNTH_LOCAL_LITERAL, SYNTH_LOCAL_THING, SYNTH_THING},
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
use rdf_fusion::{
    execution::results::QuerySolutionStream,
    model::{BlankNode, NamedNode, Term},
};
use unescape_zero_copy::unescape_default;
use vowlr_parser::errors::VOWLRStoreError;
use vowlr_util::prelude::{ErrorRecord, VOWLRError};

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
                        .failed_buffer
                        .push(<VOWLRStoreError as Into<ErrorRecord>>::into(e.into()));
                    continue;
                }
            };
            let Some(id_term) = solution.get("id") else {
                continue;
            };

            let subject_term_id = data_buffer.term_index.insert(id_term.to_owned());
            self.extract_label(
                &mut data_buffer,
                solution.get("label"),
                id_term,
                &subject_term_id,
            );

            let Some(node_type_term) = solution.get("nodeType") else {
                continue;
            };

            let triple = Rc::new(Triple {
                id: subject_term_id,
                element_type: data_buffer.term_index.insert(node_type_term.to_owned()),
                target: solution
                    .get("target")
                    .map(|term| data_buffer.term_index.insert(term.to_owned())),
            });

            self.write_node_triple(&mut data_buffer, triple)
                .or_else(|e| {
                    data_buffer.failed_buffer.push(e.into());
                    Ok::<SerializationStatus, VOWLRError>(SerializationStatus::Serialized)
                })?;
            count += 1;
        }
        self.check_all_unknowns(&mut data_buffer).or_else(|e| {
            data_buffer.failed_buffer.push(e.into());
            Ok::<(), VOWLRError>(())
        })?;

        // Catch permanently unresolved triples
        for (term, triples) in data_buffer.unknown_buffer.drain() {
            for triple in triples {
                let e: SerializationError = SerializationErrorKind::SerializationFailed(
                    triple,
                    format!("Unresolved reference: could not map '{}'", term),
                )
                .into();
                data_buffer.failed_buffer.push(e.into());
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
            data_buffer.node_element_buffer.len(),
            data_buffer.edge_buffer.len(),
            data_buffer.label_buffer.len(),
            data_buffer.edge_cardinality_buffer.len(),
            data_buffer.edge_characteristics.len() + data_buffer.node_characteristics.len(),
        );
        debug!("{}", data_buffer);
        let errors = if !data_buffer.failed_buffer.is_empty() {
            let total = data_buffer.failed_buffer.len();
            let err: VOWLRError = take(&mut data_buffer.failed_buffer).into();
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
        *data = data_buffer.into();
        debug!("{}", data);
        Ok(errors)
    }

    /// Extract label info from the query solution and store until
    /// they can be mapped to their ElementType.
    fn extract_label(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        label: Option<&Term>,
        id_term: &Term,
        subject_term_id: &usize,
    ) {
        // Prevent overriding labels
        if data_buffer.label_buffer.contains_key(subject_term_id) {
            return;
        }

        match label {
            // Case 1: Label is a rdfs:label OR rdfs:Resource OR rdf:ID
            Some(label_term) => {
                let str_label = label_term.to_string();

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
                    trace!("Inserting label '{clean_label}' for iri '{}'", id_term);
                    data_buffer
                        .label_buffer
                        .insert(*subject_term_id, clean_label);
                } else {
                    debug!("Empty label detected for iri '{}'", id_term);
                }
            }
            // Case 2: Try parsing the iri
            None => {
                let iri = id_term.to_string();
                match Iri::parse(trim_tag_circumfix(&iri)) {
                    // Case 2.1: Look for fragments in the iri
                    Ok(id_iri) => match id_iri.fragment() {
                        Some(frag) => {
                            trace!("Inserting fragment '{frag}' as label for iri '{}'", id_term);
                            data_buffer
                                .label_buffer
                                .insert(*subject_term_id, frag.to_string());
                        }
                        // Case 2.2: Look for path in iri
                        None => {
                            debug!("No fragment found in iri '{iri}'");
                            match id_iri.path().rsplit_once('/') {
                                Some(path) => {
                                    trace!(
                                        "Inserting path '{}' as label for iri '{}'",
                                        path.1, id_term
                                    );
                                    data_buffer
                                        .label_buffer
                                        .insert(*subject_term_id, path.1.to_string());
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
    }

    fn resolve(&self, data_buffer: &SerializationDataBuffer, x: Term) -> Option<Term> {
        let resolved = self.follow_redirection(data_buffer, &x);

        if let Some(elem) = data_buffer.node_element_buffer.get(&resolved) {
            debug!("Resolved: {}: {}", resolved, elem);
            return Some(resolved);
        }

        if let Some(elem) = data_buffer.edge_element_buffer.get(&resolved) {
            debug!("Resolved: {}: {}", resolved, elem);
            return Some(resolved);
        }

        None
    }

    fn resolve_so(
        &self,
        data_buffer: &SerializationDataBuffer,
        triple: &Triple,
    ) -> (Option<Term>, Option<Term>) {
        let resolved_subject = self.resolve(data_buffer, triple.id.clone());
        let resolved_object = match &triple.target {
            Some(target) => self.resolve(data_buffer, target.clone()),
            None => {
                warn!("No object in triple:\n {}", triple);
                None
            }
        };
        (resolved_subject, resolved_object)
    }

    /// Add subject of triple to the element buffer.
    ///
    /// In the future, this function will handle cases where an element
    /// identifies itself as multiple elements. E.g. an element is both an rdfs:Class and a owl:class.
    fn add_to_element_buffer(
        &self,
        element_buffer: &mut HashMap<Term, ElementType>,
        triple: &Triple,
        element_type: ElementType,
    ) {
        if let Some(element) = element_buffer.get(&triple.id) {
            warn!(
                "Attempted to register '{}' to subject '{}' already registered as '{}'. Skipping",
                element_type, triple.id, element
            );
        } else {
            trace!("Adding to element buffer: {}: {}", triple.id, element_type);
            element_buffer.insert(triple.id.clone(), element_type);
        }
    }

    /// Add an IRI to the unresolved, unknown buffer.
    fn add_to_unknown_buffer(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        element_iri: Term,
        triple: Triple,
    ) {
        trace!("Adding to unknown buffer: {}: {}", element_iri, triple);
        if let Some(id_unknowns) = data_buffer.unknown_buffer.get_mut(&element_iri) {
            id_unknowns.insert(triple);
        } else {
            let mut id_unknowns = HashSet::new();
            id_unknowns.insert(triple);
            data_buffer.unknown_buffer.insert(element_iri, id_unknowns);
        }
    }

    /// Insert an edge into the element's edge set.
    fn insert_edge_include(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        element_iri: &Term,
        edge: Edge,
    ) {
        data_buffer
            .edges_include_map
            .entry(element_iri.clone())
            .or_default()
            .insert(edge);
    }

    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
    pub fn redirect_iri(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        old: &Term,
        new: &Term,
    ) -> Result<(), SerializationError> {
        debug!("Redirecting '{}' to '{}'", old, new);
        data_buffer
            .edge_redirection
            .insert(old.clone(), new.clone());
        self.check_unknown_buffer(data_buffer, old)?;
        Ok(())
    }

    fn follow_redirection(&self, data_buffer: &SerializationDataBuffer, term: &Term) -> Term {
        let mut current = term.clone();

        while let Some(next) = data_buffer.edge_redirection.get(&current) {
            if *next == current {
                break;
            }
            current = next.clone();
        }

        current
    }

    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
    pub fn check_unknown_buffer(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        term: &Term,
    ) -> Result<(), SerializationError> {
        let triple = data_buffer.unknown_buffer.remove(term);
        if let Some(triples) = triple {
            for triple in triples {
                self.write_node_triple(data_buffer, triple)?;
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
    fn insert_node(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        triple: &Triple,
        node_type: ElementType,
    ) -> Result<(), SerializationError> {
        self.insert_node_impl(data_buffer, triple, node_type, true)
    }

    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
    fn insert_node_impl(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        triple: &Triple,
        node_type: ElementType,
        retry_restrictions: bool,
    ) -> Result<(), SerializationError> {
        if data_buffer.edge_redirection.contains_key(&triple.id) {
            debug!(
                "Skipping insert_node for '{}': already redirected",
                triple.id
            );
            return Ok(());
        }

        let new_type = if self.is_external(data_buffer, &triple.id) {
            ElementType::Owl(OwlType::Node(OwlNode::ExternalClass))
        } else {
            node_type
        };

        self.add_to_element_buffer(&mut data_buffer.node_element_buffer, triple, new_type);
        self.check_unknown_buffer(data_buffer, &triple.id)?;

        if retry_restrictions {
            self.retry_restrictions(data_buffer)?;
        }

        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
    fn insert_node_without_retry(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        triple: &Triple,
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
        triple: &Triple,
        edge_type: ElementType,
        label: Option<String>,
    ) -> Option<Edge> {
        let external_probe = if PROPERTY_EDGE_TYPES.contains(&edge_type) {
            &triple.element_type
        } else {
            &triple.id
        };

        // Skip external check for NoDraw edges - they should always retain their type
        let new_type =
            if edge_type != ElementType::NoDraw && self.is_external(data_buffer, external_probe) {
                ElementType::Owl(OwlType::Edge(OwlEdge::ExternalProperty))
            } else {
                edge_type
            };
        match self.resolve_so(data_buffer, triple) {
            (Some(sub_iri), Some(obj_iri)) => {
                let should_hash_property = [
                    ElementType::Owl(OwlType::Edge(OwlEdge::ObjectProperty)),
                    ElementType::Owl(OwlType::Edge(OwlEdge::DatatypeProperty)),
                    ElementType::Owl(OwlType::Edge(OwlEdge::DeprecatedProperty)),
                    ElementType::Owl(OwlType::Edge(OwlEdge::ExternalProperty)),
                ];
                let property = if should_hash_property.contains(&new_type) {
                    Some(triple.element_type.clone())
                } else {
                    None
                };
                let edge = Edge {
                    subject: sub_iri.clone(),
                    element_type: new_type,
                    object: obj_iri.clone(),
                    property,
                };
                data_buffer
                    .edge_element_buffer
                    .insert(triple.element_type.clone(), edge.element_type);
                data_buffer.edge_buffer.insert(edge.clone());
                trace!(
                    "Inserting edge: {} -> {} -> {}",
                    edge.subject, edge.element_type, edge.object
                );
                data_buffer.edge_buffer.insert(edge.clone());
                self.insert_edge_include(data_buffer, &sub_iri, edge.clone());
                self.insert_edge_include(data_buffer, &obj_iri, edge.clone());

                data_buffer
                    .edge_label_buffer
                    .insert(edge.clone(), label.unwrap_or(new_type.to_string()));
                return Some(edge);
            }
            (None, Some(_)) => {
                warn!("Cannot resolve subject of triple:\n {}", triple);
                self.add_to_unknown_buffer(data_buffer, triple.id.clone(), triple.clone());
            }
            (Some(_), None) => {
                if let Some(obj_iri) = &triple.target {
                    warn!("Cannot resolve object of triple:\n {}", triple);
                    // resolve_so already warns about unresolved object. No need to repeat it here.
                    self.add_to_unknown_buffer(data_buffer, obj_iri.clone(), triple.clone());
                }
            }
            _ => {
                warn!("Cannot resolve subject and object of triple:\n {}", triple);
                self.add_to_unknown_buffer(data_buffer, triple.id.clone(), triple.clone());
            }
        }
        None
    }

    fn is_external(&self, data_buffer: &SerializationDataBuffer, iri: &Term) -> bool {
        if iri.is_blank_node() {
            return false;
        }
        let clean_iri = trim_tag_circumfix(&iri.to_string());
        match &data_buffer.document_base {
            Some(base) => !(clean_iri.contains(base) || is_reserved(iri) || is_synthetic(iri)),
            None => {
                warn!("Cannot determine externals: Missing document base!");
                false
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
    fn merge_nodes(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        old: &Term,
        new: &Term,
    ) -> Result<(), SerializationError> {
        if old == new {
            return Ok(());
        }

        debug!("Merging node '{old}' into '{new}'");
        self.merge_restriction_state(data_buffer, old, new);
        data_buffer.node_element_buffer.remove(old);
        self.update_edges(data_buffer, old, new);
        self.merge_individual_counts(data_buffer, old, new);
        self.redirect_iri(data_buffer, old, new)?;
        self.retry_restrictions(data_buffer)?;
        Ok(())
    }

    fn merge_restriction_state(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        old: &Term,
        new: &Term,
    ) {
        let Some(old_state) = data_buffer.restriction_buffer.remove(old) else {
            return;
        };

        let super::RestrictionState {
            on_property,
            filler,
            cardinality,
            self_restriction,
            requires_filler,
            render_mode,
        } = old_state;

        let new_state = data_buffer
            .restriction_buffer
            .entry(new.clone())
            .or_default();

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
    }

    fn update_edges(&self, data_buffer: &mut SerializationDataBuffer, old: &Term, new: &Term) {
        let old_edges = data_buffer.edges_include_map.remove(old);
        if let Some(old_edges) = old_edges {
            debug!("Updating edges from '{}' to '{}'", old, new);
            for mut edge in old_edges {
                let old_edge = edge.clone();
                let label = data_buffer.edge_label_buffer.remove(&old_edge);
                let cardinality = data_buffer.edge_cardinality_buffer.remove(&old_edge);
                let characteristics = data_buffer.edge_characteristics.remove(&old_edge);

                data_buffer.edge_buffer.remove(&old_edge);

                if old_edge.subject != *old {
                    self.remove_edge_include(data_buffer, &old_edge.subject, &old_edge);
                }
                if old_edge.object != *old {
                    self.remove_edge_include(data_buffer, &old_edge.object, &old_edge);
                }

                if edge.object == *old {
                    edge.object = new.clone();
                }
                if edge.subject == *old {
                    edge.subject = new.clone();
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
                self.insert_edge_include(data_buffer, new, edge.clone());
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
    }

    fn upgrade_node_type(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        iri: &Term,
        new_element: ElementType,
    ) {
        let old_elem_opt = data_buffer.node_element_buffer.get(iri).cloned();
        match old_elem_opt {
            Some(old_elem) => {
                if Self::can_upgrade_node_type(old_elem, new_element) {
                    data_buffer
                        .node_element_buffer
                        .insert(iri.clone(), new_element);
                }
                debug!(
                    "Upgraded subject '{}' from {} to {}",
                    iri, old_elem, new_element
                )
            }
            None => {
                warn!("Upgraded unresolved subject '{}' to {}", iri, new_element)
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

    fn has_named_equivalent_aliases(data_buffer: &SerializationDataBuffer, iri: &Term) -> bool {
        data_buffer
            .edge_redirection
            .iter()
            .any(|(alias, target)| target == iri && matches!(alias, Term::NamedNode(_)))
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

    fn upgrade_deprecated_node_type(&self, data_buffer: &mut SerializationDataBuffer, iri: &Term) {
        let old_elem_opt = data_buffer.node_element_buffer.get(iri).copied();
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
                    .insert(iri.clone(), new_element);
                debug!(
                    "Upgraded deprecated class '{}' from {} to {}",
                    iri, old_elem, new_element
                );
            }
            Some(old_elem) => {
                warn!(
                    "Skipping owl:Deprecated node upgrade for '{}': {} is not a class",
                    iri, old_elem
                );
            }
            None => {
                warn!(
                    "Cannot upgrade unresolved subject '{}' to DeprecatedClass",
                    iri
                );
            }
        }
    }

    fn upgrade_property_type(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        property_iri: &Term,
        new_element: ElementType,
    ) {
        let old_elem_opt = data_buffer.edge_element_buffer.get(property_iri).copied();
        let Some(old_elem) = old_elem_opt else {
            warn!(
                "Cannot upgrade unresolved property '{}' to {}",
                property_iri, new_element
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
                property_iri, old_elem
            );
            return;
        }

        data_buffer
            .edge_element_buffer
            .insert(property_iri.clone(), new_element);

        let Some(old_edge) = data_buffer.property_edge_map.get(property_iri).cloned() else {
            debug!(
                "Upgraded property '{}' from {} to {} before edge materialization",
                property_iri, old_elem, new_element
            );
            return;
        };

        if old_edge.element_type == ElementType::Owl(OwlType::Edge(OwlEdge::InverseOf)) {
            debug!(
                "Keeping merged inverse edge for '{}' as {} instead of downgrading it to {}",
                property_iri, old_edge.element_type, new_element
            );
            return;
        }

        let mut new_edge = old_edge.clone();
        new_edge.element_type = new_element;

        data_buffer.edge_buffer.remove(&old_edge);
        data_buffer.edge_buffer.insert(new_edge.clone());

        let label = data_buffer
            .label_buffer
            .get(property_iri)
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

        if let Some(edges) = data_buffer.edges_include_map.get_mut(&old_edge.subject) {
            edges.remove(&old_edge);
            edges.insert(new_edge.clone());
        }
        if let Some(edges) = data_buffer.edges_include_map.get_mut(&old_edge.object) {
            edges.remove(&old_edge);
            edges.insert(new_edge.clone());
        }

        data_buffer
            .property_edge_map
            .insert(property_iri.clone(), new_edge);

        debug!(
            "Upgraded deprecated property '{}' from {} to {}",
            property_iri, old_elem, new_element
        );
    }

    fn remove_edge_include(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        element_iri: &Term,
        edge: &Edge,
    ) {
        if let Some(edges) = data_buffer.edges_include_map.get_mut(element_iri) {
            edges.remove(edge);
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
    fn merge_properties(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        old: &Term,
        new: &Term,
    ) -> Result<(), SerializationError> {
        if old == new {
            return Ok(());
        }

        debug!("Merging property '{old}' into '{new}'");

        data_buffer.edge_element_buffer.remove(old);

        // Remove stale node placeholders for property aliases.
        data_buffer.node_element_buffer.remove(old);
        data_buffer.label_buffer.remove(old);
        data_buffer.node_characteristics.remove(old);

        if let Some(domains) = data_buffer.property_domain_map.remove(old) {
            data_buffer
                .property_domain_map
                .entry(new.clone())
                .or_default()
                .extend(domains);
        }

        if let Some(ranges) = data_buffer.property_range_map.remove(old) {
            data_buffer
                .property_range_map
                .entry(new.clone())
                .or_default()
                .extend(ranges);
        }

        self.redirect_iri(data_buffer, old, new)?;
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
    fn normalize_inverse_endpoint(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        endpoint: &Term,
        opposite: &Term,
    ) -> Result<Term, SerializationError> {
        let Some(element_type) = data_buffer.node_element_buffer.get(endpoint).copied() else {
            return Ok(endpoint.clone());
        };

        match element_type {
            ElementType::Owl(OwlType::Node(
                OwlNode::Complement
                | OwlNode::IntersectionOf
                | OwlNode::UnionOf
                | OwlNode::DisjointUnion
                | OwlNode::EquivalentClass,
            )) => self.get_or_create_anchor_thing(data_buffer, opposite),
            _ => Ok(endpoint.clone()),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
    fn inverse_edge_endpoints(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        property_iri: &Term,
    ) -> Result<Option<(Term, Term)>, SerializationError> {
        let domain = data_buffer
            .property_domain_map
            .get(property_iri)
            .and_then(|domains| domains.iter().next())
            .cloned();
        let range = data_buffer
            .property_range_map
            .get(property_iri)
            .and_then(|ranges| ranges.iter().next())
            .cloned();

        match (domain, range) {
            (Some(domain), Some(range)) => {
                let subject = self.normalize_inverse_endpoint(data_buffer, &domain, &range)?;
                let object = self.normalize_inverse_endpoint(data_buffer, &range, &domain)?;
                Ok(Some((subject, object)))
            }
            _ => Ok(None),
        }
    }

    fn insert_inverse_of(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        triple: Triple,
    ) -> SerializationStatus {
        let left_property_raw = triple.id.clone();
        let Some(right_property_raw) = triple.target.clone() else {
            warn!("owl:inverseOf triple is missing a target: {}", triple);
            return SerializationStatus::Serialized;
        };

        let Some(left_property) = self.resolve(data_buffer, left_property_raw.clone()) else {
            self.add_to_unknown_buffer(data_buffer, left_property_raw, triple);
            return SerializationStatus::Deferred;
        };

        let Some(right_property) = self.resolve(data_buffer, right_property_raw.clone()) else {
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
                    self.add_to_unknown_buffer(data_buffer, left_property.clone(), triple);
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
                    self.add_to_unknown_buffer(data_buffer, right_property.clone(), triple);
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
            self.remove_edge_include(data_buffer, &left_edge.subject, left_edge);
            self.remove_edge_include(data_buffer, &left_edge.object, left_edge);
            data_buffer.edge_buffer.remove(left_edge);
            data_buffer.edge_label_buffer.remove(left_edge);
        }

        if let Some(right_edge) = right_edge.as_ref() {
            self.remove_edge_include(data_buffer, &right_edge.subject, right_edge);
            self.remove_edge_include(data_buffer, &right_edge.object, right_edge);
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

        let inverse_property = Some(left_property.clone());
        let inverse_edges = [
            Edge {
                subject: left_subject.clone(),
                element_type: ElementType::Owl(OwlType::Edge(OwlEdge::InverseOf)),
                object: left_object.clone(),
                property: inverse_property.clone(),
            },
            Edge {
                subject: left_object,
                element_type: ElementType::Owl(OwlType::Edge(OwlEdge::InverseOf)),
                object: left_subject,
                property: inverse_property,
            },
        ];

        let canonical_edge = inverse_edges[0].clone();

        for edge in inverse_edges {
            data_buffer.edge_buffer.insert(edge.clone());
            self.insert_edge_include(data_buffer, &edge.subject, edge.clone());
            self.insert_edge_include(data_buffer, &edge.object, edge.clone());
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
            .insert(left_property.clone(), canonical_edge);
        data_buffer.property_edge_map.remove(&right_property);

        SerializationStatus::Serialized
    }

    /// Appends a string to an element's label.
    fn extend_element_label(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        element: &Term,
        label_to_append: String,
    ) {
        debug!(
            "Extending element '{}' with label '{}'",
            element, label_to_append
        );
        if let Some(label) = data_buffer.label_buffer.get_mut(element) {
            label.push_str(format!("\n{}", label_to_append).as_str());
        } else {
            data_buffer
                .label_buffer
                .insert(element.clone(), label_to_append.clone());
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
    fn create_named_node(&self, iri: String) -> Result<NamedNode, SerializationError> {
        Ok(NamedNode::new(&iri).map_err(|e| SerializationErrorKind::IriParseError(iri, e))?)
    }

    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
    fn create_blank_node(&self, id: String) -> Result<BlankNode, SerializationError> {
        Ok(BlankNode::new(&id).map_err(|e| SerializationErrorKind::BlankNodeParseError(id, e))?)
    }

    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
    fn create_triple(
        &self,
        id: String,
        element_type: NamedNode,
        object_iri: Option<String>,
    ) -> Result<Triple, SerializationError> {
        let subject = match NamedNode::new(id.clone()) {
            Ok(node) => Term::NamedNode(node),
            Err(_) => Term::BlankNode(self.create_blank_node(id)?),
        };

        let object = match object_iri {
            Some(iri) => Some(Term::NamedNode(self.create_named_node(iri)?)),
            None => None,
        };

        let t = Triple::new(subject, Term::NamedNode(element_type), object);
        debug!("Created new triple: {}", t);
        Ok(t)
    }
    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
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

            for (term, triples) in current {
                if !data_buffer.label_buffer.contains_key(&term) {
                    self.extract_label(data_buffer, None, &term);
                }

                if self.is_external(data_buffer, &term) {
                    // Dummy triple, only subject matters.
                    let external_triple = Triple::new(
                        term,
                        Term::BlankNode(self.create_blank_node("_:external_class".to_string())?),
                        None,
                    );

                    self.insert_node(
                        data_buffer,
                        &external_triple,
                        ElementType::Owl(OwlType::Node(OwlNode::ExternalClass)),
                    )?;
                } else if let Some(element_type) = try_resolve_reserved(&term) {
                    // Dummy triple, only subject matters.
                    let reserved_triple = Triple::new(
                        term,
                        Term::BlankNode(self.create_blank_node("_:reserved_class".to_string())?),
                        None,
                    );

                    self.insert_node(data_buffer, &reserved_triple, element_type)?;
                }

                for triple in triples {
                    match self.write_node_triple(data_buffer, triple.clone()) {
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
    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
    fn write_node_triple(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        triple: Rc<Triple>,
    ) -> Result<SerializationStatus, SerializationError> {
        let predicate_term = data_buffer
            .term_index
            .get(&triple.element_type)
            .ok_or_else(|| {
                SerializationErrorKind::SerializationFailed(
                    triple.clone(),
                    "Failed to find predicate in term index".to_string(),
                )
            })?;
        match *predicate_term {
            Term::BlankNode(bnode) => {
                // The query must never put blank nodes in the ?nodeType variable
                let msg = format!("Illegal blank node during serialization: '{bnode}'");
                return Err(SerializationErrorKind::SerializationFailed(triple, msg).into());
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
                            &triple,
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
                        return Err(SerializationErrorKind::SerializationFailed(
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
                        return Err(SerializationErrorKind::SerializationFailed(
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
                        if triple.target.as_ref().is_some_and(|target| {
                            target == &triple.id
                                && is_synthetic(&triple.id)
                                && data_buffer.node_element_buffer.get(&triple.id).copied()
                                    == Some(ElementType::Owl(OwlType::Node(OwlNode::Thing)))
                        }) {
                            debug!("Skipping synthetic owl:Thing self-subclass triple");
                            return Ok(SerializationStatus::Serialized);
                        }

                        match self.insert_edge(
                            data_buffer,
                            &triple,
                            ElementType::Rdfs(RdfsType::Edge(RdfsEdge::SubclassOf)),
                            None,
                        ) {
                            Some(_) => {
                                if let Some(restriction) = triple.target.as_ref() {
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
                        let state = data_buffer.restriction_mut(&triple.id);
                        state.filler = triple.target.clone();
                        state.cardinality = Some(("∀".to_string(), None));
                        return self.try_materialize_restriction(data_buffer, &triple.id);
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
                        data_buffer.restriction_mut(&triple.id).cardinality =
                            Some((exact.clone(), Some(exact)));
                        return self.try_materialize_restriction(data_buffer, &triple.id);
                    }
                    owl::QUALIFIED_CARDINALITY => {
                        let exact = Self::cardinality_literal(&triple)?;
                        let state = data_buffer.restriction_mut(&triple.id);
                        state.cardinality = Some((exact.clone(), Some(exact)));
                        state.requires_filler = true;
                        return self.try_materialize_restriction(data_buffer, &triple.id);
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
                        if let Some(target) = triple.target.as_ref()
                            && self.should_skip_structural_operand(
                                data_buffer,
                                &triple.id,
                                target,
                                "owl:complementOf",
                            )
                        {
                            return Ok(SerializationStatus::Serialized);
                        }

                        let edge =
                            self.insert_edge(data_buffer, &triple, ElementType::NoDraw, None);

                        if triple.target.is_some()
                            && let Some(index) = self.resolve(data_buffer, triple.id.clone())
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
                        self.check_unknown_buffer(data_buffer, &triple.id)?;
                        self.retry_restrictions(data_buffer)?;
                        return Ok(SerializationStatus::Serialized);
                    }

                    //TODO: OWL1 (deprecated in OWL2, replaced by rdfs:datatype)
                    // owl::DATA_RANGE => {}
                    owl::DEPRECATED => {
                        let Some(resolved_iri) = self.resolve(data_buffer, triple.id.clone())
                        else {
                            debug!(
                                "Deferring owl:Deprecated for '{}': subject type unresolved",
                                triple.id
                            );
                            self.add_to_unknown_buffer(data_buffer, triple.id.clone(), triple);
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
                        if let Some(target) = triple.target.as_ref()
                            && self.should_skip_structural_operand(
                                data_buffer,
                                &triple.id,
                                target,
                                "owl:disjointUnionOf",
                            )
                        {
                            return Ok(SerializationStatus::Serialized);
                        }

                        match self.insert_edge(data_buffer, &triple, ElementType::NoDraw, None) {
                            Some(edge) => {
                                if !Self::has_named_equivalent_aliases(data_buffer, &edge.subject) {
                                    self.upgrade_node_type(
                                        data_buffer,
                                        &edge.subject,
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
                                    matches!(triple.target.as_ref(), Some(Term::BlankNode(_)));

                                self.merge_nodes(data_buffer, &index_o, &index_s)?;

                                let index_s_element = data_buffer
                                    .node_element_buffer
                                    .get(&index_s)
                                    .ok_or_else(|| {
                                        let msg = "subject not present in node_element_buffer"
                                            .to_string();
                                        SerializationErrorKind::SerializationFailed(triple, msg)
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
                            (Some(_), None) => match triple.target.clone() {
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
                                    triple.id.clone(),
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
                            triple.target.as_ref(),
                            Some(Term::Literal(lit)) if lit.value() == "true"
                        );

                        if truthy {
                            let state = data_buffer.restriction_mut(&triple.id);
                            state.self_restriction = true;
                            state.cardinality = Some(("self".to_string(), None));
                        }

                        return self.try_materialize_restriction(data_buffer, &triple.id);
                    }

                    owl::HAS_VALUE => {
                        let state = data_buffer.restriction_mut(&triple.id);
                        state.filler = triple.target.clone();
                        state.cardinality = Some(("value".to_string(), None));
                        state.render_mode = RestrictionRenderMode::ExistingPropertyEdge;
                        return self.try_materialize_restriction(data_buffer, &triple.id);
                    }

                    // owl::IMPORTS => {}
                    // owl::INCOMPATIBLE_WITH => {}
                    owl::INTERSECTION_OF => {
                        if let Some(target) = triple.target.as_ref()
                            && self.should_skip_structural_operand(
                                data_buffer,
                                &triple.id,
                                target,
                                "owl:intersectionOf",
                            )
                        {
                            return Ok(SerializationStatus::Serialized);
                        }

                        match self.insert_edge(data_buffer, &triple, ElementType::NoDraw, None) {
                            Some(edge) => {
                                if !Self::has_named_equivalent_aliases(data_buffer, &edge.subject) {
                                    self.upgrade_node_type(
                                        data_buffer,
                                        &edge.subject,
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
                        data_buffer.restriction_mut(&triple.id).cardinality =
                            Some((String::new(), Some(max)));
                        return self.try_materialize_restriction(data_buffer, &triple.id);
                    }

                    owl::MAX_QUALIFIED_CARDINALITY => {
                        let state = data_buffer.restriction_mut(&triple.id);
                        state.cardinality =
                            Some((String::new(), Some(Self::cardinality_literal(&triple)?)));
                        state.requires_filler = true;
                        return self.try_materialize_restriction(data_buffer, &triple.id);
                    }
                    // owl::MEMBERS => {}
                    owl::MIN_CARDINALITY => {
                        let min = Self::cardinality_literal(&triple)?;
                        data_buffer.restriction_mut(&triple.id).cardinality = Some((min, None));
                        return self.try_materialize_restriction(data_buffer, &triple.id);
                    }
                    owl::MIN_QUALIFIED_CARDINALITY => {
                        let state = data_buffer.restriction_mut(&triple.id);
                        state.cardinality = Some((Self::cardinality_literal(&triple)?, None));
                        state.requires_filler = true;
                        return self.try_materialize_restriction(data_buffer, &triple.id);
                    }
                    owl::NAMED_INDIVIDUAL => {
                        let count = Self::individual_count_literal(&triple)?;
                        *data_buffer
                            .individual_count_buffer
                            .entry(triple.id.clone())
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
                        self.check_unknown_buffer(data_buffer, &triple.id)?;
                        self.retry_restrictions(data_buffer)?;
                        return Ok(SerializationStatus::Serialized);
                    }
                    owl::ONE_OF => {
                        let Some(raw_target) = triple.target.clone() else {
                            return Err(SerializationErrorKind::MissingObject(
                                triple,
                                "owl:oneOf triple is missing a target".to_string(),
                            )
                            .into());
                        };

                        let materialized_target =
                            self.materialize_one_of_target(data_buffer, &triple.id, &raw_target)?;

                        let edge_triple = Triple {
                            id: triple.id.clone(),
                            element_type: triple.element_type.clone(),
                            target: Some(materialized_target),
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
                                triple.id
                            );
                        } else {
                            let base = trim_tag_circumfix(&triple.id.to_string());
                            info!("Using document base: '{}'", base);
                            data_buffer.document_base = Some(base);
                        }
                    }

                    //TODO: OWL1
                    // owl::ONTOLOGY_PROPERTY => {}
                    owl::ON_CLASS | owl::ON_DATARANGE => {
                        let state = data_buffer.restriction_mut(&triple.id);
                        state.filler = triple.target.clone();
                        state.requires_filler = true;
                        return self.try_materialize_restriction(data_buffer, &triple.id);
                    }
                    // owl::ON_DATATYPE => {}
                    // owl::ON_PROPERTIES => {}
                    owl::ON_PROPERTY => {
                        let Some(target) = triple.target.clone() else {
                            return Err(SerializationErrorKind::MissingObject(
                                triple,
                                "owl:onProperty triple is missing a target".to_string(),
                            )
                            .into());
                        };

                        data_buffer.restriction_mut(&triple.id).on_property = Some(target);
                        return self.try_materialize_restriction(data_buffer, &triple.id);
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
                        let state = data_buffer.restriction_mut(&triple.id);
                        state.filler = triple.target.clone();
                        state.cardinality = Some(("∃".to_string(), None));
                        return self.try_materialize_restriction(data_buffer, &triple.id);
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
                        if let Some(target) = triple.target.as_ref()
                            && self.should_skip_structural_operand(
                                data_buffer,
                                &triple.id,
                                target,
                                "owl:unionOf",
                            )
                        {
                            return Ok(SerializationStatus::Serialized);
                        }

                        match self.insert_edge(data_buffer, &triple, ElementType::NoDraw, None) {
                            Some(edge) => {
                                if !Self::has_named_equivalent_aliases(data_buffer, &edge.subject) {
                                    self.upgrade_node_type(
                                        data_buffer,
                                        &edge.subject,
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
                        match triple.target.clone() {
                            Some(target) => {
                                let (node_triple, edge_triple): (
                                    Option<Vec<Triple>>,
                                    Option<Triple>,
                                ) = match (
                                    self.resolve(data_buffer, triple.id.clone()),
                                    self.resolve(data_buffer, triple.element_type.clone()),
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
                                                id: domain,
                                                element_type: property,
                                                target: Some(range),
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
                                                    id: triple.id.clone(),
                                                    element_type: triple.element_type.clone(),
                                                    target: Some(thing),
                                                }),
                                            )
                                        } else if target == rdfs::LITERAL.into() {
                                            let target_iri =
                                                synthetic_iri(&property, SYNTH_LITERAL);
                                            info!("Creating literal node: {}", target_iri);
                                            let node = self.create_triple(
                                                target_iri.clone(),
                                                rdfs::LITERAL.into(),
                                                None,
                                            )?;

                                            (
                                                Some(vec![node.clone()]),
                                                Some(Triple {
                                                    id: triple.id.clone(),
                                                    element_type: triple.element_type.clone(),
                                                    target: Some(node.id),
                                                }),
                                            )
                                        } else {
                                            // Register the property itself as an element so it can be resolved by characteristics
                                            if triple.element_type == owl::OBJECT_PROPERTY.into() {
                                                self.add_to_element_buffer(
                                                    &mut data_buffer.edge_element_buffer,
                                                    &triple,
                                                    ElementType::Owl(OwlType::Edge(
                                                        OwlEdge::ObjectProperty,
                                                    )),
                                                );
                                                self.check_unknown_buffer(data_buffer, &triple.id)?;
                                                return Ok(SerializationStatus::Serialized);
                                            } else if triple.element_type
                                                == owl::DATATYPE_PROPERTY.into()
                                            {
                                                self.add_to_element_buffer(
                                                    &mut data_buffer.edge_element_buffer,
                                                    &triple,
                                                    ElementType::Owl(OwlType::Edge(
                                                        OwlEdge::DatatypeProperty,
                                                    )),
                                                );
                                                self.check_unknown_buffer(data_buffer, &triple.id)?;
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

                                        if triple.id == owl::THING.into() {
                                            let thing_anchor = range.clone();

                                            let thing = self.get_or_create_anchor_thing(
                                                data_buffer,
                                                &thing_anchor,
                                            )?;

                                            (
                                                None,
                                                Some(Triple {
                                                    id: thing,
                                                    element_type: property,
                                                    target: Some(range),
                                                }),
                                            )
                                        } else if triple.id == rdfs::LITERAL.into() {
                                            let target_iri = synthetic_iri(&range, SYNTH_LITERAL);
                                            let node = self.create_triple(
                                                target_iri,
                                                rdfs::LITERAL.into(),
                                                None,
                                            )?;

                                            (
                                                Some(vec![node.clone()]),
                                                Some(Triple {
                                                    id: node.id,
                                                    element_type: property,
                                                    target: triple.target.clone(),
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
                                            Self::is_query_fallback_endpoint(&triple.id)
                                                && triple
                                                    .target
                                                    .as_ref()
                                                    .is_some_and(Self::is_query_fallback_endpoint);

                                        if !is_full_query_fallback {
                                            trace!(
                                                "Deferring property triple with unresolved structural domain/range: {}",
                                                triple
                                            );
                                            self.add_to_unknown_buffer(
                                                data_buffer,
                                                triple.id.clone(),
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
                                                let literal_triple = self.create_triple(
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
                                                let thing_triple = self.create_triple(
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
                                                        id: thing_triple.id.clone(),
                                                        element_type: property.clone(),
                                                        target: Some(literal_triple.id),
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
                                                        id: thing.clone(),
                                                        element_type: property.clone(),
                                                        target: Some(thing),
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
                                            triple.element_type.clone(),
                                            triple.clone(),
                                        );
                                        return Ok(SerializationStatus::Deferred);
                                    }
                                    _ => {
                                        self.add_to_unknown_buffer(
                                            data_buffer,
                                            triple.id.clone(),
                                            triple.clone(),
                                        );
                                        return Ok(SerializationStatus::Deferred);
                                    }
                                };
                                match node_triple {
                                    Some(node_triples) => {
                                        for node_triple in node_triples {
                                            if node_triple.element_type == owl::THING.into() {
                                                self.insert_node(
                                                    data_buffer,
                                                    &node_triple,
                                                    ElementType::Owl(OwlType::Node(OwlNode::Thing)),
                                                )?;
                                            } else if node_triple.element_type
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
                                        let element_type = edge_triple.element_type.clone();
                                        let dummy = || edge_triple.clone();

                                        let property= data_buffer
                                        .edge_element_buffer
                                        .get(&element_type).ok_or_else(|| {
                                            let msg = "Edge triple not present in edge_element_buffer".to_string();
                                            SerializationErrorKind::SerializationFailed(dummy(), msg)})?;
                                        let edge = self.insert_edge(
                                            data_buffer,
                                            &edge_triple,
                                            *property,
                                            data_buffer
                                                .label_buffer
                                                .get(&edge_triple.element_type)
                                                .cloned(),
                                        );
                                        if let Some(edge) = edge {
                                            // Clone the property IRI before it gets consumed below
                                            let prop_iri = edge_triple.element_type.clone();

                                            data_buffer.add_property_edge(
                                                edge_triple.element_type.clone(),
                                                edge,
                                            );
                                            data_buffer.add_property_domain(
                                                edge_triple.element_type.clone(),
                                                edge_triple.id.clone(),
                                            );
                                            data_buffer.add_property_range(
                                                edge_triple.element_type.clone(),
                                                edge_triple.target.clone().ok_or_else(|| {
                                                    SerializationErrorKind::SerializationFailed(
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
                                        return Err(SerializationErrorKind::SerializationFailed(
                                            triple,
                                            "Error creating edge".to_string(),
                                        )
                                        .into());
                                    }
                                }
                            }
                            None => {
                                return Err(SerializationErrorKind::SerializationFailed(
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

    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
    fn get_or_create_domain_thing(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        domain: &Term,
    ) -> Result<Term, SerializationError> {
        if let Some(existing) = data_buffer.anchor_thing_map.get(domain) {
            return Ok(existing.clone());
        }

        let thing_iri = synthetic_iri(domain, SYNTH_THING);
        let thing_triple = self.create_triple(thing_iri, owl::THING.into(), None)?;
        let thing_id = thing_triple.id.clone();

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

    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
    fn get_or_create_anchor_thing(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        anchor: &Term,
    ) -> Result<Term, SerializationError> {
        if let Some(existing) = data_buffer.anchor_thing_map.get(anchor) {
            return Ok(existing.clone());
        }

        let thing_iri = synthetic_iri(anchor, SYNTH_THING);
        let thing_triple = self.create_triple(thing_iri, owl::THING.into(), None)?;
        let thing_id = thing_triple.id.clone();

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
            .insert(anchor.clone(), thing_id.clone());

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
        let property_iri = triple.id.clone();

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
                if edge.element_type == ElementType::Owl(OwlType::Edge(OwlEdge::InverseOf)) {
                    data_buffer
                        .edge_buffer
                        .iter()
                        .filter(|candidate| {
                            candidate.element_type
                                == ElementType::Owl(OwlType::Edge(OwlEdge::InverseOf))
                                && candidate.property.as_ref() == Some(&resolved_iri)
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
        match triple.target.as_ref() {
            Some(Term::Literal(literal)) => Ok(literal.value().to_string()),
            Some(other) => Err(SerializationErrorKind::SerializationFailed(
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
        match triple.target.as_ref() {
            Some(Term::Literal(literal)) => literal.value().parse::<u32>().map_err(|e| {
                SerializationErrorKind::SerializationFailed(
                    triple.clone(),
                    format!(
                        "Expected individual count literal, got '{}': {}",
                        literal.value(),
                        e
                    ),
                )
                .into()
            }),
            Some(other) => Err(SerializationErrorKind::SerializationFailed(
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
        edge.element_type == ElementType::Rdfs(RdfsType::Edge(RdfsEdge::SubclassOf))
            || edge.element_type == ElementType::NoDraw
    }

    fn is_consumed_restriction(data_buffer: &SerializationDataBuffer, restriction: &Term) -> bool {
        data_buffer.edge_redirection.contains_key(restriction)
            && !data_buffer.node_element_buffer.contains_key(restriction)
            && !data_buffer.restriction_buffer.contains_key(restriction)
    }

    fn restriction_owner(
        &self,
        data_buffer: &SerializationDataBuffer,
        restriction: &Term,
    ) -> Option<Term> {
        data_buffer
            .edges_include_map
            .get(restriction)
            .and_then(|edges| {
                edges.iter().find_map(|edge| {
                    (edge.object == *restriction && Self::is_restriction_owner_edge(edge))
                        .then(|| edge.subject.clone())
                })
            })
    }

    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
    fn default_restriction_target(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        owner: &Term,
        property_iri: &Term,
    ) -> Result<Term, SerializationError> {
        match data_buffer.edge_element_buffer.get(property_iri).copied() {
            Some(ElementType::Owl(OwlType::Edge(OwlEdge::DatatypeProperty))) => {
                let literal_iri = synthetic_iri(property_iri, "_literal");
                let literal_triple = self.create_triple(literal_iri, rdfs::LITERAL.into(), None)?;
                let literal_id = literal_triple.id.clone();

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
            _ => self.get_or_create_domain_thing(data_buffer, owner),
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
        restriction: &Term,
        literal: &rdf_fusion::model::Literal,
    ) -> Result<Term, SerializationError> {
        let literal_iri = synthetic_iri(restriction, "_value");
        let literal_triple = self.create_triple(literal_iri, rdfs::LITERAL.into(), None)?;
        let literal_id = literal_triple.id.clone();

        if !data_buffer.node_element_buffer.contains_key(&literal_id) {
            self.insert_node_without_retry(
                data_buffer,
                &literal_triple,
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
        subject: Term,
        property_iri: Term,
        object: Term,
        label: String,
        cardinality: Option<(String, Option<String>)>,
    ) -> Edge {
        let edge = Edge {
            subject: subject.clone(),
            element_type: ElementType::Owl(OwlType::Edge(OwlEdge::ValuesFrom)),
            object: object.clone(),
            property: Some(property_iri.clone()),
        };

        data_buffer.edge_buffer.insert(edge.clone());
        self.insert_edge_include(data_buffer, &subject, edge.clone());
        self.insert_edge_include(data_buffer, &object, edge.clone());

        data_buffer.edge_label_buffer.insert(edge.clone(), label);

        if let Some(cardinality) = cardinality {
            data_buffer
                .edge_cardinality_buffer
                .insert(edge.clone(), cardinality);
        }

        edge
    }

    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
    fn try_materialize_restriction(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        restriction: &Term,
    ) -> Result<SerializationStatus, SerializationError> {
        let Some(state) = data_buffer.restriction_buffer.get(restriction).cloned() else {
            return Ok(SerializationStatus::Deferred);
        };

        let Some(raw_property) = state.on_property else {
            return Ok(SerializationStatus::Deferred);
        };
        let Some(property_iri) = self.resolve(data_buffer, raw_property.clone()) else {
            return Ok(SerializationStatus::Deferred);
        };
        let Some(raw_subject) = self.restriction_owner(data_buffer, restriction) else {
            return Ok(SerializationStatus::Deferred);
        };
        let subject = self
            .resolve(data_buffer, raw_subject.clone())
            .unwrap_or_else(|| self.follow_redirection(data_buffer, &raw_subject));

        let restriction_label = data_buffer
            .label_buffer
            .get(&raw_property)
            .cloned()
            .or_else(|| data_buffer.label_buffer.get(&property_iri).cloned())
            .or_else(|| {
                data_buffer
                    .property_edge_map
                    .get(&property_iri)
                    .and_then(|edge| data_buffer.edge_label_buffer.get(edge).cloned())
            })
            .unwrap_or_else(|| OwlEdge::ValuesFrom.to_string());

        if state.requires_filler && !state.self_restriction && state.filler.is_none() {
            return Ok(SerializationStatus::Deferred);
        }

        if state.render_mode == RestrictionRenderMode::ExistingPropertyEdge {
            let Some(existing_edge) = data_buffer.property_edge_map.get(&property_iri).cloned()
            else {
                return Ok(SerializationStatus::Deferred);
            };

            let object = if let Some(filler) = state.filler.as_ref() {
                match filler {
                    Term::Literal(literal) => {
                        data_buffer
                            .label_buffer
                            .insert(existing_edge.object.clone(), literal.value().to_string());
                        existing_edge.object.clone()
                    }
                    other => match self.resolve(data_buffer, other.clone()) {
                        Some(resolved) => resolved,
                        None => return Ok(SerializationStatus::Deferred),
                    },
                }
            } else {
                existing_edge.object.clone()
            };

            let edge = self
                .rewrite_property_edge(data_buffer, &property_iri, subject.clone(), object)
                .ok_or_else(|| {
                    SerializationErrorKind::SerializationFailed(
                        Triple::new(subject.clone(), property_iri.clone(), None),
                        "Failed to rewrite canonical property edge for hasValue restriction"
                            .to_string(),
                    )
                })?;

            data_buffer
                .edge_label_buffer
                .insert(edge.clone(), restriction_label);

            if let Some(cardinality) = state.cardinality {
                data_buffer
                    .edge_cardinality_buffer
                    .insert(edge, cardinality);
            }

            self.remove_restriction_stub(data_buffer, restriction);
            self.remove_restriction_node(data_buffer, restriction);

            if subject != *restriction {
                self.redirect_iri(data_buffer, restriction, &subject)?;
            }

            return Ok(SerializationStatus::Serialized);
        }

        let object = if state.self_restriction {
            subject.clone()
        } else if let Some(filler) = state.filler {
            match filler {
                Term::Literal(literal) => {
                    self.materialize_literal_value_target(data_buffer, restriction, &literal)?
                }
                other => match self.resolve(data_buffer, other.clone()) {
                    Some(resolved) => resolved,
                    None => {
                        self.materialize_named_value_target(data_buffer, &property_iri, &other)?
                    }
                },
            }
        } else {
            self.default_restriction_target(data_buffer, &subject, &property_iri)?
        };

        self.remove_property_fallback_edge(data_buffer, &property_iri);

        let edge = self.insert_restriction_edge(
            data_buffer,
            subject.clone(),
            property_iri.clone(),
            object,
            restriction_label,
            state.cardinality,
        );

        data_buffer
            .property_edge_map
            .insert(property_iri.clone(), edge);

        self.remove_restriction_stub(data_buffer, restriction);
        self.remove_restriction_node(data_buffer, restriction);

        if subject != *restriction {
            self.redirect_iri(data_buffer, restriction, &subject)?;
        }

        Ok(SerializationStatus::Serialized)
    }

    fn remove_restriction_stub(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        restriction: &Term,
    ) {
        if let Some(edges) = data_buffer.edges_include_map.get(restriction).cloned() {
            for edge in edges {
                if edge.object == *restriction && Self::is_restriction_owner_edge(&edge) {
                    self.remove_edge_include(data_buffer, &edge.subject, &edge);
                    self.remove_edge_include(data_buffer, &edge.object, &edge);
                    data_buffer.edge_buffer.remove(&edge);
                    data_buffer.edge_label_buffer.remove(&edge);
                    data_buffer.edge_cardinality_buffer.remove(&edge);
                    data_buffer.edge_characteristics.remove(&edge);
                }
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "fixed when serializer is refactored to use pointers instead of values"
    )]
    fn materialize_named_value_target(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        property_iri: &Term,
        target: &Term,
    ) -> Result<Term, SerializationError> {
        match data_buffer.edge_element_buffer.get(property_iri).copied() {
            Some(ElementType::Owl(OwlType::Edge(OwlEdge::ObjectProperty)))
            | Some(ElementType::Owl(OwlType::Edge(OwlEdge::ExternalProperty)))
            | Some(ElementType::Owl(OwlType::Edge(OwlEdge::DeprecatedProperty)))
            | Some(ElementType::Rdf(RdfType::Edge(RdfEdge::RdfProperty))) => {
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
            _ => Err(SerializationErrorKind::SerializationFailed(
                Triple::new(target.clone(), property_iri.clone(), None),
                format!(
                    "Cannot materialize named value target '{target}' for non-object restriction"
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
        restriction: &Term,
    ) {
        data_buffer.node_element_buffer.remove(restriction);
        data_buffer.label_buffer.remove(restriction);
        data_buffer.node_characteristics.remove(restriction);
        data_buffer.edges_include_map.remove(restriction);
        data_buffer.restriction_buffer.remove(restriction);
        data_buffer.individual_count_buffer.remove(restriction);
    }

    fn is_synthetic_property_fallback(edge: &Edge) -> bool {
        let is_property_edge = matches!(
            edge.element_type,
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

        let subject = trim_tag_circumfix(&edge.subject.to_string());
        let object = trim_tag_circumfix(&edge.object.to_string());

        let synthetic_subject =
            subject.ends_with(SYNTH_THING) || subject.ends_with(SYNTH_LOCAL_THING);
        let synthetic_object = object.ends_with(SYNTH_THING)
            || object.ends_with(SYNTH_LOCAL_THING)
            || object.ends_with(SYNTH_LITERAL)
            || object.ends_with(SYNTH_LOCAL_LITERAL);

        synthetic_subject && synthetic_object
    }

    fn remove_orphan_synthetic_node(&self, data_buffer: &mut SerializationDataBuffer, iri: &Term) {
        let clean = trim_tag_circumfix(&iri.to_string());
        let looks_synthetic = clean.ends_with(SYNTH_THING)
            || clean.ends_with(SYNTH_LOCAL_THING)
            || clean.ends_with(SYNTH_LITERAL)
            || clean.ends_with(SYNTH_LOCAL_LITERAL);

        if !looks_synthetic {
            return;
        }

        let still_used = data_buffer
            .edges_include_map
            .get(iri)
            .is_some_and(|edges| !edges.is_empty());

        if still_used {
            return;
        }

        data_buffer.edges_include_map.remove(iri);
        data_buffer.node_element_buffer.remove(iri);
        data_buffer.label_buffer.remove(iri);
        data_buffer.node_characteristics.remove(iri);
        data_buffer.anchor_thing_map.retain(|_, value| value != iri);
        data_buffer.individual_count_buffer.remove(iri);
    }

    fn remove_property_fallback_edge(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        property_iri: &Term,
    ) {
        let Some(edge) = data_buffer.property_edge_map.get(property_iri).cloned() else {
            return;
        };

        if !Self::is_synthetic_property_fallback(&edge) {
            return;
        }

        self.remove_edge_include(data_buffer, &edge.subject, &edge);
        self.remove_edge_include(data_buffer, &edge.object, &edge);

        data_buffer.edge_buffer.remove(&edge);
        data_buffer.edge_label_buffer.remove(&edge);
        data_buffer.edge_cardinality_buffer.remove(&edge);
        data_buffer.edge_characteristics.remove(&edge);

        data_buffer.property_edge_map.remove(property_iri);
        data_buffer.property_domain_map.remove(property_iri);
        data_buffer.property_range_map.remove(property_iri);

        self.remove_orphan_synthetic_node(data_buffer, &edge.subject);
        self.remove_orphan_synthetic_node(data_buffer, &edge.object);
    }

    fn rewrite_property_edge(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        property_iri: &Term,
        new_subject: Term,
        new_object: Term,
    ) -> Option<Edge> {
        let old_edge = data_buffer.property_edge_map.get(property_iri).cloned()?;

        if old_edge.subject == new_subject && old_edge.object == new_object {
            return Some(old_edge);
        }

        let mut new_edge = old_edge.clone();
        new_edge.subject = new_subject.clone();
        new_edge.object = new_object.clone();

        let label = data_buffer.edge_label_buffer.remove(&old_edge);
        let characteristics = data_buffer.edge_characteristics.remove(&old_edge);
        let cardinality = data_buffer.edge_cardinality_buffer.remove(&old_edge);

        self.remove_edge_include(data_buffer, &old_edge.subject, &old_edge);
        self.remove_edge_include(data_buffer, &old_edge.object, &old_edge);
        data_buffer.edge_buffer.remove(&old_edge);

        data_buffer.edge_buffer.insert(new_edge.clone());
        self.insert_edge_include(data_buffer, &new_edge.subject, new_edge.clone());
        self.insert_edge_include(data_buffer, &new_edge.object, new_edge.clone());

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
            .insert(property_iri.clone(), new_edge.clone());

        data_buffer
            .property_domain_map
            .insert(property_iri.clone(), HashSet::from([new_subject.clone()]));
        data_buffer
            .property_range_map
            .insert(property_iri.clone(), HashSet::from([new_object.clone()]));

        self.remove_orphan_synthetic_node(data_buffer, &old_edge.subject);
        self.remove_orphan_synthetic_node(data_buffer, &old_edge.object);

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
                id: example_com.clone(),
                element_type: owl_ontology.clone(),
                target: None,
            },
        );
        serializer.write_node_triple(
            &mut data_buffer,
            Triple {
                id: example_parent.clone(),
                element_type: owl_class.clone(),
                target: None,
            },
        );
        serializer.write_node_triple(
            &mut data_buffer,
            Triple {
                id: example_mother.clone(),
                element_type: owl_class.clone(),
                target: None,
            },
        );
        serializer.write_node_triple(
            &mut data_buffer,
            Triple {
                id: example_guardian.clone(),
                element_type: owl_class.clone(),
                target: None,
            },
        );
        serializer.write_node_triple(
            &mut data_buffer,
            Triple {
                id: example_warden.clone(),
                element_type: owl_class.clone(),
                target: None,
            },
        );
        serializer.write_node_triple(
            &mut data_buffer,
            Triple {
                id: example_warden1.clone(),
                element_type: owl_class.clone(),
                target: None,
            },
        );
        serializer.write_node_triple(
            &mut data_buffer,
            Triple {
                id: example_warden.clone(),
                element_type: rdfs_subclass_of.clone(),
                target: Some(example_guardian.clone()),
            },
        );
        serializer.write_node_triple(
            &mut data_buffer,
            Triple {
                id: example_mother.clone(),
                element_type: rdfs_subclass_of.clone(),
                target: Some(example_parent.clone()),
            },
        );
        serializer.write_node_triple(
            &mut data_buffer,
            Triple {
                id: blanknode1.clone(),
                element_type: Term::Literal(Literal::new_simple_literal("blanknode".to_string())),
                target: None,
            },
        );
        serializer.write_node_triple(
            &mut data_buffer,
            Triple {
                id: example_warden1.clone(),
                element_type: Term::NamedNode(
                    NamedNode::new("http://www.w3.org/2002/07/owl#unionOf").unwrap(),
                ),
                target: Some(example_warden.clone()),
            },
        );

        print_graph_display_data(&data_buffer);
        println!("--------------------------------");

        let triple = Triple {
            id: example_guardian.clone(),
            element_type: Term::NamedNode(
                NamedNode::new("http://www.w3.org/2002/07/owl#equivalentClass").unwrap(),
            ),
            target: Some(example_warden.clone()),
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
            subject: example_warden1,
            element_type: ElementType::NoDraw,
            object: example_guardian.clone(),
            property: None
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
                id: Term::NamedNode(NamedNode::new("http://example.com#Guardian").unwrap()),
                element_type: Term::NamedNode(
                    NamedNode::new("http://www.w3.org/2002/07/owl#equivalentClass").unwrap(),
                ),
                target: Some(blanknode1.clone()),
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
                edge.subject, edge.element_type, edge.object
            );
        }
    }
}

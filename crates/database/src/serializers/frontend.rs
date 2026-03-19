use std::{
    collections::{HashMap, HashSet},
    mem::take,
    time::{Duration, Instant},
};

use super::{Edge, SerializationDataBuffer, Triple};
use crate::{
    errors::{SerializationError, SerializationErrorKind},
    serializers::util::{get_reserved_iris, trim_tag_circumfix},
    vocab::owl,
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
    model::{
        BlankNode, NamedNode, Term,
        vocab::{rdf, rdfs},
    },
};
use vowlr_parser::errors::VOWLRStoreError;
use vowlr_util::prelude::VOWLRError;

pub struct GraphDisplayDataSolutionSerializer {
    pub resolvable_iris: HashSet<String>,
}

pub enum SerializationStatus {
    Serialized,
    Deferred,
}

impl GraphDisplayDataSolutionSerializer {
    pub fn new() -> Self {
        Self {
            resolvable_iris: get_reserved_iris(),
        }
    }

    pub async fn serialize_nodes_stream(
        &self,
        data: &mut GraphDisplayData,
        mut solution_stream: QuerySolutionStream,
    ) -> Result<(), VOWLRError> {
        let mut count: u32 = 0;
        info!("Serializing query solution stream...");
        let start_time = Instant::now();
        let mut data_buffer = SerializationDataBuffer::new();
        while let Some(maybe_solution) = solution_stream.next().await {
            let solution = match maybe_solution {
                Ok(solution) => solution,
                Err(e) => {
                    let a: VOWLRStoreError = e.into();
                    data_buffer.failed_buffer.push(a.into());
                    continue;
                }
            };
            let Some(id_term) = solution.get("id") else {
                continue;
            };
            let Some(node_type_term) = solution.get("nodeType") else {
                continue;
            };

            self.extract_label(&mut data_buffer, solution.get("label"), id_term);

            let triple: Triple = Triple {
                id: id_term.to_owned(),
                element_type: node_type_term.to_owned(),
                target: solution.get("target").map(|term| term.to_owned()),
            };

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
            0,
            data_buffer.edge_characteristics.len() + data_buffer.node_characteristics.len(),
        );
        debug!("{}", data_buffer);
        if !data_buffer.failed_buffer.is_empty() {
            let total = data_buffer.failed_buffer.len();
            let err: VOWLRError = take(&mut data_buffer.failed_buffer).into();
            error!("Failed to serialize {} triples:\n{}", total, err);
            return Err(err);
        }
        *data = data_buffer.into();
        debug!("{}", data);
        Ok(())
    }

    /// Extract label info from the query solution and store until
    /// they can be mapped to their ElementType.
    fn extract_label(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        label: Option<&Term>,
        id_term: &Term,
    ) {
        // Prevent overriding labels
        if data_buffer.label_buffer.contains_key(id_term) {
            return;
        }

        match label {
            // Case 1: Label is a rdfs:label OR rdfs:Resource OR rdf:ID
            Some(label) => {
                if label.to_string() != "" {
                    data_buffer
                        .label_buffer
                        .insert(id_term.clone(), label.to_string());
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
                            data_buffer
                                .label_buffer
                                .insert(id_term.clone(), frag.to_string());
                        }
                        // Case 2.2: Look for path in iri
                        None => {
                            debug!("No fragment found in iri '{iri}'");
                            match id_iri.path().rsplit_once('/') {
                                Some(path) => {
                                    data_buffer
                                        .label_buffer
                                        .insert(id_term.clone(), path.1.to_string());
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

    fn resolve(&self, data_buffer: &SerializationDataBuffer, mut x: Term) -> Option<Term> {
        if let Some(elem) = data_buffer.node_element_buffer.get(&x) {
            debug!("Resolved: {}: {}", x, elem);
            return Some(x);
        } else if let Some(elem) = data_buffer.edge_element_buffer.get(&x) {
            debug!("Resolved: {}: {}", x, elem);
            return Some(x);
        }

        while let Some(redirected) = data_buffer.edge_redirection.get(&x) {
            trace!("Redirected: {} -> {}", x, redirected);
            let new_x = redirected.clone();
            if let Some(elem) = data_buffer.node_element_buffer.get(&new_x) {
                debug!("Resolved: {}: {}", new_x, elem);
                return Some(new_x);
            } else if let Some(elem) = data_buffer.edge_element_buffer.get(&new_x) {
                debug!("Resolved: {}: {}", new_x, elem);
                return Some(new_x);
            }
            debug!("Checked: {} ", new_x);
            x = new_x;
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
        // Skip insertion if this node was already merged into another node
        if data_buffer.edge_redirection.contains_key(&triple.id) {
            debug!(
                "Skipping insert_node for '{}': already redirected",
                triple.id
            );
            return Ok(());
        }

        self.add_to_element_buffer(&mut data_buffer.node_element_buffer, triple, node_type);
        self.check_unknown_buffer(data_buffer, &triple.id)?;
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
        triple: &Triple,
        edge_type: ElementType,
        label: Option<String>,
    ) -> Option<Edge> {
        // Skip external check for NoDraw edges - they should always retain their type
        let new_type =
            if edge_type != ElementType::NoDraw && self.is_external(data_buffer, &triple.id) {
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
            Some(base) => !clean_iri.contains(base) && !self.resolvable_iris.contains(&clean_iri),
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
        debug!("Merging node '{old}' into '{new}'");
        data_buffer.node_element_buffer.remove(old);
        self.update_edges(data_buffer, old, new);
        self.redirect_iri(data_buffer, old, new)?;
        Ok(())
    }

    fn update_edges(&self, data_buffer: &mut SerializationDataBuffer, old: &Term, new: &Term) {
        let old_edges = data_buffer.edges_include_map.remove(old);
        if let Some(old_edges) = old_edges {
            debug!("Updating edges from '{}' to '{}'", old, new);
            // info!("old_edges: ");
            // for edge in old_edges.iter() {
            //     info!("edge: {} ", edge);
            // }

            for mut edge in old_edges.into_iter() {
                data_buffer.edge_buffer.remove(&edge);
                if edge.object == *old {
                    edge.object = new.clone();
                }
                if edge.subject == *old {
                    edge.subject = new.clone();
                }
                data_buffer.edge_buffer.insert(edge.clone());
                self.insert_edge_include(data_buffer, new, edge.clone());
            }
            // info!("new_edges: ");
            // for edge in data_buffer.edge_buffer.iter() {
            //     info!("edge: {} ", edge);
            // }
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
                if matches!(
                    old_elem,
                    ElementType::Owl(OwlType::Node(OwlNode::Class))
                        | ElementType::Owl(OwlType::Node(OwlNode::AnonymousClass))
                ) {
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
            for characteristic in right_characteristics {
                if !merged_characteristics
                    .iter()
                    .any(|existing| existing == &characteristic)
                {
                    merged_characteristics.push(characteristic);
                }
            }
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
        info!("Second pass: Resolving all possible unknowns");

        let mut pending = take(&mut data_buffer.unknown_buffer);
        let mut pass: usize = 0;
        let max_passes: usize = 4;

        while !pending.is_empty() && pass < max_passes {
            pass += 1;

            let pending_before: usize = pending.values().map(|set| set.len()).sum();
            trace!(
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
                trace!(
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
        triple: Triple,
    ) -> Result<SerializationStatus, SerializationError> {
        // TODO: Collect errors and show to frontend
        debug!("{}", triple);
        match &triple.element_type {
            Term::BlankNode(bnode) => {
                // The query must never put blank nodes in the ?nodeType variable
                let msg = format!("Illegal blank node during serialization: '{bnode}'");
                return Err(SerializationErrorKind::SerializationFailed(triple, msg).into());
            }
            Term::Literal(literal) => {
                // NOTE: Any string literal goes here, e.g, every BIND("someString" AS ?nodeType)
                let value = literal.value();
                match value {
                    "blanknode" => {
                        debug!("Visualizing blank node: {}", triple.id);
                        self.insert_node(
                            data_buffer,
                            &triple,
                            ElementType::Owl(OwlType::Node(OwlNode::AnonymousClass)),
                        )?;
                    }
                    &_ => {
                        warn!("Visualization of literal '{value}' is not supported");
                    }
                }
            }
            Term::NamedNode(uri) => {
                // NOTE: Only supports RDF 1.1
                match uri.as_ref() {
                    // ----------- RDF ----------- //

                    // rdf::ALT => {}
                    // rdf::BAG => {}
                    // rdf::FIRST => {}
                    // rdf::HTML => {}
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
                    // rdf::XML_LITERAL => {}

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
                        match self.insert_edge(
                            data_buffer,
                            &triple,
                            ElementType::Rdfs(RdfsType::Edge(RdfsEdge::SubclassOf)),
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
                    //rdfs::SUB_PROPERTY_OF => {},

                    // ----------- OWL 2 ----------- //

                    //TODO: OWL1
                    // owl::ALL_DIFFERENT => {},

                    // owl::ALL_DISJOINT_CLASSES => {},
                    // owl::ALL_DISJOINT_PROPERTIES => {},

                    //TODO: OWL1
                    // owl::ALL_VALUES_FROM => {}

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
                            Characteristic::AsymmetricProperty.to_string(),
                        ));
                    }

                    // owl::AXIOM => {},
                    // owl::BACKWARD_COMPATIBLE_WITH => {},
                    // owl::BOTTOM_DATA_PROPERTY => {},
                    // owl::BOTTOM_OBJECT_PROPERTY => {},

                    //TODO: OWL1
                    // owl::CARDINALITY => {}
                    owl::CLASS => {
                        self.insert_node(
                            data_buffer,
                            &triple,
                            ElementType::Owl(OwlType::Node(OwlNode::Class)),
                        )?;
                        return Ok(SerializationStatus::Serialized);
                    }
                    owl::COMPLEMENT_OF => {
                        let edge =
                            self.insert_edge(data_buffer, &triple, ElementType::NoDraw, None);

                        if triple.target.is_some()
                            && let Some(index) = self.resolve(data_buffer, triple.id.clone())
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
                        let edge =
                            self.insert_edge(data_buffer, &triple, ElementType::NoDraw, None);
                        if triple.target.is_some()
                            && let Some(index) = self.resolve(data_buffer, triple.id.clone())
                        {
                            self.upgrade_node_type(
                                data_buffer,
                                &index,
                                ElementType::Owl(OwlType::Node(OwlNode::DisjointUnion)),
                            );
                        }
                        if edge.is_some() {
                            return Ok(SerializationStatus::Serialized);
                        } else {
                            return Ok(SerializationStatus::Deferred);
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
                                    self.upgrade_node_type(
                                        data_buffer,
                                        &index_s,
                                        ElementType::Owl(OwlType::Node(OwlNode::EquivalentClass)),
                                    );
                                    // AnonymousClass does not have label!
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
                            Characteristic::FunctionalProperty.to_string(),
                        ));
                    }

                    // owl::HAS_KEY => {}
                    // owl::HAS_SELF => {}

                    //TODO: OWL1
                    // owl::HAS_VALUE => {}

                    // owl::IMPORTS => {}
                    // owl::INCOMPATIBLE_WITH => {}
                    owl::INTERSECTION_OF => {
                        let edge =
                            self.insert_edge(data_buffer, &triple, ElementType::NoDraw, None);
                        if let Some(edge) = edge {
                            self.upgrade_node_type(
                                data_buffer,
                                &edge.subject,
                                ElementType::Owl(OwlType::Node(OwlNode::IntersectionOf)),
                            );
                            return Ok(SerializationStatus::Serialized);
                        }
                    }
                    owl::INVERSE_FUNCTIONAL_PROPERTY => {
                        return Ok(self.insert_characteristic(
                            data_buffer,
                            triple,
                            Characteristic::InverseFunctionalProperty.to_string(),
                        ));
                    }

                    owl::INVERSE_OF => {
                        return Ok(self.insert_inverse_of(data_buffer, triple));
                    }

                    owl::IRREFLEXIVE_PROPERTY => {
                        return Ok(self.insert_characteristic(
                            data_buffer,
                            triple,
                            Characteristic::IrreflexiveProperty.to_string(),
                        ));
                    }

                    //TODO: OWL1
                    // owl::MAX_CARDINALITY => {}

                    // owl::MAX_QUALIFIED_CARDINALITY => {}
                    // owl::MEMBERS => {}

                    //TODO: OWL1
                    // owl::MIN_CARDINALITY => {}
                    // owl::MIN_QUALIFIED_CARDINALITY => {}
                    // owl::NAMED_INDIVIDUAL => {}
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
                        return Ok(SerializationStatus::Serialized);
                    }
                    // owl::ONE_OF => {}
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

                    // owl::ON_CLASS => {}
                    // owl::ON_DATARANGE => {}
                    // owl::ON_DATATYPE => {}
                    // owl::ON_PROPERTIES => {}

                    //TODO: OWL1
                    // owl::ON_PROPERTY => {}

                    // owl::PRIOR_VERSION => {}
                    // owl::PROPERTY_CHAIN_AXIOM => {}
                    // owl::PROPERTY_DISJOINT_WITH => {}
                    // owl::QUALIFIED_CARDINALITY => {}
                    owl::REFLEXIVE_PROPERTY => {
                        return Ok(self.insert_characteristic(
                            data_buffer,
                            triple,
                            Characteristic::ReflexiveProperty.to_string(),
                        ));
                    }

                    //TODO: OWL1
                    // owl::RESTRICTION => {}

                    //TODO: OWL1
                    // owl::SAME_AS => {}

                    //TODO: OWL1
                    // owl::SOME_VALUES_FROM => {}
                    // owl::SOURCE_INDIVIDUAL => {}
                    owl::SYMMETRIC_PROPERTY => {
                        return Ok(self.insert_characteristic(
                            data_buffer,
                            triple,
                            Characteristic::SymmetricProperty.to_string(),
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
                            Characteristic::TransitiveProperty.to_string(),
                        ));
                    }
                    owl::UNION_OF => {
                        match self.insert_edge(data_buffer, &triple, ElementType::NoDraw, None) {
                            Some(edge) => {
                                self.upgrade_node_type(
                                    data_buffer,
                                    &edge.subject,
                                    ElementType::Owl(OwlType::Node(OwlNode::UnionOf)),
                                );
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
                                                Self::synthetic_iri(&property, "_literal");
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
                                            let target_iri =
                                                Self::synthetic_iri(&range, "_literal");
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
                                            trace!(
                                                "Adding unknown buffer: target: {}, triple: {}",
                                                target, triple
                                            );
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
                                                    Self::synthetic_iri(&property, "_localliteral");
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
                                                    Self::synthetic_iri(&property, "_localthing");
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
                                        trace!(
                                            "Adding unknown buffer: element type: {}, triple: {}",
                                            triple.element_type, triple
                                        );
                                        self.add_to_unknown_buffer(
                                            data_buffer,
                                            triple.element_type.clone(),
                                            triple.clone(),
                                        );
                                        return Ok(SerializationStatus::Deferred);
                                    }
                                    _ => {
                                        trace!("Adding unknown buffer: triple: {}", triple);
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

        let thing_iri = Self::synthetic_iri(domain, "_thing");
        let thing_triple = self.create_triple(thing_iri, owl::THING.into(), None)?;
        let thing_id = thing_triple.id.clone();

        self.insert_node(
            data_buffer,
            &thing_triple,
            ElementType::Owl(OwlType::Node(OwlNode::Thing)),
        )?;

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

        let thing_iri = Self::synthetic_iri(anchor, "_thing");
        let thing_triple = self.create_triple(thing_iri, owl::THING.into(), None)?;
        let thing_id = thing_triple.id.clone();

        self.insert_node(
            data_buffer,
            &thing_triple,
            ElementType::Owl(OwlType::Node(OwlNode::Thing)),
        )?;

        data_buffer
            .anchor_thing_map
            .insert(anchor.clone(), thing_id.clone());

        Ok(thing_id)
    }

    fn is_query_fallback_endpoint(term: &Term) -> bool {
        *term == owl::THING.into() || *term == rdfs::LITERAL.into()
    }

    fn synthetic_iri(base: &Term, suffix: &str) -> String {
        let clean = trim_tag_circumfix(&base.to_string());
        format!("{clean}{suffix}")
    }

    fn insert_characteristic(
        &self,
        data_buffer: &mut SerializationDataBuffer,
        triple: Triple,
        arg: String,
    ) -> SerializationStatus {
        let property_iri = triple.id.clone();

        let Some(resolved_iri) = self.resolve(data_buffer, property_iri.clone()) else {
            debug!(
                "Deferring characteristic '{}' for '{}': property unresolved",
                arg, property_iri
            );
            self.add_to_unknown_buffer(data_buffer, property_iri, triple);
            return SerializationStatus::Deferred;
        };

        // Characteristic can attach only after a concrete edge exists
        if let Some(edge) = data_buffer.property_edge_map.get(&resolved_iri).cloned() {
            debug!("Inserting edge characteristic: {} -> {}", resolved_iri, arg);

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
                let entry = data_buffer
                    .edge_characteristics
                    .entry(target_edge)
                    .or_default();
                if !entry.iter().any(|existing| existing == &arg) {
                    entry.push(arg.clone());
                }
            }

            return SerializationStatus::Serialized;
        }

        // Property is known, but edge not materialized yet
        if data_buffer.edge_element_buffer.contains_key(&resolved_iri) {
            debug!(
                "Deferring characteristic '{}' for '{}': property known, edge not materialized yet",
                arg, resolved_iri
            );
            self.add_to_unknown_buffer(data_buffer, resolved_iri, triple);
            return SerializationStatus::Deferred;
        }

        // No attach point yet
        debug!(
            "Deferring characteristic '{}' for '{}': no attach point available yet",
            arg, resolved_iri
        );
        self.add_to_unknown_buffer(data_buffer, resolved_iri, triple);
        SerializationStatus::Deferred
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

//! Funxtions related to characteristics.

use std::collections::HashSet;

use grapher::prelude::{
    Characteristic, ElementType, OwlEdge, OwlNode, OwlType, RdfsNode, RdfsType,
};
use log::{debug, trace, warn};
use vowlgrapher_util::prelude::ErrorRecord;

use crate::{
    datastructures::{
        ArcTriple, SerializationStatus, serialization_data_buffer::SerializationDataBuffer,
    },
    errors::{SerializationError, SerializationErrorKind},
    serializer_util::{
        buffers::{
            add_term_to_element_buffer, add_to_unknown_buffer, check_unknown_buffer,
            insert_edge_include, remove_edge_include, resolve,
        },
        edges::{collect_property_render_edges, follow_redirection, merge_properties},
        entity_creation::{create_edge_from_id, get_or_create_anchor_thing},
        is_reserved, is_synthetic,
        labels::merge_optional_labels,
        nodes::is_query_fallback_endpoint,
    },
};

pub fn normalize_inverse_endpoint(
    data_buffer: &mut SerializationDataBuffer,
    endpoint_term_id: usize,
    opposite_term_id: usize,
) -> Result<usize, SerializationError> {
    let Some(element_type) = ({
        data_buffer
            .node_element_buffer
            .read()?
            .get(&endpoint_term_id)
            .copied()
    }) else {
        return Ok(endpoint_term_id);
    };

    match element_type {
        ElementType::Owl(OwlType::Node(
            OwlNode::Complement
            | OwlNode::IntersectionOf
            | OwlNode::UnionOf
            | OwlNode::DisjointUnion
            | OwlNode::EquivalentClass,
        )) => get_or_create_anchor_thing(data_buffer, opposite_term_id),
        _ => Ok(endpoint_term_id),
    }
}

pub fn inverse_edge_endpoints(
    data_buffer: &mut SerializationDataBuffer,
    property_term_id: usize,
) -> Result<Option<(usize, usize)>, SerializationError> {
    let domain = {
        let declared_property_domain_map = data_buffer.declared_property_domain_map.read()?;
        match declared_property_domain_map.get(&property_term_id) {
            Some(domains) => select_property_endpoint(data_buffer, domains)?,
            None => None,
        }
    };

    let range = {
        let declared_property_range_map = data_buffer.declared_property_range_map.read()?;
        match declared_property_range_map.get(&property_term_id) {
            Some(ranges) => select_property_endpoint(data_buffer, ranges)?,
            None => None,
        }
    };

    match (domain, range) {
        (Some(domain), Some(range)) => {
            let subject = normalize_inverse_endpoint(data_buffer, domain, range)?;
            let object = normalize_inverse_endpoint(data_buffer, range, domain)?;
            Ok(Some((subject, object)))
        }
        _ => Ok(None),
    }
}

pub fn insert_inverse_of(
    data_buffer: &mut SerializationDataBuffer,
    triple: ArcTriple,
) -> Result<SerializationStatus, SerializationError> {
    let left_property_raw = triple.subject_term_id;
    let Some(right_property_raw) = triple.object_term_id else {
        let msg = format!(
            "owl:inverseOf triple is missing a target: {}",
            data_buffer.term_index.display_triple(&triple)?
        );
        let e = SerializationErrorKind::SerializationWarning(msg.clone());
        warn!("{msg}");
        data_buffer
            .failed_buffer
            .write()?
            .push(<SerializationError as Into<ErrorRecord>>::into(e.into()));

        return Ok(SerializationStatus::Serialized);
    };

    let left_is_blank = data_buffer.term_index.is_blank_node(left_property_raw)?;
    let right_is_blank = data_buffer.term_index.is_blank_node(right_property_raw)?;

    match (left_is_blank, right_is_blank) {
        (true, false) => {
            ensure_object_property_registration(data_buffer, right_property_raw)?;
            merge_properties(data_buffer, left_property_raw, right_property_raw)?;
            return Ok(SerializationStatus::Serialized);
        }
        (false, true) => {
            ensure_object_property_registration(data_buffer, left_property_raw)?;
            merge_properties(data_buffer, right_property_raw, left_property_raw)?;
            return Ok(SerializationStatus::Serialized);
        }
        (true, true) => {
            add_to_unknown_buffer(data_buffer, left_property_raw, triple)?;
            return Ok(SerializationStatus::Deferred);
        }
        (false, false) => {}
    }

    ensure_object_property_registration(data_buffer, left_property_raw)?;
    ensure_object_property_registration(data_buffer, right_property_raw)?;

    let Some(left_property) = resolve(data_buffer, left_property_raw)? else {
        add_to_unknown_buffer(data_buffer, left_property_raw, triple)?;
        return Ok(SerializationStatus::Deferred);
    };

    let Some(right_property) = resolve(data_buffer, right_property_raw)? else {
        add_to_unknown_buffer(data_buffer, right_property_raw, triple)?;
        return Ok(SerializationStatus::Deferred);
    };

    if left_property == right_property {
        return Ok(SerializationStatus::Serialized);
    }

    let Some((left_subject_raw, left_object_raw)) =
        inverse_edge_endpoints(data_buffer, left_property)?
    else {
        add_to_unknown_buffer(data_buffer, left_property, triple)?;
        return Ok(SerializationStatus::Deferred);
    };

    let Some((right_subject_raw, right_object_raw)) =
        inverse_edge_endpoints(data_buffer, right_property)?
    else {
        add_to_unknown_buffer(data_buffer, right_property, triple)?;
        return Ok(SerializationStatus::Deferred);
    };
    let left_subject_is_fallback = is_inverse_fallback_term_id(data_buffer, left_subject_raw)?;
    let left_object_is_fallback = is_inverse_fallback_term_id(data_buffer, left_object_raw)?;
    let right_subject_is_fallback = is_inverse_fallback_term_id(data_buffer, right_subject_raw)?;
    let right_object_is_fallback = is_inverse_fallback_term_id(data_buffer, right_object_raw)?;

    let left_subject = if left_subject_is_fallback && !right_object_is_fallback {
        right_object_raw
    } else {
        left_subject_raw
    };
    let left_object = if left_object_is_fallback && !right_subject_is_fallback {
        right_subject_raw
    } else {
        left_object_raw
    };
    let right_subject = if right_subject_is_fallback && !left_object_is_fallback {
        left_object_raw
    } else {
        right_subject_raw
    };
    let right_object = if right_object_is_fallback && !left_subject_is_fallback {
        left_subject_raw
    } else {
        right_object_raw
    };

    let compatible = left_subject == right_object && left_object == right_subject;
    if !compatible {
        let msg = format!(
            "Cannot merge owl:inverseOf '{}'<->'{}': normalized edges do not align ({} -> {}, {} -> {})",
            data_buffer.term_index.get(left_property)?,
            data_buffer.term_index.get(right_property)?,
            data_buffer.term_index.get(left_subject)?,
            data_buffer.term_index.get(left_object)?,
            data_buffer.term_index.get(right_subject)?,
            data_buffer.term_index.get(right_object)?
        );
        let e = SerializationErrorKind::SerializationWarning(msg.clone());
        warn!("{msg}");
        data_buffer
            .failed_buffer
            .write()?
            .push(<SerializationError as Into<ErrorRecord>>::into(e.into()));

        return Ok(SerializationStatus::Serialized);
    }

    let property_edges_to_remove =
        collect_property_render_edges(data_buffer, &[left_property, right_property])?;

    let merged_label = {
        let left_edge = data_buffer
            .property_edge_map
            .read()?
            .get(&left_property)
            .cloned();
        let right_edge = data_buffer
            .property_edge_map
            .read()?
            .get(&right_property)
            .cloned();

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

    merge_properties(data_buffer, right_property, left_property)?;

    let merged_characteristics = {
        let mut merged_characteristics = HashSet::new();

        for edge in &property_edges_to_remove {
            remove_edge_include(data_buffer, edge.domain_term_id, edge)?;
            remove_edge_include(data_buffer, edge.range_term_id, edge)?;
            data_buffer.edge_buffer.write()?.remove(edge);
            data_buffer.edge_label_buffer.write()?.remove(edge);
            data_buffer.edge_cardinality_buffer.write()?.remove(edge);

            let removed_characteristics =
                { data_buffer.edge_characteristics.write()?.remove(edge) };

            if let Some(characteristics) = removed_characteristics {
                merged_characteristics.extend(characteristics);
            }
        }

        merged_characteristics
    };

    let inverse_property = Some(left_property);
    let edge_type = ElementType::Owl(OwlType::Edge(OwlEdge::InverseOf));
    let inverse_edges = [
        create_edge_from_id(
            &data_buffer.term_index,
            left_subject,
            edge_type,
            left_object,
            inverse_property,
        )?,
        create_edge_from_id(
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
        insert_edge_include(data_buffer, edge.domain_term_id, edge.clone())?;
        insert_edge_include(data_buffer, edge.range_term_id, edge.clone())?;
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

    {
        let mut property_edge_map = data_buffer.property_edge_map.write()?;
        property_edge_map.insert(left_property, canonical_edge);
        property_edge_map.remove(&right_property);
    }
    Ok(SerializationStatus::Serialized)
}

pub fn ensure_object_property_registration(
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

    let property_iri = data_buffer.term_index.get(property_term_id)?;
    if is_reserved(&property_iri) {
        return Ok(());
    }

    add_term_to_element_buffer(
        &data_buffer.term_index,
        &data_buffer.edge_element_buffer,
        property_term_id,
        ElementType::Owl(OwlType::Edge(OwlEdge::ObjectProperty)),
    )?;

    check_unknown_buffer(data_buffer, property_term_id)?;
    Ok(())
}

pub fn insert_characteristic(
    data_buffer: &mut SerializationDataBuffer,
    triple: ArcTriple,
    characteristic: Characteristic,
) -> Result<SerializationStatus, SerializationError> {
    ensure_object_property_registration(data_buffer, triple.subject_term_id)?;

    let Some(resolved_property_term_id) = resolve(data_buffer, triple.subject_term_id)? else {
        let property_iri = data_buffer.term_index.get(triple.subject_term_id)?;
        if is_reserved(&property_iri) {
            debug!(
                "Skipping characteristic '{characteristic}' for reserved built-in '{property_iri}'"
            );
            return Ok(SerializationStatus::Serialized);
        }

        debug!(
            "Deferring characteristic '{}' for '{}': property unresolved",
            characteristic,
            data_buffer.term_index.get(triple.subject_term_id)?
        );
        add_to_unknown_buffer(data_buffer, triple.subject_term_id, triple)?;
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
        trace!(
            "Inserting edge characteristic: {} -> {}",
            data_buffer.term_index.get(resolved_property_term_id)?,
            characteristic
        );

        let target_edges = if edge.edge_type == ElementType::Owl(OwlType::Edge(OwlEdge::InverseOf))
        {
            data_buffer
                .edge_buffer
                .read()?
                .iter()
                .filter(|candidate| {
                    candidate.edge_type == ElementType::Owl(OwlType::Edge(OwlEdge::InverseOf))
                        && candidate.property_term_id.as_ref() == Some(&resolved_property_term_id)
                })
                .cloned()
                .collect()
        } else {
            vec![edge]
        };

        {
            let mut edge_characteristics = data_buffer.edge_characteristics.write()?;
            for target_edge in target_edges {
                edge_characteristics
                    .entry(target_edge)
                    .or_default()
                    .insert(characteristic);
            }
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
            data_buffer.term_index.get(resolved_property_term_id)?
        );
        add_to_unknown_buffer(data_buffer, resolved_property_term_id, triple)?;
        return Ok(SerializationStatus::Deferred);
    }

    let resolved_iri = data_buffer.term_index.get(resolved_property_term_id)?;
    if is_reserved(&resolved_iri) {
        debug!("Skipping characteristic '{characteristic}' for reserved built-in '{resolved_iri}'");
        return Ok(SerializationStatus::Serialized);
    }

    // No attach point yet
    debug!(
        "Deferring characteristic '{}' for '{}': no attach point available yet",
        characteristic,
        data_buffer.term_index.get(resolved_property_term_id)?
    );
    add_to_unknown_buffer(data_buffer, resolved_property_term_id, triple)?;
    Ok(SerializationStatus::Deferred)
}

pub fn is_inverse_fallback_term_id(
    data_buffer: &SerializationDataBuffer,
    term_id: usize,
) -> Result<bool, SerializationError> {
    let resolved_term_id = follow_redirection(data_buffer, term_id)?;
    let term = data_buffer.term_index.get(resolved_term_id)?;

    if is_query_fallback_endpoint(&term) {
        return Ok(true);
    }

    if !is_synthetic(&term) {
        return Ok(false);
    }

    let node_type = data_buffer
        .node_element_buffer
        .read()?
        .get(&resolved_term_id)
        .copied();

    Ok(matches!(
        node_type,
        Some(
            ElementType::Owl(OwlType::Node(OwlNode::Thing))
                | ElementType::Rdfs(RdfsType::Node(RdfsNode::Literal))
        )
    ))
}

pub fn is_preferred_inverse_endpoint(
    data_buffer: &SerializationDataBuffer,
    term_id: usize,
) -> Result<bool, SerializationError> {
    let resolved_term_id = follow_redirection(data_buffer, term_id)?;
    let term = data_buffer.term_index.get(resolved_term_id)?;

    if term.is_blank_node() || is_synthetic(&term) {
        return Ok(false);
    }

    let node_type = {
        data_buffer
            .node_element_buffer
            .read()?
            .get(&resolved_term_id)
            .copied()
    };

    Ok(!matches!(
        node_type,
        Some(ElementType::Owl(OwlType::Node(
            OwlNode::AnonymousClass
                | OwlNode::Complement
                | OwlNode::IntersectionOf
                | OwlNode::UnionOf
                | OwlNode::DisjointUnion
                | OwlNode::EquivalentClass
        )))
    ))
}

fn select_property_endpoint(
    data_buffer: &SerializationDataBuffer,
    candidates: &HashSet<usize>,
) -> Result<Option<usize>, SerializationError> {
    let mut concrete_fallback = None;
    let mut non_query_fallback = None;
    let mut any_fallback = None;

    for candidate in candidates {
        let resolved_candidate = follow_redirection(data_buffer, *candidate)?;

        if any_fallback.is_none() {
            any_fallback = Some(resolved_candidate);
        }

        if is_preferred_inverse_endpoint(data_buffer, resolved_candidate)? {
            return Ok(Some(resolved_candidate));
        }

        let term = data_buffer.term_index.get(resolved_candidate)?;
        if concrete_fallback.is_none() && !is_synthetic(&term) {
            concrete_fallback = Some(resolved_candidate);
        }
        if non_query_fallback.is_none() && !is_query_fallback_endpoint(&term) {
            non_query_fallback = Some(resolved_candidate);
        }
    }

    Ok(concrete_fallback.or(non_query_fallback).or(any_fallback))
}

//! Functions to add/remove/inspect buffers

use std::{
    collections::HashMap,
    mem::take,
    sync::{Arc, RwLock},
};

use grapher::prelude::{ElementType, OwlNode, OwlType};
use log::{debug, info, trace, warn};

use crate::{
    datastructures::{
        ArcEdge, ArcTriple, index::TermIndex, serialization_data_buffer::SerializationDataBuffer,
    },
    errors::SerializationError,
    serializer_util::{
        edges::{follow_redirection, restrictions::retry_restrictions},
        entity_creation::create_triple_from_id,
        is_external,
        labels::extract_label,
        nodes::insert_node,
        try_resolve_reserved,
        write_triple::serialize_triple,
    },
};

/// Returns the term if its element type is known.
pub fn resolve(
    data_buffer: &SerializationDataBuffer,
    term_id: usize,
) -> Result<Option<usize>, SerializationError> {
    let resolved = follow_redirection(data_buffer, term_id)?;

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
pub fn resolve_so(
    data_buffer: &SerializationDataBuffer,
    triple: &ArcTriple,
) -> Result<(Option<usize>, Option<usize>), SerializationError> {
    let resolved_subject = resolve(data_buffer, triple.subject_term_id)?;
    let resolved_object = match &triple.object_term_id {
        Some(target) => resolve(data_buffer, *target)?,
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
pub fn add_triple_to_element_buffer(
    term_index: &TermIndex,
    element_buffer: &mut Arc<RwLock<HashMap<usize, ElementType>>>,
    triple: &ArcTriple,
    element_type: ElementType,
) -> Result<(), SerializationError> {
    add_term_to_element_buffer(
        term_index,
        element_buffer,
        triple.subject_term_id,
        element_type,
    )
}

/// Add a term id to a node/edge element buffer.
pub fn add_term_to_element_buffer(
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
pub fn add_to_unknown_buffer(
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

pub fn check_unknown_buffer(
    data_buffer: &mut SerializationDataBuffer,
    term_id: &usize,
) -> Result<(), SerializationError> {
    let maybe_triples = { data_buffer.unknown_buffer.write()?.remove(term_id) };

    if let Some(triples) = maybe_triples {
        for triple in triples {
            serialize_triple(data_buffer, triple)?;
        }
    }
    Ok(())
}

/// Insert an edge into the element's edge set.
pub fn insert_edge_include(
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

/// Remove an edge from the element's edge set.
pub fn remove_edge_include(
    data_buffer: &mut SerializationDataBuffer,
    element_id: &usize,
    edge: &ArcEdge,
) -> Result<(), SerializationError> {
    if let Some(edges) = data_buffer.edges_include_map.write()?.get_mut(element_id) {
        edges.remove(edge);
    }
    Ok(())
}

/// Try to serialize the triples of all unknown terms until a fixpoint is reached (i.e. trying again doesn't change the outcome).
pub fn check_all_unknowns(
    data_buffer: &mut SerializationDataBuffer,
) -> Result<(), SerializationError> {
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

        retry_restrictions(data_buffer)?;
        let current = pending;

        for (term_id, triples) in current {
            let term = data_buffer.term_index.get(&term_id)?;

            if !data_buffer.label_buffer.read()?.contains_key(&term_id) {
                extract_label(data_buffer, None, &term, &term_id)?;
            }

            if is_external(data_buffer, &term)? {
                let external_triple =
                    create_triple_from_id(&data_buffer.term_index, term_id, None, None)?;

                insert_node(
                    data_buffer,
                    &external_triple,
                    ElementType::Owl(OwlType::Node(OwlNode::ExternalClass)),
                )?;
            } else if let Some(element_type) = try_resolve_reserved(&term) {
                let reserved_triple =
                    create_triple_from_id(&data_buffer.term_index, term_id, None, None)?;

                insert_node(data_buffer, &reserved_triple, element_type)?;
            } else if term.is_blank_node() {
                let anonymous_triple =
                    create_triple_from_id(&data_buffer.term_index, term_id, None, None)?;

                insert_node(
                    data_buffer,
                    &anonymous_triple,
                    ElementType::Owl(OwlType::Node(OwlNode::AnonymousClass)),
                )?;
            }

            for triple in triples {
                match serialize_triple(data_buffer, triple) {
                    Ok(()) => {}
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

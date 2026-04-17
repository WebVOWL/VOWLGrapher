//! Functions related to edges and properties.

pub mod characteristics;
pub mod restrictions;
use std::collections::HashSet;

use grapher::prelude::{ElementType, OwlEdge, OwlType, RdfEdge, RdfType, RdfsEdge, RdfsType};
use log::{debug, trace, warn};
use vowlgrapher_util::prelude::ErrorRecord;

use crate::{
    datastructures::{ArcEdge, ArcTriple, serialization_data_buffer::SerializationDataBuffer},
    errors::{SerializationError, SerializationErrorKind},
    serializer_util::{
        PROPERTY_EDGE_TYPES,
        buffers::{
            add_term_to_element_buffer, add_to_unknown_buffer, check_unknown_buffer,
            insert_edge_include, remove_edge_include, resolve_so,
        },
        entity_creation::create_edge_from_id,
        is_external, is_synthetic_property_fallback,
        nodes::remove_orphan_synthetic_node,
    },
};

pub fn redirect_iri(
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
    check_unknown_buffer(data_buffer, &old_term_id)?;
    Ok(())
}

pub fn follow_redirection(
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

/// Inserts an edge triple into the serialization buffer,
/// where subject and object are both nodes.
///
/// Note that tuples or any triple where the subject is an edge iri,
/// not present in the element buffer, will NEVER be resolved!
pub fn insert_edge(
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
    ) && is_external(data_buffer, &data_buffer.term_index.get(external_probe)?)?
    {
        ElementType::Owl(OwlType::Edge(OwlEdge::ExternalProperty))
    } else {
        edge_type
    };

    match resolve_so(data_buffer, &triple)? {
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
            let edge = create_edge_from_id(
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
            insert_edge_include(data_buffer, subject_term_id, edge.clone())?;
            insert_edge_include(data_buffer, object_term_id, edge.clone())?;

            data_buffer
                .edge_label_buffer
                .write()?
                .insert(edge.clone(), label.unwrap_or(new_type.to_string()));
            return Ok(Some(edge));
        }
        (None, Some(_)) => {
            debug!(
                "Cannot resolve subject of triple:\n {}",
                data_buffer.term_index.display_triple(&triple)?
            );
            add_to_unknown_buffer(data_buffer, triple.subject_term_id, triple)?;
        }
        (Some(_), None) => {
            if let Some(object_term_id) = &triple.object_term_id {
                // resolve_so already warns about unresolved object. No need to repeat it here.
                add_to_unknown_buffer(data_buffer, *object_term_id, triple)?;
            }
        }
        _ => {
            debug!(
                "Cannot resolve subject and object of triple:\n {}",
                data_buffer.term_index.display_triple(&triple)?
            );
            add_to_unknown_buffer(data_buffer, triple.subject_term_id, triple)?;
        }
    }
    Ok(None)
}

pub fn update_edges(
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
            let characteristics = { data_buffer.edge_characteristics.write()?.remove(&old_edge) };

            {
                data_buffer.edge_buffer.write()?.remove(&old_edge);
            }

            if old_edge.domain_term_id != old_term_id {
                remove_edge_include(data_buffer, &old_edge.domain_term_id, &old_edge)?;
            }
            if old_edge.range_term_id != old_term_id {
                remove_edge_include(data_buffer, &old_edge.range_term_id, &old_edge)?;
            }

            let is_degenerate_structural_edge = old_edge.domain_term_id == old_edge.range_term_id
                && matches!(
                    old_edge.edge_type,
                    ElementType::NoDraw | ElementType::Rdfs(RdfsType::Edge(RdfsEdge::SubclassOf))
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
            let new_edge = create_edge_from_id(
                &data_buffer.term_index,
                new_domain_term_id,
                old_edge.edge_type,
                new_range_term_id,
                old_edge.property_term_id,
            )?;

            {
                data_buffer.edge_buffer.write()?.insert(new_edge.clone());
            }
            insert_edge_include(data_buffer, new_term_id, new_edge.clone())?;
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

pub fn upgrade_property_type(
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
        let e = SerializationErrorKind::SerializationWarning(msg.to_string());
        warn!("{msg}");
        data_buffer
            .failed_buffer
            .write()?
            .push(<SerializationError as Into<ErrorRecord>>::into(e.into()));

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
        let e = SerializationErrorKind::SerializationWarning(msg.to_string());
        warn!("{msg}");
        data_buffer
            .failed_buffer
            .write()?
            .push(<SerializationError as Into<ErrorRecord>>::into(e.into()));

        return Ok(());
    }

    add_term_to_element_buffer(
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

    let new_edge = create_edge_from_id(
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
            .or_else(|| edge_label_buffer.remove(&old_edge))
            .unwrap_or_else(|| new_element.to_string());

        edge_label_buffer.insert(new_edge.clone(), label);
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

pub fn merge_properties(
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

    redirect_iri(data_buffer, *old_term_id, *new_term_id)?;
    Ok(())
}

pub fn remove_property_fallback_edge(
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

    if !is_synthetic_property_fallback(&data_buffer.term_index, &edge)? {
        return Ok(());
    }

    remove_edge_include(data_buffer, &edge.domain_term_id, &edge)?;
    remove_edge_include(data_buffer, &edge.range_term_id, &edge)?;

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
    remove_orphan_synthetic_node(data_buffer, &edge.domain_term_id)?;
    remove_orphan_synthetic_node(data_buffer, &edge.range_term_id)?;
    Ok(())
}

pub fn rewrite_property_edge(
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

    let new_edge = create_edge_from_id(
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

    remove_edge_include(data_buffer, &old_edge.domain_term_id, &old_edge)?;
    remove_edge_include(data_buffer, &old_edge.range_term_id, &old_edge)?;
    {
        let mut edge_buffer = data_buffer.edge_buffer.write()?;
        edge_buffer.remove(&old_edge);

        edge_buffer.insert(new_edge.clone());
    }
    insert_edge_include(data_buffer, new_edge.domain_term_id, new_edge.clone())?;
    insert_edge_include(data_buffer, new_edge.range_term_id, new_edge.clone())?;

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

    remove_orphan_synthetic_node(data_buffer, &old_edge.domain_term_id)?;
    remove_orphan_synthetic_node(data_buffer, &old_edge.range_term_id)?;

    Ok(Some(new_edge))
}

pub fn has_enumeration_member_edge(
    data_buffer: &SerializationDataBuffer,
    subject_term_id: usize,
    object_term_id: usize,
) -> Result<bool, SerializationError> {
    let canonical_subject_term_id = canonical_count_term_id(data_buffer, subject_term_id)?;
    let canonical_object_term_id = canonical_count_term_id(data_buffer, object_term_id)?;

    let candidate = create_edge_from_id(
        &data_buffer.term_index,
        canonical_subject_term_id,
        ElementType::NoDraw,
        canonical_object_term_id,
        None,
    )?;

    Ok(data_buffer.edge_buffer.read()?.contains(&candidate))
}

pub fn canonical_count_term_id(
    data_buffer: &SerializationDataBuffer,
    term_id: usize,
) -> Result<usize, SerializationError> {
    follow_redirection(data_buffer, term_id)
}

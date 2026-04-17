use grapher::prelude::{ElementType, OwlNode, OwlType, RdfsNode, RdfsType};
use log::{debug, warn};
use oxrdf::Term;
use vowlgrapher_util::prelude::ErrorRecord;

use crate::{
    datastructures::{ArcTriple, serialization_data_buffer::SerializationDataBuffer},
    errors::{SerializationError, SerializationErrorKind},
    serializer_util::{
        buffers::{add_term_to_element_buffer, add_triple_to_element_buffer, check_unknown_buffer},
        can_upgrade_node_type,
        edges::{
            canonical_count_term_id, redirect_iri, restrictions::merge_restriction_state,
            update_edges,
        },
        is_external, is_synthetic,
    },
};

pub fn insert_node(
    data_buffer: &mut SerializationDataBuffer,
    triple: &ArcTriple,
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

    let new_type = if is_external(
        data_buffer,
        &data_buffer.term_index.get(&triple.subject_term_id)?,
    )? {
        ElementType::Owl(OwlType::Node(OwlNode::ExternalClass))
    } else {
        node_type
    };

    add_triple_to_element_buffer(
        &data_buffer.term_index,
        &mut data_buffer.node_element_buffer,
        &triple,
        new_type,
    )?;
    check_unknown_buffer(data_buffer, &triple.subject_term_id)?;

    Ok(())
}

pub fn merge_nodes(
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
    merge_restriction_state(data_buffer, old_term_id, new_term_id)?;
    {
        data_buffer
            .node_element_buffer
            .write()?
            .remove(&old_term_id);
    }
    update_edges(data_buffer, old_term_id, new_term_id)?;
    merge_individual_counts(data_buffer, &old_term_id, new_term_id)?;
    redirect_iri(data_buffer, old_term_id, new_term_id)?;
    Ok(())
}

pub fn upgrade_node_type(
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
                add_term_to_element_buffer(
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
            let e = SerializationErrorKind::SerializationWarning(msg.to_string());
            warn!("{msg}");
            data_buffer
                .failed_buffer
                .write()?
                .push(<SerializationError as Into<ErrorRecord>>::into(e.into()));
        }
    }
    Ok(())
}

pub fn has_named_equivalent_aliases(
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

pub fn upgrade_deprecated_node_type(
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
            add_term_to_element_buffer(
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
            let e = SerializationErrorKind::SerializationWarning(msg.to_string());
            warn!("{msg}");
            data_buffer
                .failed_buffer
                .write()?
                .push(<SerializationError as Into<ErrorRecord>>::into(e.into()));
        }
        None => {
            let msg = format!(
                "Cannot upgrade unresolved subject '{}' to DeprecatedClass",
                data_buffer.term_index.get(term_id)?
            );
            let e = SerializationErrorKind::SerializationWarning(msg.to_string());
            warn!("{msg}");
            data_buffer
                .failed_buffer
                .write()?
                .push(<SerializationError as Into<ErrorRecord>>::into(e.into()));
        }
    }
    Ok(())
}

pub fn remove_orphan_synthetic_node(
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

pub fn individual_count_literal(
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

pub fn increment_individual_count(
    data_buffer: &mut SerializationDataBuffer,
    term_id: usize,
    delta: u32,
) -> Result<(), SerializationError> {
    let canonical_term_id = canonical_count_term_id(data_buffer, term_id)?;
    *data_buffer
        .individual_count_buffer
        .write()?
        .entry(canonical_term_id)
        .or_default() += delta;
    Ok(())
}

pub fn merge_individual_counts(
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

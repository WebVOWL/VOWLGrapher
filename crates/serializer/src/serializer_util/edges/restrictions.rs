use grapher::prelude::{
    ElementType, OwlEdge, OwlNode, OwlType, RdfEdge, RdfType, RdfsNode, RdfsType,
};
use log::{debug, trace};
use oxrdf::{Literal, Term};

use crate::{
    datastructures::{
        ArcEdge, ArcTriple, SerializationStatus,
        restriction_data::{RestrictionRenderMode, RestrictionState},
        serialization_data_buffer::SerializationDataBuffer,
    },
    errors::{SerializationError, SerializationErrorKind},
    serializer_util::{
        buffers::{insert_edge_include, remove_edge_include, resolve},
        edges::{
            follow_redirection, redirect_iri, remove_property_fallback_edge, rewrite_property_edge,
        },
        entity_creation::{
            create_edge_from_id, create_term, create_triple_from_id, create_triple_from_iri,
            get_or_create_domain_thing,
        },
        is_query_fallback_endpoint, is_restriction_owner_edge,
        labels::extract_label,
        nodes::insert_node,
        synthetic::{SYNTH_LITERAL, SYNTH_LITERAL_VALUE},
        synthetic_iri, try_resolve_reserved,
    },
    vocab::rdfs,
};

pub fn merge_restriction_state(
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

pub fn should_skip_structural_operand(
    data_buffer: &SerializationDataBuffer,
    subject_term_id: &usize,
    object_term_id: &usize,
    operator: &str,
) -> Result<bool, SerializationError> {
    if is_consumed_restriction(data_buffer, object_term_id)? {
        debug!(
            "Skipping {} operand '{}': restriction already materialized",
            operator,
            data_buffer.term_index.get(object_term_id)?
        );
        return Ok(true);
    }

    if let (Some(resolved_subject), Some(resolved_target)) = (
        resolve(data_buffer, *subject_term_id)?,
        resolve(data_buffer, *object_term_id)?,
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

pub fn cardinality_literal(
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

pub fn is_consumed_restriction(
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

pub fn is_ephemeral_restriction_node(
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

pub fn restriction_owner(
    data_buffer: &SerializationDataBuffer,
    restriction_term_id: &usize,
) -> Result<Option<usize>, SerializationError> {
    // After an owl:equivalentClass merge, restriction state can live on the
    // named class IRI itself. In that case, the class is the owner and must
    // not be inferred from incoming subclass edges.
    if !is_ephemeral_restriction_node(data_buffer, restriction_term_id)? {
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

pub fn default_restriction_target(
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
                    if let Some(resolved_range_term_id) = resolve(data_buffer, range_term_id)? {
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
                                    _ => data_buffer.term_index.insert(rdfs::RESOURCE.into())?,
                                };

                                let range_triple = create_triple_from_id(
                                    &data_buffer.term_index,
                                    range_term_id,
                                    Some(predicate),
                                    None,
                                )?;

                                insert_node(data_buffer, &range_triple, element_type)?;
                                return Ok(range_term_id);
                            }

                            data_buffer.term_index.insert(rdfs::DATATYPE.into())?
                        };

                        let range_triple = create_triple_from_id(
                            &data_buffer.term_index,
                            range_term_id,
                            Some(predicate_term_id),
                            None,
                        )?;

                        insert_node(
                            data_buffer,
                            &range_triple,
                            ElementType::Rdfs(RdfsType::Node(RdfsNode::Datatype)),
                        )?;
                    }

                    return Ok(range_term_id);
                }
            }

            let property_term = data_buffer.term_index.get(property_term_id)?;
            let literal_iri = synthetic_iri(&property_term, SYNTH_LITERAL);
            let literal_triple = create_triple_from_iri(
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
                insert_node(data_buffer, &literal_triple, element_type)?;
            }

            {
                data_buffer
                    .label_buffer
                    .write()?
                    .insert(literal_triple.subject_term_id, element_type.to_string());
            }

            Ok(literal_triple.subject_term_id)
        }
        _ => get_or_create_domain_thing(data_buffer, owner_term_id),
    }
}

pub fn materialize_one_of_target(
    data_buffer: &mut SerializationDataBuffer,
    owner_term_id: &usize,
    target_term_id: &usize,
) -> Result<usize, SerializationError> {
    let target_term = data_buffer.term_index.get(target_term_id)?;
    match target_term.as_ref() {
        Term::Literal(literal) => {
            materialize_literal_value_target(data_buffer, owner_term_id, literal)
        }
        Term::NamedNode(_) | Term::BlankNode(_) => {
            if let Some(resolved) = resolve(data_buffer, *target_term_id)? {
                return Ok(resolved);
            }

            if !data_buffer
                .label_buffer
                .read()?
                .contains_key(target_term_id)
            {
                extract_label(data_buffer, None, &target_term, target_term_id)?;
            }

            let node_exists = {
                data_buffer
                    .node_element_buffer
                    .read()?
                    .contains_key(target_term_id)
            };
            if !node_exists {
                let predicate_term_id = { data_buffer.term_index.insert(rdfs::RESOURCE.into())? };

                let resource_triple = create_triple_from_id(
                    &data_buffer.term_index,
                    *target_term_id,
                    Some(predicate_term_id),
                    None,
                )?;

                insert_node(
                    data_buffer,
                    &resource_triple,
                    ElementType::Rdfs(RdfsType::Node(RdfsNode::Resource)),
                )?;
            }

            Ok(*target_term_id)
        }
    }
}

pub fn materialize_literal_value_target(
    data_buffer: &mut SerializationDataBuffer,
    restriction_term_id: &usize,
    literal: &Literal,
) -> Result<usize, SerializationError> {
    let subject_term_id = {
        let literal_iri = synthetic_iri(
            &data_buffer.term_index.get(restriction_term_id)?,
            SYNTH_LITERAL_VALUE,
        );
        data_buffer.term_index.insert(create_term(&literal_iri)?)?
    };

    let node_exists = {
        data_buffer
            .node_element_buffer
            .read()?
            .contains_key(&subject_term_id)
    };

    if !node_exists {
        let predicate_term_id = { data_buffer.term_index.insert(rdfs::LITERAL.into())? };
        let literal_triple = create_triple_from_id(
            &data_buffer.term_index,
            subject_term_id,
            Some(predicate_term_id),
            None,
        )?;

        insert_node(
            data_buffer,
            &literal_triple,
            ElementType::Rdfs(RdfsType::Node(RdfsNode::Literal)),
        )?;
    }

    data_buffer
        .label_buffer
        .write()?
        .insert(subject_term_id, literal.value().to_string());

    Ok(subject_term_id)
}

#[expect(clippy::too_many_arguments)]
pub fn insert_restriction_edge(
    data_buffer: &mut SerializationDataBuffer,
    subject_term_id: usize,
    property_term_id: usize,
    object_term_id: usize,
    edge_type: ElementType,
    label: String,
    cardinality: Option<(String, Option<String>)>,
) -> Result<ArcEdge, SerializationError> {
    let edge = create_edge_from_id(
        &data_buffer.term_index,
        subject_term_id,
        edge_type,
        object_term_id,
        Some(property_term_id),
    )?;
    {
        data_buffer.edge_buffer.write()?.insert(edge.clone());
    }
    insert_edge_include(data_buffer, subject_term_id, edge.clone())?;
    insert_edge_include(data_buffer, object_term_id, edge.clone())?;

    {
        data_buffer
            .edge_label_buffer
            .write()?
            .insert(edge.clone(), label);
    }
    if let Some(cardinality) = cardinality {
        data_buffer
            .edge_cardinality_buffer
            .write()?
            .insert(edge.clone(), cardinality);
    }

    Ok(edge)
}

pub fn restriction_edge_type(
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
        | Some(edge_type @ ElementType::Rdf(RdfType::Edge(RdfEdge::RdfProperty))) => Ok(edge_type),
        Some(_) | None => Ok(ElementType::Owl(OwlType::Edge(OwlEdge::ObjectProperty))),
    }
}

pub fn is_numeric_cardinality(cardinality: &(String, Option<String>)) -> bool {
    let (min, max) = cardinality;

    let min_ok = min.is_empty() || min.chars().all(|c| c.is_ascii_digit());
    let max_ok = max
        .as_ref()
        .is_none_or(|value| value.is_empty() || value.chars().all(|c| c.is_ascii_digit()));

    min_ok && max_ok
}

pub fn try_materialize_restriction(
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
    let Some(property_term_id) = resolve(data_buffer, raw_property_term_id)? else {
        debug!(
            "Deferring restriction for term '{}': cannot resolve restriction property",
            data_buffer.term_index.get(restriction_term_id)?
        );
        return Ok(SerializationStatus::Deferred);
    };
    let Some(raw_subject_term_id) = restriction_owner(data_buffer, restriction_term_id)? else {
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

    let subject_term_id = match resolve(data_buffer, raw_subject_term_id)? {
        Some(term_id) => term_id,
        None => follow_redirection(data_buffer, raw_subject_term_id)?,
    };

    let restriction_edge_type =
        restriction_edge_type(data_buffer, &property_term_id, state.render_mode)?;

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
                    data_buffer
                        .label_buffer
                        .write()?
                        .insert(existing_edge.range_term_id, literal.value().to_string());
                    existing_edge.range_term_id
                }
                _ => match resolve(data_buffer, *filler_id)? {
                    Some(resolved) => resolved,
                    None => {
                        debug!(
                            "Deferring restriction for term '{}': cannot resolve filler",
                            data_buffer.term_index.get(restriction_term_id)?
                        );
                        return Ok(SerializationStatus::Deferred);
                    }
                },
            }
        } else {
            existing_edge.range_term_id
        };

        let edge = {
            let rewritten = rewrite_property_edge(
                data_buffer,
                &property_term_id,
                subject_term_id,
                object_term_id,
            )?;

            match rewritten {
                Some(edge) => edge,
                None => {
                    let triple = create_triple_from_id(
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
        {
            data_buffer
                .edge_label_buffer
                .write()?
                .insert(edge.clone(), restriction_label);
        }
        if let Some(cardinality) = &state.cardinality {
            data_buffer
                .edge_cardinality_buffer
                .write()?
                .insert(edge, cardinality.clone());
        }

        remove_restriction_stub(data_buffer, restriction_term_id)?;
        remove_restriction_node(data_buffer, restriction_term_id)?;

        if subject_term_id != *restriction_term_id {
            redirect_iri(data_buffer, *restriction_term_id, subject_term_id)?;
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
            Term::Literal(literal) => {
                materialize_literal_value_target(data_buffer, restriction_term_id, literal)?
            }
            _ => match resolve(data_buffer, filler_id)? {
                Some(resolved) => resolved,
                None => materialize_named_value_target(data_buffer, &property_term_id, &filler_id)?,
            },
        }
    } else {
        default_restriction_target(data_buffer, &subject_term_id, &property_term_id)?
    };

    let maybe_numeric_cardinality = state
        .cardinality
        .as_ref()
        .filter(|cardinality| is_numeric_cardinality(cardinality))
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
            remove_property_fallback_edge(data_buffer, &property_term_id)?;

            let edge = match rewrite_property_edge(
                data_buffer,
                &property_term_id,
                subject_term_id,
                object_term_id,
            )? {
                Some(edge) => edge,
                None => existing_edge,
            };

            {
                data_buffer
                    .edge_cardinality_buffer
                    .write()?
                    .insert(edge, cardinality);
            }

            remove_restriction_stub(data_buffer, restriction_term_id)?;
            remove_restriction_node(data_buffer, restriction_term_id)?;

            if subject_term_id != *restriction_term_id {
                redirect_iri(data_buffer, *restriction_term_id, subject_term_id)?;
            }

            trace!(
                "Successfully materialized numeric cardinality restriction '{}' on existing property edge",
                data_buffer.term_index.get(restriction_term_id)?
            );
            return Ok(SerializationStatus::Serialized);
        }
    }

    remove_property_fallback_edge(data_buffer, &property_term_id)?;

    let edge = insert_restriction_edge(
        data_buffer,
        subject_term_id,
        property_term_id,
        object_term_id,
        restriction_edge_type,
        restriction_label,
        state.cardinality.clone(),
    )?;

    {
        data_buffer
            .property_edge_map
            .write()?
            .insert(property_term_id, edge);
    }
    remove_restriction_stub(data_buffer, restriction_term_id)?;
    remove_restriction_node(data_buffer, restriction_term_id)?;

    if subject_term_id != *restriction_term_id {
        redirect_iri(data_buffer, *restriction_term_id, subject_term_id)?;
    }

    trace!(
        "Succesfully materialized restriction '{}'",
        data_buffer.term_index.get(restriction_term_id)?
    );
    Ok(SerializationStatus::Serialized)
}

pub fn remove_restriction_stub(
    data_buffer: &mut SerializationDataBuffer,
    restriction_term_id: &usize,
) -> Result<(), SerializationError> {
    if !is_ephemeral_restriction_node(data_buffer, restriction_term_id)? {
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
            }
        }
    }
    Ok(())
}

pub fn materialize_named_value_target(
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
                extract_label(data_buffer, None, &target_term, target_term_id)?;
            }

            let node_exists = {
                data_buffer
                    .node_element_buffer
                    .read()?
                    .contains_key(target_term_id)
            };
            if !node_exists {
                let predicate_term_id = { data_buffer.term_index.insert(rdfs::RESOURCE.into())? };
                let resource_triple = create_triple_from_id(
                    &data_buffer.term_index,
                    *target_term_id,
                    Some(predicate_term_id),
                    None,
                )?;

                insert_node(
                    data_buffer,
                    &resource_triple,
                    ElementType::Rdfs(RdfsType::Node(RdfsNode::Resource)),
                )?;
            }

            Ok(*target_term_id)
        }
        _ => {
            let triple = create_triple_from_id(
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

pub fn retry_restrictions(
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
        try_materialize_restriction(data_buffer, &restriction)?;
    }

    Ok(())
}

pub fn remove_restriction_node(
    data_buffer: &mut SerializationDataBuffer,
    restriction_term_id: &usize,
) -> Result<(), SerializationError> {
    // Named classes can temporarily carry restriction state after merging
    // an anonymous equivalentClass expression. Clear only the restriction state.
    if !is_ephemeral_restriction_node(data_buffer, restriction_term_id)? {
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

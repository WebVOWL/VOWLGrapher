use grapher::prelude::{
    Characteristic, ElementType, OwlEdge, OwlNode, OwlType, RdfEdge, RdfType, RdfsEdge, RdfsNode,
    RdfsType,
};
use log::{debug, info, trace, warn};
use oxrdf::Term;
use vowlgrapher_util::prelude::ErrorRecord;

use crate::{
    datastructures::{
        ArcTriple, SerializationStatus, restriction_data::RestrictionRenderMode,
        serialization_data_buffer::SerializationDataBuffer,
    },
    errors::{SerializationError, SerializationErrorKind},
    serializer_util::{
        buffers::{
            add_to_unknown_buffer, add_triple_to_element_buffer, check_unknown_buffer, resolve,
            resolve_so,
        },
        edges::{
            characteristics::{insert_characteristic, insert_inverse_of},
            has_enumeration_member_edge, insert_edge,
            restrictions::{
                cardinality_literal, materialize_one_of_target, should_skip_structural_operand,
                try_materialize_restriction,
            },
            upgrade_property_type,
        },
        entity_creation::{
            create_triple_from_id, create_triple_from_iri, get_or_create_anchor_thing,
            get_or_create_domain_thing,
        },
        is_ontology, is_query_fallback_endpoint, is_structural_set_node, is_synthetic,
        labels::extend_element_label,
        nodes::{
            has_named_equivalent_aliases, increment_individual_count, individual_count_literal,
            insert_node, merge_nodes, upgrade_deprecated_node_type, upgrade_node_type,
        },
        synthetic::{SYNTH_LITERAL, SYNTH_LOCAL_LITERAL, SYNTH_LOCAL_THING},
        synthetic_iri, trim_tag_circumfix,
    },
    vocab::{owl, rdf, rdfs, xsd},
};

/// Serialize a triple to the data buffer.
pub fn serialize_triple(
    data_buffer: &mut SerializationDataBuffer,
    triple: ArcTriple,
) -> Result<(), SerializationError> {
    match _serialize_triple(data_buffer, triple.clone()).or_else(|e| {
        data_buffer.failed_buffer.write()?.push(e.into());
        Ok::<SerializationStatus, SerializationError>(SerializationStatus::Serialized)
    })? {
        SerializationStatus::Serialized => {}
        SerializationStatus::Deferred => {}
        SerializationStatus::NotSupported => {
            let msg = format!("Serialization of {} is not supported", triple);
            data_buffer.failed_buffer.write()?.push(
                SerializationErrorKind::SerialiationNotSupported(
                    data_buffer.term_index.display_triple(&triple)?,
                    msg,
                )
                .into(),
            );
        }
    }
    Ok(())
}

/// Internal implementation detail of [`write_triple`].
fn _serialize_triple(
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
                insert_node(
                    data_buffer,
                    &triple,
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
                    insert_node(
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
                    match insert_edge(
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
                    insert_node(
                        data_buffer,
                        &triple,
                        ElementType::Rdfs(RdfsType::Node(RdfsNode::Datatype)),
                    )?;
                    return Ok(SerializationStatus::Serialized);
                }
                rdf::PLAIN_LITERAL => {
                    insert_node(
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
                    insert_node(
                        data_buffer,
                        &triple,
                        ElementType::Rdfs(RdfsType::Node(RdfsNode::Class)),
                    )?;
                    return Ok(SerializationStatus::Serialized);
                }

                rdfs::COMMENT => match triple.object_term_id {
                    Some(object_term_id) => {
                        data_buffer
                            .metadata
                            .comments
                            .write()?
                            .insert(triple.subject_term_id, object_term_id);
                        return Ok(SerializationStatus::Serialized);
                    }
                    None => {
                        return Err(SerializationErrorKind::MissingObject(
                            data_buffer.term_index.display_triple(&triple)?,
                            "Comment triple has no object".to_string(),
                        )
                        .into());
                    }
                },

                // rdfs::CONTAINER => {}
                // rdfs::CONTAINER_MEMBERSHIP_PROPERTY => {}
                rdfs::DATATYPE => {
                    insert_node(
                        data_buffer,
                        &triple,
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

                rdfs::IS_DEFINED_BY => match triple.object_term_id {
                    Some(object_term_id) => {
                        data_buffer
                            .metadata
                            .is_defined_by
                            .write()?
                            .insert(triple.subject_term_id, object_term_id);
                        return Ok(SerializationStatus::Serialized);
                    }
                    None => {
                        return Err(SerializationErrorKind::MissingObject(
                            data_buffer.term_index.display_triple(&triple)?,
                            "isDefinedBy triple has no object".to_string(),
                        )
                        .into());
                    }
                },

                // Is handled by [`extract_label`]
                rdfs::LABEL => {
                    return Err(SerializationErrorKind::SerializationFailedTriple(
                        data_buffer.term_index.display_triple(&triple)?,
                        "SPARQL query should not have rdfs:label triples".to_string(),
                    )
                    .into());
                }

                rdfs::LITERAL => {
                    insert_node(
                        data_buffer,
                        &triple,
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
                    insert_node(
                        data_buffer,
                        &triple,
                        ElementType::Rdfs(RdfsType::Node(RdfsNode::Resource)),
                    )?;
                    return Ok(SerializationStatus::Serialized);
                }
                rdfs::SEE_ALSO => match triple.object_term_id {
                    Some(object_term_id) => {
                        data_buffer
                            .metadata
                            .see_also
                            .write()?
                            .entry(triple.subject_term_id)
                            .or_default()
                            .insert(object_term_id);
                        return Ok(SerializationStatus::Serialized);
                    }
                    None => {
                        return Err(SerializationErrorKind::MissingObject(
                            data_buffer.term_index.display_triple(&triple)?,
                            "seeAlso triple has no object".to_string(),
                        )
                        .into());
                    }
                },
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

                    match insert_edge(
                        data_buffer,
                        triple.clone(),
                        ElementType::Rdfs(RdfsType::Edge(RdfsEdge::SubclassOf)),
                        None,
                    )? {
                        Some(_) => {
                            if let Some(restriction_term_id) = triple.object_term_id.as_ref() {
                                try_materialize_restriction(data_buffer, restriction_term_id)?;
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

                    return try_materialize_restriction(data_buffer, &triple.subject_term_id);
                }

                // owl::ANNOTATED_PROPERTY => {},
                // owl::ANNOTATED_SOURCE => {},
                // owl::ANNOTATED_TARGET => {},
                // owl::ANNOTATION => {},

                //TODO: OWL1
                // owl::ANNOTATION_PROPERTY => {},

                // owl::ASSERTION_PROPERTY => {},
                owl::ASYMMETRIC_PROPERTY => {
                    return insert_characteristic(
                        data_buffer,
                        triple,
                        Characteristic::AsymmetricProperty,
                    );
                }

                // owl::AXIOM => {},
                owl::BACKWARD_COMPATIBLE_WITH => match triple.object_term_id {
                    Some(object_term_id) => {
                        let current_term_id =
                            { *data_buffer.metadata.backward_compatible_with.read()? };
                        if let Some(term_id) = current_term_id {
                            let msg = format!(
                                "Attempting to override existing backwardCompatibleWith annotation '{}' with new annotation '{}'. Skipping",
                                data_buffer.term_index.get(&term_id)?,
                                data_buffer.term_index.get(&object_term_id)?
                            );
                            warn!("{msg}");
                            return Err(SerializationErrorKind::SerializationWarningTriple(
                                data_buffer.term_index.display_triple(&triple)?,
                                msg,
                            )
                            .into());
                        } else if is_ontology(&data_buffer.term_index.get(&triple.subject_term_id)?)
                        {
                            *data_buffer.metadata.backward_compatible_with.write()? =
                                Some(object_term_id);
                            return Ok(SerializationStatus::Serialized);
                        } else {
                            let msg = "The usage of backwardCompatibleWith annotation property on entities other than ontologies is discouraged, according to: https://www.w3.org/TR/owl-syntax/#Ontology_Annotations".to_string();
                            return Err(SerializationErrorKind::SerializationFailedTriple(
                                data_buffer.term_index.display_triple(&triple)?,
                                msg,
                            )
                            .into());
                        }
                    }
                    None => {
                        return Err(SerializationErrorKind::MissingObject(
                            data_buffer.term_index.display_triple(&triple)?,
                            "backwardCompatibleWith triple has no object".to_string(),
                        )
                        .into());
                    }
                },
                // owl::BOTTOM_DATA_PROPERTY => {},
                // owl::BOTTOM_OBJECT_PROPERTY => {},
                owl::CARDINALITY => {
                    let exact = cardinality_literal(data_buffer, &triple)?;
                    {
                        let mut restriction_buffer = data_buffer.restriction_buffer.write()?;
                        let mut state = restriction_buffer
                            .entry(triple.subject_term_id)
                            .or_default()
                            .write()?;
                        state.cardinality = Some((exact.clone(), None));
                    }

                    return try_materialize_restriction(data_buffer, &triple.subject_term_id);
                }
                owl::QUALIFIED_CARDINALITY => {
                    let exact = cardinality_literal(data_buffer, &triple)?;
                    {
                        let mut restriction_buffer = data_buffer.restriction_buffer.write()?;
                        let mut state = restriction_buffer
                            .entry(triple.subject_term_id)
                            .or_default()
                            .write()?;
                        state.cardinality = Some((exact.clone(), Some(exact)));
                        state.requires_filler = true;
                    }

                    return try_materialize_restriction(data_buffer, &triple.subject_term_id);
                }
                owl::CLASS => {
                    insert_node(
                        data_buffer,
                        &triple,
                        ElementType::Owl(OwlType::Node(OwlNode::Class)),
                    )?;
                    return Ok(SerializationStatus::Serialized);
                }
                owl::COMPLEMENT_OF => {
                    if let Some(target) = triple.object_term_id.as_ref()
                        && should_skip_structural_operand(
                            data_buffer,
                            &triple.subject_term_id,
                            target,
                            "owl:complementOf",
                        )?
                    {
                        return Ok(SerializationStatus::Serialized);
                    }

                    let edge = insert_edge(data_buffer, triple.clone(), ElementType::NoDraw, None)?;

                    if triple.object_term_id.is_some()
                        && let Some(index) = resolve(data_buffer, triple.subject_term_id)?
                        && !has_named_equivalent_aliases(data_buffer, &index)?
                    {
                        upgrade_node_type(
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
                    add_triple_to_element_buffer(
                        &data_buffer.term_index,
                        &mut data_buffer.edge_element_buffer,
                        &triple,
                        e,
                    )?;
                    check_unknown_buffer(data_buffer, &triple.subject_term_id)?;
                    return Ok(SerializationStatus::Serialized);
                }

                //TODO: OWL1 (deprecated in OWL2, replaced by rdfs:datatype)
                // owl::DATA_RANGE => {}
                owl::DEPRECATED => {
                    let Some(resolved_term_id) = resolve(data_buffer, triple.subject_term_id)?
                    else {
                        debug!(
                            "Deferring owl:Deprecated for '{}': subject type unresolved",
                            data_buffer.term_index.get(&triple.subject_term_id)?
                        );
                        add_to_unknown_buffer(data_buffer, triple.subject_term_id, triple)?;
                        return Ok(SerializationStatus::Deferred);
                    };

                    if data_buffer
                        .node_element_buffer
                        .read()?
                        .contains_key(&resolved_term_id)
                    {
                        upgrade_deprecated_node_type(data_buffer, &resolved_term_id)?;
                        return Ok(SerializationStatus::Serialized);
                    }

                    if data_buffer
                        .edge_element_buffer
                        .read()?
                        .contains_key(&resolved_term_id)
                    {
                        upgrade_property_type(
                            data_buffer,
                            &resolved_term_id,
                            ElementType::Owl(OwlType::Edge(OwlEdge::DeprecatedProperty)),
                        )?;
                        check_unknown_buffer(data_buffer, &resolved_term_id)?;
                        return Ok(SerializationStatus::Serialized);
                    }

                    debug!(
                        "Skipping owl:Deprecated for '{}': resolved subject has no node/edge entry",
                        data_buffer.term_index.get(&resolved_term_id)?
                    );
                    return Ok(SerializationStatus::Deferred);
                }

                owl::DEPRECATED_CLASS => {
                    insert_node(
                        data_buffer,
                        &triple,
                        ElementType::Owl(OwlType::Node(OwlNode::DeprecatedClass)),
                    )?;
                    return Ok(SerializationStatus::Serialized);
                }
                owl::DEPRECATED_PROPERTY => {
                    match insert_edge(
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
                        && should_skip_structural_operand(
                            data_buffer,
                            &triple.subject_term_id,
                            target,
                            "owl:disjointUnionOf",
                        )?
                    {
                        return Ok(SerializationStatus::Serialized);
                    }

                    match insert_edge(data_buffer, triple, ElementType::NoDraw, None)? {
                        Some(edge) => {
                            if !has_named_equivalent_aliases(data_buffer, &edge.domain_term_id)? {
                                upgrade_node_type(
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
                    match insert_edge(
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
                owl::EQUIVALENT_CLASS => match resolve_so(data_buffer, &triple)? {
                    (Some(resolved_subject_term_id), Some(resolved_object_term_id)) => {
                        merge_nodes(
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
                                    let msg =
                                        "subject not present in node_element_buffer".to_string();
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
                                && !has_named_equivalent_aliases(
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
                                    _ => ElementType::Owl(OwlType::Node(OwlNode::EquivalentClass)),
                                }
                            } else {
                                ElementType::Owl(OwlType::Node(OwlNode::EquivalentClass))
                            };

                            upgrade_node_type(
                                data_buffer,
                                resolved_subject_term_id,
                                upgraded_element,
                            )?;

                            let maybe_label = {
                                data_buffer
                                    .label_buffer
                                    .read()?
                                    .get(&resolved_object_term_id)
                                    .cloned()
                            };
                            if let Some(label) = maybe_label {
                                extend_element_label(
                                    data_buffer,
                                    &resolved_subject_term_id,
                                    label,
                                )?;
                            }
                        }
                    }
                    (Some(_), None) => match triple.object_term_id {
                        Some(target) => {
                            add_to_unknown_buffer(data_buffer, target, triple)?;
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
                        add_to_unknown_buffer(data_buffer, resolved_object_term_id, triple)?;
                        return Ok(SerializationStatus::Deferred);
                    }
                    (None, None) => {
                        add_to_unknown_buffer(data_buffer, triple.subject_term_id, triple)?;
                        return Ok(SerializationStatus::Deferred);
                    }
                },
                // owl::EQUIVALENT_PROPERTY => {}
                owl::FUNCTIONAL_PROPERTY => {
                    return insert_characteristic(
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
                            let mut restriction_buffer = data_buffer.restriction_buffer.write()?;
                            let mut state = restriction_buffer
                                .entry(triple.subject_term_id)
                                .or_default()
                                .write()?;
                            state.self_restriction = true;
                            state.cardinality = Some(("self".to_string(), None));
                        }
                    }

                    return try_materialize_restriction(data_buffer, &triple.subject_term_id);
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

                    return try_materialize_restriction(data_buffer, &triple.subject_term_id);
                }

                // owl::IMPORTS => {}
                owl::INCOMPATIBLE_WITH => match triple.object_term_id {
                    Some(object_term_id) => {
                        let current_term_id = { *data_buffer.metadata.incompatible_with.read()? };
                        if let Some(term_id) = current_term_id {
                            let msg = format!(
                                "Attempting to override existing incompatibleWith annotation '{}' with new annotation '{}'. Skipping",
                                data_buffer.term_index.get(&term_id)?,
                                data_buffer.term_index.get(&object_term_id)?
                            );
                            warn!("{msg}");
                            return Err(SerializationErrorKind::SerializationWarningTriple(
                                data_buffer.term_index.display_triple(&triple)?,
                                msg,
                            )
                            .into());
                        } else if is_ontology(&data_buffer.term_index.get(&triple.subject_term_id)?)
                        {
                            *data_buffer.metadata.incompatible_with.write()? = Some(object_term_id);
                            return Ok(SerializationStatus::Serialized);
                        } else {
                            let msg = "The usage of incompatibleWith annotation property on entities other than ontologies is discouraged, according to: https://www.w3.org/TR/owl-syntax/#Ontology_Annotations".to_string();
                            return Err(SerializationErrorKind::SerializationFailedTriple(
                                data_buffer.term_index.display_triple(&triple)?,
                                msg,
                            )
                            .into());
                        }
                    }
                    None => {
                        return Err(SerializationErrorKind::MissingObject(
                            data_buffer.term_index.display_triple(&triple)?,
                            "incompatibleWith triple has no object".to_string(),
                        )
                        .into());
                    }
                },

                owl::INTERSECTION_OF => {
                    if let Some(target) = triple.object_term_id.as_ref()
                        && should_skip_structural_operand(
                            data_buffer,
                            &triple.subject_term_id,
                            target,
                            "owl:intersectionOf",
                        )?
                    {
                        return Ok(SerializationStatus::Serialized);
                    }

                    match insert_edge(data_buffer, triple, ElementType::NoDraw, None)? {
                        Some(edge) => {
                            if !has_named_equivalent_aliases(data_buffer, &edge.domain_term_id)? {
                                upgrade_node_type(
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
                    return insert_characteristic(
                        data_buffer,
                        triple,
                        Characteristic::InverseFunctionalProperty,
                    );
                }

                owl::INVERSE_OF => {
                    return insert_inverse_of(data_buffer, triple);
                }

                owl::IRREFLEXIVE_PROPERTY => {
                    return insert_characteristic(
                        data_buffer,
                        triple,
                        Characteristic::IrreflexiveProperty,
                    );
                }

                owl::MAX_CARDINALITY => {
                    let max = cardinality_literal(data_buffer, &triple)?;
                    {
                        let mut restriction_buffer = data_buffer.restriction_buffer.write()?;
                        let mut state = restriction_buffer
                            .entry(triple.subject_term_id)
                            .or_default()
                            .write()?;
                        state.cardinality = Some((String::new(), Some(max)));
                    }

                    return try_materialize_restriction(data_buffer, &triple.subject_term_id);
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
                            Some(cardinality_literal(data_buffer, &triple)?),
                        ));
                        state.requires_filler = true;
                    }

                    return try_materialize_restriction(data_buffer, &triple.subject_term_id);
                }
                // owl::MEMBERS => {}
                owl::MIN_CARDINALITY => {
                    let min = cardinality_literal(data_buffer, &triple)?;
                    {
                        let mut restriction_buffer = data_buffer.restriction_buffer.write()?;
                        let mut state = restriction_buffer
                            .entry(triple.subject_term_id)
                            .or_default()
                            .write()?;
                        state.cardinality = Some((min, Some("*".to_string())));
                    }

                    return try_materialize_restriction(data_buffer, &triple.subject_term_id);
                }
                owl::MIN_QUALIFIED_CARDINALITY => {
                    {
                        let mut restriction_buffer = data_buffer.restriction_buffer.write()?;
                        let mut state = restriction_buffer
                            .entry(triple.subject_term_id)
                            .or_default()
                            .write()?;
                        state.cardinality = Some((
                            cardinality_literal(data_buffer, &triple)?,
                            Some("*".to_string()),
                        ));
                        state.requires_filler = true;
                    }

                    return try_materialize_restriction(data_buffer, &triple.subject_term_id);
                }
                owl::NAMED_INDIVIDUAL => {
                    let count = individual_count_literal(data_buffer, &triple)?;
                    increment_individual_count(data_buffer, triple.subject_term_id, count)?;
                    return Ok(SerializationStatus::Serialized);
                }
                // owl::NEGATIVE_PROPERTY_ASSERTION => {}

                //TODO: OWL1
                //owl::NOTHING => {}
                owl::OBJECT_PROPERTY => {
                    add_triple_to_element_buffer(
                        &data_buffer.term_index,
                        &mut data_buffer.edge_element_buffer,
                        &triple,
                        ElementType::Owl(OwlType::Edge(OwlEdge::ObjectProperty)),
                    )?;
                    check_unknown_buffer(data_buffer, &triple.subject_term_id)?;
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

                    let materialized_target = materialize_one_of_target(
                        data_buffer,
                        &triple.subject_term_id,
                        &raw_target,
                    )?;

                    let member_already_present = if should_count_member {
                        has_enumeration_member_edge(
                            data_buffer,
                            triple.subject_term_id,
                            materialized_target,
                        )?
                    } else {
                        false
                    };

                    let edge_triple = create_triple_from_id(
                        &data_buffer.term_index,
                        triple.subject_term_id,
                        triple.predicate_term_id,
                        Some(materialized_target),
                    )?;

                    match insert_edge(data_buffer, edge_triple, ElementType::NoDraw, None)? {
                        Some(_) => {
                            if should_count_member && !member_already_present {
                                increment_individual_count(data_buffer, triple.subject_term_id, 1)?;
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

                    return try_materialize_restriction(data_buffer, &triple.subject_term_id);
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

                    return try_materialize_restriction(data_buffer, &triple.subject_term_id);
                }

                owl::PRIOR_VERSION => match triple.object_term_id {
                    Some(object_term_id) => {
                        let current_term_id = { *data_buffer.metadata.prior_version.read()? };
                        if let Some(term_id) = current_term_id {
                            let msg = format!(
                                "Attempting to override existing priorVersion annotation '{}' with new annotation '{}'. Skipping",
                                data_buffer.term_index.get(&term_id)?,
                                data_buffer.term_index.get(&object_term_id)?
                            );
                            warn!("{msg}");
                            return Err(SerializationErrorKind::SerializationWarningTriple(
                                data_buffer.term_index.display_triple(&triple)?,
                                msg,
                            )
                            .into());
                        } else if is_ontology(&data_buffer.term_index.get(&triple.subject_term_id)?)
                        {
                            *data_buffer.metadata.prior_version.write()? = Some(object_term_id);
                            return Ok(SerializationStatus::Serialized);
                        } else {
                            let msg = "The usage of priorVersion annotation property on entities other than ontologies is discouraged, according to: https://www.w3.org/TR/owl-syntax/#Ontology_Annotations".to_string();
                            return Err(SerializationErrorKind::SerializationFailedTriple(
                                data_buffer.term_index.display_triple(&triple)?,
                                msg,
                            )
                            .into());
                        }
                    }
                    None => {
                        return Err(SerializationErrorKind::MissingObject(
                            data_buffer.term_index.display_triple(&triple)?,
                            "priorVersion triple has no object".to_string(),
                        )
                        .into());
                    }
                },

                // owl::PROPERTY_CHAIN_AXIOM => {}
                // owl::PROPERTY_DISJOINT_WITH => {}
                // owl::QUALIFIED_CARDINALITY => {}
                owl::REFLEXIVE_PROPERTY => {
                    return insert_characteristic(
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

                    return try_materialize_restriction(data_buffer, &triple.subject_term_id);
                }
                // owl::SOURCE_INDIVIDUAL => {}
                owl::SYMMETRIC_PROPERTY => {
                    return insert_characteristic(
                        data_buffer,
                        triple,
                        Characteristic::SymmetricProperty,
                    );
                }
                // owl::TARGET_INDIVIDUAL => {}
                // owl::TARGET_VALUE => {}
                owl::THING => {
                    insert_node(
                        data_buffer,
                        &triple,
                        ElementType::Owl(OwlType::Node(OwlNode::Thing)),
                    )?;
                    return Ok(SerializationStatus::Serialized);
                }
                // owl::TOP_DATA_PROPERTY => {}
                // owl::TOP_OBJECT_PROPERTY => {}
                owl::TRANSITIVE_PROPERTY => {
                    return insert_characteristic(
                        data_buffer,
                        triple,
                        Characteristic::TransitiveProperty,
                    );
                }
                owl::UNION_OF => {
                    if let Some(target) = triple.object_term_id.as_ref()
                        && should_skip_structural_operand(
                            data_buffer,
                            &triple.subject_term_id,
                            target,
                            "owl:unionOf",
                        )?
                    {
                        return Ok(SerializationStatus::Serialized);
                    }

                    match insert_edge(data_buffer, triple, ElementType::NoDraw, None)? {
                        Some(edge) => {
                            if !has_named_equivalent_aliases(data_buffer, &edge.domain_term_id)? {
                                upgrade_node_type(
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
                owl::VERSION_INFO => match triple.object_term_id {
                    Some(object_term_id) => {
                        data_buffer
                            .metadata
                            .version_info
                            .write()?
                            .insert(triple.subject_term_id, object_term_id);
                        return Ok(SerializationStatus::Serialized);
                    }
                    None => {
                        return Err(SerializationErrorKind::MissingObject(
                            data_buffer.term_index.display_triple(&triple)?,
                            "versionInfo triple has no object".to_string(),
                        )
                        .into());
                    }
                },
                owl::VERSION_IRI => match triple.object_term_id {
                    Some(object_term_id) => {
                        let current_term_id = { *data_buffer.metadata.version_iri.read()? };
                        if let Some(term_id) = current_term_id {
                            let msg = format!(
                                "Attempting to override existing versionIRI annotation '{}' with new annotation '{}'. Skipping",
                                data_buffer.term_index.get(&term_id)?,
                                data_buffer.term_index.get(&object_term_id)?
                            );
                            warn!("{msg}");
                            return Err(SerializationErrorKind::SerializationWarningTriple(
                                data_buffer.term_index.display_triple(&triple)?,
                                msg,
                            )
                            .into());
                        } else {
                            *data_buffer.metadata.version_iri.write()? = Some(object_term_id);
                            return Ok(SerializationStatus::Serialized);
                        }
                    }
                    None => {
                        return Err(SerializationErrorKind::MissingObject(
                            data_buffer.term_index.display_triple(&triple)?,
                            "versionIRI triple has no object".to_string(),
                        )
                        .into());
                    }
                },
                // owl::WITH_RESTRICTIONS => {}
                owl::REAL => {
                    insert_node(
                        data_buffer,
                        &triple,
                        ElementType::Rdfs(RdfsType::Node(RdfsNode::Datatype)),
                    )?;
                    return Ok(SerializationStatus::Serialized);
                }
                owl::RATIONAL => {
                    insert_node(
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
                    insert_node(
                        data_buffer,
                        &triple,
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
                                resolve(data_buffer, triple.subject_term_id)?,
                                resolve(data_buffer, predicate_term_id)?,
                                resolve(data_buffer, object_term_id)?,
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
                                        Some(create_triple_from_id(
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
                                        let thing_term_id = get_or_create_domain_thing(
                                            data_buffer,
                                            &domain_term_id,
                                        )?;

                                        (
                                            None,
                                            Some(create_triple_from_id(
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
                                        let node = create_triple_from_iri(
                                            &mut data_buffer.term_index,
                                            &target_iri,
                                            &rdfs::LITERAL.as_str().to_string(),
                                            None,
                                        )?;

                                        (
                                            Some(vec![node.clone()]),
                                            Some(create_triple_from_id(
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
                                            add_triple_to_element_buffer(
                                                &data_buffer.term_index,
                                                &mut data_buffer.edge_element_buffer,
                                                &triple,
                                                ElementType::Owl(OwlType::Edge(
                                                    OwlEdge::ObjectProperty,
                                                )),
                                            )?;
                                            check_unknown_buffer(
                                                data_buffer,
                                                &triple.subject_term_id,
                                            )?;
                                            return Ok(SerializationStatus::Serialized);
                                        } else if *predicate_term == owl::DATATYPE_PROPERTY.into() {
                                            add_triple_to_element_buffer(
                                                &data_buffer.term_index,
                                                &mut data_buffer.edge_element_buffer,
                                                &triple,
                                                ElementType::Owl(OwlType::Edge(
                                                    OwlEdge::DatatypeProperty,
                                                )),
                                            )?;
                                            check_unknown_buffer(
                                                data_buffer,
                                                &triple.subject_term_id,
                                            )?;
                                            return Ok(SerializationStatus::Serialized);
                                        }

                                        add_to_unknown_buffer(
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
                                        let thing_term_id = get_or_create_anchor_thing(
                                            data_buffer,
                                            &range_term_id,
                                        )?;

                                        (
                                            None,
                                            Some(create_triple_from_id(
                                                &data_buffer.term_index,
                                                thing_term_id,
                                                Some(property_term_id),
                                                Some(range_term_id),
                                            )?),
                                        )
                                    } else if *subject_term == rdfs::LITERAL.into() {
                                        let range_term =
                                            data_buffer.term_index.get(&range_term_id)?;
                                        let target_iri = synthetic_iri(&range_term, SYNTH_LITERAL);
                                        let node = create_triple_from_iri(
                                            &mut data_buffer.term_index,
                                            &target_iri,
                                            &rdfs::LITERAL.as_str().to_string(),
                                            None,
                                        )?;

                                        (
                                            Some(vec![node.clone()]),
                                            Some(create_triple_from_id(
                                                &data_buffer.term_index,
                                                node.subject_term_id,
                                                Some(property_term_id),
                                                triple.object_term_id,
                                            )?),
                                        )
                                    } else {
                                        add_to_unknown_buffer(data_buffer, object_term_id, triple)?;
                                        return Ok(SerializationStatus::Deferred);
                                    }
                                }
                                (None, Some(property_term_id), None) => {
                                    trace!(
                                        "Missing domain and range: {}",
                                        data_buffer.term_index.display_triple(&triple)?
                                    );

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
                                        add_to_unknown_buffer(
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
                                            let property_term =
                                                data_buffer.term_index.get(&property_term_id)?;

                                            let local_literal_iri =
                                                synthetic_iri(&property_term, SYNTH_LOCAL_LITERAL);
                                            let literal_triple = create_triple_from_iri(
                                                &mut data_buffer.term_index,
                                                &local_literal_iri,
                                                &rdfs::LITERAL.as_str().to_string(),
                                                None,
                                            )?;

                                            let local_thing_iri =
                                                synthetic_iri(&property_term, SYNTH_LOCAL_THING);
                                            let thing_triple = create_triple_from_iri(
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
                                                Some(create_triple_from_id(
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
                                                data_buffer.term_index.insert(owl::THING.into())?
                                            };
                                            let thing_term_id = get_or_create_anchor_thing(
                                                data_buffer,
                                                &thing_anchor_term_id,
                                            )?;

                                            (
                                                None,
                                                Some(create_triple_from_id(
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
                                    add_to_unknown_buffer(data_buffer, predicate_term_id, triple)?;
                                    return Ok(SerializationStatus::Deferred);
                                }
                                _ => {
                                    add_to_unknown_buffer(
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
                                            insert_node(
                                                data_buffer,
                                                &node_triple,
                                                ElementType::Owl(OwlType::Node(OwlNode::Thing)),
                                            )?;
                                        } else if *predicate_term == rdfs::LITERAL.into() {
                                            insert_node(
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
                                    let maybe_edge = insert_edge(
                                        data_buffer,
                                        edge_triple.clone(),
                                        property,
                                        label,
                                    )?;

                                    if let Some(edge) = maybe_edge {
                                        {
                                            data_buffer
                                                .property_edge_map
                                                .write()?
                                                .insert(edge_triple_predicate_term_id, edge);
                                        }
                                        {
                                            data_buffer
                                                .property_domain_map
                                                .write()?
                                                .entry(edge_triple_predicate_term_id)
                                                .or_default()
                                                .insert(edge_triple.subject_term_id);
                                        }
                                        {
                                            let object_term_id = {
                                                match edge_triple.object_term_id {
                                                    Some(id) => id,
                                                    None => {
                                                        let msg = "Failed to update range for edge"
                                                            .to_string();
                                                        let display_edge =
                                                            data_buffer
                                                                .term_index
                                                                .display_triple(&edge_triple)?;
                                                        return Err(
                                                            SerializationErrorKind::MissingObject(
                                                                display_edge,
                                                                msg,
                                                            ),
                                                        )?;
                                                    }
                                                }
                                            };
                                            data_buffer
                                                .property_range_map
                                                .write()?
                                                .entry(edge_triple_predicate_term_id)
                                                .or_default()
                                                .insert(object_term_id);
                                        }

                                        // Re-evaluate any characteristics waiting for this edge to exist
                                        check_unknown_buffer(
                                            data_buffer,
                                            &edge_triple_predicate_term_id,
                                        )?;
                                    }
                                }
                                None => {
                                    return Err(SerializationErrorKind::SerializationFailedTriple(
                                        data_buffer.term_index.display_triple(&triple)?,
                                        "Error creating edge".to_string(),
                                    )
                                    .into());
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

//! Functions related to creating new entities, e.g., triples, edges etc.

use grapher::prelude::{ElementType, OwlNode, OwlType};
use log::debug;
use oxrdf::{BlankNode, NamedNode, Term};

use crate::{
    datastructures::{
        ArcEdge, ArcTriple, edge_data::Edge, index::TermIndex,
        serialization_data_buffer::SerializationDataBuffer, triple::Triple,
    },
    errors::{SerializationError, SerializationErrorKind},
    serializer_util::{nodes::insert_node, synthetic::SYNTH_THING, synthetic_iri},
    vocab::owl,
};

/// Creates an edge from term IDs.
pub fn create_edge_from_id(
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

/// Creates a named node from an IRI.
pub fn create_named_node(iri: &String) -> Result<NamedNode, SerializationError> {
    Ok(NamedNode::new(iri)
        .map_err(|e| SerializationErrorKind::IriParseError(iri.clone(), Box::new(e)))?)
}

/// Creates a blank node from a blank node ID.
pub fn create_blank_node(id: &String) -> Result<BlankNode, SerializationError> {
    Ok(BlankNode::new(id)
        .map_err(|e| SerializationErrorKind::BlankNodeParseError(id.clone(), Box::new(e)))?)
}

/// Creates a term from a string, automatically handling named/blank nodes.
pub fn create_term(term: &String) -> Result<Term, SerializationError> {
    match create_named_node(term) {
        Ok(named_node) => Ok(Term::NamedNode(named_node)),
        Err(_) => Ok(Term::BlankNode(create_blank_node(term)?)),
    }
}

/// Creates a triple of subject-predicate-object terms, automatically handling named/blank nodes.
///
/// The new terms are automatically registered in the term index.
pub fn create_triple_from_iri(
    term_index: &mut TermIndex,
    subject_iri: &String,
    predicate_iri: &String,
    object_iri: Option<&String>,
) -> Result<ArcTriple, SerializationError> {
    let subject_term_id = {
        let subject_term = create_term(subject_iri)?;
        term_index.insert(subject_term)?
    };

    let predicate_term_id = term_index.insert(create_term(predicate_iri)?)?;

    let object_term_id = match object_iri {
        Some(iri) => Some(term_index.insert(create_term(iri)?)?),
        None => None,
    };

    create_triple_from_id(
        term_index,
        subject_term_id,
        Some(predicate_term_id),
        object_term_id,
    )
}

/// Creates a triple of subject-predicate-object term IDs.
pub fn create_triple_from_id(
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

pub fn get_or_create_domain_thing(
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
    let thing_triple = create_triple_from_iri(
        &mut data_buffer.term_index,
        &thing_iri,
        &owl::THING.as_str().to_string(),
        None,
    )?;
    let thing_element = ElementType::Owl(OwlType::Node(OwlNode::Thing));

    insert_node(data_buffer, &thing_triple, thing_element)?;

    {
        data_buffer
            .label_buffer
            .write()?
            .insert(thing_triple.subject_term_id, thing_element.to_string());
    }
    {
        data_buffer
            .anchor_thing_map
            .write()?
            .insert(*domain_term_id, thing_triple.subject_term_id);
    }
    Ok(thing_triple.subject_term_id)
}

pub fn get_or_create_anchor_thing(
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
    let thing_triple = create_triple_from_iri(
        &mut data_buffer.term_index,
        &thing_iri,
        &owl::THING.as_str().to_string(),
        None,
    )?;
    let thing_element = ElementType::Owl(OwlType::Node(OwlNode::Thing));

    insert_node(data_buffer, &thing_triple, thing_element)?;
    {
        data_buffer
            .label_buffer
            .write()?
            .insert(thing_triple.subject_term_id, thing_element.to_string());
    }
    {
        data_buffer
            .anchor_thing_map
            .write()?
            .insert(*anchor_term_id, thing_triple.subject_term_id);
    }
    Ok(thing_triple.subject_term_id)
}

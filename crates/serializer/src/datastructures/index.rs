use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use oxrdf::{Term, TermRef};

use crate::{
    datastructures::{ArcEdge, ArcTerm, ArcTriple},
    errors::{SerializationError, SerializationErrorKind},
};

#[derive(Debug, Default)]
pub struct TermIndex {
    /// Maps an RDF term to a corresponding id.
    str_index: RwLock<HashMap<ArcTerm, usize>>,
    /// Maps an id to a corresponding RDF term.
    int_index: RwLock<HashMap<usize, ArcTerm>>,
}

impl TermIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a term into the index and returns its corresponding id.
    ///
    /// If the index did not have this term present, a new id is returned.
    ///
    /// If the index did have this term present, the existing id is returned.
    ///
    /// # Errors
    /// Returns an error if the underlying lock is poisoned when accessed.
    pub fn insert(&self, term: Term) -> Result<usize, SerializationError> {
        let mut str_index = self.str_index.write()?;
        if let Some(id) = str_index.get(&term) {
            Ok(*id)
        } else {
            let arc_term = Arc::new(term);
            let id = str_index.len();

            str_index.insert(arc_term.clone(), id);
            drop(str_index);
            self.int_index.write()?.insert(id, arc_term);

            Ok(id)
        }
    }

    #[expect(unused)]
    /// Removes a term from the index, returning the term corresponding to the id if
    /// the id was previously in the index.
    ///
    /// # Errors
    /// Returns an error if the underlying lock is poisoned when accessed.
    pub fn remove(&self, term_id: usize) -> Result<Option<ArcTerm>, SerializationError> {
        let value = self.int_index.write()?.remove(&term_id);
        if let Some(term) = value {
            self.str_index.write()?.remove(&term);
            return Ok(Some(term));
        }
        Ok(None)
    }

    /// Returns a reference to the term corresponding to the id.
    ///
    /// # Errors
    /// Returns an error if no term corresponding to the id was found in the index.
    ///
    /// Returns an error if the underlying lock is poisoned when accessed.
    pub fn get(&self, id: usize) -> Result<ArcTerm, SerializationError> {
        let term = self
            .int_index
            .read()?
            .get(&id)
            .ok_or_else(|| {
                SerializationErrorKind::TermIndexError(format!(
                    "Failed to find term with id '{id}' in the term index"
                ))
            })?
            .clone();
        Ok(term)
    }

    /// Returns a reference to the id corresponding to the term.
    ///
    /// # Errors
    /// Returns an error if no id corresponding to the term was found in the index.
    ///
    /// Returns an error if the underlying lock is poisoned when accessed.
    pub fn get_id(&self, term: &ArcTerm) -> Result<usize, SerializationError> {
        let term_id = *self.str_index.read()?.get(term).ok_or_else(|| {
            SerializationErrorKind::TermIndexError(format!(
                "Failed to find id with with term '{term}' in the term index"
            ))
        })?;
        Ok(term_id)
    }

    /// Returns true if the term corresponding to the id exists and is a named node.
    ///
    /// # Errors
    /// Returns an error if the underlying lock is poisoned when accessed.
    pub fn is_named_node(&self, id: usize) -> Result<bool, SerializationError> {
        let result = self
            .int_index
            .read()?
            .get(&id)
            .is_some_and(|term| term.is_named_node());
        Ok(result)
    }

    /// Returns true if the term corresponding to the id exists and is a blank node.
    ///
    /// # Errors
    /// Returns an error if the underlying lock is poisoned when accessed.
    pub fn is_blank_node(&self, id: usize) -> Result<bool, SerializationError> {
        let result = self
            .int_index
            .read()?
            .get(&id)
            .is_some_and(|term| term.is_blank_node());
        Ok(result)
    }

    #[expect(unused)]
    /// Returns true if the term corresponding to the id exists and is a literal.
    ///
    /// # Errors
    /// Returns an error if the underlying lock is poisoned when accessed.
    pub fn is_literal(&self, id: usize) -> Result<bool, SerializationError> {
        let result = self
            .int_index
            .read()?
            .get(&id)
            .is_some_and(|term| term.is_literal());
        Ok(result)
    }

    /// Returns true if the term corresponding to the id exists and is a literal with the value "true".
    ///
    /// # Errors
    /// Returns an error if the underlying lock is poisoned when accessed.
    pub fn is_literal_truthy(&self, id: usize) -> Result<bool, SerializationError> {
        let result = self.int_index.read()?.get(&id).is_some_and(
            |term| matches!(term.as_ref().as_ref(), TermRef::Literal(lit) if lit.value() == "true"),
        );

        Ok(result)
    }

    /// Returns a pretty-printed version of a triple with term ids translated to
    /// their corresponding terms.
    ///
    /// # Errors
    /// Returns an error if the underlying lock is poisoned when accessed.
    pub fn display_triple(&self, triple: &ArcTriple) -> Result<String, SerializationError> {
        let subject = self.get(triple.subject_term_id)?.to_string();
        let predicate = if let Some(predicate_term_id) = triple.predicate_term_id {
            self.get(predicate_term_id)?.to_string()
        } else {
            String::new()
        };
        let object = if let Some(object_term_id) = triple.object_term_id {
            self.get(object_term_id)?.to_string()
        } else {
            String::new()
        };
        Ok(format!("Triple{{ {subject} - {predicate} - {object} }}"))
    }

    /// Returns a pretty-printed version of an edge with term ids translated to
    /// their corresponding terms.
    ///
    /// # Errors
    /// Returns an error if the underlying lock is poisoned when accessed.
    pub fn display_edge(&self, edge: &ArcEdge) -> Result<String, SerializationError> {
        let domain = self.get(edge.domain_term_id)?;
        let range = self.get(edge.range_term_id)?;
        // let property = if let Some(property_term_id) = edge.property_term_id {
        //     self.get(&property_term_id)?.to_string()
        // } else {
        //     String::new()
        // };
        Ok(format!(
            "Edge{{ {} - {} - {} }}",
            domain, edge.edge_type, range,
        ))
    }
}

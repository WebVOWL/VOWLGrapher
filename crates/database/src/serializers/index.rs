use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use oxrdf::Term;

use crate::errors::{SerializationError, SerializationErrorKind};

#[derive(Debug, Default)]
pub struct TermIndex {
    /// Maps an RDF term to a corresponding id.
    str_index: RwLock<HashMap<Arc<Term>, usize>>,
    /// Maps an id to a corresponding RDF term.
    int_index: RwLock<HashMap<usize, Arc<Term>>>,
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
    pub fn insert(&mut self, term: Term) -> Result<usize, SerializationError> {
        let mut str_index = self.str_index.write()?;
        match str_index.get(&term) {
            Some(id) => Ok(*id),
            None => {
                let arc_term = Arc::new(term);
                let id = str_index.len();

                str_index.insert(arc_term.clone(), id);
                self.int_index.write()?.insert(id, arc_term);

                Ok(id)
            }
        }
    }

    /// Removes a term from the index, returning the term corresponding to the id if
    /// the id was previously in the index.
    pub fn remove(&mut self, id: &usize) -> Result<Option<Arc<Term>>, SerializationError> {
        if let Some(term) = self.int_index.write()?.remove(id) {
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
    pub fn get(&self, id: &usize) -> Result<Arc<Term>, SerializationError> {
        let term = self
            .int_index
            .read()?
            .get(id)
            .ok_or_else(|| {
                SerializationErrorKind::TermIndexError(format!(
                    "Failed to find term with '{id}' in the term index"
                ))
            })?
            .clone();
        Ok(term)
    }

    /// Returns true if the term corresponding to the id exists and is a named node.
    ///
    /// # Errors
    /// Returns an error if the underlying lock is poisoned when accessed.
    pub fn is_named_node(&self, id: &usize) -> Result<bool, SerializationError> {
        let result = self
            .int_index
            .read()?
            .get(id)
            .is_some_and(|term| term.is_named_node());
        Ok(result)
    }

    /// Returns true if the term corresponding to the id exists and is a blank node.
    ///
    /// # Errors
    /// Returns an error if the underlying lock is poisoned when accessed.
    pub fn is_blank_node(&self, id: &usize) -> Result<bool, SerializationError> {
        let result = self
            .int_index
            .read()?
            .get(id)
            .is_some_and(|term| term.is_blank_node());
        Ok(result)
    }

    /// Returns true if the term corresponding to the id exists and is a literal.
    ///
    /// # Errors
    /// Returns an error if the underlying lock is poisoned when accessed.
    pub fn is_literal(&self, id: &usize) -> Result<bool, SerializationError> {
        let result = self
            .int_index
            .read()?
            .get(id)
            .is_some_and(|term| term.is_literal());
        Ok(result)
    }
}

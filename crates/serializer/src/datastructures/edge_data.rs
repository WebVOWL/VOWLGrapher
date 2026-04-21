#![expect(clippy::struct_field_names)]

use grapher::prelude::ElementType;
use std::{
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
};

use crate::serializer_util::{PROPERTY_EDGE_TYPES, SYMMETRIC_EDGE_TYPES};

#[derive(Debug, Clone, Eq)]
pub struct Edge {
    /// The domain of the edge.
    ///
    /// Also called the "source".
    pub domain_term_id: usize,
    /// The type of the edge, e.g., "Object Property".
    pub edge_type: ElementType,
    /// The range of the edge.
    ///
    /// Also called the "target".
    pub range_term_id: usize,
    /// The property.
    pub property_term_id: Option<usize>,
}

impl Edge {
    pub const fn new(
        domain_term_id: usize,
        edge_type: ElementType,
        range_term_id: usize,
        property_term_id: Option<usize>,
    ) -> Self {
        Self {
            domain_term_id,
            edge_type,
            range_term_id,
            property_term_id,
        }
    }
}

impl PartialEq for Edge {
    fn eq(&self, other: &Self) -> bool {
        // Element type and property must always match
        if self.edge_type != other.edge_type || self.property_term_id != other.property_term_id {
            return false;
        }

        // For symmetric relations, treat (A, B) and (B, A) as equal
        if SYMMETRIC_EDGE_TYPES.contains(&self.edge_type) {
            (self.domain_term_id == other.domain_term_id
                && self.range_term_id == other.range_term_id)
                || (self.domain_term_id == other.range_term_id
                    && self.range_term_id == other.domain_term_id)
        } else {
            self.domain_term_id == other.domain_term_id && self.range_term_id == other.range_term_id
        }
    }
}

impl Hash for Edge {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if SYMMETRIC_EDGE_TYPES.contains(&self.edge_type) {
            // For symmetric relations, hash the sorted pair
            let (first, second) =
                if self.domain_term_id.to_string() <= self.range_term_id.to_string() {
                    (&self.domain_term_id, &self.range_term_id)
                } else {
                    (&self.range_term_id, &self.domain_term_id)
                };

            first.hash(state);
            second.hash(state);
            self.edge_type.hash(state);
        } else if PROPERTY_EDGE_TYPES.contains(&self.edge_type) {
            self.domain_term_id.hash(state);
            self.edge_type.hash(state);
            self.range_term_id.hash(state);
            self.property_term_id.hash(state);
        } else {
            self.domain_term_id.hash(state);
            self.edge_type.hash(state);
            self.range_term_id.hash(state);
        }
    }
}

impl Display for Edge {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Edge{{ {} - {:?} - {} }}",
            self.domain_term_id, self.edge_type, self.range_term_id
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use grapher::prelude::{OwlEdge, OwlType};

    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_disjoint_with_edge_symmetry() {
        // Create two edges with swapped subject and object
        let x = 1;
        let y = 2;
        let edge1 = Edge {
            domain_term_id: x,
            edge_type: ElementType::Owl(OwlType::Edge(OwlEdge::DisjointWith)),
            range_term_id: y,
            property_term_id: None,
        };

        let edge2 = Edge {
            domain_term_id: y,
            edge_type: ElementType::Owl(OwlType::Edge(OwlEdge::DisjointWith)),
            range_term_id: x,
            property_term_id: None,
        };

        // Test that they are equal
        assert_eq!(
            edge1, edge2,
            "DisjointWith edges should be equal regardless of subject/object order"
        );

        // Test that they hash to the same value by inserting into a HashSet
        let mut edge_set = HashSet::new();
        edge_set.insert(edge1);
        edge_set.insert(edge2);

        assert_eq!(
            edge_set.len(),
            1,
            "HashSet should only contain one edge when both are DisjointWith with swapped subject/object"
        );
    }

    #[test]
    fn test_non_symmetric_edge_distinction() {
        // Create two edges with swapped subject and object for a non-symmetric relation
        let x = 1;
        let y = 2;
        let prop1 = 3;
        let edge1 = Edge {
            domain_term_id: x,
            edge_type: ElementType::Owl(OwlType::Edge(OwlEdge::ObjectProperty)),
            range_term_id: y,
            property_term_id: Some(prop1),
        };

        let edge2 = Edge {
            domain_term_id: y,
            edge_type: ElementType::Owl(OwlType::Edge(OwlEdge::ObjectProperty)),
            range_term_id: x,
            property_term_id: Some(prop1),
        };

        // Test that they are NOT equal
        assert_ne!(
            edge1, edge2,
            "Non-symmetric edges should NOT be equal when subject/object are swapped"
        );

        // Test that they both appear in the HashSet
        let mut edge_set = HashSet::new();
        edge_set.insert(edge1);
        edge_set.insert(edge2);

        assert_eq!(
            edge_set.len(),
            2,
            "HashSet should contain both edges when they are non-symmetric"
        );
    }
}

#![expect(clippy::struct_field_names)]

use std::fmt::{Display, Formatter};

#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub struct Triple {
    /// The subject.
    pub subject_term_id: usize,
    /// The predicate.
    pub predicate_term_id: Option<usize>,
    /// The object.
    pub object_term_id: Option<usize>,
}

impl Display for Triple {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Triple{{ ")?;
        write!(f, "{} - ", self.subject_term_id)?;
        write!(
            f,
            "{}",
            self.predicate_term_id
                .as_ref()
                .map(std::string::ToString::to_string)
                .unwrap_or_default(),
        )?;
        write!(
            f,
            "{}",
            self.object_term_id
                .as_ref()
                .map(std::string::ToString::to_string)
                .unwrap_or_default(),
        )?;
        write!(f, "}}")
    }
}

impl Triple {
    pub const fn new(
        subject_term_id: usize,
        predicate_term_id: Option<usize>,
        object_term_id: Option<usize>,
    ) -> Self {
        Self {
            subject_term_id,
            predicate_term_id,
            object_term_id,
        }
    }
}

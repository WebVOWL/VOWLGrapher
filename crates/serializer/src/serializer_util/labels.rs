//! Functions related to labels of terms.

use fluent_uri::Iri;
use log::{debug, error, trace};
use oxrdf::Term;
use unescape_zero_copy::unescape_default;

use crate::{
    datastructures::serialization_data_buffer::SerializationDataBuffer, errors::SerializationError,
    serializer_util::trim_tag_circumfix,
};

/// Extract label info from the query solution and store until
/// they can be mapped to their [`ElementType`].
pub fn insert_label(
    data_buffer: &SerializationDataBuffer,
    maybe_label_term: Option<&Term>,
    term: &Term,
    term_id: usize,
) -> Result<(), SerializationError> {
    // Prevent overriding labels
    if data_buffer.label_buffer.read()?.contains_key(&term_id) {
        return Ok(());
    }

    if let Some(label) = extract_label(maybe_label_term, term) {
        // TODO: Refactor label_buffer to handle language tags
        data_buffer
            .label_buffer
            .write()?
            .insert(term_id, Some(label));
    }

    Ok(())
}

/// Returns the label extracted from the term or None if no label was found.
///
/// Labels are extracted with the following priority:
///
/// 1. Use `maybe_label_term` if it's Some().
/// 2. Use the fragment component of the term, if it exist.
/// 3. Use the path component of the term, if it exist.
pub fn extract_label(maybe_label_term: Option<&Term>, term: &Term) -> Option<String> {
    if let Some(label_term) = maybe_label_term {
        // Handle language tags and cases where label contains "
        let stripped_label = trim_tag_circumfix(&label_term.to_string())
            .trim_start_matches("\"")
            .trim_end_matches("\"")
            .to_string();
        let (label, lang_tag) = {
            stripped_label
                .rsplit_once("@")
                .map(|(label, lang_tag)| {
                    (
                        label.trim_start_matches("\"").trim_end_matches("\""),
                        lang_tag,
                    )
                })
                .unwrap_or_else(|| (&stripped_label, ""))
        };
        let clean_label = unescape_default(label).map_or_else(
            |_| label.to_string(),
            |escaped_label| escaped_label.to_string(),
        );
        error!("Debug check label: '{clean_label}'");

        if clean_label.is_empty() {
            debug!("Empty label detected for term '{term}'");
        } else {
            trace!(
                "Inserting {}label '{clean_label}' for term '{term}'",
                if lang_tag.is_empty() {
                    "".to_string()
                } else {
                    format!("{lang_tag} ")
                }
            );

            return Some(clean_label);
        }
    } else {
        let iri = term.to_string();
        match Iri::parse(trim_tag_circumfix(&iri)) {
            // Case 2.1: Look for fragments in the iri
            Ok(parsed_iri) => {
                if let Some(frag) = parsed_iri.fragment() {
                    trace!("Inserting fragment '{frag}' as label for iri '{term}'");
                    return Some(frag.to_string());
                }
                debug!("No fragment found in iri '{iri}'");
                match parsed_iri.path().rsplit_once('/') {
                    Some(path) => {
                        trace!("Inserting path '{}' as label for iri '{}'", path.1, term);
                        return Some(path.1.to_string());
                    }
                    None => {
                        debug!("No path found in iri '{iri}'");
                    }
                }
            }
            Err(e) => {
                // Do not make a 'warn!'. A parse error is allowed to happen (e.g. on blank nodes).
                trace!("Failed to parse iri '{iri}':\n{e:?}");
            }
        }
    }
    None
}

pub fn merge_optional_labels(left: Option<&String>, right: Option<&String>) -> Option<String> {
    match (left, right) {
        (Some(left), Some(right)) if left == right => Some(left.clone()),
        (Some(left), Some(right)) => Some(format!("{left}\n{right}")),
        (Some(label), None) | (None, Some(label)) => Some(label.clone()),
        (None, None) => None,
    }
}

/// Appends a string to an element's label.
pub fn extend_element_label(
    data_buffer: &SerializationDataBuffer,
    element_id: usize,
    label_to_append: String,
) -> Result<(), SerializationError> {
    debug!(
        "Extending element '{}' with label '{}'",
        data_buffer.term_index.get(element_id)?,
        label_to_append
    );
    {
        let mut label_buffer = data_buffer.label_buffer.write()?;
        if let Some(Some(label)) = label_buffer.get_mut(&element_id) {
            label.push_str(format!("\n{label_to_append}").as_str());
        } else {
            label_buffer.insert(element_id, Some(label_to_append));
        }
    }
    Ok(())
}

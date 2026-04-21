//! Functions related to labels of terms.

use fluent_uri::Iri;
use log::{debug, trace};
use oxrdf::Term;
use unescape_zero_copy::unescape_default;

use crate::{
    datastructures::serialization_data_buffer::SerializationDataBuffer, errors::SerializationError,
    serializer_util::trim_tag_circumfix,
};

/// Extract label info from the query solution and store until
/// they can be mapped to their [`ElementType`].
pub fn extract_label(
    data_buffer: &SerializationDataBuffer,
    maybe_label: Option<&Term>,
    term: &Term,
    term_id: usize,
) -> Result<(), SerializationError> {
    // Prevent overriding labels
    if data_buffer.label_buffer.read()?.contains_key(&term_id) {
        return Ok(());
    }

    if let Some(label) = maybe_label {
        let str_label = label.to_string();

        // Handle cases where label is: "Some Label"@en or contains "
        let split_label = str_label.split_inclusive('"').collect::<Vec<_>>();
        let clean_label = if split_label.len() > 2 {
            let joined_label = split_label[0..split_label.len() - 1].join("");
            let stripped_label = joined_label
                .strip_prefix("\"")
                .and_then(|sub_str| sub_str.strip_suffix("\""))
                .unwrap_or_else(|| &joined_label);

            // Unescape string sequences like "\"" into """
            unescape_default(stripped_label)
                .unwrap_or_default()
                .to_string()
        } else {
            str_label
        };

        if clean_label.is_empty() {
            debug!("Empty label detected for term '{term}'");
        } else {
            trace!("Inserting label '{clean_label}' for term '{term}'");
            data_buffer
                .label_buffer
                .write()?
                .insert(term_id, Some(clean_label));
        }
    } else {
        let iri = term.to_string();
        match Iri::parse(trim_tag_circumfix(&iri)) {
            // Case 2.1: Look for fragments in the iri
            Ok(parsed_iri) => {
                if let Some(frag) = parsed_iri.fragment() {
                    trace!("Inserting fragment '{frag}' as label for iri '{term}'");
                    data_buffer
                        .label_buffer
                        .write()?
                        .insert(term_id, Some(frag.to_string()));
                } else {
                    debug!("No fragment found in iri '{iri}'");
                    match parsed_iri.path().rsplit_once('/') {
                        Some(path) => {
                            trace!("Inserting path '{}' as label for iri '{}'", path.1, term);
                            data_buffer
                                .label_buffer
                                .write()?
                                .insert(term_id, Some(path.1.to_string()));
                        }
                        None => {
                            debug!("No path found in iri '{iri}'");
                        }
                    }
                }
            }
            Err(e) => {
                // Do not make a 'warn!'. A parse error is allowed to happen (e.g. on blank nodes).
                trace!("Failed to parse iri '{iri}':\n{e:?}");
            }
        }
    }
    Ok(())
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

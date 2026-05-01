use crate::{
    blocks::right_sidebar::{LanguageSelection, default_metadata_value, metadata_value},
    components::{accordion::Accordion, user_input::internal_sparql::GraphDataContext},
    events::EventContext,
};
use leptos::{either::Either, prelude::*};

#[component]
pub fn SelectionDetails() -> impl IntoView {
    let GraphDataContext { graph_metadata, .. } = expect_context::<GraphDataContext>();
    let EventContext { show_metadata } = expect_context::<EventContext>();
    let selected_language = expect_context::<LanguageSelection>();

    let selection_data = create_read_slice(graph_metadata, |graph_metadata| {
        graph_metadata.metadata_type.clone()
    });

    let shown_selection_data = move || {
        if let Some(idx) = *show_metadata.read() {
            selection_data.read().get(&idx).cloned().map_or_else(
                Vec::new,
                |metadata_types| {
                    metadata_types
                        .into_iter()
                        .map(|(metadata_type_literal, metadata_type_map)| {
                            let value = metadata_type_map.clone();
                            let default_value = Memo::new(move |_| {default_metadata_value(value.clone())});
                            view! {
                                 <p>{move || {metadata_type_literal.as_ref().clone()}}": "{move || metadata_value(metadata_type_map.clone(), default_value, selected_language.0)}</p>
                            }
                        })
                        .collect_view()
                },
            )
        } else {
            vec![]
        }
    };

    view! {
        <Accordion title="Selection Details">
            <Show
                when=move || show_metadata.get().is_some()
                fallback=|| {
                    view! { <p>"Select an element in the visualization."</p> }
                }
            >
    {move ||
        let data = shown_selection_data();
        if data.is_empty() {
            Either::Left(
                view! {
                    <p>"No supported selection details to display."</p>
                }
            )
        } else {
            data
        }
    }
        </Show>
        </Accordion>
    }
}

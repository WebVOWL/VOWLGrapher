mod ontology_header;
mod selection_details;

use std::collections::HashMap;

use crate::{
    blocks::right_sidebar::{ontology_header::OntologyHeader, selection_details::SelectionDetails},
    components::buttons::graph_interaction_buttons::GraphInteractionButtons,
};
use leptos::prelude::*;

#[derive(Clone, Default)]
pub struct LanguageSelection(RwSignal<Option<String>>);

/// Returns the default value of a metadata type, which is the union of all languages.
pub fn default_metadata_value_signal(
    metadata_type: Signal<HashMap<String, Vec<String>>>,
) -> Vec<String> {
    metadata_type
        .get()
        .into_values()
        .reduce(|mut buffer, item| {
            buffer.extend(item);
            buffer
        })
        .unwrap_or_default()
}

/// Returns the default value of a metadata type, which is the union of all languages.
pub fn default_metadata_value(metadata_type: HashMap<String, Vec<String>>) -> Vec<String> {
    metadata_type
        .into_values()
        .reduce(|mut buffer, item| {
            buffer.extend(item);
            buffer
        })
        .unwrap_or_default()
}

/// Returns the value of the metadata type associated with the language tag,
/// or the default value if no value if found.
pub fn metadata_value_signal(
    metadata_type: Signal<HashMap<String, Vec<String>>>,
    default_value: Memo<Vec<String>>,
    selected_language: RwSignal<Option<String>>,
) -> Signal<Vec<String>> {
    Signal::derive(move || {
        selected_language.get().map_or_else(default_value, |tag| {
            { metadata_type.read().get(&tag).cloned() }.unwrap_or_else(default_value)
        })
    })
}

/// Returns the value of the metadata type associated with the language tag,
/// or the default value if no value if found.
pub fn metadata_value(
    metadata_type: HashMap<String, Vec<String>>,
    default_value: Memo<Vec<String>>,
    selected_language: RwSignal<Option<String>>,
) -> Signal<Vec<String>> {
    Signal::derive(move || {
        selected_language.get().map_or_else(default_value, |tag| {
            { metadata_type.get(&tag).cloned() }.unwrap_or_else(default_value)
        })
    })
}

#[component]
pub fn RightSidebar() -> impl IntoView {
    let selected_language_tag = LanguageSelection::default();
    provide_context(selected_language_tag);

    let is_open = RwSignal::new(false);
    view! {
        <div data-sidebar-open=move || is_open.get().to_string()>
            <button
                class="flex fixed justify-center items-center w-6 h-6 bg-white border border-black duration-500 cursor-pointer top-[5%] z-[3] transition-[right] hover:bg-[#dd9900]"
                class=("right-[22%]", move || is_open.get())
                class=("right-0", move || !is_open.get())
                on:click=move |_| {
                    is_open.update(|value| *value = !*value);
                }
            >
                {move || if is_open.get() { ">" } else { "<" }}
            </button>
            <div
                class="overflow-y-auto overflow-x-hidden fixed top-0 right-0 h-screen text-gray-500 bg-white duration-500 transition-[width]"
                class=("w-[22%]", move || is_open.get())
                class=("w-0", move || !is_open.get())
            >
                <OntologyHeader />
                <SelectionDetails />
            </div>
            <GraphInteractionButtons is_sidebar_open=is_open />
        </div>
    }
}

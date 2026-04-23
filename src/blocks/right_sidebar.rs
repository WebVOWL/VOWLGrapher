mod ontology_header;

use crate::{
    blocks::right_sidebar::ontology_header::OntologyHeader,
    components::{
        accordion::Accordion, buttons::graph_interaction_buttons::GraphInteractionButtons,
        user_input::internal_sparql::GraphDataContext,
    },
    events::EventContext,
};
use leptos::prelude::*;

#[component]
pub fn SelectionDetails() -> impl IntoView {
    let GraphDataContext { graph_metadata, .. } = expect_context::<GraphDataContext>();
    let EventContext { show_metadata } = expect_context::<EventContext>();

    let comments = create_read_slice(graph_metadata, |graph_metadata| {
        graph_metadata.comments.clone()
    });

    let comment = Memo::new(move |_| {
        if let Some(idx) = *show_metadata.read() {
            comments
                .read()
                .get(&idx)
                .cloned()
                .unwrap_or_else(|| "No".to_string())
        } else {
            "No ".to_string()
        }
    });

    view! {
        <Accordion title="Selection Details">
            <Show when=move || show_metadata.get().is_some() fallback=|| view! { <p>"Select an element in the visualization."</p>}>
            <p>"Comment: "{move || comment.get()}</p>
            </Show>
        </Accordion>
    }
}

#[component]
pub fn RightSidebar() -> impl IntoView {
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

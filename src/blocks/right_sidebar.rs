mod ontology_header;
mod selection_details;

use crate::{
    blocks::right_sidebar::{ontology_header::OntologyHeader, selection_details::SelectionDetails},
    components::buttons::graph_interaction_buttons::GraphInteractionButtons,
};
use leptos::prelude::*;

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

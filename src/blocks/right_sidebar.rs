mod ontology_header;

use crate::{
    blocks::right_sidebar::ontology_header::{Author, Description, Language, OntologyIri, Version},
    components::{
        accordion::Accordion, buttons::graph_interaction_buttons::GraphInteractionButtons,
        user_input::internal_sparql::GraphDataContext,
    },
};
use leptos::prelude::*;

#[component]
pub fn MetaData() -> impl IntoView {
    let metadata = RwSignal::new("The Friend of a Friend (FOAF) RDF vocabulary, described using W3C RDF Schema and the Web Ontology Language.The Friend of a Friend (FOAF) RDF vocabulary, described using W3C RDF Schema and the Web Ontology Language.The Friend of a Friend (FOAF) RDF vocabulary, described using W3C RDF Schema and the Web Ontology Language.The Friend of a Friend (FOAF) RDF vocabulary, described using W3C RDF Schema and the Web Ontology Language.The Friend of a Friend (FOAF) RDF vocabulary, described using W3C RDF Schema and the Web Ontology Language.The Friend of a Friend (FOAF) RDF vocabulary, described using W3C RDF Schema and the Web Ontology Language.The Friend of a Friend (FOAF) RDF vocabulary, described using W3C RDF Schema and the Web Ontology Language.The Friend of a Friend (FOAF) RDF vocabulary, described using W3C RDF Schema and the Web Ontology Language.The Friend of a Friend (FOAF) RDF vocabulary, described using W3C RDF Schema and the Web Ontology Language.".to_string());
    view! {
        <Accordion title="Metadata">
            <p>{move || metadata.get()}</p>
        </Accordion>
    }
}

#[component]
pub fn SelectionDetails() -> impl IntoView {
    let selection_details = RwSignal::new("Select an element in the visualization.".to_string());
    view! {
        <Accordion title="Selection Details">
            <p>{move || selection_details.get()}</p>
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

                <p class="py-4 font-thin text-center text-gray-500 text-[1.5em]">
                    "Friend of a Friend (FOAF) vocabulary"
                </p>
                <OntologyIri />
                <Version />
                <Author />
                <Language />
                <Description />
                <MetaData />
                <SelectionDetails />
            </div>
            <GraphInteractionButtons is_sidebar_open=is_open />
        </div>
    }
}

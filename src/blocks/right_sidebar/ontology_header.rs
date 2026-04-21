use crate::components::{accordion::Accordion, user_input::internal_sparql::GraphDataContext};
use leptos::prelude::*;

#[expect(unused, reason = "pending implementation")]
#[component]
pub fn OntologyHeader() -> impl IntoView {
    let GraphDataContext { graph_metadata, .. } = expect_context::<GraphDataContext>();
}

#[component]
pub fn OntologyIri() -> impl IntoView {
    let ontologyiri = RwSignal::new("http://xmlns.com/foaf/0.1/".to_string());
    view! {
        <p class="flex gap-2 justify-center items-center py-2 my-2 text-sm text-gray-500">
            <a
                href=move || ontologyiri.get()
                target="_blank"
                class="text-blue-600 hover:underline"
            >
                {move || ontologyiri.get()}
            </a>
        </p>
    }
}

#[component]
pub fn Version() -> impl IntoView {
    let ontologyversion = RwSignal::new("0.99".to_string());
    view! {
        <p class="flex gap-2 justify-center items-center py-2 my-2 text-sm text-gray-500">
            "Version: "{move || ontologyversion.get()}
        </p>
    }
}

#[component]
pub fn Author() -> impl IntoView {
    let ontologyauthors = RwSignal::new("Alice, Bob, Charlie".to_string());
    view! {
        <p class="flex gap-2 justify-center items-center py-2 my-2 text-sm text-gray-500">
            Author(s): {move || ontologyauthors.get()}
        </p>
    }
}

#[component]
pub fn Language() -> impl IntoView {
    let ontologylanguages = RwSignal::new(vec![
        "english".to_string(),
        "german".to_string(),
        "french".to_string(),
    ]);
    view! {
        <p class="flex gap-2 justify-center items-center py-2 my-2 text-sm text-gray-500">
            "Language(s):"
            <select class="py-1 px-2 text-sm text-gray-500 rounded-md border border-gray-300 focus:ring-2 focus:ring-blue-500 focus:outline-none w-[100px] h-[30px]">
                {move || {
                    ontologylanguages
                        .get()
                        .into_iter()
                        .map(|lang| view! { <option>{lang}</option> })
                        .collect_view()
                }}
            </select>
        </p>
    }
}

#[component]
pub fn Description() -> impl IntoView {
    let ontologydescription = RwSignal::new("The Friend of a Friend (FOAF) RDF vocabulary, described using W3C RDF Schema and the Web Ontology Language.".to_string());
    view! {
        <Accordion title="Description">
            <p>{move || ontologydescription.get()}</p>
        </Accordion>
    }
}

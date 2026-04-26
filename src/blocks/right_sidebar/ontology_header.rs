use crate::components::{accordion::Accordion, user_input::internal_sparql::GraphDataContext};
use leptos::prelude::*;

#[component]
pub fn Title(#[prop(into)] title: Signal<String>) -> impl IntoView {
    view! {
        <p class="py-4 font-thin text-center text-gray-500 text-[1.5em]">
            {move || title.get()}
        </p>
    }
}

#[component]
pub fn DocumentBase(#[prop(into)] base: Signal<String>) -> impl IntoView {
    view! {
        <p class="flex gap-2 justify-center items-center py-2 my-2 text-sm text-gray-500">
            <a
                href=move || base.get()
                target="_blank"
                class="text-blue-600 hover:underline"
            >
                {move || base.get()}
            </a>
        </p>
    }
}

#[component]
pub fn Version(
    #[prop(into)] version_iri: Signal<Option<String>>,
    #[prop(into)] prior_version: Signal<Option<String>>,
    #[prop(into)] incompatible_with: Signal<Option<String>>,
    #[prop(into)] backward_compatible_with: Signal<Option<String>>,
) -> impl IntoView {
    view! {
        <p class="flex gap-2 justify-center items-center py-2 my-2 text-sm text-gray-500">
            "Version: "{move || version_iri.get().unwrap_or("None".to_string())}
            <br /> "Prior Version: "
            {move || prior_version.get().unwrap_or("None".to_string())} <br />
            "Incompatible With: "
            {move || incompatible_with.get().unwrap_or("None".to_string())}
            <br /> "Backward Compatible With: "
            {move || {
                backward_compatible_with.get().unwrap_or("None".to_string())
            }}
        </p>
    }
}

#[component]
pub fn Author(
    #[prop(into)] creators: Signal<Vec<String>>,
    #[prop(into)] contributors: Signal<Vec<String>>,
) -> impl IntoView {
    view! {
        <p class="flex gap-2 justify-center items-center py-2 my-2 text-sm text-gray-500">
            Author(s): {move || creators.get()} <br />Contributor(s):
            {move || contributors.get()}
        </p>
    }
}

#[component]
pub fn Language(#[prop(into)] lang: Signal<Vec<String>>) -> impl IntoView {
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
pub fn Description(#[prop(into)] desc: Signal<Vec<String>>) -> impl IntoView {
    view! {
        <Accordion title="Description">
            <p>{move || desc.get()}</p>
        </Accordion>
    }
}

#[component]
pub fn OntologyHeader() -> impl IntoView {
    let GraphDataContext { graph_metadata, .. } = expect_context::<GraphDataContext>();

    let document_base = create_read_slice(graph_metadata, |graph_metadata| {
        graph_metadata.graph_header.document_base.clone()
    });
    let title = create_read_slice(graph_metadata, |graph_metadata| {
        graph_metadata.graph_header.title.clone()
    });
    let description = create_read_slice(graph_metadata, |graph_metadata| {
        graph_metadata.graph_header.description.clone()
    });
    let creators = create_read_slice(graph_metadata, |graph_metadata| {
        graph_metadata.graph_header.creator.clone()
    });
    let contributors = create_read_slice(graph_metadata, |graph_metadata| {
        graph_metadata.graph_header.contributor.clone()
    });
    let version_iri = create_read_slice(graph_metadata, |graph_metadata| {
        graph_metadata.graph_header.version_iri.clone()
    });
    let prior_version = create_read_slice(graph_metadata, |graph_metadata| {
        graph_metadata.graph_header.prior_version.clone()
    });
    let incompatible_with = create_read_slice(graph_metadata, |graph_metadata| {
        graph_metadata.graph_header.incompatible_with.clone()
    });
    let backward_compatible_with = create_read_slice(graph_metadata, |graph_metadata| {
        graph_metadata.graph_header.backward_compatible_with.clone()
    });

    view! {
        <div>
            <Title title=title />
            <DocumentBase base=document_base />
            <Version
                version_iri=version_iri
                prior_version=prior_version
                incompatible_with=incompatible_with
                backward_compatible_with=backward_compatible_with
            />
            <Author creators=creators contributors=contributors />
            <Language lang=Vec::new() />
            <Description desc=description />
        </div>
    }
}

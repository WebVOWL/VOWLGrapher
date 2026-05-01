use std::{collections::HashMap, iter::once};

use crate::{
    blocks::right_sidebar::{
        LanguageSelection, default_metadata_value_signal, metadata_value_signal,
    },
    components::{accordion::Accordion, user_input::internal_sparql::GraphDataContext},
};
use leptos::prelude::*;
use web_sys::{Event, HtmlInputElement};

#[component]
pub fn Title(
    #[prop(into)] selected_language: LanguageSelection,
    #[prop(into)] title: Signal<HashMap<String, Vec<String>>>,
) -> impl IntoView {
    let default_title = Memo::new(move |_| default_metadata_value_signal(title));
    let shown_title = metadata_value_signal(title, default_title, selected_language.0);
    view! {
        <p class="py-4 font-thin text-center text-gray-500 text-[1.5em]">
            {move || shown_title.get()}
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
    #[prop(into)] selected_language: LanguageSelection,
    #[prop(into)] creators: Signal<HashMap<String, Vec<String>>>,
    #[prop(into)] contributors: Signal<HashMap<String, Vec<String>>>,
) -> impl IntoView {
    let default_creator = Memo::new(move |_| default_metadata_value_signal(creators));
    let shown_creator = metadata_value_signal(creators, default_creator, selected_language.0);
    let default_contributor = Memo::new(move |_| default_metadata_value_signal(contributors));
    let shown_contributor =
        metadata_value_signal(contributors, default_contributor, selected_language.0);
    view! {
        <p class="flex gap-2 justify-center items-center py-2 my-2 text-sm text-gray-500">
            Author(s): {move || { shown_creator.get() }} <br />Contributor(s):
            {move || shown_contributor.get()}
        </p>
    }
}

#[component]
pub fn Language(
    #[prop(into)] selected_language: LanguageSelection,
    #[prop(into)] language_tags: Signal<Vec<Option<String>>>,
) -> impl IntoView {
    let update_selected_language = move |ev: Event| {
        let target: HtmlInputElement = event_target::<HtmlInputElement>(&ev);
        let name = target.value();
        if name.is_empty() {
            return;
        }

        let tag = match name.as_str() {
            "None" => None,
            _ => Some(name),
        };
        selected_language.0.update(|lan_tag| *lan_tag = tag);
    };

    let shown_languages = move || {
        once(selected_language.0.get().map_or_else(
            || {
                view! {
                    <option value="None"
                        .to_string()>{"None".to_string()}</option>
                }
                .into_any()
            },
            |_| ().into_any(),
        ))
        .chain(language_tags.get().into_iter().map(|tag| {
            view! { <option value=tag.unwrap_or_else(|| "None".to_string())>{tag.clone().unwrap_or_else(|| "None".to_string())}</option> }.into_any()
        }))
        .collect_view()
    };

    view! {
        <p class="flex gap-2 justify-center items-center py-2 my-2 text-sm text-gray-500">
            "Languages:"
            <select class="py-1 px-2 text-sm text-gray-500 rounded-md border border-gray-300 focus:ring-2 focus:ring-blue-500 focus:outline-none w-[100px] h-[30px]"
            prop:value=selected_language.0.get().map_or_else(|| "None".to_string(), |tag| tag)
            on:change=update_selected_language
            >
                {shown_languages()}
            </select>
        </p>
    }
}

#[component]
pub fn Description(
    #[prop(into)] selected_language: LanguageSelection,
    #[prop(into)] desc: Signal<HashMap<String, Vec<String>>>,
) -> impl IntoView {
    let default_desc = Memo::new(move |_| default_metadata_value_signal(desc));
    let shown_desc = metadata_value_signal(desc, default_desc, selected_language.0);

    view! {
        <Accordion title="Description">
            <p>{move || shown_desc.get()}</p>
        </Accordion>
    }
}

#[component]
pub fn OntologyHeader() -> impl IntoView {
    let GraphDataContext { graph_metadata, .. } = expect_context::<GraphDataContext>();
    let selected_language_tag = expect_context::<LanguageSelection>();

    let document_base = create_read_slice(graph_metadata, |graph_metadata| {
        graph_metadata.graph_header.document_base.clone()
    });
    let language_tags = create_read_slice(graph_metadata, |graph_metadata| {
        graph_metadata.languages.clone()
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
            <Title selected_language=selected_language_tag.clone() title=title />
            <DocumentBase base=document_base />
            <Version
                version_iri=version_iri
                prior_version=prior_version
                incompatible_with=incompatible_with
                backward_compatible_with=backward_compatible_with
            />
            <Author selected_language=selected_language_tag.clone() creators=creators contributors=contributors />
            <Language selected_language=selected_language_tag.clone() language_tags=language_tags />
            <Description selected_language=selected_language_tag desc=description />
        </div>
    }
}

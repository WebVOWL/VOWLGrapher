use super::WorkbenchMenuItems;
use crate::components::progress_bar::LoadingCircle;
use crate::components::user_input::internal_sparql::GraphDataContext;
use crate::components::user_input::internal_sparql::load_graph;
use crate::components::user_input::stored_ontology::StoredOntology;
use crate::components::user_input::stored_ontology::list_uploaded_ontologies;
use crate::components::user_input::stored_ontology::load_stored_ontology;
use crate::components::{icon::Icon, user_input::file_upload::FileUpload};
use crate::errors::ClientErrorKind;
use crate::errors::ErrorLogContext;
use leptos::prelude::*;
use leptos::task::spawn_local_scoped_with_cancellation;
use leptos_use::use_textarea_autosize;
use log::info;
use std::iter::once;
use strum::IntoEnumIterator;
use vowlgrapher_sparql_queries::prelude::{
    DEFAULT_QUERY, DefaultPrefixTable, QueryAssembler, QueryNormalizer,
};
use vowlgrapher_util::prelude::VOWLGrapherEnviron;
use web_sys::Event;
use web_sys::HtmlInputElement;

#[derive(Copy, Clone)]
struct ActiveMenuTask(RwSignal<&'static str>);

#[derive(Copy, Clone)]
struct UploadedListRefresh(RwSignal<u32>);

#[component]
pub fn SelectStaticInput() -> impl IntoView {
    let error_context = expect_context::<ErrorLogContext>();
    let GraphDataContext {
        active_graph_name, ..
    } = expect_context::<GraphDataContext>();

    let selected_ontology: RwSignal<Option<StoredOntology>> =
        RwSignal::new(Some(StoredOntology::FriendOfAFriend));
    let stored_stage: RwSignal<Option<&'static str>> = RwSignal::new(None);

    let active_task = expect_context::<ActiveMenuTask>().0;

    Effect::new(move || {
        let task = active_task.get();
        if !task.is_empty() && task != "stored" {
            if stored_stage.get_untracked() == Some("Done") {
                stored_stage.set(None);
            }
            if selected_ontology.get_untracked().is_some() {
                selected_ontology.set(None);
            }
        }
    });

    let stored_res = LocalResource::new(move || async move {
        if let Some(stored) = selected_ontology.get() {
            active_task.set("stored");
            stored_stage.set(Some("Loading"));
            active_graph_name.set(stored.path().to_string());
            match load_stored_ontology(stored).await {
                Ok(warning) => {
                    if let Some(e) = warning {
                        error_context.extend(e.records);
                    }
                    stored_stage.set(Some("Serializing"));
                    load_graph(DEFAULT_QUERY.to_string(), true).await;
                    stored_stage.set(Some("Done"));
                }
                Err(e) => {
                    error_context.extend(e.records);
                    stored_stage.set(None);
                }
            }
        }
    });

    let update_selected_ontology = move |ev: Event| {
        let target: HtmlInputElement = event_target::<HtmlInputElement>(&ev);
        let name = target.value();
        if name.is_empty() {
            return;
        }
        error_context.clear();
        match name.try_into() {
            Ok(ontology) => {
                selected_ontology.set(Some(ontology));
            }
            Err(e) => {
                error_context.push(e.into());
            }
        }
    };

    let ontologies = move || {
        once(selected_ontology.read().map_or_else(
            || {
                view! {
                    <option value="Select an ontology"
                        .to_string()>{"Select an ontology".to_string()}</option>
                }
                .into_any()
            },
            |_| ().into_any(),
        ))
        .chain(StoredOntology::iter().map(|ontology| {
            view! { <option value=ontology.to_string()>{ontology.to_string()}</option> }.into_any()
        }))
        .collect_view()
    };

    view! {
        <div class="mb-2">
            <label class="block mb-1">"Premade Ontology:"</label>
            <select
                class="p-1 w-full text-sm bg-gray-200 rounded border-b-0"
                prop:value=move || {
                    selected_ontology
                        .read()
                        .map_or_else(
                            || "Select an ontology".to_string(),
                            |ontology| ontology.to_string(),
                        )
                }
                on:change=update_selected_ontology
            >
                {ontologies()}
            </select>
            <Suspense fallback=move || {
                view! { <LoadingCircle /> }
            }>
                {move || Suspend::new(async move {
                    stored_res.await;
                })}
            </Suspense>
            {move || {
                match stored_stage.get() {
                    Some("Done") => {
                        view! {
                            <p class="mt-1 text-sm font-bold text-center">
                                "Loading done"
                            </p>
                        }
                            .into_any()
                    }
                    Some(stage) => {
                        view! {
                            <p class="mt-1 text-sm text-center">
                                <span class="inline-flex relative items-center">
                                    <span>{stage}</span>
                                    <span class="absolute left-full text-left loading-dots-anim">
                                        "......"
                                    </span>
                                </span>
                            </p>
                        }
                            .into_any()
                    }
                    None => ().into_any(),
                }
            }}
        </div>
    }
}

#[component]
pub fn UploadInput() -> impl IntoView {
    let error_context = expect_context::<ErrorLogContext>();
    let GraphDataContext {
        active_graph_name, ..
    } = expect_context::<GraphDataContext>();
    let upload = FileUpload::new();
    let local_loading_done = upload.local_action.value();
    let remote_loading_done = upload.remote_action.value();
    let local_pending = upload.local_action.pending();
    let remote_pending = upload.remote_action.pending();
    let tracker_url = upload.tracker.clone();
    let tracker_file = upload.tracker.clone();
    let file_name = upload.tracker.filename;
    let url_name = upload.tracker.url_name;

    let file_stage: RwSignal<Option<&'static str>> = RwSignal::new(None);
    let url_stage: RwSignal<Option<&'static str>> = RwSignal::new(None);

    let url_input_val = RwSignal::new(String::new());
    let file_input_ref = NodeRef::<leptos::html::Input>::new();

    let active_task = expect_context::<ActiveMenuTask>().0;
    let uploaded_refresh = expect_context::<UploadedListRefresh>().0;

    Effect::new(move || {
        if active_task.get() != "file" {
            if file_stage.get_untracked() == Some("Done") {
                file_stage.set(None);
            }
            if !file_name.get_untracked().is_empty() {
                file_name.set(String::new());
            }
            if let Some(input) = file_input_ref.get() {
                input.set_value("");
            }
        }
    });

    Effect::new(move || {
        if active_task.get() != "url" {
            if url_stage.get_untracked() == Some("Done") {
                url_stage.set(None);
            }
            if !url_input_val.get_untracked().is_empty() {
                url_input_val.set(String::new());
            }
        }
    });

    Effect::new(move || {
        if local_pending.get() {
            active_task.set("file");
            file_stage.set(Some("Uploading"));
        }
    });

    Effect::new(move || {
        if let Some(value) = local_loading_done.get() {
            file_stage.set(Some("Serializing"));
            active_graph_name.set(file_name.get_untracked());

            match value {
                Ok((_, _, warning)) => {
                    if let Some(e) = warning {
                        error_context.extend(e.records);
                    }
                    let stage = file_stage;
                    spawn_local_scoped_with_cancellation(async move {
                        load_graph(DEFAULT_QUERY.to_string(), true).await;
                        stage.set(Some("Done"));
                        uploaded_refresh.update(|n| *n += 1);
                    });
                }
                Err(e) => {
                    error_context.extend(e.records);
                    file_stage.set(None);
                }
            }
        }
    });

    Effect::new(move || {
        if remote_pending.get() {
            active_task.set("url");
            url_stage.set(Some("Uploading"));
        }
    });

    Effect::new(move || {
        if let Some(value) = remote_loading_done.get() {
            url_stage.set(Some("Serializing"));
            active_graph_name.set(url_name.get_untracked());

            match value {
                Ok((_, _, warning)) => {
                    if let Some(e) = warning {
                        error_context.extend(e.records);
                    }
                    let stage = url_stage;
                    spawn_local_scoped_with_cancellation(async move {
                        load_graph(DEFAULT_QUERY.to_string(), true).await;
                        stage.set(Some("Done"));
                        uploaded_refresh.update(|n| *n += 1);
                    });
                }
                Err(e) => {
                    error_context.extend(e.records);
                    url_stage.set(None);
                }
            }
        }
    });

    let upload_files = move |ev: Event| {
        let VOWLGrapherEnviron {
            max_input_size_bytes,
            ..
        } = expect_context::<VOWLGrapherEnviron>();

        let input: HtmlInputElement = event_target(&ev);
        if let Some(files) = input.files() {
            #[expect(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "decimals don't matter in comparison"
            )]
            if let Some(file) = files.item(0)
                && file.size() as u64 > max_input_size_bytes.0
            {
                let err_msg = format!(
                    "File '{}' exceeds the maximum allowed size of {}",
                    file.name(),
                    max_input_size_bytes.display().si()
                );
                error_context.push(ClientErrorKind::FileUploadError(err_msg).into());
                input.set_value("");
                return;
            }

            if let Err(e) = tracker_file.upload_files(&files, move |form| {
                info!("Uploading files");
                error_context.clear();
                upload.local_action.dispatch_local(form);
                upload.mode.set("local".to_string());
            }) {
                error_context.extend(e.records);
            }
        } else {
            info!("Found no files to upload");
        }
    };

    view! {
        <div class="mb-2">
            <label class="block mb-1">"From URL:"</label>
            <input
                class="p-1 w-full bg-gray-200 rounded border-b-0"
                placeholder="Enter input URL"
                prop:value=url_input_val
                on:input=move |ev| {
                    let target: HtmlInputElement = event_target(&ev);
                    let url = target.value();
                    url_input_val.set(url.clone());
                    tracker_url
                        .upload_url(
                            &url,
                            move |u| {
                                error_context.clear();
                                upload.remote_action.dispatch(u);
                                upload.mode.set("remote".to_string());
                            },
                        );
                }
            />
            {move || {
                match url_stage.get() {
                    Some("Done") => {
                        view! {
                            <div class="mt-2">
                                <p class="mt-1 text-sm font-bold text-center">
                                    "Loading done"
                                </p>
                            </div>
                        }
                            .into_any()
                    }
                    Some(stage) => {
                        view! {
                            <div class="mt-2">
                                <LoadingCircle />
                                <p class="mt-1 text-sm text-center">
                                    <span class="inline-flex relative items-center">
                                        <span>{stage}</span>
                                        <span class="absolute left-full text-left loading-dots-anim">
                                            "......"
                                        </span>
                                    </span>
                                </p>
                            </div>
                        }
                            .into_any()
                    }
                    None => ().into_any(),
                }
            }}
        </div>

        <div class="mb-2">
            <label class="block mb-1">"From File:"</label>
            <div class="relative">
                <input
                    node_ref=file_input_ref
                    id="file-upload"
                    type="file"
                    class="absolute inset-0 w-full h-full opacity-0 cursor-pointer"
                    multiple=""
                    accept=".owl,.ofn,.owx,.xml,.json,.ttl,.rdf,.nt,.nq,.trig,.jsonld,.n3,.srj,.srx,.json,.xml,.csv,.tsv"
                    on:input=upload_files
                />
                <label
                    for="file-upload"
                    class="block p-1 w-full bg-gray-200 rounded border-b-0"
                >
                    {move || {
                        if file_name.get().is_empty() {
                            "Select ontology file".to_string()
                        } else {
                            file_name.get()
                        }
                    }}
                </label>
            </div>
            {move || {
                match file_stage.get() {
                    Some("Done") => {
                        view! {
                            <div class="mt-2">
                                <p class="mt-1 text-sm font-bold text-center">
                                    "Loading done"
                                </p>
                            </div>
                        }
                            .into_any()
                    }
                    Some(stage) => {
                        view! {
                            <div class="mt-2">
                                <LoadingCircle />
                                <p class="mt-1 text-sm text-center">
                                    <span class="inline-flex relative items-center">
                                        <span>{stage}</span>
                                        <span class="absolute left-full text-left loading-dots-anim">
                                            "......"
                                        </span>
                                    </span>
                                </p>
                            </div>
                        }
                            .into_any()
                    }
                    None => ().into_any(),
                }
            }}
        </div>
    }
}

#[component]
pub fn FetchData() -> impl IntoView {
    let error_context = expect_context::<ErrorLogContext>();
    let fetch = Action::new(|(): &()| async move {
        load_graph(DEFAULT_QUERY.to_string(), true).await;
    });

    view! {
        <div class="flex flex-col gap-2">
            <button
                class="flex relative justify-center items-center p-1 mt-1 text-xs bg-gray-200 rounded text-[#000000]"
                on:click=move |_| {
                    error_context.clear();
                    fetch.dispatch(());
                }
            >
                <Icon class="pr-0.5" icon=icondata::AiReloadOutlined />
                "reload data"
            </button>
        </div>
    }
}

#[component]
pub fn Sparql() -> impl IntoView {
    let error_context = expect_context::<ErrorLogContext>();
    let GraphDataContext {
        active_graph_name, ..
    } = expect_context::<GraphDataContext>();
    let upload = FileUpload::new();
    let upload_progress = upload.tracker.upload_progress;
    let parsing_status = upload.tracker.parsing_status;
    let parsing_done = upload.tracker.parsing_done;
    let tracker_sparql = upload.tracker.clone();
    let sparql_loading_done = upload.sparql_action.value();

    let textarea_class = "overflow-hidden p-1 w-full text-xs bg-gray-200 rounded border-b-0 resize-none font-jetbrains min-h-24";

    let endpoint_signal = RwSignal::new(String::new());
    let sparql_stage: RwSignal<Option<&'static str>> = RwSignal::new(None);

    let textarea_query = NodeRef::new();
    let textarea_query_props = use_textarea_autosize(textarea_query);

    let textarea_triple = NodeRef::new();
    let textarea_triple_props = use_textarea_autosize(textarea_triple);

    let query_variables = Memo::new(move |old| {
        match QueryNormalizer::get_significant_query_variables(&textarea_query_props.content.read())
        {
            Ok(vars) => vars,
            Err(e) => {
                error_context.push(e.into());
                old.cloned().unwrap_or_default()
            }
        }
    });
    let variable_count_greater_than_2 = Signal::derive(move || {
        let value = query_variables.read().len() > 2;
        if !value {
            textarea_triple_props.set_content.set(String::new());
        }
        value
    });

    let is_loading = RwSignal::new(false);

    Effect::new(move || {
        if let Some(value) = sparql_loading_done.get() {
            sparql_stage.set(Some("Serializing"));
            active_graph_name.set(format!("sparql-{}", endpoint_signal.get_untracked()));

            match value {
                Ok((_, _, warning)) => {
                    if let Some(e) = warning {
                        error_context.extend(e.records);
                    }
                    let stage = sparql_stage;
                    spawn_local_scoped_with_cancellation(async move {
                        load_graph(DEFAULT_QUERY.to_string(), true).await;
                        stage.set(Some("Done"));
                        is_loading.set(false);
                    });
                }
                Err(e) => {
                    error_context.extend(e.records);
                    sparql_stage.set(None);
                    is_loading.set(false);
                }
            }
        }
    });

    let run_sparql = move |_| match QueryAssembler::assemble_user_query_endpoint(
        &textarea_query_props.content.get_untracked(),
        &textarea_triple_props.content.get_untracked(),
    ) {
        Ok(query) => {
            is_loading.set(true);
            sparql_stage.set(Some("Querying"));
            let endpoint = endpoint_signal.get_untracked();
            tracker_sparql.upload_sparql_endpoint(&endpoint, &query, move |(ep, q)| {
                upload.sparql_action.dispatch((ep, q, None));
                upload.mode.set("sparql".to_string());
            });
        }
        Err(e) => {
            error_context.push(e.into());
        }
    };

    view! {
        <fieldset>
            <legend>"SPARQL Query:"</legend>
            <div class="flex flex-col gap-2">
                <div>
                    <label class="block mb-1 text-xs text-gray">
                        "Query Endpoint"
                    </label>
                    <input
                        class="p-1 w-full text-xs bg-gray-200 rounded border-b-0"
                        placeholder="Enter query endpoint"
                        on:input=move |ev| {
                            let t: HtmlInputElement = event_target(&ev);
                            endpoint_signal.set(t.value());
                        }
                    />
                </div>

                <div>
                    <label class="block mb-1 text-xs text-gray">"Query"</label>
                    <textarea
                        prop:value=textarea_query_props.content
                        on:input=move |evt| {
                            textarea_query_props
                                .set_content
                                .set(event_target_value(&evt))
                        }
                        node_ref=textarea_query
                        class=textarea_class
                        placeholder="Enter query"
                    />
                </div>

                <Show when=move || variable_count_greater_than_2.get()>
                    <div>
                        <p>
                            "Your query includes more than two variables. Please enter below which variables should form triples (if any).
                            For instance, write \"?s ?p ?o\" (without the quotes) if you intend for variables ?s, ?p, and ?o to be a triple.
                            Please enter each triple on a new line.
                            Leave blank to treat all variables as independent.
                            "
                        </p>
                        <div>
                            <textarea
                                prop:value=textarea_triple_props.content
                                on:input=move |evt| {
                                    textarea_triple_props
                                        .set_content
                                        .set(event_target_value(&evt))
                                }
                                node_ref=textarea_triple
                                class=textarea_class
                                placeholder="Enter triples"
                            />
                        </div>
                    </div>
                </Show>

                <button
                    class="p-1 mt-1 text-xs text-white bg-blue-500 rounded"
                    disabled=move || is_loading.get()
                    on:click=run_sparql
                >
                    {move || {
                        if is_loading.get() {
                            "Running query..."
                        } else {
                            "Run query"
                        }
                    }}
                </button>

                <Show when=move || is_loading.get()>
                    <div class="overflow-hidden w-full h-1 bg-gray-100 rounded-full">
                        <div class="w-full h-full bg-blue-500 animate-pulse"></div>
                    </div>
                </Show>

                {move || {
                    let progress = upload_progress.get();
                    let parsing = parsing_status.get();
                    let done = parsing_done.get();
                    if progress > 0 {

                        view! {
                            <div class="mt-2">
                                <div class="mt-2 w-full h-2.5 bg-gray-200 rounded-full dark:bg-gray-700">
                                    <div
                                        class="h-2.5 bg-blue-500 rounded-full transition-all duration-300"
                                        style=format!("width: {}%", std::cmp::min(progress, 100))
                                    ></div>
                                </div>
                                {if progress >= 100 {
                                    view! {
                                        <div class="mt-1 text-sm font-bold text-center">
                                            "Upload done"
                                        </div>
                                        {if done {
                                            view! {
                                                <div class="mt-1 text-sm font-bold text-center">
                                                    "Parsing done"
                                                </div>
                                            }
                                                .into_any()
                                        } else {
                                            view! {
                                                <div class="mt-1 text-sm text-center">{parsing}</div>
                                            }
                                                .into_any()
                                        }}
                                    }
                                        .into_any()
                                } else {
                                    ().into_any()
                                }}
                            </div>
                        }
                            .into_any()
                    } else {
                        ().into_any()
                    }
                }}

                <div>
                    <p>
                        "Most prefixes are currently not included automatically. Use full IRIs for any namespace not listed below."
                    </p>
                    <p class="mb-1 font-bold text-gray-500 uppercase text-[10px]">
                        "Included prefixes"
                    </p>
                    <div class="overflow-hidden rounded border border-gray-200">
                        <DefaultPrefixTable />
                    </div>
                </div>
            </div>
        </fieldset>
    }
}

#[component]
pub fn UploadedOntology() -> impl IntoView {
    let error_context = expect_context::<ErrorLogContext>();
    let GraphDataContext {
        active_graph_name, ..
    } = expect_context::<GraphDataContext>();

    let active_task = expect_context::<ActiveMenuTask>().0;
    let refresh = expect_context::<UploadedListRefresh>().0;

    let uploaded_stage: RwSignal<Option<&'static str>> = RwSignal::new(None);
    let selected_iri: RwSignal<Option<String>> = RwSignal::new(None);
    let select_ref = NodeRef::<leptos::html::Select>::new();

    Effect::new(move || {
        if active_task.get() != "uploaded" {
            if uploaded_stage.get_untracked() == Some("Done") {
                uploaded_stage.set(None);
            }
            if selected_iri.get_untracked().is_some() {
                selected_iri.set(None);
            }
            if let Some(el) = select_ref.get() {
                el.set_value("");
            }
        }
    });

    let list_ontologies = LocalResource::new(move || {
        let _ = refresh.get();
        async move { list_uploaded_ontologies().await }
    });

    let load_uploaded_ontology = LocalResource::new(move || async move {
        if let Some(iri) = selected_iri.get() {
            active_task.set("uploaded");
            uploaded_stage.set(Some("Loading"));
            active_graph_name.set(iri);
            uploaded_stage.set(Some("Serializing"));
            load_graph(DEFAULT_QUERY.to_string(), true).await;
            uploaded_stage.set(Some("Done"));
        }
    });

    let on_select = move |ev: Event| {
        let target: HtmlInputElement = event_target::<HtmlInputElement>(&ev);
        let value = target.value();
        if value.is_empty() {
            return;
        }
        error_context.clear();
        selected_iri.set(Some(value));
    };

    view! {
        <div class="my-2">
            <label class="block mb-1">
                "Previously Uploaded:"
                <button
                    class="ml-1 text-xs text-gray-500 hover:text-gray-800"
                    title="Refresh list"
                    on:click=move |_| {
                        refresh.update(|n| *n += 1);
                    }
                >
                    <Icon class="inline" icon=icondata::AiReloadOutlined />
                </button>
            </label>
            <Suspense fallback=move || {
                view! { <p class="text-xs text-gray-500">"Loading list…"</p> }
            }>
                {move || Suspend::new(async move {
                    match list_ontologies.await {
                        Ok(entries) if entries.is_empty() => {
                            view! {
                                <p class="text-xs italic text-gray-400">
                                    "No uploaded ontologies yet."
                                </p>
                            }
                                .into_any()
                        }
                        Ok(entries) => {
                            view! {
                                <select
                                    node_ref=select_ref
                                    class="p-1 w-full text-sm bg-gray-200 rounded border-b-0"
                                    on:change=on_select
                                >
                                    <option value="">"Select an uploaded ontology"</option>
                                    {entries
                                        .into_iter()
                                        .map(|e| {
                                            let iri = e.graph_iri.clone();
                                            view! { <option value=iri>{e.label}</option> }
                                        })
                                        .collect_view()}
                                </select>
                            }
                                .into_any()
                        }
                        Err(e) => {
                            error_context.extend(e.records);
                            view! {
                                <p class="text-xs text-red-500">"Failed to load list."</p>
                            }
                                .into_any()
                        }
                    }
                })}
            </Suspense>

            <Suspense fallback=move || {
                view! { <LoadingCircle /> }
            }>
                {move || Suspend::new(async move {
                    load_uploaded_ontology.await;
                })}
            </Suspense>
            {move || {
                match uploaded_stage.get() {
                    Some("Done") => {
                        view! {
                            <p class="mt-1 text-sm font-bold text-center">
                                "Loading done"
                            </p>
                        }
                            .into_any()
                    }
                    Some(stage) => {
                        view! {
                            <p class="mt-1 text-sm text-center">
                                <span class="inline-flex relative items-center">
                                    <span>{stage}</span>
                                    <span class="absolute left-full text-left loading-dots-anim">
                                        "......"
                                    </span>
                                </span>
                            </p>
                        }
                            .into_any()
                    }
                    None => ().into_any(),
                }
            }}
        </div>
    }
}

#[component]
pub fn OntologyMenu() -> impl IntoView {
    let active_task = ActiveMenuTask(RwSignal::new(""));
    provide_context(active_task);
    let refresh = UploadedListRefresh(RwSignal::new(0));
    provide_context(refresh);

    view! {
        <style>
            "
            .loading-dots-anim {
                display: inline-block;
                overflow: hidden;
                vertical-align: bottom;
                white-space: nowrap;
                font-family: monospace;
                animation: loading-dots 3s steps(7, end) infinite;
            }
            @keyframes loading-dots {
                0% { width: 0ch; }
                100% { width: 7ch; }
            }
            "
        </style>
        <WorkbenchMenuItems title="Load Ontology">
            <SelectStaticInput />
            <UploadInput />
            <UploadedOntology />
            <Sparql />
            <FetchData />
        </WorkbenchMenuItems>
    }
}

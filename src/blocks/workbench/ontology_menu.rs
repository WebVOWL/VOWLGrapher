use super::WorkbenchMenuItems;
use crate::components::progress_bar::LoadingCircle;
use crate::components::user_input::internal_sparql::GraphDataContext;
use crate::components::user_input::internal_sparql::load_graph;
use crate::components::user_input::stored_ontology::StoredOntology;
use crate::components::user_input::stored_ontology::load_stored_ontology;
use crate::components::{icon::Icon, user_input::file_upload::FileUpload};
use crate::errors::ClientErrorKind;
use crate::errors::ErrorLogContext;
use leptos::prelude::*;
use leptos::task::spawn_local_scoped_with_cancellation;
use log::info;
use std::iter::once;
use strum::IntoEnumIterator;
use vowlr_sparql_queries::prelude::DEFAULT_QUERY;
use web_sys::Event;
use web_sys::HtmlInputElement;

const MAX_FILE_SIZE_BYTES: f64 = 50.0 * 1024.0 * 1024.0;

#[component]
pub fn SelectStaticInput() -> impl IntoView {
    let error_context = expect_context::<ErrorLogContext>();
    let GraphDataContext {
        active_graph_name, ..
    } = expect_context::<GraphDataContext>();

    let selected_ontology: RwSignal<Option<StoredOntology>> = RwSignal::new(None);

    let stored_res = LocalResource::new(move || async move {
        if let Some(stored) = selected_ontology.get() {
            active_graph_name.set(stored.path().to_string());
            match load_stored_ontology(stored).await {
                Ok(()) => {
                    load_graph(DEFAULT_QUERY.to_string(), true).await;
                }
                Err(e) => {
                    error_context.extend(e.records);
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
                prop:value=selected_ontology
                    .read()
                    .map_or_else(
                        || "Select an ontology".to_string(),
                        |ontology| ontology.to_string(),
                    )
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
    let upload_progress = upload.tracker.upload_progress;
    let parsing_status = upload.tracker.parsing_status;
    let parsing_done = upload.tracker.parsing_done;
    let tracker_url = upload.tracker.clone();
    let tracker_file = upload.tracker.clone();
    let file_name = upload.tracker.filename;
    let url_name = upload.tracker.url_name;

    Effect::new(move || {
        if let Some(value) = local_loading_done.get() {
            active_graph_name.set(file_name.get_untracked());

            match value {
                Ok((_, _, warning)) => {
                    if let Some(e) = warning {
                        error_context.extend(e.records);
                    }
                    spawn_local_scoped_with_cancellation(async move {
                        load_graph(DEFAULT_QUERY.to_string(), true).await;
                    });
                }
                Err(e) => {
                    error_context.extend(e.records);
                }
            }
        }
    });

    Effect::new(move || {
        if let Some(value) = remote_loading_done.get() {
            active_graph_name.set(url_name.get_untracked());

            match value {
                Ok((_, _, warning)) => {
                    if let Some(e) = warning {
                        error_context.extend(e.records);
                    }
                    spawn_local_scoped_with_cancellation(async move {
                        load_graph(DEFAULT_QUERY.to_string(), true).await;
                    });
                }
                Err(e) => {
                    error_context.extend(e.records);
                }
            }
        }
    });

    let upload_files = move |ev: Event| {
        let input: HtmlInputElement = event_target(&ev);
        if let Some(files) = input.files() {
            if let Some(file) = files.item(0)
                && file.size() > MAX_FILE_SIZE_BYTES
            {
                let err_msg = format!(
                    "File {} exceeds the maximum allowed size of {}MB.",
                    file.name(),
                    MAX_FILE_SIZE_BYTES / 1024.0 / 1024.0
                );
                error_context.push(ClientErrorKind::FileUploadError(err_msg).into());
                input.set_value("");
                return;
            }

            if let Err(e) = tracker_file.upload_files(&files, move |form| {
                info!("Uploading files");
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
                on:input=move |ev| {
                    let target: HtmlInputElement = event_target(&ev);
                    let url = target.value();
                    tracker_url
                        .upload_url(
                            &url,
                            move |u| {
                                upload.remote_action.dispatch(u);
                                upload.mode.set("remote".to_string());
                            },
                        );
                }
            />
        </div>

        <div class="mb-2">
            <label class="block mb-1">"From File:"</label>
            <div class="relative">
                <input
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
                let progress = upload_progress.get();
                let parsing = parsing_status.get();
                let done = parsing_done.get();
                if progress > 0 {
                    // let msg = message.get();
                    // (!msg.is_empty()).then(|| view! {<p class="mt-1 text-green">{msg}</p>})

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
        </div>
    }
}

#[component]
pub fn FetchData() -> impl IntoView {
    let fetch = Action::new(|(): &()| async move {
        load_graph(DEFAULT_QUERY.to_string(), true).await;
    });

    view! {
        <div class="flex flex-col gap-2">
            <button
                class="flex relative justify-center items-center p-1 mt-1 text-xs bg-gray-200 rounded text-[#000000]"
                on:click=move |_| {
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
    let upload = FileUpload::new();
    let upload_progress = upload.tracker.upload_progress;
    let parsing_status = upload.tracker.parsing_status;
    let parsing_done = upload.tracker.parsing_done;
    let tracker_sparql = upload.tracker.clone();

    let endpoint_signal = RwSignal::new(String::new());
    let query_signal = RwSignal::new(String::new());

    let textarea_ref = NodeRef::<leptos::html::Textarea>::new();

    let handle_input = move |()| {
        if let Some(el) = textarea_ref.get() {
            el.style("height: auto");

            let scroll = el.scroll_height();
            let new_height = scroll - 16;

            el.style(("height", format!("{new_height}px")));
        }
    };

    let run_sparql = move || {
        tracker_sparql.upload_sparql(
            &endpoint_signal.get(),
            &query_signal.get(),
            move |(ep, q, fmt)| {
                upload.sparql_action.dispatch((ep, q, fmt));
                upload.mode.set("sparql".to_string());
            },
        );
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
                        node_ref=textarea_ref
                        class="overflow-hidden p-1 w-full text-xs bg-gray-200 rounded border-b-0 resize-none min-h-24"
                        rows=1
                        placeholder="Enter query"
                        on:input=move |ev| {
                            let t: HtmlInputElement = event_target(&ev);
                            query_signal.set(t.value());
                            handle_input(());
                        }
                    />
                </div>

                <button
                    class="p-1 mt-1 text-xs text-white bg-blue-500 rounded"
                    on:click=move |_| run_sparql()
                >
                    "Run query"
                </button>

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
            </div>
        </fieldset>
    }
}

#[component]
pub fn OntologyMenu() -> impl IntoView {
    view! {
        <WorkbenchMenuItems title="Load Ontology">
            <SelectStaticInput />
            <UploadInput />
            <Sparql />
            <FetchData />
        </WorkbenchMenuItems>
    }
}

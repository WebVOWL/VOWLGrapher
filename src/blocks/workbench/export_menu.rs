use super::WorkbenchMenuItems;
use crate::components::icon::Icon;
use crate::components::user_input::internal_sparql::GraphDataContext;
use crate::errors::{ClientErrorKind, ErrorLogContext};
use futures::StreamExt;
use leptos::prelude::*;
use leptos::server_fn::codec::{ByteStream, Streaming};
use strum::IntoEnumIterator;
#[cfg(feature = "server")]
use vowlgrapher_database::prelude::VOWLGrapherStore;
#[cfg(feature = "ssr")]
use vowlgrapher_util::prelude::manage_user_id;
use vowlgrapher_util::prelude::{DataType, VOWLGrapherError};
use web_sys::{Blob, BlobPropertyBag, HtmlAnchorElement, Url, js_sys, wasm_bindgen::JsCast};
#[server(output = Streaming)]
/// Export a graph from the database
pub async fn export_graph(
    resource_type: DataType,
    graph_name: String,
) -> Result<ByteStream<VOWLGrapherError>, VOWLGrapherError> {
    let store = VOWLGrapherStore::new_for_user(manage_user_id().await?);
    let stream = store.serialize_stream(resource_type, &graph_name).await?;
    Ok(ByteStream::new(stream.map(|chunk| {
        chunk
            .map(bytes::Bytes::from)
            .map_err(std::convert::Into::into)
    })))
}

pub async fn download_ontology(
    resource_type: DataType,
    progress_message: RwSignal<String>,
    graph_name: String,
) -> Result<(), VOWLGrapherError> {
    let byte_stream = export_graph(resource_type, graph_name).await?;

    // Download data from server
    progress_message.set("Downloaded: 0 MB".to_string());
    let mut stream = byte_stream.into_inner();
    let mut data = Vec::new();
    let mut downloaded = 0;
    while let Some(chunk) = stream.next().await {
        let bytes = chunk?;
        downloaded += bytes.len();
        let mb = downloaded / 1_024 / 1_024;
        progress_message.set(format!("Downloaded: {mb:.2} MB"));
        data.extend(bytes);
    }
    progress_message.set("Processing...".to_string());

    // Package received data into a blob object with apropriate metadata
    let window = web_sys::window().ok_or_else(|| {
        ClientErrorKind::JavaScriptError("Failed to get the Window object".to_string())
    })?;
    let document = window.document().ok_or_else(|| {
        ClientErrorKind::JavaScriptError("Failed to get Document object".to_string())
    })?;
    let body = document.body().ok_or_else(|| {
        ClientErrorKind::JavaScriptError("Failed to get the docoment body".to_string())
    })?;

    let blob_parts = js_sys::Array::new();
    let uint8_array = js_sys::Uint8Array::from(data.as_slice());
    blob_parts.push(&uint8_array.into());

    let blob_options = BlobPropertyBag::new();
    blob_options.set_type(resource_type.mime_type());

    let blob = Blob::new_with_str_sequence_and_options(&blob_parts, &blob_options)
        .map_err(|e| ClientErrorKind::JavaScriptError(format!("{e:#?}")))?;

    // Create the URL and anchor button to show download in the browser.
    let url = Url::create_object_url_with_blob(&blob)
        .map_err(|e| ClientErrorKind::JavaScriptError(format!("{e:#?}")))?;
    let a: HtmlAnchorElement = document
        .create_element("a")
        .map_err(|e| ClientErrorKind::JavaScriptError(format!("{e:#?}")))?
        // SAFETY: Creating HTML element "a" will always have type 'HtmlAnchorElement'.
        .unchecked_into::<HtmlAnchorElement>();
    a.set_href(&url);
    a.set_download(format!("ontology.{}", resource_type.extension()).as_str());
    a.set_attribute("style", "display: none")
        .map_err(|e| ClientErrorKind::JavaScriptError(format!("{e:#?}")))?;

    body.append_child(&a)
        .map_err(|e| ClientErrorKind::JavaScriptError(format!("{e:#?}")))?;
    a.click();

    // Cleanup
    body.remove_child(&a)
        .map_err(|e| ClientErrorKind::JavaScriptError(format!("{e:#?}")))?;
    Url::revoke_object_url(&url)
        .map_err(|e| ClientErrorKind::JavaScriptError(format!("{e:#?}")))?;
    progress_message.set("Download complete".to_string());

    Ok(())
}

#[component]
pub fn ExportButton(
    #[prop(into)] label: String,
    #[prop(into)] icon: icondata::Icon,
    on_click: impl Fn() + 'static,
) -> impl IntoView {
    view! {
        <button
            class="flex relative gap-1 justify-center items-center m-2 w-40 h-10 font-semibold bg-gray-200 rounded-sm transition-colors cursor-pointer text-[#000000] hover:bg-[#dd9900]"
            on:click=move |_| { on_click() }
        >
            <Icon icon=icon />
            {label}
        </button>
    }
}

#[component]
pub fn ExportMenu() -> impl IntoView {
    let progress_message = RwSignal::new(String::new());

    let download = Action::new(|input: &(DataType, RwSignal<String>)| {
        let (resource_type, progress_message) = input.to_owned();
        async move {
            let graph_name = expect_context::<GraphDataContext>()
                .active_graph_name
                .get_untracked();
            match download_ontology(resource_type, progress_message, graph_name).await {
                Ok(()) => (),
                Err(e) => {
                    let error_context = expect_context::<ErrorLogContext>();
                    error_context.extend(e.records);
                }
            }
        }
    });

    let export_buttons = move || {
        DataType::iter()
            .filter(|dtype| {
                !matches!(
                    dtype,
                    DataType::SPARQLCSV
                        | DataType::SPARQLJSON
                        | DataType::SPARQLTSV
                        | DataType::SPARQLXML
                        | DataType::UNKNOWN
                )
            })
            .map(|dtype| {
                view! {
                    <ExportButton
                        label=dtype.to_string()
                        icon=icondata::BiExportRegular
                        on_click=move || {
                            download.dispatch((dtype, progress_message));
                        }
                    />
                }
                .into_any()
            })
            .collect_view()
    };

    view! {
        <WorkbenchMenuItems title="Export Ontology">
            <div class="flex flex-wrap justify-center w-full">
                // <ExportButton label="SVG" icon=icondata::BiExportRegular />
                // <ExportButton label="TeX" icon=icondata::BiExportRegular />
                {export_buttons()}
            </div>
            {move || {
                let msg = progress_message.get();
                (!msg.is_empty())
                    .then(|| {
                        view! {
                            <div class="mt-2 w-full text-sm text-center text-gray-600">
                                {msg}
                            </div>
                        }
                    })
            }}
        </WorkbenchMenuItems>
    }
}

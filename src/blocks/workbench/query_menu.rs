use super::WorkbenchMenuItems;
use crate::{components::user_input::internal_sparql::load_graph, errors::ErrorLogContext};
use leptos::{prelude::*, task::spawn_local_scoped_with_cancellation};
use vowlgrapher_sparql_queries::prelude::{QueryAssembler, QueryNormalizer};
use web_sys::HtmlInputElement;

#[component]
pub fn CustomSparql() -> impl IntoView {
    let error_context = expect_context::<ErrorLogContext>();

    let query_input = RwSignal::new(String::new());
    let triple_input = RwSignal::new(String::new());

    let query_variables = Memo::new(move |old| {
        match QueryNormalizer::get_significant_query_variables(&query_input.read()) {
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
            triple_input.set(String::new());
        }
        value
    });

    let is_loading = RwSignal::new(false);
    let textarea_ref = NodeRef::<leptos::html::Textarea>::new();
    let textarea_ref2 = NodeRef::<leptos::html::Textarea>::new();

    let handle_input = move |()| {
        if let Some(el) = textarea_ref.get() {
            el.style("height: auto");

            let scroll = el.scroll_height();
            let new_height = scroll - 16;

            el.style(("height", format!("{new_height}px")));
        }

        if let Some(el) = textarea_ref2.get() {
            el.style("height: auto");

            let scroll = el.scroll_height();
            let new_height = scroll - 16;

            el.style(("height", format!("{new_height}px")));
        }
    };

    let run_query = move |_| match QueryAssembler::assemble_user_query(
        &query_input.get_untracked(),
        &triple_input.get_untracked(),
    ) {
        Ok(query) => {
            is_loading.set(true);

            spawn_local_scoped_with_cancellation(async move {
                load_graph(query, true).await;
                is_loading.set(false);
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
                    <textarea
                        node_ref=textarea_ref
                        class="overflow-hidden p-1 w-full text-xs bg-gray-200 rounded border-b-0 resize-none font-jetbrains min-h-24"
                        rows=1
                        placeholder="Enter query"
                        prop:value=move || query_input.get()
                        on:input=move |ev| {
                            let t: HtmlInputElement = event_target(&ev);
                            query_input.set(t.value());
                            handle_input(());
                        }
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
                                node_ref=textarea_ref2
                                class="overflow-hidden p-1 w-full text-xs bg-gray-200 rounded border-b-0 resize-none font-jetbrains min-h-24"
                                rows=1
                                placeholder="Enter triples"
                                prop:value=move || triple_input.get()
                                on:input=move |ev| {
                                    let t: HtmlInputElement = event_target(&ev);
                                    triple_input.set(t.value());
                                    handle_input(());
                                }
                            />
                        </div>
                    </div>
                </Show>

                <button
                    class="p-1 mt-1 text-xs text-white bg-blue-500 rounded"
                    disabled=move || is_loading.get()
                    on:click=run_query
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

                <div>
                    <p>
                        "Most prefixes are currently not included automatically. Use full IRIs for any namespace not listed below."
                    </p>
                    <p class="mb-1 font-bold text-gray-500 uppercase text-[10px]">
                        "Included prefixes"
                    </p>
                    <div class="overflow-hidden rounded border border-gray-200">
                        <table class="w-full text-left border-collapse text-[10px]">
                            <thead class="bg-gray-100 border-b border-gray-200">
                                <tr>
                                    <th class="p-1 font-semibold">"Prefix"</th>
                                    <th class="p-1 font-semibold">"Namespace IRI"</th>
                                </tr>
                            </thead>
                            <tbody class="bg-white divide-y divide-gray-100">
                                <tr>
                                    <td>owl</td>
                                    <td>"http://www.w3.org/2002/07/owl#"</td>
                                </tr>
                                <tr>
                                    <td>rdfs</td>
                                    <td>"http://www.w3.org/2000/01/rdf-schema#"</td>
                                </tr>
                                <tr>
                                    <td>rdf</td>
                                    <td>"http://www.w3.org/1999/02/22-rdf-syntax-ns#"</td>
                                </tr>
                                <tr>
                                    <td>xsd</td>
                                    <td>"http://www.w3.org/2001/XMLSchema#"</td>
                                </tr>
                                <tr>
                                    <td>xml</td>
                                    <td>"http://www.w3.org/XML/1998/namespace"</td>
                                </tr>
                                <tr>
                                    <td>dc</td>
                                    <td>"http://purl.org/dc/elements/1.1/"</td>
                                </tr>
                                <tr>
                                    <td>dcterms</td>
                                    <td>"http://purl.org/dc/terms/"</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </fieldset>
    }
}

#[component]
pub fn QueryMenu() -> impl IntoView {
    view! {
        <WorkbenchMenuItems title=format!(
            "Query from {} database",
            env!("CARGO_PKG_NAME"),
        )>
            <CustomSparql />
        </WorkbenchMenuItems>
    }
}

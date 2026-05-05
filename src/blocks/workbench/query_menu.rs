use super::WorkbenchMenuItems;
use crate::components::user_input::internal_sparql::load_graph;
use leptos::{prelude::*, task::spawn_local_scoped_with_cancellation};
use vowlgrapher_sparql_queries::prelude::QueryAssembler;
use web_sys::HtmlInputElement;

#[component]
pub fn CustomSparql() -> impl IntoView {
    let query_input = RwSignal::new(String::new());
    let is_loading = RwSignal::new(false);
    let textarea_ref = NodeRef::<leptos::html::Textarea>::new();

    let handle_input = move |()| {
        if let Some(el) = textarea_ref.get() {
            el.style("height: auto");

            let scroll = el.scroll_height();
            let new_height = scroll - 16;

            el.style(("height", format!("{new_height}px")));
        }
    };

    let run_query = move |_| {
        let user_query = query_input.get_untracked();
        let final_query = QueryAssembler::assemble_custom_query(&user_query);
        is_loading.set(true);

        spawn_local_scoped_with_cancellation(async move {
            load_graph(final_query, false).await;
            is_loading.set(false);
        });
    };

    view! {
        <fieldset>
            <legend>"SPARQL Query:"</legend>
            <div class="flex flex-col gap-2">
                <div>
                    <textarea
                        node_ref=textarea_ref
                        class="font-jetbrains overflow-hidden p-1 w-full text-xs bg-gray-200 rounded border-b-0 resize-none min-h-24"
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

                <button
                    class="p-1 mt-1 text-xs text-white bg-blue-500 rounded"
                    disabled =move || is_loading.get()
                    on:click=run_query
                >
                    {move || if is_loading.get() { "Running query..." } else { "Run query" }}
                </button>

                <Show when=move || is_loading.get()>
                    <div class="w-full h-1 bg-gray-100 rounded-full overflow-hidden">
                        <div class="h-full bg-blue-500 animate-pulse w-full"></div>
                    </div>
                </Show>

                <div>
                    <p>"Ensure your query binds results to the standard triple variables ?s (subject), ?p (predicate), and ?o (object) if used. 
                    Automatic prefix fetching is currently disabled. Use full IRIs for any namespace not listed below."
                    </p>
                    <p class="text-[10px] font-bold text-gray-500 uppercase mb-1">"Available Prefixes"</p>
                    <div class="border rounded border-gray-200 overflow-hidden">
                        <table class="w-full text-[10px] text-left border-collapse">
                            <thead class="bg-gray-100 border-b border-gray-200">
                                <tr>
                                    <th class="p-1 font-semibold">"Prefix"</th>
                                    <th class="p-1 font-semibold">"Namespace IRI"</th>
                                </tr>
                            </thead>
                            <tbody class="divide-y divide-gray-100 bg-white">
                                <tr><td>owl</td><td>"http://www.w3.org/2002/07/owl#"</td></tr>
                                <tr><td>rdfs</td><td>"http://www.w3.org/2000/01/rdf-schema#"</td></tr>
                                <tr><td>rdf</td><td>"http://www.w3.org/1999/02/22-rdf-syntax-ns#"</td></tr>
                                <tr><td>xsd</td><td>"http://www.w3.org/2001/XMLSchema#"</td></tr>
                                <tr><td>xml</td><td>"http://www.w3.org/XML/1998/namespace"</td></tr>
                                <tr><td>dc</td><td>"http://purl.org/dc/elements/1.1/"</td></tr>
                                <tr><td>dcterms</td><td>"http://purl.org/dc/terms/"</td></tr>
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
        <WorkbenchMenuItems title={format!("Query from {} database", env!("CARGO_PKG_NAME"))}>
            <CustomSparql />
        </WorkbenchMenuItems>
    }
}

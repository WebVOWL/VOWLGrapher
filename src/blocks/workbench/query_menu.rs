use crate::components::user_input::internal_sparql::load_graph;
use super::WorkbenchMenuItems;
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

    let run_query = move |_|{
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
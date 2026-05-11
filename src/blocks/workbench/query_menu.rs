use super::WorkbenchMenuItems;
use crate::{components::user_input::internal_sparql::load_graph, errors::ErrorLogContext};
use leptos::{prelude::*, task::spawn_local_scoped_with_cancellation};
use leptos_use::use_textarea_autosize;
use vowlgrapher_sparql_queries::prelude::{DefaultPrefixTable, QueryAssembler, QueryNormalizer};

#[component]
pub fn CustomSparql() -> impl IntoView {
    let error_context = expect_context::<ErrorLogContext>();
    let textarea_class = "overflow-hidden p-1 w-full text-xs bg-gray-200 rounded border-b-0 resize-none font-jetbrains min-h-24";

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

    let run_query = move |_| match QueryAssembler::assemble_user_query(
        &textarea_query_props.content.get_untracked(),
        &textarea_triple_props.content.get_untracked(),
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
                        <DefaultPrefixTable />
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

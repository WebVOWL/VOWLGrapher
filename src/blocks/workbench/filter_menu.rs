mod classes;
mod element_legend_injection;
mod filtergroup;
mod filtertype;
mod meta_filter;
mod properties;
mod special_operators;

use crate::components::user_input::internal_sparql::GraphDataContext;

use crate::errors::ClientErrorKind;
use crate::errors::ErrorLogContext;

use super::WorkbenchMenuItems;
use grapher::prelude::EVENT_DISPATCHER;
use grapher::prelude::ElementType;
use grapher::prelude::SimulatorEvent;
use leptos::prelude::*;

use classes::{is_owl_class, is_rdf_class};
use filtergroup::FilterGroup;
use meta_filter::filter;
use properties::is_property;
use special_operators::is_set_operator;

#[component]
pub fn FilterMenu() -> impl IntoView {
    let GraphDataContext {
        element_counts,
        element_checks,
        ..
    } = expect_context::<GraphDataContext>();

    // Accordion State
    let open_owl = RwSignal::new(false);
    let open_rdf = RwSignal::new(false);
    let open_set_operations = RwSignal::new(false);
    let open_properties = RwSignal::new(false);

    let last_checked = RwSignal::new(0_u64);
    provide_context(last_checked);

    Effect::new(move || {
        if *last_checked.read() > 0 {
            let msg = SimulatorEvent::HideEntities(element_checks.get());
            if let Err(e) = EVENT_DISPATCHER.sim_write_chan.send(msg) {
                let error_context = expect_context::<ErrorLogContext>();
                error_context.push(ClientErrorKind::EventHandlingError(e.to_string()).into());
            }
        }
    });

    view! {
        <WorkbenchMenuItems title="Filter by Type">
            <div class="flex gap-2 items-center pb-3 mb-3 border-b">
                <button
                    class="text-sm text-blue-600 hover:text-blue-800"
                    on:click=move |_| {
                        let counts = element_counts.get();
                        element_checks
                            .update(|map| {
                                let all_enabled = counts
                                    .keys()
                                    .all(|k| *map.get(k).unwrap_or(&true));
                                let target = !all_enabled;
                                for k in counts.keys() {
                                    map.insert(*k, target);
                                }
                            });
                        last_checked.update(|old| { *old += 1 });
                    }
                >
                    {move || {
                        let counts = element_counts.get();
                        let map = element_checks.get();
                        let all_elem = counts
                            .keys()
                            .all(|k| *map.get(k).unwrap_or(&true));
                        if all_elem { "Disable All" } else { "Enable All" }
                    }}
                </button>
            </div>

            <FilterGroup<
            ElementType,
        >
                name="OWL Classes"
                is_open=open_owl
                items=Signal::derive(move || filter(
                    element_counts.get().into_keys().collect::<Vec<_>>(),
                    &[is_owl_class],
                ))
                checks=element_checks
                counts=element_counts
            />

            <FilterGroup<
            ElementType,
        >
                name="RDF"
                is_open=open_rdf
                items=Signal::derive(move || filter(
                    element_counts.get().into_keys().collect::<Vec<_>>(),
                    &[is_rdf_class],
                ))
                checks=element_checks
                counts=element_counts
            />

            <FilterGroup<
            ElementType,
        >
                name="Set Operators"
                is_open=open_set_operations
                items=Signal::derive(move || filter(
                    element_counts.get().into_keys().collect::<Vec<_>>(),
                    &[is_set_operator],
                ))
                checks=element_checks
                counts=element_counts
            />

            <FilterGroup<
            ElementType,
        >
                name="Properties"
                is_open=open_properties
                items=Signal::derive(move || filter(
                    element_counts.get().into_keys().collect::<Vec<_>>(),
                    &[is_property],
                ))
                checks=element_checks
                counts=element_counts
            />

        </WorkbenchMenuItems>
    }
}

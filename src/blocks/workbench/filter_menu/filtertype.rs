use super::element_legend_injection::ElementLegend;
use leptos::either::Either;
use leptos::prelude::*;
use std::{collections::HashMap, hash::Hash};

#[component]
pub fn FilterType<T>(
    #[prop(into)] is_open: RwSignal<bool>,
    #[prop(into)] items: Signal<Vec<T>>,
    #[prop(into)] checks: RwSignal<HashMap<T, bool>>,
    #[prop(into)] counts: Signal<HashMap<T, usize>>,
) -> impl IntoView
where
    T: std::fmt::Display + ElementLegend + Copy + Clone + Eq + Hash + Send + Sync + 'static,
{
    let last_checked = expect_context::<RwSignal<u64>>();
    view! {
        <div style=move || {
            if is_open.get() {
                "max-height: 1000px; opacity: 1; overflow: hidden; transition: max-height 0.5s ease, opacity 0.35s ease; margin-top: 0.5rem; padding-left: 1rem;"
            } else {
                "max-height: 0px; opacity: 0; overflow: hidden; transition: max-height 0.5s ease, opacity 0.35s ease; margin-top: 0; padding-left: 1rem;"
            }
        }>
            {move || {
                let mut sorted_items = items.get();
                sorted_items.sort_by_key(ToString::to_string);
                sorted_items
                    .into_iter()
                    .map(|item| {
                        let legend_view = item
                            .legend()
                            .map_or_else(
                                || Either::Right(
                                    view! {
                                        <div
                                            class="w-8 h-8 bg-gray-50 rounded border border-gray-200 border-dashed"
                                            aria-hidden="true"
                                        ></div>
                                    },
                                ),
                                |file| Either::Left(
                                    view! {
                                        <img
                                            src=file
                                            alt=format!("{} legend", item.to_string())
                                            class="object-contain w-8 h-8"
                                        />
                                    },
                                ),
                            );
                        view! {
                            <div class="flex justify-between items-center py-1 text-sm text-gray-700">
                                <label class="flex gap-3 items-center cursor-pointer">
                                    <input
                                        type="checkbox"
                                        prop:checked=move || {
                                            *checks.read().get(&item).unwrap_or(&true)
                                        }
                                        on:change=move |_| {
                                            checks
                                                .update(|map| {
                                                    let current = *map.get(&item).unwrap_or(&true);
                                                    map.insert(item, !current);
                                                });
                                            last_checked.update(|old| { *old += 1 });
                                        }
                                    />
                                    <div class="flex gap-2 items-center">
                                        {legend_view} <span>{item.to_string()}</span>
                                    </div>
                                </label>
                                <div class="text-sm text-gray-600">
                                    {move || {
                                        if *checks.read().get(&item).unwrap_or(&true) {
                                            format!("{}", *counts.read().get(&item).unwrap_or(&0))
                                        } else {
                                            format!("(0/{})", *counts.read().get(&item).unwrap_or(&0))
                                        }
                                    }}
                                </div>
                            </div>
                        }
                    })
                    .collect::<Vec<_>>()
            }}
        </div>
    }
}

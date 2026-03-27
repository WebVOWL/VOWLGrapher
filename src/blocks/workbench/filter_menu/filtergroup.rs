use super::element_legend_injection::ElementLegend;
use super::filtertype::FilterType;
use leptos::{either::Either, prelude::*};
use std::{collections::HashMap, hash::Hash};

#[component]
pub fn FilterGroup<T>(
    #[prop(into)] name: &'static str,
    #[prop(into)] is_open: RwSignal<bool>,
    #[prop(into)] items: Signal<Vec<T>>,
    #[prop(into)] checks: RwSignal<HashMap<T, bool>>,
    #[prop(into)] counts: Signal<HashMap<T, usize>>,
) -> impl IntoView
where
    T: std::fmt::Display + ElementLegend + Copy + Clone + Eq + Hash + Send + Sync + 'static,
{
    let last_checked = expect_context::<RwSignal<u64>>();
    move || {
        let current_items = items.get();
        if current_items.is_empty() {
            Either::Left(())
        } else {
            Either::Right(view! {
                <div class="pb-2 mb-2 border-b">
                    <div class="flex gap-2 justify-between items-center">
                        <button
                            class="flex-1 py-2 text-left hover:bg-gray-100"
                            on:click=move |_| is_open.update(|v| *v = !*v)
                        >
                            <div class="flex justify-between items-center">
                                <div class="font-medium">
                                    {move || {
                                        let i = items.get();
                                        let read_counts = counts.read();
                                        let read_checks = checks.read();
                                        let total_count: usize = i
                                            .iter()
                                            .map(|item| read_counts.get(item).unwrap_or(&0))
                                            .sum();
                                        let rendered: usize = i
                                            .iter()
                                            .map(|item| {
                                                if *read_checks.get(item).unwrap_or(&true) {
                                                    *read_counts.get(item).unwrap_or(&0)
                                                } else {
                                                    0
                                                }
                                            })
                                            .sum();
                                        format!("{name}: ({rendered}/{total_count})")
                                    }}
                                </div>
                                <div class="text-sm text-gray-500">
                                    {move || if *is_open.read() { "▾" } else { "▸" }}
                                </div>
                            </div>
                        </button>
                        <label class="flex gap-1 items-center">
                            <input
                                type="checkbox"
                                class="w-4 h-4 cursor-pointer"
                                prop:checked=move || {
                                    let i = items.get();
                                    let read_checks = checks.get();
                                    i.iter().all(|item| *read_checks.get(item).unwrap_or(&true))
                                }
                                on:change=move |_| {
                                    checks
                                        .update(|map| {
                                            let items_list = items.get();
                                            let all_enabled = items_list
                                                .iter()
                                                .all(|item| *map.get(item).unwrap_or(&true));
                                            let target_state = !all_enabled;
                                            for item in &items_list {
                                                map.insert(*item, target_state);
                                            }
                                        });
                                    last_checked.update(|old| { *old += 1 });
                                }
                            />
                        </label>
                    </div>

                    <FilterType<
                    T,
                > is_open=is_open items=items checks=checks counts=counts />
                </div>
            })
        }
    }
}

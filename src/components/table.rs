use crate::components::icon::MaybeShowIcon;
use leptos::{html::Div, prelude::*};
use leptos_use::on_click_outside;
use vowlr_util::prelude::{ErrorRecord, TableHTML};

/// A table
#[component]
pub fn Table<T>(#[prop(into)] items: Signal<Vec<T>>) -> impl IntoView
where
    T: TableHTML + Send + Sync + 'static,
{
    view! {
        {move || {
            let stuff = items.read();
            view! {
                <table class="text-left text-sm font-light text-surface dark:text-white rounded border-solid border-collapse border-1 table-auto ">
                    <thead class="border-b border-neutral-200 font-medium dark:border-white/10">
                        {stuff
                            .iter()
                            .map(T::header)
                            .next()
                            .collect_view()}
                    </thead>
                    <tbody>
                        {stuff.iter().map(T::row).collect_view()}
                    </tbody>
                </table>
            }
                .into_any()
        }}
    }
}

use leptos::prelude::*;
use vowlr_util::prelude::TableHTML;

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
                <table class="text-sm font-light text-left rounded-sm border border-separate table-auto dark:text-white text-surface border-spacing-0 border-tools-table-outline">
                    <thead class="font-medium border-b border-neutral-200 dark:border-white/10">
                        {stuff.iter().map(T::header).next().collect_view()}
                    </thead>
                    <tbody>{stuff.iter().map(T::row).collect_view()}</tbody>
                </table>
            }
                .into_any()
        }}
    }
}

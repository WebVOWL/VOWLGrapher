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
    // view! {
    //     <table class="table-auto">
    //     {move || {
    //         let a = items.read();

    //         view!{
    //             a.iter().map(|item| item.row()).collect_view()
    //         }.into_any()
    //     }}
    //     </table>
    // }

    view! {
        {move || {
            let stuff =  items.read();
            view! {
                <table class="border-1 border-solid border-collapse table-auto md:table-fixed md:w-[80vw]">
                   {stuff.iter().map(|item| item.header()).next().collect_view()}
                   {stuff.iter().map(|item| item.row()).collect_view()}
                </table>
            }
                .into_any()
        }}
    }
}

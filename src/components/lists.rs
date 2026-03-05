use crate::components::icon::MaybeShowIcon;
use leptos::{html::Div, prelude::*};
use leptos_use::on_click_outside;

/// A generic list element.
///
/// The `children` use an "absolute" position. Use the "relative" position on
/// any parent element higher in the DOM tree to position `children` relative to it.
#[component]
pub fn ListElement(
    #[prop(into)] title: Signal<String>,
    #[prop(optional, into)] icon: MaybeProp<icondata::Icon>,
    children: Children,
) -> impl IntoView {
    let show_element = RwSignal::new(false);
    let target = NodeRef::<Div>::new();

    let _ = on_click_outside(target, move |_| show_element.update(|show| *show = false));
    view! {
        <li on:click=move |_| show_element.update(|show| *show = true)>
            <a
                href="#"
                class="flex gap-2 items-center py-2 px-4 text-gray-500 rounded-lg hover:text-gray-700 hover:bg-gray-100"
            >
                <MaybeShowIcon icon=icon></MaybeShowIcon>
                <span class="text-sm font-medium">{move || title.get()}</span>
            </a>
                <div
                    node_ref=target
                    class="overflow-y-scroll absolute top-0 left-full bg-white border-gray-100 w-fit max-h-[80vh] min-h-[80vh]"
                    style=move || {
                        if show_element.get() { "" } else { "display: none" }
                    }
                >
                    {children()}
                </div>
        </li>
    }
}

/// A list with a dropdown button containing children.
#[component]
pub fn ListDetails(
    #[prop(into)] title: String,
    #[prop(optional, into)] icon: MaybeProp<icondata::Icon>,
    children: Children,
) -> impl IntoView {
    view! {
        <li>
            <details class="group [&amp;_summary::-webkit-details-marker]:hidden">
                <summary class="flex justify-between items-center py-2 px-4 text-gray-500 rounded-lg cursor-pointer hover:text-gray-700 hover:bg-gray-100">
                    <div class="flex gap-2 items-center">
                        <MaybeShowIcon icon=icon></MaybeShowIcon>
                        <span class="text-sm font-medium">{title}</span>
                    </div>

                    <span class="transition duration-300 shrink-0 group-open:-rotate-180">
                        <MaybeShowIcon icon=icondata::BiChevronDownRegular></MaybeShowIcon>
                    </span>
                </summary>

                <ul class="px-4 mt-2 space-y-1">{children()}</ul>
            </details>
        </li>
    }
}

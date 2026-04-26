use crate::{
    components::{accordion::Accordion, user_input::internal_sparql::GraphDataContext},
    events::EventContext,
};
use leptos::prelude::*;

#[component]
pub fn SelectionDetails() -> impl IntoView {
    let GraphDataContext { graph_metadata, .. } = expect_context::<GraphDataContext>();
    let EventContext { show_metadata } = expect_context::<EventContext>();

    let comments = create_read_slice(graph_metadata, |graph_metadata| {
        graph_metadata.comments.clone()
    });

    let comment = Memo::new(move |_| {
        if let Some(idx) = *show_metadata.read() {
            comments
                .read()
                .get(&idx)
                .cloned()
                .unwrap_or_else(|| Vec::from(["No".to_string()]))
        } else {
            Vec::new()
        }
    });

    view! {
        <Accordion title="Selection Details">
            <Show
                when=move || show_metadata.get().is_some()
                fallback=|| {
                    view! { <p>"Select an element in the visualization."</p> }
                }
            >
                <p>"Comment: "{move || comment.get()}</p>
            </Show>
        </Accordion>
    }
}

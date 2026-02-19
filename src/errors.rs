use leptos::prelude::RwSignal;

#[derive(Clone)]
pub struct ErrorLogContext {
    pub errors: RwSignal<Vec<String>>,
}

use leptos::IntoView;

pub trait TableHTML {
    fn header(&self) -> impl IntoView;
    fn row(&self) -> impl IntoView;
}

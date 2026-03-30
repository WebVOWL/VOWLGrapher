use leptos::IntoView;

/// A trait allowing implementors to be visualized as an HTML table in Leptos.
pub trait TableHTML {
    /// The table header.
    fn header(&self) -> impl IntoView;
    /// A row in the table.
    fn row(&self) -> impl IntoView;
}

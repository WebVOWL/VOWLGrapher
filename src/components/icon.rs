use leptos::{either::Either, prelude::*, svg};

/// The Icon component.
#[component]
pub fn Icon(
    /// The icon to render.
    #[prop(into)]
    icon: Signal<icondata::Icon>,
    #[prop(into, optional)] class: MaybeProp<String>,
    #[prop(into, optional)] style: MaybeProp<String>,
    #[prop(into, optional)] width: MaybeProp<String>,
    #[prop(into, optional)] height: MaybeProp<String>,
) -> impl IntoView {
    move || {
        let icon = *icon.read();
        svg::svg()
            .style(match (style.get(), icon.style) {
                (Some(a), Some(b)) => Some(format!("{b} {a}")),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b.to_string()),
                _ => None,
            })
            .class(class.get())
            .attr("x", icon.x)
            .attr("y", icon.y)
            .attr("width", width.get().unwrap_or_else(|| "1em".to_string()))
            .attr("height", height.get().unwrap_or_else(|| "1em".to_string()))
            .attr("viewBox", icon.view_box)
            .attr("stroke-linecap", icon.stroke_linecap)
            .attr("stroke-linejoin", icon.stroke_linejoin)
            .attr("stroke-width", icon.stroke_width)
            .attr("stroke", icon.stroke)
            .attr("fill", icon.fill.unwrap_or("currentColor"))
            .attr("role", "graphics-symbol")
            .inner_html(icon.data) // Using inner_html due to https://github.com/carloskiki/leptos-icons/issues/64
        // .child(svg::InertElement::new(icon.data))
    }
}

/// Show the icon if given.
#[component]
pub fn MaybeShowIcon(#[prop(optional, into)] icon: MaybeProp<icondata::Icon>) -> impl IntoView {
    move || {
        icon.read().map_or_else(
            || Either::Right(()),
            |ico| Either::Left(view! { <Icon icon=ico></Icon> }),
        )
    }
}

mod about_menu;
mod error_log;
mod export_menu;
mod filter_menu;
mod ontology_menu;
mod options_menu;

// mod search_menu;1
use crate::components::lists::{ListDetails, ListElement};
use crate::components::menu::vertical_menu::VerticalMenu;
use crate::components::user_input::internal_sparql::GraphDataContext;
use crate::errors::ErrorLogContext;
use about_menu::AboutMenu;
use error_log::ErrorMenu;
use export_menu::ExportMenu;
use filter_menu::FilterMenu;
use leptos::prelude::*;
use ontology_menu::OntologyMenu;
use options_menu::OptionsMenu;
// use search_menu::SearchMenu;

#[component]
fn WorkbenchMenuItems(#[prop(into)] title: String, children: Children) -> impl IntoView {
    view! {
        <div class="flex flex-col justify-center p-2 min-w-250 md:min-w-[30vw]">
            <div class="workbench-menu-header">
                <h3>{title}</h3>
            </div>
            {children()}
        </div>
    }
}

#[component]
pub fn NewWorkbench() -> impl IntoView {
    let error_context = ErrorLogContext::default();
    provide_context(error_context);

    let graph_context = GraphDataContext::default();
    provide_context(graph_context);

    let error_title = Signal::derive(move || {
        let count = error_context.len();
        if count > 0 {
            format!("Error Log ({count})")
        } else {
            "Error Log".to_string()
        }
    });

    view! {
        <VerticalMenu>
            <ListElement title="Load Ontology" icon=icondata::BiMenuRegular>
                <OntologyMenu />
            </ListElement>

            // <ListElement title="Search" icon=icondata::BiMenuRegular>
            // <SearchMenu />
            // </ListElement>

            <ListElement title="Filter" icon=icondata::BiMenuRegular>
                <FilterMenu />
            </ListElement>

            <ListElement title="Export" icon=icondata::BiMenuRegular>
                <ExportMenu />
            </ListElement>

            <ListDetails title="Settings" icon=icondata::IoSettingsOutline>
                <ListElement title="Simulator">
                    <OptionsMenu />
                </ListElement>
            </ListDetails>

            <ListElement title="About" icon=icondata::BiMenuRegular>
                <AboutMenu />
            </ListElement>
            <ListElement title=error_title icon=icondata::BiErrorAltRegular>
                <ErrorMenu />
            </ListElement>
        </VerticalMenu>
    }
}

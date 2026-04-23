use crate::errors::{ClientErrorKind, ErrorLogContext};
use crate::events::{EventContext, EventHandler};
use crate::pages::home::Home;
use leptos::prelude::*;
use leptos::task::spawn_local_scoped_with_cancellation;
use leptos_meta::Link;
use leptos_meta::{Stylesheet, provide_meta_context};
use leptos_router::{
    StaticSegment,
    components::{FlatRoutes, Route, Router},
};
use vowlgrapher_util::prelude::{VOWLGrapherEnviron, environ};

#[component]
pub fn App() -> impl IntoView {
    provide_meta_context();

    // Initialize errorlog context
    let error_context = ErrorLogContext::default();
    provide_context(error_context);

    // Initialize event context
    let event_context = EventContext::new();
    provide_context(event_context);

    // Try to fetch environment from server. Use the defaults on failure.
    spawn_local_scoped_with_cancellation(async move {
        let environ = match environ().await {
            Ok(env) => env,
            Err(e) => {
                error_context.push(
                    ClientErrorKind::EnvironmentFetchError(
                        "Failed to fetch server environment. Using the default environment, which most likely differs from the server".to_string(),
                        e.into(),
                    )
                    .into(),
                );
                VOWLGrapherEnviron::default()
            }
        };
        provide_context(environ);
    });

    // Initiate the event handler
    spawn_local_scoped_with_cancellation(async move { EventHandler::handle_event().await });

    view! {
        <Stylesheet id="vowlgrapher" href="/pkg/vowlgrapher.css" />
        <Link rel="shortcut icon" type_="image/ico" href="/favicon.ico" />
        <Router>
            <FlatRoutes fallback=|| "404 - Page not found.">
                <Route path=StaticSegment("") view=Home />
            </FlatRoutes>
        </Router>
    }
}

#![allow(non_snake_case)]

use anyhow::anyhow;
use std::sync::{Arc, Mutex};

use actix_files::Files;
use actix_session::{SessionMiddleware, storage::CookieSessionStore};
use actix_web::cookie::Key;
use actix_web::web::Data;
use actix_web::{App, HttpServer, middleware, web};
use env_logger::Env;
use leptos::prelude::*;
use leptos_actix::{LeptosRoutes, generate_route_list};
use leptos_meta::MetaTags;
use log::info;
use vowlgrapher::app::App;
use vowlgrapher::env::environ;
use vowlgrapher::hydration_scripts::HydrationScripts as Hydro;
use vowlgrapher_database::prelude::{UserSessionExpiries, cleanup_task};

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("trace")).init();

    let pkg_name = env!("CARGO_PKG_NAME");
    let pkg_version = env!("CARGO_PKG_VERSION");

    info!("Starting {pkg_name} server [v{pkg_version}]");

    let conf = get_configuration(None)?;
    let addr = conf.leptos_options.site_addr;

    let secret_key = Key::generate();

    let session_expiries = Arc::new(Mutex::new(UserSessionExpiries::default()));
    let env = environ().await.map_err(|e| anyhow!(e))?;

    let (cleanup_handle, cleanup_cancel) =
        cleanup_task(session_expiries.clone(), env.database_cleanup_interval);

    HttpServer::new(move || {
        // Generate the list of routes in your Leptos App
        let routes = generate_route_list(App);
        let leptos_options = &conf.leptos_options;
        let site_root = &leptos_options.site_root;


        App::new()
            .app_data(web::PayloadConfig::new(1024 * 1024 * 1024))
            .app_data(web::FormConfig::default().limit(1024 * 1024 * 1024))
            .app_data(Data::from(Arc::clone(&session_expiries)))
            .leptos_routes(routes, {
                let leptos_options = leptos_options.clone();
                move || {
                    use leptos::prelude::*;
                    use leptos_use::use_preferred_dark;

                    let is_dark = use_preferred_dark();

                    view! {
                        <!DOCTYPE html>
                        <html class=("dark", move || { *is_dark.read() }) lang="en">
                            <head>
                                <meta charset="utf-8" />
                                <meta description="WebVOWL rebuilt from stratch with a strong focus on performance and scalability" />
                                <meta
                                    name="viewport"
                                    content="width=device-width, initial-scale=1"
                                />
                                <meta apple-mobile-web-app-capable="yes" />
                                <AutoReload options=leptos_options.clone() />
                                <Hydro options=leptos_options.clone() />
                                <MetaTags />
                            </head>
                            <body>
                                <App />
                            </body>
                        </html>
                    }
                }
            })
            .service(Files::new("/", site_root.as_ref()))
            .wrap(middleware::Compress::default())
            .wrap(SessionMiddleware::builder(CookieSessionStore::default(), secret_key.clone())
                .cookie_secure(!cfg!(debug_assertions))
                .build()
            )
            .wrap(
                middleware::DefaultHeaders::new()
                    .add(("Cross-Origin-Opener-Policy", "same-origin"))
                    .add(("Cross-Origin-Embedder-Policy", "require-corp")),
            )
    })
    .bind(&addr)?
    .run()
    .await?;

    cleanup_cancel.cancel();
    cleanup_handle.await?;

    Ok(())
}

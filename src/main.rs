#![allow(non_snake_case)]

use actix_files::Files;
use actix_session::{SessionMiddleware, storage::CookieSessionStore};
use actix_web::cookie::Key;
use actix_web::{App, HttpServer, middleware, web};
use env_logger::Env;
use leptos::prelude::*;
use leptos_actix::{LeptosRoutes, generate_route_list};
use leptos_meta::MetaTags;
use log::info;
use vowlr::app::App;
use vowlr::hydration_scripts::HydrationScripts as Hydro;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("trace")).init();

    let pkg_name = env!("CARGO_PKG_NAME");
    let pkg_version = env!("CARGO_PKG_VERSION");

    info!("Starting {pkg_name} server [v{pkg_version}]");

    // TODO: properly handle this error
    #[expect(clippy::expect_used)]
    let conf = get_configuration(None).expect("could not load config");
    let addr = conf.leptos_options.site_addr;

    let secret_key = Key::generate();

    HttpServer::new(move || {
        // Generate the list of routes in your Leptos App
        let routes = generate_route_list(App);
        let leptos_options = &conf.leptos_options;
        let site_root = &leptos_options.site_root;


        App::new()
            .app_data(web::PayloadConfig::new(1024 * 1024 * 1024))
            .app_data(web::FormConfig::default().limit(1024 * 1024 * 1024))
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
                .cookie_secure(false)
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
    .await
}

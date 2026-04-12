// Leptos' view macro tends to be heavy on recursion, especially with many nested views.
#![recursion_limit = "256"]
// For JS naming compatibility
#![allow(non_snake_case)]

// The entry-point of the client-side graph renderer.
#[cfg(all(target_family = "wasm", target_os = "unknown"))]
pub use grapher::init_render;

// Expose an async initThreadPool function in the final generated JavaScript.
// You'll need to invoke it right after instantiating your module on the main
// thread in order to prepare the threadpool before calling into actual library functions.
#[cfg(all(target_family = "wasm", target_os = "unknown"))]
pub use wasm_bindgen_rayon::init_thread_pool;

pub mod app;
pub mod blocks;
pub mod components;
pub mod env;
pub mod errors;
pub mod hydration_scripts;
pub mod pages;

// Hydration code for the frontend.
#[cfg(feature = "hydrate")]
#[wasm_bindgen::prelude::wasm_bindgen]
pub fn hydrate() {
    use crate::app::App;
    use log::info;
    console_error_panic_hook::set_once();
    if let Err(e) = console_log::init_with_level(log::Level::Info) {
        log::error!("Failed to initialize console logger: {e}");
    }
    leptos::mount::hydrate_body(App);
    info!("Hydration complete");
}

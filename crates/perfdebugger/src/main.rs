//! Run this workspace natively, i.e., without a browser.

use env_logger::Env;
#[allow(unused)]
use grapher::prelude::{EVENT_DISPATCHER, RenderEvent};
#[allow(unused)]
use grapher::run;
use std::env;
use std::path::Path;
use vowlgrapher_database::prelude::VOWLGrapherStore;
use vowlgrapher_sparql_queries::prelude::DEFAULT_QUERY;

#[expect(
    clippy::expect_used,
    reason = "code not running in production is allowed to panic"
)]
#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let args = env::args().collect::<Vec<String>>();
    if args.len() > 1 {
        let path = Path::new(&args[1]);

        let store = VOWLGrapherStore::default();
        store
            .insert_file(path, false)
            .await
            .expect("Error inserting file");

        let (data, _) = store
            .query(DEFAULT_QUERY.to_string(), Some(args[1].clone()))
            .await
            .expect("querying the store should succeed");

        // Uncomment to enable rendering
        EVENT_DISPATCHER
            .rend_write_chan
            .send(RenderEvent::LoadGraph(Box::new(data)))
            .expect("sending events should succeed");
    }
    // Uncomment to enable rendering
    run().expect("rendering graph should succeed");
}

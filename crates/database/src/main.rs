//! Run the database in standalone mode.
//!
//! Only the parser and graph database are loaded.

use env_logger::Env;
use std::env;
use std::path::Path;
use vowlgrapher_database::prelude::VOWLGrapherStore;
use vowlgrapher_sparql_queries::prelude::DEFAULT_QUERY;

/// Entrypoint
#[expect(
    clippy::expect_used,
    reason = "code not running in production is allowed to panic"
)]
#[tokio::main]
pub async fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let args = env::args().collect::<Vec<String>>();
    let path = if args.len() > 1 {
        Path::new(&args[1])
    } else {
        Path::new("crates/database/owl1-unions-simple.owl")
    };
    let store = VOWLGrapherStore::default();
    store
        .insert_file(path, false)
        .await
        .expect("Error inserting file");
    store
        .query(
            DEFAULT_QUERY.to_string(),
            Some((*path.to_string_lossy()).to_owned()),
        )
        .await
        .expect("querying the store should succeed");
}

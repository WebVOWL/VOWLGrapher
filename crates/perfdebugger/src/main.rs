use env_logger::Env;
use grapher::prelude::{EVENT_DISPATCHER, RenderEvent};
use grapher::run;
use lovet_database::prelude::LOVETStore;
use lovet_sparql_queries::prelude::DEFAULT_QUERY;
use perfdebugger::util::query;
use std::env;
use std::path::Path;

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let args = env::args().collect::<Vec<String>>();
    if args.len() > 1 {
        let path = Path::new(&args[1]);

        let store = LOVETStore::default();
        store
            .insert_file(path, false)
            .await
            .expect("Error inserting file");

        let data = query(DEFAULT_QUERY.to_string()).await.unwrap();
        EVENT_DISPATCHER
            .rend_write_chan
            .send(RenderEvent::LoadGraph(data))
            .unwrap();
    }
    run().unwrap();
}

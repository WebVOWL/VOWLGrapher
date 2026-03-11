use env_logger::Env;
use futures::StreamExt;
use grapher::prelude::GraphDisplayData;
use log::info;
use rdf_fusion::{execution::results::QueryResults, store::Store};
use std::env;
use std::path::Path;
use vowlr_database::prelude::{GraphDisplayDataSolutionSerializer, VOWLRStore};
use vowlr_sparql_queries::prelude::DEFAULT_QUERY;

#[tokio::main]
pub async fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let session = Store::default();
    let args = env::args().collect::<Vec<String>>();
    let path = if args.len() > 1 {
        Path::new(&args[1])
    } else {
        Path::new("crates/database/owl1-unions-simple.owl")
    };
    let vowlr = VOWLRStore::new(session);
    vowlr
        .insert_file(path, false)
        .await
        .expect("Error inserting file");
    info!("Loaded {} quads", vowlr.session.len().await.unwrap());

    let all_stream = vowlr
        .session
        .query("SELECT * WHERE { ?s ?p ?o }")
        .await
        .unwrap();
    if let QueryResults::Solutions(mut solutions) = all_stream {
        while let Some(solution) = solutions.next().await {
            let solution = solution.unwrap();
            let Some(s) = solution.get("s") else {
                continue;
            };
            let Some(p) = solution.get("p") else {
                continue;
            };
            let Some(o) = solution.get("o") else {
                continue;
            };
            info!("{} - {} - {}", s, p, o);
        }
    } else {
        panic!("Query stream is not a solutions stream");
    }

    let mut data_buffer = GraphDisplayData::new();
    let solution_serializer = GraphDisplayDataSolutionSerializer::new();
    let query_stream = vowlr.session.query(DEFAULT_QUERY.as_str()).await.unwrap();
    if let QueryResults::Solutions(solutions) = query_stream {
        solution_serializer
            .serialize_nodes_stream(&mut data_buffer, solutions)
            .await
            .unwrap();
    } else {
        panic!("Query stream is not a solutions stream");
    }
    info!("--- GraphDisplayData ---");
    print_graph_display_data(&data_buffer);
}

pub fn print_graph_display_data(data_buffer: &GraphDisplayData) {
    info!("--- Elements ---");
    for (index, (element, label)) in data_buffer
        .elements
        .iter()
        .zip(data_buffer.labels.iter())
        .enumerate()
    {
        info!("{index}: {element:?} -> {label:?}");
    }
    info!("--- Edges ---");
    for edge in data_buffer.edges.iter() {
        info!(
            "{:?} -> {:?} -> {:?}",
            data_buffer.labels[edge[0]], data_buffer.elements[edge[1]], data_buffer.labels[edge[2]]
        );
    }
    info!("--- Characteristics ---");
    for (iri, characteristics) in data_buffer.characteristics.iter() {
        info!("{} -> {:?}", iri, characteristics);
    }
    info!("--- Cardinalities ---");
    for (iri, cardinality) in data_buffer.cardinalities.iter() {
        info!("{} -> {:?}", iri, cardinality);
    }
}

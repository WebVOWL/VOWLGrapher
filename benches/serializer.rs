#![expect(clippy::expect_used, reason = "Benching is allowed to panic")]

use criterion::{Criterion, criterion_group, criterion_main};
use std::path::Path;
use vowlr_database::prelude::VOWLRStore;
use vowlr_sparql_queries::prelude::DEFAULT_QUERY;

/// Parallel serialization.
fn par_serialize(c: &mut Criterion) {
    let path = Path::new("crates/database/data/owl-rdf/envo.owl");
    let store = VOWLRStore::default();

    pollster::block_on(async {
        store
            .insert_file(path, false)
            .await
            .expect("Error inserting file");
    });

    let mut group = c.benchmark_group("serializer");
    group.bench_function("Insert", |b| {
        b.to_async(tokio::runtime::Runtime::new().expect("runtime should work"))
            .iter(async || {
                // TODO: Make the store use the parallel version
                let _ = store
                    .query(DEFAULT_QUERY.to_string())
                    .await
                    .expect("query should work");
            });
    });
    group.finish();
}

criterion_group!(serializer, par_serialize);
criterion_main!(serializer);

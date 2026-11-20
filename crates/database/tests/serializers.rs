//! Test the serializer by comparing to the sovs test suite

#![expect(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "tests are allowed to panic"
)]

use anyhow::{Context, anyhow, ensure};
use log::{error, info};
use rdf_fusion::store::Store;
use sovs_parser::{Specification, TestCase};
use std::{io::Cursor, path::Path};
use tokio::task::JoinSet;
use vowlgrapher_database::prelude::VOWLGrapherStore;
use vowlgrapher_parser::parser_util::{self, path_type};
use vowlgrapher_sparql_queries::prelude::DEFAULT_QUERY;

async fn test_sovs_spec(test_case: TestCase) -> anyhow::Result<()> {
    let session = Store::new_in_memory().await;
    let store = VOWLGrapherStore::new(session);
    let path = Path::new(test_case.name);
    let graph_iri = store.get_graph_name(test_case.name);
    let quads = parser_util::parser_from_reader(
        Cursor::new(test_case.text),
        path_type(path).context("test case should have valid format")?,
        false,
        &graph_iri,
    )
    .expect("quads should be loaded");
    store
        .session
        .extend(quads)
        .await
        .context("store should load quads")?;
    info!("Loaded {} quads", store.session.len().await.unwrap());
    let (display_data, e) = store
        .query(DEFAULT_QUERY.to_string(), Some(test_case.name.to_owned()))
        .await
        .unwrap();
    if let Some(e) = e {
        error!("Errors serializing ontology: {e:?}");
    }
    let spec = Specification::try_from(display_data).context("spec should be built properly")?;
    ensure!(
        spec.is_isomorphic_to(&test_case.specification),
        "{spec:#?} should be isomorphic to {:#?}",
        test_case.specification
    );
    Ok(())
}

#[tokio::test]
async fn test_sovs_specs() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let mut joins = JoinSet::new();
    for test_case in sovs_parser::test_cases() {
        joins.spawn(async move {
            {
                (
                    test_case.name.to_string(),
                    test_sovs_spec(test_case.clone())
                        .await
                        .with_context(|| test_case.name.to_string()),
                )
            }
        });
    }

    let should_fail = [
        // Issue #211
        "and.ofn",
        "not.ofn",
        // Issue #210
        "object-property-assertion.ofn",
        "object-property-asymmetric.ofn",
        "object-property-functional.ofn",
        "object-property-inverse-functional.ofn",
        "object-property-irreflexive.ofn",
        "object-property-reflexive.ofn",
        "object-property-symmetric.ofn",
        "object-property-transitive.ofn",
        // Issue #103
        "only.ofn",
        // Issue #211
        "or.ofn",
        // Issue #103
        "recursing_class.ofn",
        "some.ofn",
        "and.owl",
        "not.owl",
        "object-property-assertion.owl",
        "object-property-asymmetric.owl",
        "object-property-functional.owl",
        "object-property-inverse-functional.owl",
        "object-property-irreflexive.owl",
        "object-property-reflexive.owl",
        "object-property-symmetric.owl",
        "object-property-transitive.owl",
        "only.owl",
        "or.owl",
        "recursing_class.owl",
        "some.owl",
        "and.ttl",
        "external-class.ttl",
        "not.ttl",
        "object-property-assertion.ttl",
        "object-property-asymmetric.ttl",
        "object-property-functional.ttl",
        "object-property-inverse-functional.ttl",
        "object-property-irreflexive.ttl",
        "object-property-reflexive.ttl",
        "object-property-symmetric.ttl",
        "object-property-transitive.ttl",
        "only.ttl",
        "or.ttl",
        "recursing_class.ttl",
        "some.ttl",
        "and.owx",
        "external-class.owx",
        "not.owx",
        "object-property-assertion.owx",
        "object-property-asymmetric.owx",
        "object-property-functional.owx",
        "object-property-inverse-functional.owx",
        "object-property-irreflexive.owx",
        "object-property-reflexive.owx",
        "object-property-symmetric.owx",
        "object-property-transitive.owx",
        "only.owx",
        "or.owx",
        "recursing_class.owx",
        "some.owx",
    ];
    let results = joins.join_all().await;
    let fails: Vec<_> = results
        .into_iter()
        .filter_map(|(case, r)| {
            if should_fail.contains(&&*case) {
                if r.is_ok() {
                    Some(anyhow!("test case {case} should fail but succeeded"))
                } else {
                    None
                }
            } else {
                r.err()
            }
        })
        .collect();
    for fail in &fails {
        error!("test case failed {fail:#}");
    }
    assert!(fails.is_empty(), "some test cases failed");
}

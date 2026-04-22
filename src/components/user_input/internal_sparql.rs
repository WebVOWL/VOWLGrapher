use std::{collections::HashMap, mem::take, sync::Arc};

use crate::errors::{ClientErrorKind, ErrorLogContext};
use grapher::prelude::{
    EVENT_DISPATCHER, ElementType, GraphDisplayData, GraphMetadata, RenderEvent,
};
use leptos::{prelude::*, server_fn::codec::Rkyv};
use log::debug;
#[cfg(feature = "server")]
use vowlgrapher_database::prelude::VOWLGrapherStore;
use vowlgrapher_util::prelude::VOWLGrapherError;
#[cfg(feature = "ssr")]
use vowlgrapher_util::prelude::manage_user_id;

#[server (input = Rkyv, output = Rkyv)]
pub async fn handle_internal_sparql(
    query: String,
    graph_name: String,
) -> Result<(GraphDisplayData, Option<VOWLGrapherError>), VOWLGrapherError> {
    let store = VOWLGrapherStore::new_for_user(manage_user_id().await?);
    store.query(query, Some(graph_name)).await
}

pub async fn load_graph(query: String, clean_load: bool) {
    let error_context = expect_context::<ErrorLogContext>();
    let GraphDataContext {
        element_counts,
        element_checks,
        active_graph_name,
        graph_metadata,
    } = expect_context::<GraphDataContext>();
    let graph_name = active_graph_name.get_untracked();
    debug!("Loading graph with name: {graph_name}");
    match handle_internal_sparql(query, graph_name.clone()).await {
        Ok((mut result, non_fatal_error)) => {
            if clean_load {
                let new_graph_data = take(&mut result.graph_metadata);
                let new_context = GraphDataContext::new(&result, new_graph_data, graph_name);
                element_counts
                    .update(|counts| *counts = new_context.element_counts.get_untracked());
                element_checks
                    .update(|checks| *checks = new_context.element_checks.get_untracked());
                active_graph_name
                    .update(|name| *name = new_context.active_graph_name.get_untracked());
                graph_metadata
                    .update(|metadata| *metadata = new_context.graph_metadata.get_untracked());
            }

            if let Err(e) = EVENT_DISPATCHER
                .rend_write_chan
                .send(RenderEvent::LoadGraph(Box::new(result)))
            {
                error_context.push(ClientErrorKind::EventHandlingError(e.to_string()).into());
            }
            if let Some(e) = non_fatal_error {
                error_context.extend(e.records);
            }
        }
        Err(e) => {
            error_context.extend(e.records);
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct GraphDataContext {
    pub element_counts: RwSignal<HashMap<ElementType, usize>>,
    pub element_checks: RwSignal<HashMap<ElementType, bool>>,
    pub active_graph_name: RwSignal<String>,
    pub graph_metadata: RwSignal<Arc<GraphMetadata>>,
}

impl GraphDataContext {
    pub fn new(
        graph_data: &GraphDisplayData,
        graph_metadata: GraphMetadata,
        graph_name: String,
    ) -> Self {
        let mut element_counts: HashMap<ElementType, usize> = HashMap::new();
        let mut element_checks: HashMap<ElementType, bool> = HashMap::new();
        for element in &graph_data.elements {
            *element_counts.entry(*element).or_insert(0) += 1;
        }
        for k in element_counts.keys() {
            element_checks.insert(*k, true);
        }
        Self {
            element_counts: RwSignal::new(element_counts),
            element_checks: RwSignal::new(element_checks),
            active_graph_name: RwSignal::new(graph_name),
            graph_metadata: RwSignal::new(graph_metadata.into()),
        }
    }
}

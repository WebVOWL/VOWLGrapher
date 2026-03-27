use std::collections::HashMap;

use grapher::prelude::{EVENT_DISPATCHER, ElementType, GraphDisplayData, RenderEvent};
use leptos::{prelude::*, server_fn::codec::Rkyv};
#[cfg(feature = "server")]
use vowlr_database::prelude::VOWLRStore;
use vowlr_util::prelude::VOWLRError;

use crate::errors::{ClientErrorKind, ErrorLogContext};

#[server (input = Rkyv, output = Rkyv)]
pub async fn handle_internal_sparql(
    query: String,
) -> Result<(GraphDisplayData, Option<VOWLRError>), VOWLRError> {
    let store = VOWLRStore::default();
    store.query(query).await
}

pub async fn load_graph(query: String, clean_load: bool) {
    let error_context = expect_context::<ErrorLogContext>();

    match handle_internal_sparql(query).await {
        Ok((result, non_fatal_error)) => {
            if clean_load {
                let GraphDataContext {
                    element_counts,
                    element_checks,
                } = expect_context::<GraphDataContext>();
                let new_context = GraphDataContext::new(&result);
                element_counts.update(|counts| *counts = new_context.element_counts.get());
                element_checks.update(|checks| *checks = new_context.element_checks.get());
            }

            if let Err(e) = EVENT_DISPATCHER
                .rend_write_chan
                .send(RenderEvent::LoadGraph(result))
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
}

impl GraphDataContext {
    pub fn new(graph_data: &GraphDisplayData) -> Self {
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
        }
    }
}

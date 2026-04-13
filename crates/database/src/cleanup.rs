//! Cleanup jobs for the database.
//!
//! Anytime a user uploads a file, we need to keep that file in the database until the user is
//! done. However, we might not be notified that the user is done, so we will occassionally clean
//! up files that are too old.

use log::{debug, error};
use rdf_fusion::model::NamedNode;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::store::GLOBAL_STORE;

/// Map from graph name to time of expiry
#[derive(Default, Debug)]
pub struct UserSessionExpiries(pub HashMap<NamedNode, Instant>);

impl UserSessionExpiries {
    /// Insert a session into the cache with a given duration.
    /// The session will be removed at the first cleanup where it has expired.
    ///
    /// `graph` must be a valid IRI
    pub fn insert_with_duration(&mut self, graph: &str, duration: Duration) {
        let Ok(key) = NamedNode::new(graph) else {
            error!("cannot add cleanup job. `{graph}` is not a valid IRI");
            return;
        };
        let now = Instant::now();
        self.0.insert(key, now + duration);
    }
}

/// Get a task for cleaning up the sessions in `ages` every `interval`
pub fn cleanup_task(
    ages: Arc<Mutex<UserSessionExpiries>>,
    interval: Duration,
) -> (JoinHandle<()>, CancellationToken) {
    let token = CancellationToken::new();
    (
        tokio::spawn(run_cleanup(ages, interval, token.clone())),
        token,
    )
}

async fn run_cleanup(
    ages: Arc<Mutex<UserSessionExpiries>>,
    interval: Duration,
    stop_signal: CancellationToken,
) {
    loop {
        debug!("running cleanup");
        // scoped so we don't hold the mutex lock over awaits
        let to_remove = {
            let mut ages = match ages.as_ref().lock() {
                Ok(ages) => ages,
                Err(e) => {
                    error!(
                        "user ages mutex was poisoned! database cleanup jobs will no longer run. error: {e:?}"
                    );
                    return;
                }
            };
            let remove_start_time = Instant::now();

            let to_remove: HashSet<_> = ages
                .0
                .iter()
                .filter(|(_, expire_time)| **expire_time > remove_start_time)
                .map(|(graph_name, _)| graph_name)
                .cloned()
                .collect();

            ages.0
                .retain(|graph_name, _| !to_remove.contains(graph_name));
            to_remove
        };

        // PERF: these can probably be run in parallel with a `JoinSet`
        for graph_name in to_remove {
            if let Err(e) = GLOBAL_STORE.remove_named_graph(&graph_name).await {
                error!("error while cleaning named graph {graph_name}: {e:?}");
            }
        }

        tokio::select! {
            // PERF: sleep interval should maybe have some jitter
            () = tokio::time::sleep(interval) => {}
            () = stop_signal.cancelled() => {
                break;
            }
        };
    }
}

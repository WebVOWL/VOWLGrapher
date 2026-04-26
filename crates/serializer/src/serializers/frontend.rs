use std::{mem::take, num::NonZero, thread::available_parallelism, time::Instant};

use crate::{
    datastructures::serialization_data_buffer::SerializationDataBuffer,
    errors::{SerializationError, SerializationErrorKind},
    serializer_util::{
        buffers::check_all_unknowns, entity_creation::create_triple_from_id, labels::insert_label,
        serialize_triple::serialize_triple,
    },
};
use futures::StreamExt;
use grapher::prelude::GraphDisplayData;
use log::{debug, error, info};

use rayon::ThreadPoolBuilder;
use rdf_fusion::execution::results::{QuerySolution, QuerySolutionStream};

use vowlgrapher_parser::errors::VOWLGrapherStoreError;
use vowlgrapher_util::prelude::{ErrorRecord, VOWLGrapherError};

/// Serializes a [`QuerySolutionStream`] into a [`GraphDisplayData`].
#[derive(Default)]
pub struct GraphDisplayDataSolutionSerializer;

impl GraphDisplayDataSolutionSerializer {
    /// Creates an instance of [`self`]
    pub const fn new() -> Self {
        Self
    }

    /// Serializes a query solution stream into the data buffer using all available threads.
    ///
    /// This method tries to continue serializing despite errors.
    /// As such, the `Ok` value contains non-fatal errors encountered during
    /// serialization. The `Err` value contains fatal errors, preventing serialization.
    ///
    /// # Note
    /// The performance of this method is currently less than the single-threaded variant [`Self::serialize_solution_stream`].
    ///
    /// # Errors
    /// Returns any fatal error encountered while serializing.
    pub async fn par_serialize_solution_stream(
        &self,
        data: &mut GraphDisplayData,
        mut solution_stream: QuerySolutionStream,
    ) -> Result<Option<VOWLGrapherError>, VOWLGrapherError> {
        let thread_count = available_parallelism()
            .unwrap_or(NonZero::new(1).ok_or_else(|| {
                SerializationErrorKind::ThreadPoolFailure(
                    "Threadpool initialized with illegal thread count".to_string(),
                )
            })?)
            .into();

        info!("Serializing query solution stream using {thread_count} threads...");

        // TODO: Make a global threadpool instead of making a new one for each call to this method.
        // Should prolly work together with PR #223.
        let pool = ThreadPoolBuilder::new()
            .num_threads(thread_count)
            .build()
            .map_err(|e| <SerializationError as Into<VOWLGrapherError>>::into(e.into()))?;

        let mut count: u64 = 0;
        let mut data_buffer = SerializationDataBuffer::new();
        let mut query_time = None;
        let start_time = Instant::now();

        while let Some(maybe_solution) = solution_stream.next().await {
            if query_time.is_none() {
                query_time = Some(Instant::now());
            }

            let solution = match maybe_solution {
                Ok(solution) => solution,
                Err(e) => {
                    data_buffer
                        .failed_buffer
                        .write()
                        .map_err(|pe| {
                            <SerializationError as Into<VOWLGrapherError>>::into(pe.into())
                        })?
                        .push(<VOWLGrapherStoreError as Into<ErrorRecord>>::into(e.into()));
                    continue;
                }
            };

            pool.install(|| Self::serialize_solution(&solution, &mut data_buffer))?;

            count += 1;
        }

        check_all_unknowns(&mut data_buffer).or_else(|e| {
            data_buffer
                .failed_buffer
                .write()
                .map_err(|pe| <SerializationError as Into<VOWLGrapherError>>::into(pe.into()))?
                .push(e.into());
            Ok::<(), VOWLGrapherError>(())
        })?;

        // Catch permanently unresolved triples
        for (term_id, triples) in data_buffer
            .unknown_buffer
            .write()
            .map_err(|pe| <SerializationError as Into<VOWLGrapherError>>::into(pe.into()))?
            .drain()
        {
            for triple in triples {
                let e: SerializationError = SerializationErrorKind::SerializationFailedTriple(
                    data_buffer.term_index.display_triple(&triple)?,
                    format!("Unresolved reference: could not map '{term_id}'"),
                )
                .into();
                data_buffer
                    .failed_buffer
                    .write()
                    .map_err(|pe| <SerializationError as Into<VOWLGrapherError>>::into(pe.into()))?
                    .push(e.into());
            }
        }

        let all_errors =
            Self::post_serialization_cleanup(data, data_buffer, start_time, query_time, count)
                .map_err(<SerializationError as Into<VOWLGrapherError>>::into)?;

        Ok(all_errors)
    }

    /// Serializes a query solution stream into the data buffer.
    ///
    /// This method tries to continue serializing despite errors.
    /// As such, the `Ok` value contains non-fatal errors encountered during
    /// serialization. The `Err` value contains fatal errors, preventing serialization.
    ///
    /// # Errors
    /// Returns any fatal error encountered while serializing.
    pub async fn serialize_solution_stream(
        &self,
        data: &mut GraphDisplayData,
        mut solution_stream: QuerySolutionStream,
    ) -> Result<Option<VOWLGrapherError>, VOWLGrapherError> {
        info!("Serializing query solution stream...");
        let mut count: u64 = 0;
        let mut data_buffer = SerializationDataBuffer::new();
        let mut query_time = None;
        let start_time = Instant::now();

        while let Some(maybe_solution) = solution_stream.next().await {
            if query_time.is_none() {
                query_time = Some(Instant::now());
            }

            let solution = match maybe_solution {
                Ok(solution) => solution,
                Err(e) => {
                    data_buffer
                        .failed_buffer
                        .write()
                        .map_err(|pe| {
                            <SerializationError as Into<VOWLGrapherError>>::into(pe.into())
                        })?
                        .push(<VOWLGrapherStoreError as Into<ErrorRecord>>::into(e.into()));
                    continue;
                }
            };

            Self::serialize_solution(&solution, &mut data_buffer)?;

            count += 1;
        }

        check_all_unknowns(&mut data_buffer).or_else(|e| {
            data_buffer
                .failed_buffer
                .write()
                .map_err(|pe| <SerializationError as Into<VOWLGrapherError>>::into(pe.into()))?
                .push(e.into());
            Ok::<(), VOWLGrapherError>(())
        })?;

        // Catch permanently unresolved triples
        for (term_id, triples) in data_buffer
            .unknown_buffer
            .write()
            .map_err(|pe| <SerializationError as Into<VOWLGrapherError>>::into(pe.into()))?
            .drain()
        {
            for triple in triples {
                let e: SerializationError = SerializationErrorKind::SerializationFailedTriple(
                    data_buffer.term_index.display_triple(&triple)?,
                    format!(
                        "Unresolved reference: could not map '{}'",
                        data_buffer.term_index.get(term_id)?
                    ),
                )
                .into();
                data_buffer
                    .failed_buffer
                    .write()
                    .map_err(|pe| <SerializationError as Into<VOWLGrapherError>>::into(pe.into()))?
                    .push(e.into());
            }
        }

        let all_errors =
            Self::post_serialization_cleanup(data, data_buffer, start_time, query_time, count)
                .map_err(<SerializationError as Into<VOWLGrapherError>>::into)?;

        Ok(all_errors)
    }

    /// Serializes one solution into the data buffer.
    fn serialize_solution(
        solution: &QuerySolution,
        data_buffer: &mut SerializationDataBuffer,
    ) -> Result<(), SerializationError> {
        let Some(subject_term) = solution.get("id") else {
            return Ok(());
        };

        // Label must be extracted between getting id and nodeType from solutions due to "continue" in the else clause.
        let subject_term_id = data_buffer.term_index.insert(subject_term.to_owned())?;
        insert_label(
            data_buffer,
            solution.get("label"),
            subject_term,
            subject_term_id,
        )?;

        let Some(node_type_term) = solution.get("nodeType") else {
            return Ok(());
        };

        let predicate_term_id = data_buffer.term_index.insert(node_type_term.to_owned())?;
        let object_term_id = match solution.get("target") {
            Some(term) => Some(data_buffer.term_index.insert(term.to_owned())?),
            None => None,
        };

        let triple = create_triple_from_id(
            &data_buffer.term_index,
            subject_term_id,
            Some(predicate_term_id),
            object_term_id,
        )?;

        serialize_triple(data_buffer, &triple)
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "this method runs single-threaded"
    )]
    /// Performs post-serialization cleanup.
    ///
    /// Must be called exactly once in any serialization implementation.
    fn post_serialization_cleanup(
        data: &mut GraphDisplayData,
        data_buffer: SerializationDataBuffer,
        start_time: Instant,
        query_time: Option<Instant>,
        count: u64,
    ) -> Result<Option<VOWLGrapherError>, SerializationError> {
        let (element_count, edge_count, label_count, cardinality_count, characteristics_count) = {
            (
                data_buffer.node_element_buffer.read()?.len(),
                data_buffer.edge_buffer.read()?.len(),
                data_buffer.label_buffer.read()?.len(),
                data_buffer.edge_cardinality_buffer.read()?.len(),
                data_buffer.edge_characteristics.read()?.len()
                    + data_buffer.node_characteristics.read()?.len(),
            )
        };

        debug!("{data_buffer}");
        let serializer_errors = if data_buffer.failed_buffer.read()?.is_empty() {
            None
        } else {
            let mut failed_buffer = data_buffer.failed_buffer.write()?;
            let total = failed_buffer.len();
            let err: VOWLGrapherError = take(&mut *failed_buffer).into();
            error!(
                "Failed to serialize {} triple{}:\n{}",
                total,
                if total == 1 { "" } else { "s" },
                err
            );
            Some(err)
        };
        let (converted, convert_errors) = data_buffer.convert_into()?;
        *data = converted;
        debug!("{data}");

        let all_errors = match (serializer_errors, convert_errors) {
            (Some(mut e), Some(mut ce)) => {
                let ue = take(&mut e.records)
                    .into_iter()
                    .chain(take(&mut ce.records))
                    .collect::<Vec<_>>();
                Some(ue.into())
            }
            (Some(e), None) => Some(e),
            (None, Some(ce)) => Some(ce),
            (None, None) => None,
        };

        let finish_time = Instant::now()
            .checked_duration_since(start_time)
            .unwrap_or_default()
            .as_secs_f32();
        let query_finish_time = query_time.map_or(0.0, |qtime| {
            qtime
                .checked_duration_since(start_time)
                .unwrap_or_default()
                .as_secs_f32()
        });
        info!(
            "Serialization completed\n \
            \tQuery execution time: {:.5} s\n \
            \tSerialization time  : {:.5} s\n \
            \tTotal solutions     : {count}\n \
            \tElements            : {}\n \
            \tEdges               : {}\n \
            \tLabels              : {}\n \
            \tCardinalities       : {}\n \
            \tCharacteristics     : {}\n\n \
        ",
            query_finish_time,
            finish_time - query_finish_time,
            element_count,
            edge_count,
            label_count,
            cardinality_count,
            characteristics_count
        );

        Ok(all_errors)
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    // use super::*;
    // use oxrdf::{BlankNode, Literal, NamedNode};

    #[ignore = "Not refactored yet"]
    #[test]
    fn test_replace_node() {
        // let _ = env_logger::builder().is_test(true).try_init();
        // let serializer = GraphDisplayDataSolutionSerializer::new();
        // let mut data_buffer = SerializationDataBuffer::new();

        // let example_com = Term::NamedNode(NamedNode::new("http://example.com#").unwrap());
        // let owl_ontology =
        //     Term::NamedNode(NamedNode::new("http://www.w3.org/2002/07/owl#Ontology").unwrap());
        // let example_parent = Term::NamedNode(NamedNode::new("http://example.com#Parent").unwrap());
        // let owl_class =
        //     Term::NamedNode(NamedNode::new("http://www.w3.org/2002/07/owl#Class").unwrap());
        // let example_mother = Term::NamedNode(NamedNode::new("http://example.com#Mother").unwrap());
        // let example_guardian =
        //     Term::NamedNode(NamedNode::new("http://example.com#Guardian").unwrap());
        // let example_warden = Term::NamedNode(NamedNode::new("http://example.com#Warden").unwrap());
        // let example_warden1 =
        //     Term::NamedNode(NamedNode::new("http://example.com#Warden1").unwrap());
        // let rdfs_subclass_of = Term::NamedNode(
        //     NamedNode::new("http://www.w3.org/2000/01/rdf-schema#subClassOf").unwrap(),
        // );
        // let blanknode1 =
        //     Term::BlankNode(BlankNode::new("e1013e66f734c508511575854b0c9396").unwrap());

        // let t1 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)
        // let t2 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)
        // let t3 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)
        // let t4 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)
        // let t5 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)
        // let t6 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)
        // let t7 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)
        // let t8 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)
        // let t9 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)
        // let t10 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)
        // let t11 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)
        // let t12 = serializer.create_triple_from_iri(term_index, subject_iri, predicate_iri, object_iri)

        // serializer.serialize_triple(
        //     &mut data_buffer,
        //     Triple {
        //         subject_term_id: example_com.clone(),
        //         predicate_term_id: owl_ontology.clone(),
        //         object_term_id: None,
        //     },
        // );
        // serializer.serialize_triple(
        //     &mut data_buffer,
        //     Triple {
        //         subject_term_id: example_parent.clone(),
        //         predicate_term_id: owl_class.clone(),
        //         object_term_id: None,
        //     },
        // );
        // serializer.serialize_triple(
        //     &mut data_buffer,
        //     Triple {
        //         subject_term_id: example_mother.clone(),
        //         predicate_term_id: owl_class.clone(),
        //         object_term_id: None,
        //     },
        // );
        // serializer.serialize_triple(
        //     &mut data_buffer,
        //     Triple {
        //         subject_term_id: example_guardian.clone(),
        //         predicate_term_id: owl_class.clone(),
        //         object_term_id: None,
        //     },
        // );
        // serializer.serialize_triple(
        //     &mut data_buffer,
        //     Triple {
        //         subject_term_id: example_warden.clone(),
        //         predicate_term_id: owl_class.clone(),
        //         object_term_id: None,
        //     },
        // );
        // serializer.serialize_triple(
        //     &mut data_buffer,
        //     Triple {
        //         subject_term_id: example_warden1.clone(),
        //         predicate_term_id: owl_class.clone(),
        //         object_term_id: None,
        //     },
        // );
        // serializer.serialize_triple(
        //     &mut data_buffer,
        //     Triple {
        //         subject_term_id: example_warden.clone(),
        //         predicate_term_id: rdfs_subclass_of.clone(),
        //         object_term_id: Some(example_guardian.clone()),
        //     },
        // );
        // serializer.serialize_triple(
        //     &mut data_buffer,
        //     Triple {
        //         subject_term_id: example_mother.clone(),
        //         predicate_term_id: rdfs_subclass_of.clone(),
        //         object_term_id: Some(example_parent.clone()),
        //     },
        // );
        // serializer.serialize_triple(
        //     &mut data_buffer,
        //     Triple {
        //         subject_term_id: blanknode1.clone(),
        //         predicate_term_id: Term::Literal(Literal::new_simple_literal(
        //             "blanknode".to_string(),
        //         )),
        //         object_term_id: None,
        //     },
        // );
        // serializer.serialize_triple(
        //     &mut data_buffer,
        //     Triple {
        //         subject_term_id: example_warden1.clone(),
        //         predicate_term_id: Term::NamedNode(
        //             NamedNode::new("http://www.w3.org/2002/07/owl#unionOf").unwrap(),
        //         ),
        //         object_term_id: Some(example_warden.clone()),
        //     },
        // );

        // print_graph_display_data(&data_buffer);
        // println!("--------------------------------");

        // let triple = Triple {
        //     subject_term_id: example_guardian.clone(),
        //     predicate_term_id: Term::NamedNode(
        //         NamedNode::new("http://www.w3.org/2002/07/owl#equivalentClass").unwrap(),
        //     ),
        //     object_term_id: Some(example_warden.clone()),
        // };
        // serializer.serialize_triple(&mut data_buffer, triple);
        // for (k, v) in data_buffer.node_element_buffer.iter() {
        //     println!("element_buffer: {} -> {}", k, v);
        // }
        // for (k, v) in data_buffer.edges_include_map.iter() {
        //     println!("edges_include_map: {} -> {:?}", k, v);
        // }
        // for (k, v) in data_buffer.edge_redirection.iter() {
        //     println!("edge_redirection: {} -> {}", k, v);
        // }
        // assert!(
        //     data_buffer
        //         .node_element_buffer
        //         .contains_key(&example_guardian.clone())
        // );
        // assert!(
        //     !data_buffer
        //         .node_element_buffer
        //         .contains_key(&example_warden)
        // );
        // assert!(
        //     data_buffer
        //         .node_element_buffer
        //         .contains_key(&example_warden1)
        // );
        // assert!(data_buffer.edges_include_map.contains_key(&example_warden1));

        // assert!(data_buffer.edge_buffer.contains(&Edge {
        //     domain_term_id: example_warden1,
        //     edge_type: ElementType::NoDraw,
        //     range_term_id: example_guardian.clone(),
        //     property_term_id: None
        // }));
        // assert!(data_buffer.edge_redirection.contains_key(&example_warden));
        // assert_eq!(
        //     data_buffer
        //         .edge_redirection
        //         .get(&example_warden)
        //         .unwrap()
        //         .clone(),
        //     example_guardian
        // );
        // serializer.serialize_triple(
        //     &mut data_buffer,
        //     Triple {
        //         subject_term_id: Term::NamedNode(
        //             NamedNode::new("http://example.com#Guardian").unwrap(),
        //         ),
        //         predicate_term_id: Term::NamedNode(
        //             NamedNode::new("http://www.w3.org/2002/07/owl#equivalentClass").unwrap(),
        //         ),
        //         object_term_id: Some(blanknode1.clone()),
        //     },
        // );
        // let s = serializer.resolve(&data_buffer, blanknode1.clone());
        // assert!(s.is_some());
        // for (k, v) in data_buffer.node_element_buffer.iter() {
        //     println!("element_buffer: {} -> {}", k, v);
        // }
        // for (k, v) in data_buffer.edge_redirection.iter() {
        //     println!("edge_redirection: {} -> {}", k, v);
        // }
        // assert!(s.unwrap() == example_guardian);
        // assert!(!data_buffer.edges_include_map.contains_key(&blanknode1));
        // assert!(!data_buffer.edges_include_map.contains_key(&example_warden));
    }
}

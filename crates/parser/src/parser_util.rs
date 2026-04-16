//! Various utility functions which collectively makes up the parser.

use crate::errors::{VOWLGrapherStoreError, VOWLGrapherStoreErrorKind};
use futures::{Stream, StreamExt, stream::BoxStream};
use horned_owl::{
    io::{rdf::reader::ConcreteRDFOntology, *},
    model::{RcAnnotatedComponent, RcStr},
    ontology::component_mapped::RcComponentMappedOntology,
};
use log::info;
use rdf_fusion::model::GraphName;
use rdf_fusion::{
    error::LoaderError,
    io::{JsonLdProfileSet, RdfFormat, RdfParser, RdfSerializer},
    model::{NamedNodeRef, Quad},
};
use std::io;
use std::io::BufRead;
use std::{
    io::{BufReader, Cursor, Write},
    path::Path,
    time::{Duration, Instant},
};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use vowlgrapher_util::prelude::DataType;

fn to_default_graph_quad(quad: Quad) -> Quad {
    Quad::new(
        quad.subject,
        quad.predicate,
        quad.object,
        GraphName::DefaultGraph,
    )
}

/// Returns the datatype of the path, if it's supported by the parser.
pub fn path_type(path: &Path) -> Option<DataType> {
    match path.extension().and_then(|s| s.to_str()) {
        Some("ofn") => Some(DataType::OFN),
        Some("owx") => Some(DataType::OWX),
        Some("rdf") => Some(DataType::RDF),
        Some("owl") => Some(DataType::OWL),
        Some("ttl") => Some(DataType::TTL),
        Some("nt") => Some(DataType::NTriples),
        Some("nq") => Some(DataType::NQuads),
        Some("trig") => Some(DataType::TriG),
        Some("jsonld") => Some(DataType::JsonLd),
        Some("n3") => Some(DataType::N3),
        _ => None,
    }
}

/// Returns the parser format for the resource type, if it's supported by the parser.
pub fn format_from_resource_type(resource_type: &DataType) -> Option<RdfFormat> {
    match resource_type {
        DataType::RDF => Some(RdfFormat::RdfXml),
        DataType::TTL => Some(RdfFormat::Turtle),
        DataType::NTriples => Some(RdfFormat::NTriples),
        DataType::NQuads => Some(RdfFormat::NQuads),
        DataType::TriG => Some(RdfFormat::TriG),
        DataType::JsonLd => Some(RdfFormat::JsonLd {
            profile: JsonLdProfileSet::default(),
        }),
        DataType::N3 => Some(RdfFormat::N3),
        DataType::OWL => Some(RdfFormat::RdfXml),
        _ => None,
    }
}

/// Converts a stream of quads to the target output format.
/// Used for OWL/OFN/OWX formats that require horned-owl conversion.
pub async fn parse_quads_to_format<E>(
    mut quads_stream: impl Stream<Item = Result<Quad, E>> + Unpin,
    output_type: DataType,
) -> Result<BoxStream<'static, Result<Vec<u8>, VOWLGrapherStoreError>>, VOWLGrapherStoreError>
where
    E: std::error::Error + Send + Sync + 'static,
{
    let mut quads = Vec::new();
    while let Some(result) = futures::stream::StreamExt::next(&mut quads_stream).await {
        let quad = result.map_err(|_e| {
            VOWLGrapherStoreError::from(VOWLGrapherStoreErrorKind::InvalidFileType(
                "Failed to read quad from stream".to_string(),
            ))
        })?;
        quads.push(quad);
    }

    match output_type {
        DataType::OFN | DataType::OWX | DataType::OWL => {
            let (tx, rx) = mpsc::unbounded_channel();
            let mut buf = Vec::new();
            let mut serializer =
                RdfSerializer::from_format(format_from_resource_type(&DataType::OWL).ok_or(
                    VOWLGrapherStoreErrorKind::InvalidFileType(format!(
                        "Unsupported output type: {:?}",
                        output_type
                    )),
                )?)
                .for_writer(&mut buf);
            for quad in quads {
                let q = to_default_graph_quad(quad);
                serializer.serialize_quad(&q)?;
            }
            serializer.finish()?;

            let mut reader = BufReader::new(Cursor::new(buf));
            tokio::task::spawn_blocking(move || {
                let mut writer = ChannelWriter { sender: tx.clone() };
                let result = (|| match output_type {
                    DataType::OFN => {
                        let (ont, _): (ConcreteRDFOntology<RcStr, RcAnnotatedComponent>, _) =
                            rdf::reader::read(&mut reader, ParserConfiguration::default())?;
                        let ont: RcComponentMappedOntology = ont.into();
                        ofn::writer::write(&mut writer, &ont, None)?;
                        writer.flush()?;
                        Ok(writer)
                    }
                    DataType::OWX => {
                        let (ont, _): (ConcreteRDFOntology<RcStr, RcAnnotatedComponent>, _) =
                            rdf::reader::read(&mut reader, ParserConfiguration::default())?;
                        let ont: RcComponentMappedOntology = ont.into();
                        owx::writer::write(&mut writer, &ont, None)?;
                        writer.flush()?;
                        Ok(writer)
                    }
                    DataType::OWL => {
                        let (ont, _): (ConcreteRDFOntology<RcStr, RcAnnotatedComponent>, _) =
                            rdf::reader::read(&mut reader, ParserConfiguration::default())?;
                        rdf::writer::write(&mut writer, &ont.into())?;
                        writer.flush()?;
                        Ok(writer)
                    }
                    _ => Err(VOWLGrapherStoreError::from(
                        VOWLGrapherStoreErrorKind::InvalidFileType(format!(
                            "Unsupported output type: {:?}",
                            output_type
                        )),
                    )),
                })();

                if let Err(e) = result {
                    let _ = tx.send(Err(e.into()));
                }
            });
            Ok(UnboundedReceiverStream::new(rx)
                .map(|result| result.map_err(VOWLGrapherStoreError::from))
                .boxed())
        }
        _ => Err(VOWLGrapherStoreErrorKind::InvalidFileType(format!(
            "parse_quads_to_format only supports OFN/OWX/OWL, got {:?}",
            output_type
        ))
        .into()),
    }
}

/// Returns the quads from parsing the file at the path.
pub fn parser_from_path(
    path: &Path,
    format: DataType,
    lenient: bool,
    graph_iri: &str,
) -> Result<Vec<Quad>, VOWLGrapherStoreError> {
    let reader = std::fs::File::open(path)?;
    let reader = BufReader::new(reader);
    parser_from_reader(reader, format, lenient, graph_iri)
}

/// Returns the quads from parsing the reader, reading from the path.
pub fn parser_from_reader(
    mut reader: impl BufRead,
    format: DataType,
    lenient: bool,
    graph_iri: &str,
) -> Result<Vec<Quad>, VOWLGrapherStoreError> {
    let make_parser = |fmt| {
        let graph_node = NamedNodeRef::new(graph_iri).expect("Failed to parse graph IRI in parser");
        let parser = RdfParser::from_format(fmt).with_default_graph(graph_node);
        if lenient { parser.lenient() } else { parser }
    };

    let collect_quads =
        |parser: RdfParser, bytes: &[u8]| -> Result<Vec<Quad>, VOWLGrapherStoreError> {
            parser
                .rename_blank_nodes()
                .for_reader(bytes)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| VOWLGrapherStoreError::from(LoaderError::from(e)))
        };

    match format {
        DataType::OFN => {
            info!("Parsing OFN input...");
            let start_time = Instant::now();

            let (ont, _): (RcComponentMappedOntology, _) =
                ofn::reader::read(&mut reader, ParserConfiguration::default())?;

            info!(
                "Parsing completed in {} s",
                Instant::now()
                    .checked_duration_since(start_time)
                    .unwrap_or(Duration::new(0, 0))
                    .as_secs_f32()
            );

            info!("Writing to RDF...");
            let start_time = Instant::now();

            let mut buf = Vec::new();
            rdf::writer::write(&mut buf, &ont)?;

            info!(
                "Writing completed in {} s",
                Instant::now()
                    .checked_duration_since(start_time)
                    .unwrap_or(Duration::new(0, 0))
                    .as_secs_f32()
            );

            collect_quads(make_parser(RdfFormat::RdfXml), &buf)
        }
        DataType::OWX => {
            info!("Parsing OWX input...");
            let start_time = Instant::now();

            let ontology = owx::reader::read::<
                RcStr,
                ConcreteRDFOntology<RcStr, RcAnnotatedComponent>,
                _,
            >(&mut reader, ParserConfiguration::default())?;

            info!(
                "Parsing completed in {} s",
                Instant::now()
                    .checked_duration_since(start_time)
                    .unwrap_or(Duration::new(0, 0))
                    .as_secs_f32()
            );

            info!("Writing to RDF...");
            let start_time = Instant::now();

            let mut buf = Vec::new();
            rdf::writer::write(&mut buf, &ontology.0.into())?;

            info!(
                "Writing completed in {} s",
                Instant::now()
                    .checked_duration_since(start_time)
                    .unwrap_or(Duration::new(0, 0))
                    .as_secs_f32()
            );

            collect_quads(make_parser(RdfFormat::RdfXml), &buf)
        }
        DataType::OWL | DataType::RDF => {
            let mut input = Vec::new();
            reader.read_to_end(&mut input)?;
            collect_quads(make_parser(RdfFormat::RdfXml), &input)
        }
        f @ DataType::TTL
        | f @ DataType::NTriples
        | f @ DataType::NQuads
        | f @ DataType::TriG
        | f @ DataType::JsonLd
        | f @ DataType::N3 => {
            let mut input = Vec::new();
            reader.read_to_end(&mut input)?;
            let format = format_from_resource_type(&f).ok_or_else(|| {
                VOWLGrapherStoreErrorKind::InvalidFileType(format!(
                    "could not convert {f:?} to format"
                ))
            })?;
            collect_quads(make_parser(format), &input)
        }
        _ => Err(VOWLGrapherStoreErrorKind::InvalidFileType(format!(
            "Unsupported parser: {}",
            format.mime_type()
        ))
        .into()),
    }
}

/// in-memory parsing
pub fn parser_from_bytes(
    bytes: &[u8],
    format: DataType,
    lenient: bool,
    graph_iri: &str,
) -> Result<Vec<Quad>, VOWLGrapherStoreError> {
    let reader = BufReader::new(Cursor::new(bytes));
    parser_from_reader(reader, format, lenient, graph_iri)
}

struct ChannelWriter {
    sender: UnboundedSender<Result<Vec<u8>, io::Error>>,
}

impl Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let data = buf.to_vec();

        self.sender
            .send(Ok(data))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Stream receiver dropped"))?;

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(()) // No internal buffering to flush
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use super::*;

    #[tokio::test]
    #[ignore = "currently broken, see #151"]
    async fn test_ofn_parser() {
        let resources = resources_with_suffix("../database/data/owl-functional", "ofn");
        test_parser_on_resources(&resources).await;
    }

    #[tokio::test]
    #[ignore = "currently broken, see #151"]
    async fn test_owl_parser() {
        let resources = resources_with_suffix("../database/data/owl-rdf", "owl");
        test_parser_on_resources(&resources).await;
    }
    #[tokio::test]
    #[ignore = "currently broken, see #151"]
    async fn test_ttl_parser() {
        let resources = resources_with_suffix("../database/data/owl-ttl", "ttl");
        test_parser_on_resources(&resources).await;
    }

    async fn test_parser_on_resources(resources: &[impl AsRef<Path>]) {
        use env_logger::{self, Env};
        use log::warn;
        use rdf_fusion::store::Store;

        // Initialize logger if it isn't already initialized
        let _ = env_logger::Builder::from_env(Env::default().default_filter_or("warn")).try_init();

        let session = Store::default();
        for resource in resources {
            if resource
                .as_ref()
                .extension()
                .is_some_and(|ext| ext == "skip")
            {
                warn!("skipping {:?}", resource.as_ref());
                continue;
            }
            let dt = path_type(resource.as_ref()).unwrap();
            let quads =
                parser_from_path(resource.as_ref(), dt, false, "urn:vowlgrapher:test_graph")
                    .unwrap();
            let _ = session.extend(quads).await;
            assert_ne!(
                session.len().await.unwrap(),
                0,
                "Expected non-zero quads for: {:?}",
                resource.as_ref()
            );
            session.clear().await.unwrap();
        }
    }

    fn resources_with_suffix(relative_dir: impl AsRef<Path>, suffix: &str) -> Vec<PathBuf> {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let base_dir = Path::new(manifest_dir).join(&relative_dir);
        let suffix = suffix.trim_start_matches(['.', '*']);
        let mut resources = Vec::new();

        let entries = std::fs::read_dir(&base_dir).unwrap_or_else(|err| {
            panic!(
                "Failed to read resources directory {}: {}",
                base_dir.display(),
                err
            )
        });

        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file()
                && path
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .is_some_and(|ext| ext == suffix)
                && let Some(file_name) = path.file_name().and_then(|n| n.to_str())
            {
                resources.push(
                    relative_dir
                        .as_ref()
                        .to_owned()
                        .join(file_name)
                        .canonicalize()
                        .unwrap(),
                );
            }
        }

        resources.sort();
        resources
    }
}

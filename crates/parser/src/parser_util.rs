//! Various utility functions which collectively makes up the parser.

use crate::errors::{VOWLRStoreError, VOWLRStoreErrorKind};
use futures::{StreamExt, stream::BoxStream};
use horned_owl::{
    io::{rdf::reader::ConcreteRDFOntology, *},
    model::{RcAnnotatedComponent, RcStr},
    ontology::component_mapped::RcComponentMappedOntology,
};
use log::info;
use rdf_fusion::{
    execution::results::QuadStream,
    io::{JsonLdProfileSet, RdfFormat, RdfParser, RdfSerializer},
    model::{GraphName, NamedNodeRef},
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
use vowlr_util::prelude::DataType;

/// Encapsulates the input of the parser.
pub enum ParserInput {
    /// The input as a byte vector of a file's contents.
    File(Vec<u8>),
    /// A buffer of the inputThe file is read into this buffer.
    Buffer(Cursor<Vec<u8>>),
}

impl ParserInput {
    #[expect(
        clippy::result_large_err,
        reason = "fixed if VOWLRStoreErrorKind contains String instead of full error types"
    )]
    /// Reads the entire file at `path` and returns the contents as a byte vector.
    pub fn from_path(path: &Path) -> Result<Self, VOWLRStoreError> {
        std::fs::read(path)
            .map(ParserInput::File)
            .map_err(VOWLRStoreError::from)
    }

    /// Returns [`self`] as a slice.
    pub fn as_slice(&self) -> &[u8] {
        match self {
            ParserInput::Buffer(cursor) => cursor.get_ref().as_slice(),
            ParserInput::File(bytes) => bytes.as_slice(),
        }
    }
}

/// Encapsulates the various parsers in use into a parse implementation usable by RDF-Fusion.
pub struct PreparedParser {
    /// The parser to use.
    pub parser: RdfParser,
    /// The input to parse.
    pub input: ParserInput,
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

/// Serializes a stream into an output type.
///
/// Useful for exporting a graph from the database.
pub async fn parse_stream_to(
    mut stream: QuadStream,
    output_type: DataType,
) -> Result<BoxStream<'static, Result<Vec<u8>, VOWLRStoreError>>, VOWLRStoreError> {
    match output_type {
        DataType::OFN | DataType::OWX | DataType::OWL => {
            let (tx, rx) = mpsc::unbounded_channel();
            let mut buf = Vec::new();
            let mut serializer =
                RdfSerializer::from_format(format_from_resource_type(&DataType::OWL).ok_or(
                    VOWLRStoreErrorKind::InvalidFileType(format!(
                        "Unsupported output type: {:?}",
                        output_type
                    )),
                )?)
                .for_writer(&mut buf);
            while let Some(quad) = stream.next().await {
                serializer.serialize_quad(&quad?)?;
            }
            serializer.finish()?;

            let mut reader = BufReader::new(Cursor::new(buf));
            tokio::task::spawn_blocking(move || {
                let mut writer = ChannelWriter { sender: tx.clone() };
                let result = (|| match output_type {
                    DataType::OFN => {
                        let (ont, prefix): (RcComponentMappedOntology, _) =
                            ofn::reader::read(&mut reader, ParserConfiguration::default())?;
                        ofn::writer::write(&mut writer, &ont, Some(&prefix))?;
                        writer.flush()?;
                        Ok(writer)
                    }
                    DataType::OWX => {
                        let (ont, prefix): (RcComponentMappedOntology, _) =
                            owx::reader::read(&mut reader, ParserConfiguration::default())?;
                        owx::writer::write(&mut writer, &ont, Some(&prefix))?;
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
                    _ => Err(VOWLRStoreError::from(VOWLRStoreErrorKind::InvalidFileType(
                        format!("Unsupported output type: {:?}", output_type),
                    ))),
                })();

                if let Err(e) = result {
                    let _ = tx.send(Err(e.into()));
                }
            });
            Ok(UnboundedReceiverStream::new(rx)
                .map(|result| result.map_err(VOWLRStoreError::from))
                .boxed())
        }
        _ => {
            let (tx, rx) = mpsc::unbounded_channel();
            tokio::task::spawn(async move {
                let mut writer = ChannelWriter { sender: tx.clone() };
                let result = async {
                    let mut serializer =
                        RdfSerializer::from_format(format_from_resource_type(&output_type).ok_or(
                            VOWLRStoreErrorKind::InvalidFileType(format!(
                                "Unsupported output type: {:?}",
                                output_type
                            )),
                        )?)
                        .for_writer(&mut writer);
                    while let Some(quad) = stream.next().await {
                        serializer.serialize_quad(&quad?)?;
                    }
                    serializer.finish()?;
                    Ok::<ChannelWriter, VOWLRStoreError>(writer)
                };

                if let Err(e) = result.await {
                    let _ = tx.send(Err(e.into()));
                }
            });
            Ok(UnboundedReceiverStream::new(rx)
                .map(|result| result.map_err(VOWLRStoreError::from))
                .boxed())
        }
    }
}

/// Returns the parser compatible with the file at the path.
#[expect(
    clippy::result_large_err,
    reason = "fixed if VOWLRStoreErrorKind contains String instead of full error types"
)]
pub fn parser_from_path(
    path: &Path,
    lenient: bool,
    graph_iri: &str,
) -> Result<PreparedParser, VOWLRStoreError> {
    let reader = std::fs::File::open(path)?;
    let reader = BufReader::new(reader);
    parser_from_reader(reader, path, lenient, graph_iri)
}

/// Returns the parser compatible with the reader, reading from the path.
#[expect(
    clippy::result_large_err,
    reason = "fixed if VOWLRStoreErrorKind contains String instead of full error types"
)]
pub fn parser_from_reader(
    mut reader: impl BufRead,
    path: &Path,
    lenient: bool,
    graph_iri: &str,
) -> Result<PreparedParser, VOWLRStoreError> {
    let make_parser = |fmt| {
        // TODO: Handle non default graph
        let graph_node = NamedNodeRef::new(graph_iri).expect("Failed to parse graph IRI in parser");
        let parser = RdfParser::from_format(fmt).with_default_graph(graph_node);
        //.with_default_graph(NamedNode::new(format!("file:://{}", path_str)).unwrap());
        if lenient { parser.lenient() } else { parser }
    };

    let Some(format) = path_type(path) else {
        return Err(VOWLRStoreErrorKind::InvalidFileType(format!(
            "Unsupported format: {:?}",
            path.file_name().unwrap_or_default()
        ))
        .into());
    };

    let prepared = match format {
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

            Ok(PreparedParser {
                parser: make_parser(RdfFormat::RdfXml),
                input: ParserInput::Buffer(Cursor::new(buf)),
            })
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
            Ok(PreparedParser {
                parser: make_parser(RdfFormat::RdfXml),
                input: ParserInput::Buffer(Cursor::new(buf)),
            })
        }
        DataType::OWL => {
            info!("Parsing OWL input...");
            let start_time = Instant::now();

            let b = horned_owl::model::Build::<RcStr>::new();
            let iri = horned_owl::resolve::path_to_file_iri(&b, path);
            let (ontology, _) = rdf::closure_reader::read::<
                RcStr,
                RcAnnotatedComponent,
                ConcreteRDFOntology<RcStr, RcAnnotatedComponent>,
            >(&iri, ParserConfiguration::default())?;

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
            rdf::writer::write(&mut buf, &ontology.into())?;

            info!(
                "Writing completed in {} s",
                Instant::now()
                    .checked_duration_since(start_time)
                    .unwrap_or(Duration::new(0, 0))
                    .as_secs_f32()
            );

            Ok(PreparedParser {
                parser: make_parser(RdfFormat::RdfXml),
                input: ParserInput::Buffer(Cursor::new(buf)),
            })
        }
        f @ DataType::TTL
        | f @ DataType::NTriples
        | f @ DataType::NQuads
        | f @ DataType::TriG
        | f @ DataType::JsonLd
        | f @ DataType::N3 => {
            let mut input = Vec::new();
            reader.read_to_end(&mut input)?;
            let input = ParserInput::File(input);
            let format = format_from_resource_type(&f).ok_or_else(|| {
                VOWLRStoreErrorKind::InvalidFileType(format!("could not convert {f:?} to format"))
            })?;
            Ok(PreparedParser {
                parser: make_parser(format),
                input,
            })
        }
        _ => Err(VOWLRStoreErrorKind::InvalidFileType(format!(
            "Unsupported parser: {}",
            format.mime_type()
        ))),
    };
    Ok(prepared?)
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
            let parser =
                parser_from_path(resource.as_ref(), false, "urn:vowlr:test_graph").unwrap();
            let _ = session
                .load_from_reader(parser.parser, parser.input.as_slice())
                .await;
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

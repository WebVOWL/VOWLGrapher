use crate::errors::SerializationErrorKind;
use crate::serializers::frontend::GraphDisplayDataSolutionSerializer;
use futures::{StreamExt, stream::BoxStream};
use grapher::prelude::GraphDisplayData;
use log::{debug, info, warn};
use rdf_fusion::execution::results::QueryResults;
use rdf_fusion::model::Quad;
use rdf_fusion::store::Store;
use std::path::Path;
use std::time::Duration;
use std::{fs::File, time::Instant};
use strum::IntoEnumIterator;
use vowlr_parser::{
    errors::{VOWLRStoreError, VOWLRStoreErrorKind},
    parser_util::{parse_stream_to, parser_from_path, path_type},
};
use vowlr_util::prelude::{DataType, VOWLRError};

static GLOBAL_STORE: std::sync::OnceLock<Store> = std::sync::OnceLock::new();

/// The graph database.
pub struct VOWLRStore {
    /// The store is the quad database and SPARQL engine.
    pub session: Store,
    /// The unique ID for the current user.
    pub user_id: Option<String>,
    upload_handle: Option<tempfile::NamedTempFile>,
}

impl VOWLRStore {
    /// Create a new database instance.
    pub fn new(session: Store) -> Self {
        Self {
            session,
            user_id: None,
            upload_handle: None,
        }
    }

    /// Create a new database instance with user_id.
    pub fn new_for_user(user_id: String) -> Self {
        let session = GLOBAL_STORE.get_or_init(Store::default).clone();
        Self {
            session,
            user_id: Some(user_id),
            upload_handle: None,
        }
    }

    /// Update graph name (4th element in quad) with a compination of user_id and name of graph.
    pub fn get_graph_iri(&self, filename: &str) -> String {
        let filename = filename
            .replace(" ", "_")
            .replace("(", "")
            .replace(")", "")
            .replace("[", "")
            .replace("]", "");

        if let Some(ref uid) = self.user_id {
            format!("urn:vowlr:user:{}:graph:{}", uid, filename)
        } else {
            format!("urn:vowlr:graph:{}", filename)
        }
    }

    /// Executes a SPARQL query and serializes the result.
    ///
    /// This method tries to continue serializing despite errors.
    /// As such, the `Ok` value contains non-fatal errors encountered during serialization.
    pub async fn query(
        &self,
        query: String,
        graph_name: Option<String>,
    ) -> Result<(GraphDisplayData, Option<VOWLRError>), VOWLRError> {
        debug!("Querying with graph_name: {:#?}", graph_name);
        let user_query = if let Some(name) = graph_name {
            let graph_iri = self.get_graph_iri(&name);
            query.replace("{GRAPH_IRI}", &graph_iri)
        } else {
            query.replace("GRAPH <{{GRAPH_IRI}}>", "")
        };

        let solution_serializer = GraphDisplayDataSolutionSerializer::new();
        let query_stream = self
            .session
            .query(&user_query)
            .await
            .map_err(|e| <VOWLRStoreError as Into<VOWLRError>>::into(e.into()))?;

        match query_stream {
            QueryResults::Solutions(query_solution_stream) => {
                let mut data_buffer = GraphDisplayData::new();

                let maybe_errors = solution_serializer
                    .serialize_solution_stream(&mut data_buffer, query_solution_stream)
                    .await?;
                Ok((data_buffer, maybe_errors))
            }
            QueryResults::Boolean(_result) => Err(SerializationErrorKind::UnsupportedQueryType(
                "Query stream is not a SELECT query".to_string(),
            )
            .into()),
            QueryResults::Graph(_query_triple_stream) => {
                // TODO: Implement to support user-defined SPARQL queries
                Err(SerializationErrorKind::UnsupportedQueryType(
                    "Query stream is not a SELECT query".to_string(),
                )
                .into())
            }
        }
    }

    // TTL format -> (oxittl) RDF XML quads -> (horned_owl) Normalize OWL/RDF -> Quads -> Insert into Oxigraph
    /// Inserts a file into the store.
    ///
    /// Files are automatically parsed.
    pub async fn insert_file(&self, fs: &Path, lenient: bool) -> Result<(), VOWLRStoreError> {
        let graph_iri = self.get_graph_iri(&fs.to_string_lossy());
        let format = path_type(fs)
            .ok_or_else(|| VOWLRStoreErrorKind::InvalidFileType("Unknown file extension".into()))?;
        let quads = parser_from_path(fs, format, lenient, &graph_iri)?;
        info!("Loading graph '{}' into database...", graph_iri);
        let start_time = Instant::now();
        self.session.extend(quads).await?;
        info!(
            "Loaded {} quads in {} s",
            self.session.len().await?,
            Instant::now()
                .checked_duration_since(start_time)
                .unwrap_or(Duration::new(0, 0))
                .as_secs_f32()
        );
        Ok(())
    }

    async fn load_file(
        &self,
        path: &Path,
        lenient: bool,
        graph_iri: &str,
    ) -> Result<(Vec<Quad>, DataType), VOWLRStoreError> {
        let dtype = path.into();
        match dtype {
            DataType::UNKNOWN => self.try_load_fallback(path, lenient, None, graph_iri).await,
            _ => {
                let result =
                    std::panic::catch_unwind(|| parser_from_path(path, dtype, lenient, graph_iri));
                match result {
                    Ok(Ok(quads)) => Ok((quads, dtype)),
                    _ => {
                        self.try_load_fallback(path, lenient, Some(dtype), graph_iri)
                            .await
                    }
                }
            }
        }
    }

    async fn try_load_fallback(
        &self,
        path: &Path,
        lenient: bool,
        skip_format: Option<DataType>,
        graph_iri: &str,
    ) -> Result<(Vec<Quad>, DataType), VOWLRStoreError> {
        for format in DataType::iter().filter(|f| *f != DataType::UNKNOWN) {
            if Some(format) == skip_format {
                continue;
            }

            let result =
                std::panic::catch_unwind(|| parser_from_path(path, format, lenient, graph_iri));
            if let Ok(Ok(quads)) = result {
                info!("Parsed file as {:?}", format);
                return Ok((quads, format));
            }
        }

        Err(VOWLRStoreErrorKind::InvalidFileType(format!(
            "Could not parse file. Tried with the following formats: {:?}",
            DataType::iter()
                .filter(|f| *f != DataType::UNKNOWN)
                .collect::<Vec<_>>()
        ))
        .into())
    }

    /// Serializes the store into a file, located at the path.
    pub async fn serialize_to_file(&self, path: &Path) -> Result<(), VOWLRStoreError> {
        let mut file = File::create(path)?;
        let mut results = parse_stream_to(self.session.stream().await?, DataType::OWL).await?;
        while let Some(result) = results.next().await {
            std::io::Write::write_all(&mut file, &result?)?;
        }

        Ok(())
    }

    /// Serializes the store into a stream of the specified resource type.
    pub async fn serialize_stream(
        &self,
        resource_type: DataType,
    ) -> Result<BoxStream<'static, Result<Vec<u8>, VOWLRStoreError>>, VOWLRStoreError> {
        debug!(
            "Store size before export: {}",
            self.session.len().await.unwrap_or(0)
        );
        let results = parse_stream_to(self.session.stream().await?, resource_type).await?;
        Ok(results)
    }

    /// Create a temporary file on the server to upload user input into.
    ///
    /// TODO: Ensure this can handle multiple users.
    pub async fn start_upload(&mut self, filename: &str) -> Result<(), VOWLRStoreError> {
        let extension = Path::new(filename)
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("owl");
        let file = tempfile::Builder::new()
            .suffix(&format!(".{}", extension))
            .tempfile()?;
        self.upload_handle = Some(file);
        Ok(())
    }

    /// Insert a chunk of data into the file currently in use.
    ///
    /// TODO: Ensure this can handle multiple users.
    pub async fn upload_chunk(&mut self, data: &[u8]) -> Result<(), VOWLRStoreError> {
        if let Some(file) = &mut self.upload_handle {
            std::io::Write::write_all(file, data)?;
            Ok(())
        } else {
            warn!("upload_chunk called without start_upload");
            Ok(())
        }
    }

    /// Inserts a file into the store.
    ///
    /// Files are automatically parsed.
    pub async fn complete_upload(&mut self, filename: &str) -> Result<DataType, VOWLRStoreError> {
        let graph_iri = self.get_graph_iri(filename);
        let path = if let Some(file) = &mut self.upload_handle {
            std::io::Write::flush(file)?;
            file.path().to_path_buf()
        } else {
            return Err(
                VOWLRStoreErrorKind::InvalidFileType("No upload handle found".to_string()).into(),
            );
        };
        let (quads, loaded_format) = self.load_file(&path, false, &graph_iri).await?;
        info!("Loading graph '{}' into database...", graph_iri);
        let start_time = Instant::now();

        self.session.extend(quads).await?;
        info!(
            "Loaded {} quads in {} s",
            self.session.len().await?,
            Instant::now()
                .checked_duration_since(start_time)
                .unwrap_or(Duration::new(0, 0))
                .as_secs_f32()
        );
        self.upload_handle = None;
        Ok(loaded_format)
    }
}

impl Default for VOWLRStore {
    fn default() -> Self {
        let session = GLOBAL_STORE.get_or_init(Store::default).clone();
        Self::new(session)
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use test_generator::test_resources;

    #[test_resources("crates/database/data/owl-functional/*.ofn")]
    async fn test_ofn_parser_format(resource: &str) -> Result<(), VOWLRStoreError> {
        let store = VOWLRStore::default();
        store
            .insert_file(Path::new(&resource), false)
            .await
            .unwrap();
        assert_ne!(
            store.session.len().await.unwrap(),
            0,
            "Expected non-zero quads for: {}",
            resource
        );
        store.session.clear().await?;
        Ok(())
    }
    #[test_resources("crates/database/data/owl-rdf/*.owl")]
    async fn test_owl_parser_format(resource: &str) -> Result<(), VOWLRStoreError> {
        let store = VOWLRStore::default();
        store
            .insert_file(Path::new(&resource), false)
            .await
            .unwrap();
        assert_ne!(
            store.session.len().await.unwrap(),
            0,
            "Expected non-zero quads for: {}",
            resource
        );
        store.session.clear().await?;
        Ok(())
    }
    #[test_resources("crates/database/data/owl-ttl/*.ttl")]
    async fn test_ttl_parser_format(resource: &str) -> Result<(), VOWLRStoreError> {
        let store = VOWLRStore::default();
        store
            .insert_file(Path::new(&resource), false)
            .await
            .unwrap();
        assert_ne!(
            store.session.len().await.unwrap(),
            0,
            "Expected non-zero quads for: {}",
            resource
        );
        store.session.clear().await?;
        Ok(())
    }
    #[test_resources("crates/database/data/owl-xml/*.owx")]
    async fn test_owx_parser_format(resource: &str) -> Result<(), VOWLRStoreError> {
        let store = VOWLRStore::default();
        store
            .insert_file(Path::new(&resource), false)
            .await
            .unwrap();
        assert_ne!(
            store.session.len().await.unwrap(),
            0,
            "Expected non-zero quads for: {}",
            resource
        );
        store.session.clear().await?;
        Ok(())
    }

    #[test_resources("crates/database/data/owl-functional/*.ofn")]
    async fn test_ofn_parser_stream(resource: &str) -> Result<(), VOWLRStoreError> {
        let mut out = vec![];
        let store = VOWLRStore::default();
        store.insert_file(Path::new(&resource), false).await?;
        let mut results = parse_stream_to(store.session.stream().await?, DataType::OWL).await?;
        while let Some(result) = results.next().await {
            out.extend(result?);
        }

        assert_ne!(out.len(), 0, "Expected non-zero quads for: {}", resource);
        store.session.clear().await?;
        Ok(())
    }
    #[test_resources("crates/database/data/owl-rdf/*.owl")]
    async fn test_owl_parser_stream(resource: &str) -> Result<(), VOWLRStoreError> {
        let mut out = vec![];
        let store = VOWLRStore::default();
        store.insert_file(Path::new(&resource), false).await?;
        let mut results = parse_stream_to(store.session.stream().await?, DataType::OWL).await?;
        while let Some(result) = results.next().await {
            out.extend(result?);
        }

        assert_ne!(out.len(), 0, "Expected non-zero quads for: {}", resource);
        store.session.clear().await?;
        Ok(())
    }
    #[test_resources("crates/database/data/owl-ttl/*.ttl")]
    async fn test_ttl_parser_stream(resource: &str) -> Result<(), VOWLRStoreError> {
        let mut out = vec![];
        let store = VOWLRStore::default();
        store.insert_file(Path::new(&resource), false).await?;
        let mut results = parse_stream_to(store.session.stream().await?, DataType::OWL).await?;
        while let Some(result) = results.next().await {
            out.extend(result?);
        }

        assert_ne!(out.len(), 0, "Expected non-zero quads for: {}", resource);
        store.session.clear().await?;
        Ok(())
    }
    #[test_resources("crates/database/data/owl-xml/*.owx")]
    async fn test_owx_parser_stream(resource: &str) -> Result<(), VOWLRStoreError> {
        let mut out = vec![];
        let store = VOWLRStore::default();
        store.insert_file(Path::new(&resource), false).await?;
        let mut results = parse_stream_to(store.session.stream().await?, DataType::OWL).await?;
        while let Some(result) = results.next().await {
            out.extend(result?);
        }

        assert_ne!(out.len(), 0, "Expected non-zero quads for: {}", resource);
        store.session.clear().await?;
        Ok(())
    }
}

use futures::{StreamExt, stream::BoxStream};
use grapher::prelude::GraphDisplayData;
use log::{debug, info, warn};
use rdf_fusion::execution::results::QueryResults;
use rdf_fusion::store::Store;
use std::path::Path;
use std::time::Duration;
use std::{fs::File, time::Instant};

use vowlr_parser::{
    errors::VOWLRStoreError,
    parser_util::{parse_stream_to, parser_from_path},
};
use vowlr_util::prelude::{DataType, VOWLRError};

use crate::errors::SerializationErrorKind;
use crate::serializers::frontend::GraphDisplayDataSolutionSerializer;

static GLOBAL_STORE: std::sync::OnceLock<Store> = std::sync::OnceLock::new();

/// The graph database.
pub struct VOWLRStore {
    /// The store is the quad database and SPARQL engine.
    pub session: Store,
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

    pub fn new_for_user(session: Store, user_id: String) -> Self {
        Self {
            session,
            user_id: Some(user_id),
            upload_handle: None,
        }
    }

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
    ) -> Result<(GraphDisplayData, Option<VOWLRError>), VOWLRError> {
        let solution_serializer = GraphDisplayDataSolutionSerializer::new();
        let query_stream = self
            .session
            .query(query.as_str())
            .await
            .map_err(|e| <VOWLRStoreError as Into<VOWLRError>>::into(e.into()))?;

        match query_stream {
            QueryResults::Solutions(query_solution_stream) => {
                let mut data_buffer = GraphDisplayData::new();

                let maybe_errors = solution_serializer
                    .serialize_nodes_stream(&mut data_buffer, query_solution_stream)
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
        let filename = fs.file_name().unwrap_or_default().to_string_lossy();
        let graph_iri = self.get_graph_iri(&filename);

        let parser = parser_from_path(fs, lenient, &graph_iri)?;
        info!("Loading input into database for graph {}...", graph_iri);
        let start_time = Instant::now();

        self.session
            .load_from_reader(parser.parser, parser.input.as_slice())
            .await?;
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

    pub async fn complete_upload(&mut self, filename: &str) -> Result<(), VOWLRStoreError> {
        let graph_iri = self.get_graph_iri(filename);
        if let Some(file) = &mut self.upload_handle {
            std::io::Write::flush(file)?;
            let path = file.path();

            let parser = parser_from_path(path, false, &graph_iri)?;

            info!("Loading input into database for graph {}...", graph_iri);
            let start_time = Instant::now();

            self.session
                .load_from_reader(parser.parser, parser.input.as_slice())
                .await?;
            info!(
                "Loaded {} quads in {} s",
                self.session.len().await?,
                Instant::now()
                    .checked_duration_since(start_time)
                    .unwrap_or(Duration::new(0, 0))
                    .as_secs_f32()
            );
        }
        self.upload_handle = None;
        Ok(())
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

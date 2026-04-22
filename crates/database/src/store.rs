use futures::stream::{BoxStream, StreamExt};
use grapher::prelude::GraphDisplayData;
use log::{debug, info, warn};
use rdf_fusion::execution::results::QueryResults;
use rdf_fusion::model::{NamedNodeRef, Quad};
use rdf_fusion::store::Store;
use reqwest::{Client, Url};
use std::collections::{HashSet, VecDeque};
use std::path::Path;
use std::time::Duration;
use std::time::Instant;
use strum::IntoEnumIterator;
use vowlgrapher_parser::errors::{VOWLGrapherStoreError, VOWLGrapherStoreErrorKind};
use vowlgrapher_parser::parser_util::{
    format_from_resource_type, parse_quads_to_format, parser_from_bytes, parser_from_path,
    path_type,
};
use vowlgrapher_serializer::prelude::GraphDisplayDataSolutionSerializer;
use vowlgrapher_util::prelude::{DataType, ErrorRecord, VOWLGRAPHER_ENVIRONMENT, VOWLGrapherError};

static GLOBAL_STORE: std::sync::OnceLock<Store> = std::sync::OnceLock::new();

/// The graph database.
pub struct VOWLGrapherStore {
    /// The store is the quad database and SPARQL engine.
    pub session: Store,
    /// The unique ID for the current user.
    pub user_id: Option<String>,
    upload_handle: Option<tempfile::NamedTempFile>,
}

impl VOWLGrapherStore {
    /// Create a new database instance.
    pub const fn new(session: Store) -> Self {
        Self {
            session,
            user_id: None,
            upload_handle: None,
        }
    }

    /// Create a new database instance with a user id.
    pub fn new_for_user(user_id: String) -> Self {
        let session = GLOBAL_STORE.get_or_init(Store::default).clone();
        Self {
            session,
            user_id: Some(user_id),
            upload_handle: None,
        }
    }

    /// Returns the unique graph name for a given filename and user.
    pub fn get_graph_name(&self, filename: &str) -> String {
        let filename = filename.replace(' ', "_").replace(['(', ')', '[', ']'], "");

        self.user_id.as_ref().map_or_else(
            || format!("urn:vowlgrapher:graph:{filename}"),
            |uid| format!("urn:vowlgrapher:user:{uid}:graph:{filename}"),
        )
    }

    /// Executes a SPARQL query and serializes the result.
    ///
    /// This method tries to continue serializing despite errors.
    /// As such, the `Ok` value contains non-fatal errors encountered during serialization.
    ///
    /// # Errors
    /// Returns an error if the query or serialization encountered a fatal problem.
    pub async fn query(
        &self,
        query: String,
        graph_name: Option<String>,
    ) -> Result<(GraphDisplayData, Option<VOWLGrapherError>), VOWLGrapherError> {
        debug!(
            "Querying with graph_name: {}",
            graph_name.clone().unwrap_or_else(|| "None".to_string())
        );
        let user_query = graph_name.map_or_else(
            || query.replace("GRAPH <{GRAPH_IRI}>", ""),
            |name| {
                let graph_name = self.get_graph_name(&name);
                query.replace("{GRAPH_IRI}", &graph_name)
            },
        );

        let solution_serializer = GraphDisplayDataSolutionSerializer::new();
        let query_stream = self
            .session
            .query(&user_query)
            .await
            .map_err(|e| <VOWLGrapherStoreError as Into<VOWLGrapherError>>::into(e.into()))?;

        match query_stream {
            QueryResults::Solutions(query_solution_stream) => {
                let mut data_buffer = GraphDisplayData::new();

                let maybe_errors = solution_serializer
                    .serialize_solution_stream(&mut data_buffer, query_solution_stream)
                    .await?;
                Ok((data_buffer, maybe_errors))
            }
            QueryResults::Boolean(_result) => Err(VOWLGrapherStoreErrorKind::UnsupportedQueryType(
                "Query stream is not a SELECT query".to_string(),
            )
            .into()),
            QueryResults::Graph(_query_triple_stream) => {
                // TODO: Implement to support user-defined SPARQL queries
                Err(VOWLGrapherStoreErrorKind::UnsupportedQueryType(
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
    ///
    /// # Errors
    /// Returns an error if the file fails to parse or fails to be inserted into the store.
    pub async fn insert_file(
        &self,
        fs: &Path,
        lenient: bool,
    ) -> Result<Option<VOWLGrapherError>, VOWLGrapherStoreError> {
        let graph_name = self.get_graph_name(&fs.to_string_lossy());
        let format = path_type(fs).ok_or_else(|| {
            VOWLGrapherStoreErrorKind::InvalidFileType("Unknown file extension".into())
        })?;

        let root_quads = parser_from_path(fs, format, lenient, &graph_name)?;
        let (quads, warnings) = self
            .flatten_import_closure(root_quads, &graph_name, lenient, ImportBase::from_path(fs))
            .await?;

        info!("Loading graph '{graph_name}' into database...");
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

        Ok(warnings)
    }

    fn load_file(
        path: &Path,
        lenient: bool,
        graph_name: &str,
    ) -> Result<(Vec<Quad>, DataType), VOWLGrapherStoreError> {
        let dtype = path.into();
        if dtype == DataType::UNKNOWN {
            Self::try_load_fallback(path, lenient, None, graph_name)
        } else {
            let result =
                std::panic::catch_unwind(|| parser_from_path(path, dtype, lenient, graph_name));
            match result {
                Ok(Ok(quads)) => Ok((quads, dtype)),
                _ => Self::try_load_fallback(path, lenient, Some(dtype), graph_name),
            }
        }
    }

    /// Fallback parsers for when the main parser failed.
    ///
    /// This iterates over remaining parse formats and tries each until it succeeds or fails.
    ///
    /// # Errors
    /// Returns an error if no format supports the file at the given path.
    fn try_load_fallback(
        path: &Path,
        lenient: bool,
        skip_format: Option<DataType>,
        graph_name: &str,
    ) -> Result<(Vec<Quad>, DataType), VOWLGrapherStoreError> {
        for format in DataType::iter().filter(|f| *f != DataType::UNKNOWN) {
            if Some(format) == skip_format {
                continue;
            }

            let result =
                std::panic::catch_unwind(|| parser_from_path(path, format, lenient, graph_name));
            if let Ok(Ok(quads)) = result {
                info!("Parsed file as {format:?}");
                return Ok((quads, format));
            }
        }

        Err(VOWLGrapherStoreErrorKind::InvalidFileType(format!(
            "Could not parse file. Tried with the following formats: {:?}",
            DataType::iter()
                .filter(|f| *f != DataType::UNKNOWN)
                .collect::<Vec<_>>()
        ))
        .into())
    }

    fn load_bytes_with_fallback(
        bytes: &[u8],
        hinted_format: DataType,
        lenient: bool,
        graph_iri: &str,
    ) -> Result<(Vec<Quad>, DataType), VOWLGrapherStoreError> {
        let formats: Vec<DataType> = if hinted_format == DataType::UNKNOWN {
            DataType::iter()
                .filter(|f| *f != DataType::UNKNOWN)
                .collect()
        } else {
            std::iter::once(hinted_format)
                .chain(DataType::iter().filter(|f| *f != DataType::UNKNOWN && *f != hinted_format))
                .collect()
        };

        for format in formats {
            let result =
                std::panic::catch_unwind(|| parser_from_bytes(bytes, format, lenient, graph_iri));
            if let Ok(Ok(quads)) = result {
                info!("Parsed import as {format:?}");
                return Ok((quads, format));
            }
        }

        Err(VOWLGrapherStoreErrorKind::InvalidFileType(
            "Could not parse imported ontology".to_string(),
        )
        .into())
    }

    /// Serializes the store into a stream of the specified resource type.
    ///
    /// # Errors
    /// Returns an error if the store fails to serialize its content.
    pub async fn serialize_stream(
        &self,
        resource_type: DataType,
        graph_name: &str,
    ) -> Result<BoxStream<'static, Result<Vec<u8>, VOWLGrapherStoreError>>, VOWLGrapherStoreError>
    {
        debug!(
            "Store size before export: {}",
            self.session.len().await.unwrap_or(0)
        );
        let graph_name = self.get_graph_name(graph_name);
        let graph_ref = NamedNodeRef::new(&graph_name)?;

        if matches!(resource_type, DataType::OWL | DataType::OFN | DataType::OWX) {
            info!("Exporting graph '{graph_name}' as {resource_type:?}...");
            let quads_stream = self
                .session
                .quads_for_pattern(None, None, None, Some(graph_ref.into()))
                .await?;
            return parse_quads_to_format(quads_stream, resource_type).await;
        }

        if let Some(format) = format_from_resource_type(&resource_type) {
            info!("Exporting graph '{graph_name}' as {format:?}...");
            let buf = self
                .session
                .dump_graph_to_writer(graph_ref, format, Vec::new())
                .await?;
            return Ok(futures::stream::once(async move { Ok(buf) }).boxed());
        }
        Err(VOWLGrapherStoreError::from(
            VOWLGrapherStoreErrorKind::InvalidFileType(format!(
                "Unsupported output type: {resource_type:?}"
            )),
        ))
    }

    /// Create a temporary file on the server to upload user input into.
    ///
    /// # Errors
    /// Returns an error if the file cannot be created.
    pub fn start_upload(&mut self, filename: &str) -> Result<(), VOWLGrapherStoreError> {
        let extension = Path::new(filename)
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("owl");
        let file = tempfile::Builder::new()
            .suffix(&format!(".{extension}"))
            .tempfile()?;
        self.upload_handle = Some(file);
        Ok(())
    }

    /// Insert a chunk of data into the file currently in use.
    ///
    /// # Errors
    /// Returns an error if the data cannot ve written to the file.
    pub fn upload_chunk(&mut self, data: &[u8]) -> Result<(), VOWLGrapherStoreError> {
        if let Some(file) = &mut self.upload_handle {
            std::io::Write::write_all(file, data)?;
        } else {
            warn!("upload_chunk called without start_upload");
        }
        Ok(())
    }

    /// Inserts a file into the store.
    ///
    /// Files are automatically parsed.
    ///
    /// # Errors
    /// Returns an error if the file fails to parse or
    /// the store fails to load the triples of the file.
    pub async fn complete_upload(
        &mut self,
        filename: &str,
    ) -> Result<(DataType, Option<VOWLGrapherError>), VOWLGrapherStoreError> {
        let graph_name = self.get_graph_name(filename);
        let path = if let Some(file) = &mut self.upload_handle {
            std::io::Write::flush(file)?;
            file.path().to_path_buf()
        } else {
            return Err(VOWLGrapherStoreErrorKind::InvalidFileType(
                "No upload handle found".to_string(),
            )
            .into());
        };

        let (root_quads, loaded_format) = Self::load_file(&path, false, &graph_name)?;
        let (quads, warnings) = self
            .flatten_import_closure(
                root_quads,
                &graph_name,
                false,
                ImportBase::from_user_input(filename),
            )
            .await?;

        info!("Loading graph '{graph_name}' into database...");
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
        Ok((loaded_format, warnings))
    }

    async fn flatten_import_closure(
        &self,
        root_quads: Vec<Quad>,
        graph_iri: &str,
        lenient: bool,
        root_base: ImportBase,
    ) -> Result<(Vec<Quad>, Option<VOWLGrapherError>), VOWLGrapherStoreError> {
        if !VOWLGRAPHER_ENVIRONMENT.resolve_imports {
            info!("Import resolution disabled via VOWLGRAPHER_RESOLVE_IMPORTS");
            return Ok((root_quads, None));
        }

        let client = Client::new();
        let mut all_quads = root_quads.clone();
        let mut visited = HashSet::<String>::new();
        let mut queue = VecDeque::<(String, ImportBase)>::new();
        let mut warnings = Vec::<ErrorRecord>::new();

        for import in extract_import_iris(&root_quads).await? {
            queue.push_back((import, root_base.clone()));
        }

        while let Some((raw_import, parent_base)) = queue.pop_front() {
            let resolved = match parent_base.resolve(&raw_import) {
                Ok(url) => url,
                Err(err) => {
                    warn!("Skipping unresolved import '{raw_import}': {err}");
                    warnings.push(err.into());
                    continue;
                }
            };
            let resolved_key = resolved.to_string();

            if !visited.insert(resolved_key.clone()) {
                continue;
            }

            let (bytes, hinted_format, next_base) =
                match fetch_import_source(&client, &resolved).await {
                    Ok(source) => source,
                    Err(err) => {
                        warn!("Skipping failed import fetch '{resolved}': {err}");
                        warnings.push(err.into());
                        continue;
                    }
                };

            let (quads, _) =
                match Self::load_bytes_with_fallback(&bytes, hinted_format, lenient, graph_iri) {
                    Ok(parsed) => parsed,
                    Err(err) => {
                        warn!("Skipping unparsable import '{resolved}': {err}");
                        warnings.push(err.into());
                        continue;
                    }
                };

            match extract_import_iris(&quads).await {
                Ok(nested_imports) => {
                    for nested in nested_imports {
                        queue.push_back((nested, next_base.clone()));
                    }
                }
                Err(err) => {
                    warn!("Failed to inspect nested imports for '{resolved}': {err}");
                    warnings.push(err.into());
                }
            }

            all_quads.extend(quads);
        }

        let warnings = if warnings.is_empty() {
            None
        } else {
            Some(warnings.into())
        };

        Ok((all_quads, warnings))
    }
}

impl Default for VOWLGrapherStore {
    fn default() -> Self {
        let session = GLOBAL_STORE.get_or_init(Store::default).clone();
        Self::new(session)
    }
}

async fn extract_import_iris(quads: &[Quad]) -> Result<Vec<String>, VOWLGrapherStoreError> {
    let tmp = Store::default();
    tmp.extend(quads.iter().cloned()).await?;

    let results = tmp
        .query(
            r"
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        SELECT DISTINCT ?import
        WHERE {
            GRAPH ?g {
                ?ontology owl:imports ?import .
                FILTER(isIRI(?import))
            }
        }
        ",
        )
        .await?;

    let mut imports = Vec::new();
    if let QueryResults::Solutions(mut solutions) = results {
        while let Some(solution) = solutions.next().await {
            let solution = solution?;
            if let Some(term) = solution.get("import") {
                imports.push(
                    term.to_string()
                        .trim_start_matches('<')
                        .trim_end_matches('>')
                        .to_string(),
                );
            }
        }
    }

    Ok(imports)
}

async fn fetch_import_source(
    client: &Client,
    url: &Url,
) -> Result<(Vec<u8>, DataType, ImportBase), VOWLGrapherStoreError> {
    let format = DataType::from(Path::new(url.path()));

    match url.scheme() {
        "file" => {
            let path = url.to_file_path().map_err(|()| {
                VOWLGrapherStoreErrorKind::ImportResolutionError(format!(
                    "Could not convert file URL to path: {url}"
                ))
            })?;
            let bytes = std::fs::read(&path)?;
            Ok((bytes, format, ImportBase::from_path(&path)))
        }
        "http" | "https" => {
            let response = client.get(url.clone()).send().await.map_err(|e| {
                VOWLGrapherStoreErrorKind::RemoteFetchError(format!(
                    "Failed to fetch import {url}: {e}"
                ))
            })?;

            let bytes = response.bytes().await.map_err(|e| {
                VOWLGrapherStoreErrorKind::RemoteFetchError(format!(
                    "Failed to read import body {url}: {e}"
                ))
            })?;

            Ok((bytes.to_vec(), format, ImportBase::Url(url.clone())))
        }
        scheme => Err(VOWLGrapherStoreErrorKind::ImportResolutionError(format!(
            "Unsupported import scheme '{scheme}' for {url}"
        ))
        .into()),
    }
}

#[derive(Clone, Debug)]
enum ImportBase {
    Url(Url),
    Unknown,
}

impl ImportBase {
    fn from_path(path: &Path) -> Self {
        Url::from_file_path(path).map_or(Self::Unknown, Self::Url)
    }

    fn from_user_input(input: &str) -> Self {
        Url::parse(input).map_or(Self::Unknown, Self::Url)
    }

    fn resolve(&self, import_iri: &str) -> Result<Url, VOWLGrapherStoreError> {
        match self {
            Self::Url(base) => base
                .join(import_iri)
                .or_else(|_| Url::parse(import_iri))
                .map_err(|e| {
                    VOWLGrapherStoreErrorKind::ImportResolutionError(e.to_string()).into()
                }),
            Self::Unknown => Url::parse(import_iri).map_err(|e| {
                VOWLGrapherStoreErrorKind::ImportResolutionError(e.to_string()).into()
            }),
        }
    }
}

#[cfg(test)]
#[expect(unused_must_use, clippy::expect_used)]
mod test {
    use super::*;
    use test_generator::test_resources;

    #[test_resources("crates/database/data/owl-functional/*.ofn")]
    async fn test_ofn_parser_format(resource: &str) -> Result<(), VOWLGrapherStoreError> {
        let store = VOWLGrapherStore::default();
        store
            .insert_file(Path::new(&resource), false)
            .await
            .expect("inserting file should succeed");
        assert_ne!(
            store
                .session
                .len()
                .await
                .expect("getting store length should succeed"),
            0,
            "Expected non-zero quads for: {resource}"
        );
        store.session.clear().await?;
        Ok(())
    }
    #[test_resources("crates/database/data/owl-rdf/*.owl")]
    async fn test_owl_parser_format(resource: &str) -> Result<(), VOWLGrapherStoreError> {
        let store = VOWLGrapherStore::default();
        store
            .insert_file(Path::new(&resource), false)
            .await
            .expect("inserting file should succeed");
        assert_ne!(
            store
                .session
                .len()
                .await
                .expect("getting store length should succeed"),
            0,
            "Expected non-zero quads for: {resource}"
        );
        store.session.clear().await?;
        Ok(())
    }
    #[test_resources("crates/database/data/owl-ttl/*.ttl")]
    async fn test_ttl_parser_format(resource: &str) -> Result<(), VOWLGrapherStoreError> {
        let store = VOWLGrapherStore::default();
        store
            .insert_file(Path::new(&resource), false)
            .await
            .expect("inserting file should succeed");
        assert_ne!(
            store
                .session
                .len()
                .await
                .expect("getting store length should succeed"),
            0,
            "Expected non-zero quads for: {resource}"
        );
        store.session.clear().await?;
        Ok(())
    }
    #[test_resources("crates/database/data/owl-xml/*.owx")]
    async fn test_owx_parser_format(resource: &str) -> Result<(), VOWLGrapherStoreError> {
        let store = VOWLGrapherStore::default();
        store
            .insert_file(Path::new(&resource), false)
            .await
            .expect("inserting file should succeed");
        assert_ne!(
            store
                .session
                .len()
                .await
                .expect("getting store length should succeed"),
            0,
            "Expected non-zero quads for: {resource}"
        );
        store.session.clear().await?;
        Ok(())
    }

    #[test_resources("crates/database/data/owl-functional/*.ofn")]
    async fn test_ofn_parser_stream(resource: &str) -> Result<(), VOWLGrapherStoreError> {
        let mut out = vec![];
        let store = VOWLGrapherStore::default();
        store.insert_file(Path::new(&resource), false).await?;
        let mut results = store.serialize_stream(DataType::OWL, resource).await?;
        while let Some(result) = futures::StreamExt::next(&mut results).await {
            out.extend(result?);
        }

        assert_ne!(out.len(), 0, "Expected non-zero quads for: {resource}");
        store.session.clear().await?;
        Ok(())
    }
    #[test_resources("crates/database/data/owl-rdf/*.owl")]
    async fn test_owl_parser_stream(resource: &str) -> Result<(), VOWLGrapherStoreError> {
        let mut out = vec![];
        let store = VOWLGrapherStore::default();
        store.insert_file(Path::new(&resource), false).await?;
        let mut stream = store.serialize_stream(DataType::OWL, resource).await?;
        while let Some(result) = stream.next().await {
            out.extend(result?);
        }

        assert_ne!(out.len(), 0, "Expected non-zero quads for: {resource}");
        store.session.clear().await?;
        Ok(())
    }
    #[test_resources("crates/database/data/owl-ttl/*.ttl")]
    async fn test_ttl_parser_stream(resource: &str) -> Result<(), VOWLGrapherStoreError> {
        let mut out = vec![];
        let store = VOWLGrapherStore::default();
        store.insert_file(Path::new(&resource), false).await?;
        let mut stream = store.serialize_stream(DataType::OWL, resource).await?;
        while let Some(result) = stream.next().await {
            out.extend(result?);
        }

        assert_ne!(out.len(), 0, "Expected non-zero quads for: {resource}");
        store.session.clear().await?;
        Ok(())
    }
    #[test_resources("crates/database/data/owl-xml/*.owx")]
    async fn test_owx_parser_stream(resource: &str) -> Result<(), VOWLGrapherStoreError> {
        let mut out = vec![];
        let store = VOWLGrapherStore::default();
        store.insert_file(Path::new(&resource), false).await?;
        let mut stream = store.serialize_stream(DataType::OWL, resource).await?;
        while let Some(result) = stream.next().await {
            out.extend(result?);
        }
        assert_ne!(out.len(), 0, "Expected non-zero quads for: {resource}");
        store.session.clear().await?;
        Ok(())
    }
}

#[cfg(feature = "server")]
use futures::StreamExt;
use gloo_timers::callback::Interval;
use leptos::prelude::*;
use leptos::server_fn::ServerFnError;
use leptos::server_fn::codec::{MultipartData, MultipartFormData, StreamingText, TextStream};
use leptos::task::spawn_local;
use log::{debug, info};
#[cfg(feature = "server")]
use reqwest::Client;
use std::cell::RefCell;
#[cfg(feature = "server")]
use std::path::Path;
use std::rc::Rc;
#[cfg(feature = "server")]
use vowlr_database::prelude::VOWLRStore;
use vowlr_util::prelude::{DataType, VOWLRError};
use web_sys::{FileList, FormData};

use crate::errors::ClientErrorKind;

const MAX_FILE_SIZE_BYTES: usize = 50 * 1024 * 1024;

#[cfg(feature = "ssr")]
pub async fn manage_user_id() -> Result<String, ServerFnError> {
    use actix_session::Session;
    use leptos_actix::extract;
    use uuid::Uuid;
    let user_session = extract::<Session>()
        .await
        .map_err(|e| ServerFnError::new(format!("Failed to extract session: {}", e)))?;

    if let Ok(Some(user_id)) = user_session.get::<String>("user_id") {
        return Ok(user_id);
    }

    let new_user_id = Uuid::new_v4().to_string();

    user_session
        .insert("user_id", &new_user_id)
        .map_err(|e| ServerFnError::new(format!("Failed to save session: {}", e)))?;

    Ok(new_user_id)
}

#[cfg(feature = "ssr")]
mod progress {
    use async_broadcast::{Receiver, Sender, broadcast};
    use dashmap::DashMap;
    use futures::Stream;
    use std::sync::LazyLock;

    struct File {
        total: usize,
        tx: Sender<usize>,
        rx: Receiver<usize>,
    }

    static FILES: LazyLock<DashMap<String, File>> = LazyLock::new(DashMap::new);

    pub async fn add_chunk(filename: &str, len: usize) {
        let mut entry = FILES.entry(filename.to_string()).or_insert_with(|| {
            let (mut tx, rx) = broadcast(128);
            tx.set_overflow(true);
            File { total: 0, tx, rx }
        });
        entry.total += len;
        let new_total = entry.total;

        let tx = entry.tx.clone();
        drop(entry);

        let _ = tx.broadcast(new_total).await;
    }

    pub fn reset(filename: &str) {
        if let Some(mut entry) = FILES.get_mut(filename) {
            entry.total = 0;
        }
    }

    pub fn remove(filename: &str) {
        if FILES.remove(filename).is_some() {
            // println!("Removed progress entry for '{}'", filename);
        }
    }

    pub fn for_file(filename: String) -> impl Stream<Item = usize> {
        let entry = FILES.entry(filename).or_insert_with(|| {
            let (mut tx, rx) = broadcast(2048);
            tx.set_overflow(true);
            File { total: 0, tx, rx }
        });
        entry.rx.clone()
    }
}

#[server(output = StreamingText)]
pub async fn ontology_progress(filename: String) -> Result<TextStream, ServerFnError> {
    debug!("Initializing progress counter with file {filename}");
    let progress = progress::for_file(filename);
    debug!("Mapping usize to String");
    let progress = progress.map(|bytes| Ok(format!("{bytes}\n")));
    debug!("Creating text stream");
    let ts = TextStream::new(progress);
    debug!("OK");
    Ok(ts)
}

#[server(
    input = MultipartFormData,
)]
pub async fn handle_local(data: MultipartData) -> Result<(DataType, usize), VOWLRError> {
    let user_id = manage_user_id().await?;
    info!("User {} is uploading a local file", user_id);

    let mut session = VOWLRStore::default();
    session.user_id = Some(user_id);

    let mut data = data
        .into_inner()
        .ok_or_else(|| ServerFnError::new("data must be server-side"))?;
    let mut count = 0;
    let mut dtype = DataType::UNKNOWN;
    while let Ok(Some(mut field)) = data.next_field().await {
        let name = field.file_name().unwrap_or_default().to_string();

        if name.is_empty() {
            return Err(ServerFnError::new("Received empty file string").into());
        }

        info!("Receiving file '{name}'");
        progress::reset(&name);
        debug!("Resetting progress");

        session.start_upload(&name).await?;

        dtype = Path::new(&name).into();

        while let Ok(Some(chunk)) = field.chunk().await {
            let len = chunk.len();
            count += len;

            if count > MAX_FILE_SIZE_BYTES {
                return Err(ServerFnError::ServerError(format!(
                    "File {name} exceeds the maximum allowed size of {}MB.",
                    MAX_FILE_SIZE_BYTES / 1024 / 1024
                ))
                .into());
            }

            session.upload_chunk(&chunk).await?;
            progress::add_chunk(&name, len).await;
        }

        if !name.is_empty() {
            progress::remove(&name);
        }
        session.complete_upload(&name).await?;
    }

    Ok((dtype, count))
}

/// Remote reads url and calls for the datatype label and returns (label, data content)
#[server]
pub async fn handle_remote(url: String) -> Result<(DataType, usize), VOWLRError> {
    let user_id = manage_user_id().await?;
    info!("User {} is uploading a remote file", user_id);

    debug!("Sending request to remote: '{url}'");
    let client = Client::new();
    let resp = match client.get(&url).send().await {
        Ok(r) => r,
        Err(e) => {
            return Err(ServerFnError::ServerError(format!("Error fetching URL: {e}")).into());
        }
    };

    if let Some(content_length) = resp.content_length() {
        let size = usize::try_from(content_length).unwrap_or(usize::MAX);
        if size > MAX_FILE_SIZE_BYTES {
            return Err(ServerFnError::ServerError(format!(
                "Remote file exceeds the maximum allowed size of {}MB.",
                MAX_FILE_SIZE_BYTES / 1024 / 1024
            ))
            .into());
        }
    }

    let mut session = VOWLRStore::default();
    session.user_id = Some(user_id);

    let progress_key = url.clone();
    progress::reset(&progress_key);
    session.start_upload(&url).await?;

    let mut total = 0;
    let dtype = Path::new(&url).into();

    let mut stream = resp.bytes_stream();
    while let Some(chunk_result) = stream.next().await {
        let chunk =
            chunk_result.map_err(|e| ServerFnError::new(format!("Error reading chunk: {e}")))?;

        total += chunk.len();
        session.upload_chunk(&chunk).await?;
        progress::add_chunk(&progress_key, chunk.len()).await;
    }

    progress::remove(&progress_key);
    session.complete_upload(&url).await?;
    Ok((dtype, total))
}

/// Sparql reads (endpoint + query) and calls for the datatype label and returns (label, data content)
#[server]
pub async fn handle_sparql(
    endpoint: String,
    query: String,
    format: Option<String>,
) -> Result<(DataType, usize), VOWLRError> {
    let user_id = manage_user_id().await?;
    info!("User {} is quering SPARQL", user_id);

    let client = Client::new();

    let mut session = VOWLRStore::default();
    session.user_id = Some(user_id);

    let accept_type = match format.as_deref() {
        Some("xml") => DataType::SPARQLXML.mime_type(),
        Some("tsv") => DataType::SPARQLTSV.mime_type(),
        Some("csv") => DataType::SPARQLCSV.mime_type(),
        Some("json") => DataType::SPARQLJSON.mime_type(),
        _ => DataType::UNKNOWN.mime_type(),
    };

    let resp = client
        .post(&endpoint)
        .header("Accept", accept_type)
        .form(&[("query", query)])
        .send()
        .await
        .map_err(|e| ServerFnError::new(format!("Error querying SPARQL endpoint: {e}")))?;

    let progress_key = format!("sparql-{endpoint}");
    progress::reset(&progress_key);
    session.start_upload(&progress_key).await?;

    let mut total = 0;
    let mut stream = resp.bytes_stream();
    while let Some(chunk_result) = stream.next().await {
        let chunk =
            chunk_result.map_err(|e| ServerFnError::new(format!("Error reading chunk: {e}")))?;

        total += chunk.len();
        session.upload_chunk(&chunk).await?;
        progress::add_chunk(&progress_key, chunk.len()).await;
    }

    progress::remove(&progress_key);
    session.complete_upload(&progress_key).await?;

    let dtype = if accept_type.contains("xml") {
        DataType::SPARQLXML
    } else {
        DataType::SPARQLJSON
    };
    Ok((dtype, total))
}

pub struct UploadProgress {
    pub filename: RwSignal<String>,
    pub url_name: RwSignal<String>,
    pub file_size: RwSignal<f64>,
    pub upload_progress: RwSignal<i32>,
    pub parsing_status: RwSignal<String>,
    pub parsing_done: RwSignal<bool>,
    pub interval_handle: Rc<RefCell<Option<Interval>>>,
}
impl UploadProgress {
    #[must_use]
    pub fn new() -> Self {
        Self {
            filename: RwSignal::new(String::new()),
            url_name: RwSignal::new(String::new()),
            file_size: RwSignal::new(0.0),
            upload_progress: RwSignal::new(0),
            parsing_status: RwSignal::new(String::new()),
            parsing_done: RwSignal::new(false),
            interval_handle: Rc::new(RefCell::new(None)),
        }
    }

    #[expect(unused, reason = "not yet implemented")]
    fn track_progress<F>(&self, key: &str, total_size: Option<f64>, is_url: bool, dispatch: F)
    where
        F: FnOnce() + 'static,
    {
        if is_url {
            self.url_name.set(key.to_string());
        } else {
            self.filename.set(key.to_string());
        }
        self.upload_progress.set(0);
        self.parsing_status.set(String::new());
        self.parsing_done.set(false);

        let progress = self.upload_progress;
        let status = self.parsing_status;
        let done = self.parsing_done;
        let interval_handle = Rc::clone(&self.interval_handle);

        spawn_local(async move {
            dispatch();
            // Code below is progress bar and it only works on Chromium-based browsers (sometimes)
            // match ontology_progress(key).await {
            //     Ok(stream_result) => {
            //         debug!("Dispatching");
            //         dispatch();
            //         let mut stream = stream_result.into_inner();
            //         while let Some(result) = stream.next().await {
            //             match result {
            //                 Ok(chunk) => {
            //                     if let Ok(bytes) = chunk.trim().parse::<usize>() {
            //                         if let Some(total) = total_size {
            //                             let percent = (bytes as f64 / total as f64) * 100.0;
            //                             progress.set(percent as i32);
            //                         } else {
            //                             let current = progress.get();
            //                             progress.set((current + 5).min(95));
            //                             // progress.set(new_progress);
            //                         }
            //                     }
            //                 }
            //                 Err(e) => error!("{}", e),
            //             }
            //         }

            //         progress.set(100);
            //         status.set("Parsing".to_string());

            //         let interval = Interval::new(1500, move || {
            //             status.update(|s| {
            //                 if s.ends_with("......") {
            //                     *s = "Parsing".to_string();
            //                 } else {
            //                     s.push('.');
            //                 }
            //             });
            //         });

            //         let mut handle = interval_handle.borrow_mut();
            //         if let Some(existing) = handle.take() {
            //             existing.cancel();
            //         }
            //         *handle = Some(interval);
            //         done.set(true);
            //     }
            //     Err(e) => {
            //         error!("Failed to connect to progress stream: {:?}", e);
            //         dispatch();
            //     }
            // }
        });
    }

    #[expect(
        clippy::missing_errors_doc,
        reason = "why does clippy only complain about this method? (TODO: Add docs to all functions)"
    )]
    pub fn upload_files<F>(&self, file_list: &FileList, dispatch: F) -> Result<(), VOWLRError>
    where
        F: FnOnce(FormData) + 'static,
    {
        let len = file_list.length();
        let form =
            FormData::new().map_err(|e| ClientErrorKind::JavaScriptError(format!("{e:#?}")))?;
        info!("Preparing filelist with {len} files");

        // let mut total_size = 0;
        if let Some(file) = file_list.item(0) {
            self.filename.set(file.name());
            self.file_size.set(file.size());
        }

        for i in 0..len {
            if let Some(file) = file_list.item(i) {
                form.append_with_blob("file_to_upload", &file)
                    .map_err(|e| ClientErrorKind::JavaScriptError(format!("{e:#?}")))?;
            }
        }

        let fname = self.filename.get_untracked();
        self.track_progress(&fname, Some(self.file_size.get()), false, move || {
            dispatch(form);
        });
        Ok(())
    }

    pub fn upload_url<F>(&self, url: &str, dispatch: F)
    where
        F: FnOnce(String) + 'static,
    {
        let url_string = url.to_string();
        self.url_name.set(url.to_string());
        self.track_progress(url, None, true, move || dispatch(url_string));
    }

    pub fn upload_sparql<F>(&self, endpoint: &str, query: &str, dispatch: F)
    where
        F: FnOnce((String, String, Option<String>)) + 'static,
    {
        let key = format!("sparql-{endpoint}");
        let ep = endpoint.to_string();
        let q = query.to_string();
        let fmt = Some("json".to_string());
        self.track_progress(&key, None, true, move || dispatch((ep, q, fmt)));
    }
}

impl Default for UploadProgress {
    fn default() -> Self {
        Self::new()
    }
}

/// handles what server side function to use (local, remote or sparql)
#[derive(Clone)]
pub struct FileUpload {
    pub mode: RwSignal<String>,
    pub local_action: Action<FormData, Result<(DataType, usize), VOWLRError>>,
    pub remote_action: Action<String, Result<(DataType, usize), VOWLRError>>,
    #[expect(clippy::type_complexity)]
    pub sparql_action:
        Action<(String, String, Option<String>), Result<(DataType, usize), VOWLRError>>,
    pub tracker: Rc<UploadProgress>,
}

impl FileUpload {
    pub fn new() -> Self {
        let mode = RwSignal::new("local".to_string());

        let local_action =
            Action::<FormData, Result<(DataType, usize), VOWLRError>>::new_local(|data| {
                handle_local(data.clone().into())
            });

        let remote_action = Action::<String, Result<(DataType, usize), VOWLRError>>::new(|url| {
            handle_remote(url.clone())
        });

        let sparql_action = Action::<
            (String, String, Option<String>),
            Result<(DataType, usize), VOWLRError>,
        >::new(|(endpoint, query, format)| {
            handle_sparql(endpoint.clone(), query.clone(), format.clone())
        });

        let tracker = Rc::new(UploadProgress::new());

        Self {
            mode,
            local_action,
            remote_action,
            sparql_action,
            tracker,
        }
    }

    pub fn get_result(&self) -> Option<Result<(DataType, usize), VOWLRError>> {
        match self.mode.get().as_str() {
            "local" => self.local_action.value().get(),
            "remote" => self.remote_action.value().get(),
            "sparql" => self.sparql_action.value().get(),
            _ => None,
        }
    }
}

impl Default for FileUpload {
    fn default() -> Self {
        Self::new()
    }
}

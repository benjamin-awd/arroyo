//! Multi-cloud storage abstraction.
//!
//! Provides a unified interface for working with S3, GCS, Azure Blob Storage,
//! and local filesystem.

use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt, future::ready};
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload, RetryConfig};
use regex::Regex;
use snafu::prelude::*;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use tracing::debug;

use crate::emit;
use crate::error::{
    AzureConfigSnafu, GcsConfigSnafu, InvalidUrlSnafu, IoSnafu, ObjectStoreSnafu, S3ConfigSnafu,
    StorageError,
};
use crate::internal_events::{
    ActiveMultipartParts, MultipartUploadCompleted, RequestStatus, StorageOperation,
    StorageRequest, StorageRequestDuration,
};

/// A reference-counted storage provider.
pub type StorageProviderRef = Arc<StorageProvider>;

/// Storage provider that abstracts over different cloud storage backends.
#[derive(Clone)]
pub struct StorageProvider {
    config: BackendConfig,
    object_store: Arc<dyn ObjectStore>,
    /// MultipartStore for parallel part uploads with explicit part numbering.
    /// Some backends (S3, GCS, Azure) support this; local filesystem does not.
    multipart_store: Option<Arc<dyn MultipartStore>>,
    canonical_url: String,
    storage_options: HashMap<String, String>,
}

impl std::fmt::Debug for StorageProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StorageProvider<{}>", self.canonical_url)
    }
}

// URL patterns for different storage backends
const S3_PATH: &str =
    r"^https://s3\.(?P<region>[\w\-]+)\.amazonaws\.com/(?P<bucket>[a-z0-9\-\.]+)(/(?P<key>.+))?$";
const S3_VIRTUAL: &str =
    r"^https://(?P<bucket>[a-z0-9\-\.]+)\.s3\.(?P<region>[\w\-]+)\.amazonaws\.com(/(?P<key>.+))?$";
const S3_URL: &str = r"^[sS]3[aA]?://(?P<bucket>[a-z0-9\-\.]+)(/(?P<key>.+))?$";
const S3_ENDPOINT_URL: &str = r"^[sS]3[aA]?::(?<protocol>https?)://(?P<endpoint>[^:/]+):(?<port>\d+)/(?P<bucket>[a-z0-9\-\.]+)(/(?P<key>.+))?$";

const FILE_URI: &str = r"^file://(?P<path>.*)$";
const FILE_URL: &str = r"^file:(?P<path>.*)$";
const FILE_PATH: &str = r"^/(?P<path>.*)$";

const GCS_VIRTUAL: &str =
    r"^https://(?P<bucket>[a-z0-9\-_\.]+)\.storage\.googleapis\.com(/(?P<key>.+))?$";
const GCS_PATH: &str =
    r"^https://storage\.googleapis\.com/(?P<bucket>[a-z0-9\-_\.]+)(/(?P<key>.+))?$";
const GCS_URL: &str = r"^[gG][sS]://(?P<bucket>[a-z0-9\-\._]+)(/(?P<key>.+))?$";

const ABFS_URL: &str = r"^abfss?://(?P<container>[a-z0-9\-]+)@(?P<account>[a-z0-9]+)\.dfs\.core\.windows\.net(/(?P<key>.+))?$";
const AZURE_HTTPS: &str = r"^https://(?P<account>[a-z0-9]+)\.(blob|dfs)\.core\.windows\.net/(?P<container>[a-z0-9\-]+)(/(?P<key>.+))?$";

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
enum Backend {
    S3,
    Gcs,
    Azure,
    Local,
}

/// Create a standard retry configuration for cloud storage operations.
///
/// Handles transient errors like 500s, rate limits, and network timeouts
/// with exponential backoff. Uses object_store defaults (max_retries: 10,
/// retry_timeout: 3 minutes).
fn default_retry_config() -> RetryConfig {
    RetryConfig::default()
}

fn matchers() -> &'static HashMap<Backend, Vec<Regex>> {
    static MATCHERS: OnceLock<HashMap<Backend, Vec<Regex>>> = OnceLock::new();
    MATCHERS.get_or_init(|| {
        let mut m = HashMap::new();

        m.insert(
            Backend::S3,
            vec![
                Regex::new(S3_PATH).unwrap(),
                Regex::new(S3_VIRTUAL).unwrap(),
                Regex::new(S3_ENDPOINT_URL).unwrap(),
                Regex::new(S3_URL).unwrap(),
            ],
        );

        m.insert(
            Backend::Gcs,
            vec![
                Regex::new(GCS_PATH).unwrap(),
                Regex::new(GCS_VIRTUAL).unwrap(),
                Regex::new(GCS_URL).unwrap(),
            ],
        );

        m.insert(
            Backend::Azure,
            vec![
                Regex::new(ABFS_URL).unwrap(),
                Regex::new(AZURE_HTTPS).unwrap(),
            ],
        );

        m.insert(
            Backend::Local,
            vec![
                Regex::new(FILE_URI).unwrap(),
                Regex::new(FILE_URL).unwrap(),
                Regex::new(FILE_PATH).unwrap(),
            ],
        );

        m
    })
}

/// S3 storage configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Config {
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub bucket: String,
    pub key: Option<Path>,
}

/// Google Cloud Storage configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcsConfig {
    pub bucket: String,
    pub key: Option<Path>,
}

/// Azure Blob Storage configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AzureConfig {
    pub account: String,
    pub container: String,
    pub key: Option<Path>,
}

/// Local filesystem configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalConfig {
    pub path: String,
    pub key: Option<Path>,
}

/// Backend configuration enum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendConfig {
    S3(S3Config),
    Gcs(GcsConfig),
    Azure(AzureConfig),
    Local(LocalConfig),
}

impl BackendConfig {
    /// Parse a URL into a backend configuration.
    pub fn parse_url(url: &str, with_key: bool) -> Result<Self, StorageError> {
        for (k, v) in matchers() {
            if let Some(matches) = v.iter().filter_map(|r| r.captures(url)).next() {
                return match k {
                    Backend::S3 => Self::parse_s3(matches),
                    Backend::Gcs => Self::parse_gcs(matches),
                    Backend::Azure => Self::parse_azure(matches),
                    Backend::Local => Self::parse_local(matches, with_key),
                };
            }
        }

        InvalidUrlSnafu {
            url: url.to_string(),
        }
        .fail()
    }

    fn parse_s3(matches: regex::Captures) -> Result<Self, StorageError> {
        let bucket = matches
            .name("bucket")
            .expect("bucket should always be available")
            .as_str()
            .to_string();

        let region = std::env::var("AWS_DEFAULT_REGION")
            .ok()
            .or_else(|| matches.name("region").map(|m| m.as_str().to_string()));

        let endpoint = std::env::var("AWS_ENDPOINT").ok().or_else(|| {
            matches.name("endpoint").map(|endpoint| {
                let port = matches
                    .name("port")
                    .and_then(|p| p.as_str().parse::<u16>().ok())
                    .unwrap_or(443);
                let protocol = matches
                    .name("protocol")
                    .map(|p| p.as_str())
                    .unwrap_or("https");
                format!("{}://{}:{}", protocol, endpoint.as_str(), port)
            })
        });

        let key = matches.name("key").map(|m| m.as_str().into());

        Ok(BackendConfig::S3(S3Config {
            endpoint,
            region,
            bucket,
            key,
        }))
    }

    fn parse_gcs(matches: regex::Captures) -> Result<Self, StorageError> {
        let bucket = matches
            .name("bucket")
            .expect("bucket should always be available")
            .as_str()
            .to_string();

        let key = matches.name("key").map(|r| r.as_str().into());

        Ok(BackendConfig::Gcs(GcsConfig { bucket, key }))
    }

    fn parse_azure(matches: regex::Captures) -> Result<Self, StorageError> {
        let container = matches
            .name("container")
            .expect("container should always be available")
            .as_str()
            .to_string();

        let account = matches
            .name("account")
            .expect("account should always be available")
            .as_str()
            .to_string();

        let key = matches.name("key").map(|r| r.as_str().into());

        Ok(BackendConfig::Azure(AzureConfig {
            account,
            container,
            key,
        }))
    }

    fn parse_local(matches: regex::Captures, with_key: bool) -> Result<Self, StorageError> {
        let path = matches
            .name("path")
            .expect("path regex must contain a path group")
            .as_str();

        let mut path = if !path.starts_with('/') {
            std::path::PathBuf::from(format!("/{path}"))
        } else {
            std::path::PathBuf::from(path)
        };

        let key = if with_key {
            let key = path
                .file_name()
                .map(|k| k.to_str().unwrap().to_string().into());
            path.pop();
            key
        } else {
            None
        };

        Ok(BackendConfig::Local(LocalConfig {
            path: path.to_str().unwrap().to_string(),
            key,
        }))
    }

    fn key(&self) -> Option<&Path> {
        match self {
            BackendConfig::S3(s3) => s3.key.as_ref(),
            BackendConfig::Gcs(gcs) => gcs.key.as_ref(),
            BackendConfig::Azure(azure) => azure.key.as_ref(),
            BackendConfig::Local(local) => local.key.as_ref(),
        }
    }
}

impl StorageProvider {
    /// Create a storage provider for the given URL with storage options.
    pub async fn for_url_with_options(
        url: &str,
        options: HashMap<String, String>,
    ) -> Result<Self, StorageError> {
        let config = BackendConfig::parse_url(url, false)?;

        match config {
            BackendConfig::S3(config) => Self::construct_s3(config, options).await,
            BackendConfig::Gcs(config) => Self::construct_gcs(config).await,
            BackendConfig::Azure(config) => Self::construct_azure(config).await,
            BackendConfig::Local(config) => Self::construct_local(config).await,
        }
    }

    async fn construct_s3(
        config: S3Config,
        options: HashMap<String, String>,
    ) -> Result<Self, StorageError> {
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(&config.bucket);

        for (key, value) in &options {
            builder = builder.with_config(key.parse().context(S3ConfigSnafu)?, value.clone());
        }

        builder = builder.with_retry(default_retry_config());

        if let Some(region) = &config.region {
            builder = builder.with_region(region);
        }

        if let Some(endpoint) = &config.endpoint {
            builder = builder
                .with_endpoint(endpoint)
                .with_virtual_hosted_style_request(false)
                .with_allow_http(true);
        }

        let canonical_url = match (&config.region, &config.endpoint) {
            (_, Some(endpoint)) => format!("s3::{}/{}", endpoint, config.bucket),
            (Some(region), _) => format!("https://s3.{}.amazonaws.com/{}", region, config.bucket),
            _ => format!("https://s3.amazonaws.com/{}", config.bucket),
        };

        let canonical_url = if let Some(key) = &config.key {
            format!("{}/{}", canonical_url, key)
        } else {
            canonical_url
        };

        let s3_store = Arc::new(builder.build().context(S3ConfigSnafu)?);
        // S3 supports MultipartStore for parallel part uploads
        let multipart_store: Option<Arc<dyn MultipartStore>> = Some(s3_store.clone());
        let object_store: Arc<dyn ObjectStore> = s3_store;

        Ok(Self {
            config: BackendConfig::S3(config),
            object_store,
            multipart_store,
            canonical_url,
            storage_options: options,
        })
    }

    async fn construct_gcs(config: GcsConfig) -> Result<Self, StorageError> {
        let mut builder = GoogleCloudStorageBuilder::from_env().with_bucket_name(&config.bucket);

        builder = builder.with_retry(default_retry_config());

        if let Ok(service_account_key) = std::env::var("GOOGLE_SERVICE_ACCOUNT_KEY") {
            debug!("Constructing GCS builder with service account key");
            builder = builder.with_service_account_key(&service_account_key);
        }

        let mut canonical_url = format!("https://{}.storage.googleapis.com", config.bucket);
        if let Some(key) = &config.key {
            canonical_url = format!("{}/{}", canonical_url, key);
        }

        let gcs_store = Arc::new(builder.build().context(GcsConfigSnafu)?);
        // GCS supports MultipartStore for parallel part uploads
        let multipart_store: Option<Arc<dyn MultipartStore>> = Some(gcs_store.clone());
        let object_store: Arc<dyn ObjectStore> = gcs_store;

        Ok(Self {
            config: BackendConfig::Gcs(config),
            object_store,
            multipart_store,
            canonical_url,
            storage_options: HashMap::new(),
        })
    }

    async fn construct_azure(config: AzureConfig) -> Result<Self, StorageError> {
        let builder = MicrosoftAzureBuilder::from_env()
            .with_container_name(&config.container)
            .with_retry(default_retry_config());

        let canonical_url = format!(
            "https://{}.blob.core.windows.net/{}",
            config.account, config.container
        );

        let azure_store = Arc::new(builder.build().context(AzureConfigSnafu)?);
        // Azure supports MultipartStore for parallel part uploads
        let multipart_store: Option<Arc<dyn MultipartStore>> = Some(azure_store.clone());
        let object_store: Arc<dyn ObjectStore> = azure_store;

        Ok(Self {
            config: BackendConfig::Azure(config),
            object_store,
            multipart_store,
            canonical_url,
            storage_options: HashMap::new(),
        })
    }

    async fn construct_local(config: LocalConfig) -> Result<Self, StorageError> {
        tokio::fs::create_dir_all(&config.path)
            .await
            .context(IoSnafu)?;

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&config.path).context(ObjectStoreSnafu)?);

        let canonical_url = format!("file://{}", config.path);

        Ok(Self {
            config: BackendConfig::Local(config),
            object_store,
            // Local filesystem does not support MultipartStore
            multipart_store: None,
            canonical_url,
            storage_options: HashMap::new(),
        })
    }

    /// List files in the storage location.
    /// Returns paths relative to the configured key prefix.
    pub async fn list(
        &self,
        include_subdirectories: bool,
    ) -> Result<impl Stream<Item = Result<Path, object_store::Error>> + '_, StorageError> {
        // Emit metric for list operation initiation
        emit!(StorageRequest {
            operation: StorageOperation::List,
            status: RequestStatus::Success,
        });

        let key_path: Option<Path> = self.config.key().map(|key| key.to_string().into());
        let key_part_count = key_path
            .as_ref()
            .map(|key| key.parts().count())
            .unwrap_or_default();

        let list = self
            .object_store
            .list(key_path.as_ref())
            .filter_map(move |meta| {
                let result = match meta {
                    Ok(metadata) => {
                        let path = metadata.location;
                        if !include_subdirectories && path.parts().count() != key_part_count + 1 {
                            None
                        } else {
                            // Strip the prefix from the path so callers get relative paths
                            // This matches the contract expected by get/put/etc which qualify paths
                            let relative_path: Path = path.parts().skip(key_part_count).collect();
                            Some(Ok(relative_path))
                        }
                    }
                    Err(err) => Some(Err(err)),
                };
                ready(result)
            });

        Ok(list)
    }

    /// Get the contents of a file.
    pub async fn get(&self, path: impl Into<Path>) -> Result<Bytes, StorageError> {
        let path = path.into();
        let start = Instant::now();
        let result = self.object_store.get(&self.qualify_path(&path)).await;

        let status = if result.is_ok() {
            RequestStatus::Success
        } else {
            RequestStatus::Error
        };
        emit!(StorageRequest {
            operation: StorageOperation::Get,
            status,
        });
        emit!(StorageRequestDuration {
            operation: StorageOperation::Get,
            duration: start.elapsed(),
        });

        let bytes = result
            .context(ObjectStoreSnafu)?
            .bytes()
            .await
            .context(ObjectStoreSnafu)?;
        Ok(bytes)
    }

    /// Get file contents if present, returning None if not found.
    pub async fn get_if_present(
        &self,
        path: impl Into<Path>,
    ) -> Result<Option<Bytes>, StorageError> {
        let path: Path = path.into();
        let start = Instant::now();
        let result = self.object_store.get(&self.qualify_path(&path)).await;

        let status = match &result {
            Ok(_) => RequestStatus::Success,
            Err(object_store::Error::NotFound { .. }) => RequestStatus::Success, // Not found is expected
            Err(_) => RequestStatus::Error,
        };
        emit!(StorageRequest {
            operation: StorageOperation::Get,
            status,
        });
        emit!(StorageRequestDuration {
            operation: StorageOperation::Get,
            duration: start.elapsed(),
        });

        match result {
            Ok(obj) => {
                let bytes = obj.bytes().await.context(ObjectStoreSnafu)?;
                Ok(Some(bytes))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(err).context(ObjectStoreSnafu),
        }
    }

    /// Check if a file exists.
    pub async fn exists<P: Into<Path>>(&self, path: P) -> Result<bool, StorageError> {
        let path: Path = path.into();
        let start = Instant::now();
        let result = self.object_store.head(&self.qualify_path(&path)).await;

        let status = match &result {
            Ok(_) => RequestStatus::Success,
            Err(object_store::Error::NotFound { .. }) => RequestStatus::Success, // Not found is expected
            Err(_) => RequestStatus::Error,
        };
        emit!(StorageRequest {
            operation: StorageOperation::Head,
            status,
        });
        emit!(StorageRequestDuration {
            operation: StorageOperation::Head,
            duration: start.elapsed(),
        });

        match result {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e).context(ObjectStoreSnafu),
        }
    }

    /// Put bytes to a path.
    pub async fn put(&self, path: impl Into<Path>, bytes: Vec<u8>) -> Result<(), StorageError> {
        let bytes = PutPayload::from(Bytes::from(bytes));
        let path = path.into();
        let path = self.qualify_path(&path);
        let start = Instant::now();
        let result = self.object_store.put(&path, bytes).await;

        let status = if result.is_ok() {
            RequestStatus::Success
        } else {
            RequestStatus::Error
        };
        emit!(StorageRequest {
            operation: StorageOperation::Put,
            status,
        });
        emit!(StorageRequestDuration {
            operation: StorageOperation::Put,
            duration: start.elapsed(),
        });

        result.context(ObjectStoreSnafu)?;
        Ok(())
    }

    /// Qualify a path with the configured key prefix.
    pub fn qualify_path<'a>(&self, path: &'a Path) -> Cow<'a, Path> {
        match self.config.key() {
            Some(prefix) => Cow::Owned(prefix.parts().chain(path.parts()).collect()),
            None => Cow::Borrowed(path),
        }
    }

    /// Get storage options for external integrations (e.g., Delta Lake).
    pub fn storage_options(&self) -> &HashMap<String, String> {
        &self.storage_options
    }

    /// Get the backend configuration.
    pub fn config(&self) -> &BackendConfig {
        &self.config
    }

    /// Put a payload to a path.
    pub async fn put_payload(&self, path: &Path, payload: PutPayload) -> Result<(), StorageError> {
        let path = self.qualify_path(path);
        let start = Instant::now();
        let result = self.object_store.put(&path, payload).await;

        let status = if result.is_ok() {
            RequestStatus::Success
        } else {
            RequestStatus::Error
        };
        emit!(StorageRequest {
            operation: StorageOperation::Put,
            status,
        });
        emit!(StorageRequestDuration {
            operation: StorageOperation::Put,
            duration: start.elapsed(),
        });

        result.context(ObjectStoreSnafu)?;
        Ok(())
    }

    /// Upload bytes using parallel multipart upload.
    ///
    /// Uses the `MultipartStore` trait which provides explicit part numbering,
    /// allowing parts to be uploaded in parallel.
    ///
    /// For backends that don't support `MultipartStore` or for small files,
    /// falls back to simple PUT.
    pub async fn put_multipart_bytes_parallel(
        &self,
        path: &Path,
        bytes: Bytes,
        part_size: usize,
        min_multipart_size: usize,
        max_concurrent_parts: usize,
    ) -> Result<(), StorageError> {
        // Small files use simple PUT
        if bytes.len() < min_multipart_size || self.multipart_store.is_none() {
            return self.put_payload(path, PutPayload::from(bytes)).await;
        }

        let multipart_store = self.multipart_store.as_ref().unwrap();
        let qualified_path = self.qualify_path(path).into_owned();

        // Start multipart upload
        let create_start = Instant::now();
        let create_result = multipart_store.create_multipart(&qualified_path).await;
        emit!(StorageRequest {
            operation: StorageOperation::CreateMultipart,
            status: if create_result.is_ok() {
                RequestStatus::Success
            } else {
                RequestStatus::Error
            },
        });
        emit!(StorageRequestDuration {
            operation: StorageOperation::CreateMultipart,
            duration: create_start.elapsed(),
        });
        let multipart_id = create_result.context(ObjectStoreSnafu)?;

        // Create parts with indices
        let parts: Vec<(usize, Bytes)> = (0..)
            .zip((0..bytes.len()).step_by(part_size))
            .map(|(i, offset)| {
                let end = std::cmp::min(offset + part_size, bytes.len());
                (i, bytes.slice(offset..end))
            })
            .collect();

        let total_parts = parts.len();
        debug!(
            "Starting parallel multipart upload for {} ({} bytes, {} parts, concurrency={})",
            path,
            bytes.len(),
            total_parts,
            max_concurrent_parts
        );

        // Upload parts in parallel using explicit part numbers
        let multipart_id = Arc::new(multipart_id);
        let multipart_store = multipart_store.clone();
        let qualified_path = Arc::new(qualified_path);
        let active_parts = Arc::new(AtomicUsize::new(0));

        let results: Vec<(usize, PartId)> = futures::stream::iter(parts)
            .map(|(idx, data)| {
                let multipart_store = multipart_store.clone();
                let qualified_path = qualified_path.clone();
                let multipart_id = multipart_id.clone();
                let active_parts = active_parts.clone();
                async move {
                    let count = active_parts.fetch_add(1, Ordering::Relaxed) + 1;
                    emit!(ActiveMultipartParts { count });

                    let part_start = Instant::now();
                    let result = multipart_store
                        .put_part(&qualified_path, &multipart_id, idx, data.into())
                        .await;

                    emit!(StorageRequest {
                        operation: StorageOperation::PutPart,
                        status: if result.is_ok() {
                            RequestStatus::Success
                        } else {
                            RequestStatus::Error
                        },
                    });
                    emit!(StorageRequestDuration {
                        operation: StorageOperation::PutPart,
                        duration: part_start.elapsed(),
                    });

                    let count = active_parts.fetch_sub(1, Ordering::Relaxed) - 1;
                    emit!(ActiveMultipartParts { count });

                    let part_id = result.context(ObjectStoreSnafu)?;
                    debug!("Uploaded part {}/{}", idx + 1, total_parts);
                    Ok::<_, StorageError>((idx, part_id))
                }
            })
            .buffer_unordered(max_concurrent_parts)
            .try_collect()
            .await?;

        // Sort by index and extract PartIds (parts may complete out of order)
        let mut results = results;
        results.sort_by_key(|(idx, _)| *idx);
        let part_ids: Vec<PartId> = results.into_iter().map(|(_, id)| id).collect();

        // Complete the upload
        let complete_start = Instant::now();
        let complete_result = self
            .multipart_store
            .as_ref()
            .unwrap()
            .complete_multipart(&qualified_path, &multipart_id, part_ids)
            .await;

        emit!(StorageRequest {
            operation: StorageOperation::CompleteMultipart,
            status: if complete_result.is_ok() {
                RequestStatus::Success
            } else {
                RequestStatus::Error
            },
        });
        emit!(StorageRequestDuration {
            operation: StorageOperation::CompleteMultipart,
            duration: complete_start.elapsed(),
        });

        complete_result.context(ObjectStoreSnafu)?;
        emit!(MultipartUploadCompleted);
        debug!("Completed parallel multipart upload for {}", path);

        Ok(())
    }
}

/// List NDJSON.gz files recursively.
pub async fn list_ndjson_files(storage: &StorageProvider) -> Result<Vec<String>, StorageError> {
    let mut files = Vec::new();
    let mut stream = storage.list(true).await?;
    let mut total_listed = 0;

    while let Some(result) = stream.next().await {
        let path = result.context(ObjectStoreSnafu)?;
        let path_str = path.to_string();
        total_listed += 1;

        if path_str.ends_with(".ndjson.gz") {
            files.push(path_str);
        }
    }

    tracing::debug!(
        "Listed {} total files, {} are .ndjson.gz",
        total_listed,
        files.len()
    );

    // Sort by path for consistent ordering
    files.sort();

    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_s3_url_parsing() {
        let config = BackendConfig::parse_url("s3://mybucket/path/to/data", false).unwrap();
        match config {
            BackendConfig::S3(s3) => {
                assert_eq!(s3.bucket, "mybucket");
                assert_eq!(s3.key, Some(Path::from("path/to/data")));
            }
            _ => panic!("Expected S3 config"),
        }
    }

    #[test]
    fn test_gcs_url_parsing() {
        let config = BackendConfig::parse_url("gs://mybucket/path/to/data", false).unwrap();
        match config {
            BackendConfig::Gcs(gcs) => {
                assert_eq!(gcs.bucket, "mybucket");
                assert_eq!(gcs.key, Some(Path::from("path/to/data")));
            }
            _ => panic!("Expected Gcs config"),
        }
    }

    #[test]
    fn test_local_url_parsing() {
        let config = BackendConfig::parse_url("/local/path/to/data", false).unwrap();
        match config {
            BackendConfig::Local(local) => {
                assert_eq!(local.path, "/local/path/to/data");
            }
            _ => panic!("Expected Local config"),
        }
    }

    #[test]
    fn test_azure_url_parsing() {
        let config = BackendConfig::parse_url(
            "abfss://mycontainer@mystorageaccount.dfs.core.windows.net/path/to/data",
            false,
        )
        .unwrap();
        match config {
            BackendConfig::Azure(azure) => {
                assert_eq!(azure.account, "mystorageaccount");
                assert_eq!(azure.container, "mycontainer");
                assert_eq!(azure.key, Some(Path::from("path/to/data")));
            }
            _ => panic!("Expected Azure config"),
        }
    }

    /// Test that list() returns paths relative to the configured prefix,
    /// ensuring that get() won't double-qualify paths (which caused the
    /// "region=.../region=..." duplication bug).
    #[tokio::test]
    async fn test_list_returns_relative_paths() {
        // Create a temp directory with nested structure simulating partitioned data
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        // Create nested directories: prefix/partition1/partition2/file.ndjson.gz
        let prefix = "region=us-east-1/subject=orders";
        let nested_path = base_path.join(prefix).join("date=2024-01-01");
        std::fs::create_dir_all(&nested_path).unwrap();

        // Create test files
        let file1 = nested_path.join("file1.ndjson.gz");
        let file2 = nested_path.join("file2.ndjson.gz");
        std::fs::write(&file1, b"test data 1").unwrap();
        std::fs::write(&file2, b"test data 2").unwrap();

        // Create storage provider pointing to the prefix
        let storage_url = format!("{}/{}", base_path.display(), prefix);
        let storage = StorageProvider::for_url_with_options(&storage_url, HashMap::new())
            .await
            .unwrap();

        // List files - should return relative paths (not including the prefix)
        let mut stream = storage.list(true).await.unwrap();
        let mut listed_paths = Vec::new();
        while let Some(result) = stream.next().await {
            let path = result.unwrap();
            listed_paths.push(path.to_string());
        }

        listed_paths.sort();

        // Paths should be relative to the prefix, e.g., "date=2024-01-01/file1.ndjson.gz"
        // NOT "region=us-east-1/subject=orders/date=2024-01-01/file1.ndjson.gz"
        assert_eq!(listed_paths.len(), 2);
        assert_eq!(listed_paths[0], "date=2024-01-01/file1.ndjson.gz");
        assert_eq!(listed_paths[1], "date=2024-01-01/file2.ndjson.gz");

        // Verify that we can get() these files using the relative paths
        // This confirms the round-trip works: list() -> get()
        for path in &listed_paths {
            let content = storage.get(path.as_str()).await.unwrap();
            assert!(!content.is_empty());
        }
    }

    /// Test that the round-trip of list() -> get() works correctly.
    /// Files listed should be retrievable with the same paths.
    #[tokio::test]
    async fn test_list_get_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        // Create a nested structure
        let nested = base_path.join("partitions/date=2024-01-01");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(nested.join("data.parquet"), b"parquet data").unwrap();

        // Storage provider at the partitions level
        let storage_url = format!("{}/partitions", base_path.display());
        let storage = StorageProvider::for_url_with_options(&storage_url, HashMap::new())
            .await
            .unwrap();

        // List files
        let mut stream = storage.list(true).await.unwrap();
        let mut paths = Vec::new();
        while let Some(result) = stream.next().await {
            paths.push(result.unwrap().to_string());
        }

        assert_eq!(paths.len(), 1);
        let file_path = &paths[0];

        // Get the file using the same path that was listed
        let content = storage.get(file_path.as_str()).await.unwrap();
        assert_eq!(content.as_ref(), b"parquet data");

        // Also verify put() with a new file and get() works
        let new_path = "date=2024-01-02/new_data.parquet";
        storage.put(new_path, b"new data".to_vec()).await.unwrap();

        let retrieved = storage.get(new_path).await.unwrap();
        assert_eq!(retrieved.as_ref(), b"new data");
    }
}

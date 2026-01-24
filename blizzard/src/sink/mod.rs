//! Sink coordinator for writing to Delta Lake.
//!
//! Provides Parquet file writing and Delta Lake commit functionality.

pub mod delta;
pub mod parquet;

/// Information about a completed Parquet file.
#[derive(Debug, Clone)]
pub struct FinishedFile {
    /// The path to the file (relative to the table root).
    pub filename: String,
    /// The size of the file in bytes.
    pub size: usize,
    /// Number of records in the file.
    pub record_count: usize,
    /// The parquet file bytes (to be uploaded to storage).
    /// None if the file was already uploaded (e.g., from checkpoint recovery).
    pub bytes: Option<bytes::Bytes>,
}

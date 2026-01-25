//! Error types for Blizzard using snafu.
//!
//! This module defines structured error types with context selectors for
//! all error conditions in the codebase.

use snafu::prelude::*;

// ============ Storage Errors ============

/// Errors that can occur during storage operations.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum StorageError {
    /// Invalid storage URL format.
    #[snafu(display("Invalid storage URL: {url}"))]
    InvalidUrl { url: String },

    /// Object store operation failed.
    #[snafu(display("Storage operation failed"))]
    ObjectStore { source: object_store::Error },

    /// IO error during storage operations.
    #[snafu(display("IO error"))]
    Io { source: std::io::Error },

    /// S3 configuration error.
    #[snafu(display("S3 configuration error"))]
    S3Config { source: object_store::Error },

    /// GCS configuration error.
    #[snafu(display("GCS configuration error"))]
    GcsConfig { source: object_store::Error },

    /// Azure configuration error.
    #[snafu(display("Azure configuration error"))]
    AzureConfig { source: object_store::Error },
}

// ============ Config Errors ============

/// Errors that can occur during configuration parsing and validation.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ConfigError {
    /// Source path is empty.
    #[snafu(display("Source path cannot be empty"))]
    EmptySourcePath,

    /// Sink path is empty.
    #[snafu(display("Sink path cannot be empty"))]
    EmptySinkPath,

    /// Checkpoint path is empty.
    #[snafu(display("Checkpoint path cannot be empty"))]
    EmptyCheckpointPath,

    /// Schema has no fields.
    #[snafu(display("Schema must have at least one field"))]
    EmptySchema,

    /// Environment variable interpolation failed.
    #[snafu(display("Environment variable interpolation failed:\n{message}"))]
    EnvInterpolation { message: String },

    /// Failed to parse YAML configuration.
    #[snafu(display("Failed to parse YAML configuration"))]
    YamlParse { source: serde_yaml::Error },

    /// Failed to read configuration file.
    #[snafu(display("Failed to read configuration file"))]
    ReadFile { source: std::io::Error },
}

// ============ Reader Errors ============

/// Errors that can occur during NDJSON file reading.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ReaderError {
    /// Gzip decompression failed.
    #[snafu(display("Gzip decompression failed for {path}"))]
    GzipDecompression {
        source: std::io::Error,
        path: String,
    },

    /// Zstd decompression failed.
    #[snafu(display("Zstd decompression failed for {path}"))]
    ZstdDecompression {
        source: std::io::Error,
        path: String,
    },

    /// Failed to build JSON decoder.
    #[snafu(display("Failed to build JSON decoder: {message}"))]
    DecoderBuild { message: String },

    /// Failed to decode JSON.
    #[snafu(display("Failed to decode JSON for {path}: {message}"))]
    JsonDecode { path: String, message: String },

    /// Failed to flush batch.
    #[snafu(display("Failed to flush batch for {path}: {message}"))]
    BatchFlush { path: String, message: String },
}

// ============ Delta Errors ============

/// Errors that can occur during Delta Lake operations.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum DeltaError {
    /// Failed to create struct type.
    #[snafu(display("Failed to create struct type: {message}"))]
    StructType { message: String },

    /// Unsupported Arrow type for Delta conversion.
    #[snafu(display("Unsupported Arrow type: {arrow_type:?}"))]
    UnsupportedArrowType {
        arrow_type: arrow::datatypes::DataType,
    },

    /// Delta Lake operation failed.
    #[snafu(display("Delta Lake operation failed"))]
    DeltaLake { source: deltalake::DeltaTableError },

    /// Failed to parse URL.
    #[snafu(display("Failed to parse URL"))]
    UrlParse { source: url::ParseError },

    /// Decimal precision error.
    #[snafu(display("Decimal precision error: {message}"))]
    DecimalPrecision { message: String },
}

// ============ Checkpoint Errors ============

/// Errors that can occur during checkpoint operations.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum CheckpointError {
    /// Invalid latest.json format.
    #[snafu(display("Invalid latest.json: missing checkpoint_path"))]
    MissingCheckpointPath,

    /// JSON serialization error.
    #[snafu(display("JSON serialization error"))]
    Json { source: serde_json::Error },

    /// Storage error during checkpoint.
    #[snafu(display("Checkpoint storage error"))]
    CheckpointStorage { source: StorageError },
}

// ============ Metrics Errors ============

/// Errors that can occur during metrics initialization.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum MetricsError {
    /// Failed to initialize Prometheus recorder.
    #[snafu(display("Failed to initialize Prometheus recorder"))]
    PrometheusInit {
        source: metrics_exporter_prometheus::BuildError,
    },
}

// ============ Parquet Errors ============

/// Errors that can occur during Parquet file writing.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ParquetError {
    /// Parquet write error.
    #[snafu(display("Parquet write error"))]
    Write {
        source: parquet::errors::ParquetError,
    },
}

// ============ Pipeline Error (top-level) ============

/// Top-level pipeline errors that aggregate all error types.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum PipelineError {
    /// Storage error.
    #[snafu(display("Storage error"))]
    PipelineStorage { source: StorageError },

    /// Configuration error.
    #[snafu(display("Configuration error"))]
    Config { source: ConfigError },

    /// Reader error.
    #[snafu(display("Reader error"))]
    Reader { source: ReaderError },

    /// Delta Lake error.
    #[snafu(display("Delta error"))]
    Delta { source: DeltaError },

    /// Checkpoint error.
    #[snafu(display("Checkpoint error"))]
    Checkpoint { source: CheckpointError },

    /// Parquet error.
    #[snafu(display("Parquet error"))]
    Parquet { source: ParquetError },

    /// Task join error.
    #[snafu(display("Task join error"))]
    TaskJoin { source: tokio::task::JoinError },

    /// Channel send error.
    #[snafu(display("Channel closed unexpectedly"))]
    ChannelClosed,

    /// Address parsing error.
    #[snafu(display("Failed to parse address"))]
    AddressParse { source: std::net::AddrParseError },

    /// Metrics error.
    #[snafu(display("Metrics error"))]
    Metrics { source: MetricsError },
}

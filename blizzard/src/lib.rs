//! blizzard: A library for streaming NDJSON.gz files to Delta Lake.
//!
//! This library provides components for reading compressed NDJSON files,
//! writing Parquet files, and committing to Delta Lake tables with
//! exactly-once semantics.
//!
//! # Example
//!
//! ```ignore
//! use ndjson_to_delta::{Config, run_pipeline};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = Config::from_file("config.yaml")?;
//!     let stats = run_pipeline(config).await?;
//!     println!("Processed {} records", stats.records_processed);
//!     Ok(())
//! }
//! ```

pub mod checkpoint;
pub mod config;
pub mod pipeline;
pub mod sink;
pub mod source;
pub mod storage;

// Re-export main types
pub use config::Config;
pub use pipeline::{Pipeline, PipelineStats, run_pipeline};
pub use storage::{StorageProvider, StorageProviderRef};

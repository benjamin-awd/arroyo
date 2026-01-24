//! Source coordinator for reading NDJSON.gz files.
//!
//! Provides a unified interface for listing and reading compressed NDJSON files
//! from various storage backends.

pub mod reader;
pub mod state;

pub use reader::{BatchReader, NdjsonReader, TrackedBatch, create_batch_reader};
pub use state::{FileReadState, SourceState};

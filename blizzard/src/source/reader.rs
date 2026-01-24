//! NDJSON.gz async reader.
//!
//! Reads gzip-compressed newline-delimited JSON files and converts them
//! to Arrow RecordBatches using a user-provided schema.

use arrow::datatypes::SchemaRef;

/// A reader for NDJSON.gz files that yields Arrow RecordBatches.
pub struct NdjsonReader {
    batch_size: usize,
}

impl NdjsonReader {
    /// Create a new NDJSON reader with the given schema.
    pub fn new(_schema: SchemaRef, batch_size: usize) -> Self {
        Self { batch_size }
    }

    /// Get the batch size.
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
}

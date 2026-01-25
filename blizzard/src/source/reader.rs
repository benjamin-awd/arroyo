//! NDJSON.gz async reader.
//!
//! Reads gzip-compressed newline-delimited JSON files and converts them
//! to Arrow RecordBatches using a user-provided schema.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_json::reader::ReaderBuilder;
use bytes::Bytes;
use snafu::prelude::*;
use std::io::Read;
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

use crate::config::CompressionFormat;
use crate::emit;
use crate::error::{
    BatchFlushSnafu, DecoderBuildSnafu, GzipDecompressionSnafu, JsonDecodeSnafu, ReaderError,
    ZstdDecompressionSnafu,
};
use crate::internal_events::{BytesRead, FileDecompressionCompleted};

/// Configuration for the NDJSON reader.
#[derive(Debug, Clone)]
pub struct NdjsonReaderConfig {
    /// Number of records per batch.
    pub batch_size: usize,
    /// Compression format of input files.
    pub compression: CompressionFormat,
}

impl NdjsonReaderConfig {
    /// Create a new reader configuration.
    pub fn new(batch_size: usize, compression: CompressionFormat) -> Self {
        Self {
            batch_size,
            compression,
        }
    }
}

/// Result of reading and parsing a file.
#[derive(Debug)]
pub struct ReadResult {
    /// Parsed record batches.
    pub batches: Vec<RecordBatch>,
    /// Total number of records read.
    pub total_records: usize,
}

/// A reader for NDJSON.gz files that yields Arrow RecordBatches.
pub struct NdjsonReader {
    schema: SchemaRef,
    config: NdjsonReaderConfig,
}

impl NdjsonReader {
    /// Create a new NDJSON reader with the given schema and configuration.
    pub fn new(schema: SchemaRef, config: NdjsonReaderConfig) -> Self {
        Self { schema, config }
    }

    /// Read compressed data and parse it into record batches.
    ///
    /// This method:
    /// 1. Decompresses the input data according to the configured compression format
    /// 2. Parses the NDJSON content into Arrow RecordBatches
    /// 3. Optionally skips a number of records (for checkpoint recovery)
    ///
    /// # Arguments
    /// * `compressed` - The compressed file data
    /// * `skip_records` - Number of records to skip from the beginning (for resuming)
    /// * `path` - File path (used for error messages and logging)
    pub fn read(
        &self,
        compressed: Bytes,
        skip_records: usize,
        path: &str,
    ) -> Result<ReadResult, ReaderError> {
        // Emit bytes read metric
        emit!(BytesRead {
            bytes: compressed.len() as u64,
        });

        // Decompress
        let decompress_start = Instant::now();
        let decompressed = match self.config.compression {
            CompressionFormat::Gzip => {
                let mut decoder = flate2::read::GzDecoder::new(&compressed[..]);
                let mut buf = Vec::new();
                decoder
                    .read_to_end(&mut buf)
                    .context(GzipDecompressionSnafu {
                        path: path.to_string(),
                    })?;
                buf
            }
            CompressionFormat::Zstd => {
                zstd::decode_all(&compressed[..]).context(ZstdDecompressionSnafu {
                    path: path.to_string(),
                })?
            }
            CompressionFormat::None => compressed.to_vec(),
        };
        emit!(FileDecompressionCompleted {
            duration: decompress_start.elapsed()
        });

        debug!(
            "Decompressed {} -> {} bytes for {}",
            compressed.len(),
            decompressed.len(),
            path
        );

        // Parse JSON to batches
        let mut decoder = ReaderBuilder::new(Arc::clone(&self.schema))
            .with_batch_size(self.config.batch_size)
            .with_strict_mode(false)
            .build_decoder()
            .map_err(|e| {
                DecoderBuildSnafu {
                    message: e.to_string(),
                }
                .build()
            })?;

        // Decode and flush in interleaved fashion - decode() stops after batch_size records,
        // so we must flush after each decode to get all records
        let mut offset = 0;
        let mut batches = Vec::new();
        let mut total_records = 0;
        let mut records_to_skip = skip_records;

        loop {
            let consumed = decoder.decode(&decompressed[offset..]).map_err(|e| {
                JsonDecodeSnafu {
                    path: path.to_string(),
                    message: e.to_string(),
                }
                .build()
            })?;

            // Flush any accumulated records after each decode
            if let Some(batch) = decoder.flush().map_err(|e| {
                BatchFlushSnafu {
                    path: path.to_string(),
                    message: e.to_string(),
                }
                .build()
            })? {
                // Apply skip logic for checkpoint recovery
                if records_to_skip > 0 {
                    let batch_rows = batch.num_rows();
                    if records_to_skip >= batch_rows {
                        records_to_skip -= batch_rows;
                    } else {
                        // Partial skip
                        let skip = records_to_skip;
                        records_to_skip = 0;
                        let sliced = batch.slice(skip, batch_rows - skip);
                        total_records += sliced.num_rows();
                        batches.push(sliced);
                    }
                } else {
                    total_records += batch.num_rows();
                    batches.push(batch);
                }
            }

            if consumed == 0 {
                // No progress - check if remaining bytes are just whitespace
                let remaining = &decompressed[offset..];
                if !remaining.iter().all(|&b| b.is_ascii_whitespace()) {
                    debug!(
                        "Could not parse {} trailing bytes in {}",
                        remaining.len(),
                        path
                    );
                }
                break;
            }
            offset += consumed;
        }

        debug!(
            "Parsed {} batches ({} records) from {}",
            batches.len(),
            total_records,
            path
        );

        Ok(ReadResult {
            batches,
            total_records,
        })
    }
}

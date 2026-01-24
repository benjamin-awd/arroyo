//! NDJSON.gz async reader.
//!
//! Reads gzip-compressed newline-delimited JSON files and converts them
//! to Arrow RecordBatches using a user-provided schema.
//!
//! # Performance
//!
//! Uses bulk byte reading instead of line-by-line parsing to maximize throughput.
//! The arrow-json Decoder handles line boundaries internally, allowing us to
//! pass large byte chunks directly for parsing.

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_json::reader::{Decoder, ReaderBuilder};
use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use std::pin::Pin;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tracing::{debug, warn};

use crate::config::CompressionFormat;
use crate::storage::{StorageProvider, StorageProviderRef};

/// Size of the read buffer for bulk reading (4MB).
/// Larger buffers reduce async overhead and allow more efficient JSON parsing.
const READ_BUFFER_SIZE: usize = 4 * 1024 * 1024;

/// Default number of retries for transient errors.
const DEFAULT_MAX_RETRIES: u32 = 3;

/// Initial backoff duration for retries.
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);

/// Check if an error is retryable (transient network/cloud errors).
fn is_retryable_error(error: &str) -> bool {
    let retryable_patterns = [
        "connection reset",
        "connection closed",
        "broken pipe",
        "timed out",
        "timeout",
        "temporary failure",
        "try again",
        "service unavailable",
        "503",
        "502",
        "504",
        "429", // rate limit
        "request or response body error",
        "end of file before message length reached",
        "connection error",
        "network error",
        "ssl error",
        "certificate",
        "handshake",
        "reset by peer",
        "socket",
    ];

    let error_lower = error.to_lowercase();
    retryable_patterns.iter().any(|p| error_lower.contains(p))
}

/// A reader for NDJSON.gz files that yields Arrow RecordBatches.
pub struct NdjsonReader {
    schema: SchemaRef,
    batch_size: usize,
}

impl NdjsonReader {
    /// Create a new NDJSON reader with the given schema.
    pub fn new(schema: SchemaRef, batch_size: usize) -> Self {
        Self { schema, batch_size }
    }

    /// Get the schema.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Get the batch size.
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
}

/// Create a batch reader for a file with retry support.
///
/// This is a standalone function to avoid lifetime issues with the stream.
pub async fn create_batch_reader(
    storage: &StorageProviderRef,
    path: &str,
    compression: CompressionFormat,
    schema: SchemaRef,
    batch_size: usize,
    skip_records: usize,
) -> Result<BatchReader> {
    create_batch_reader_with_retries(
        storage.clone(),
        path.to_string(),
        compression,
        schema,
        batch_size,
        skip_records,
        DEFAULT_MAX_RETRIES,
    )
    .await
}

/// Create a batch reader with configurable retries.
async fn create_batch_reader_with_retries(
    storage: StorageProviderRef,
    path: String,
    compression: CompressionFormat,
    schema: SchemaRef,
    batch_size: usize,
    skip_records: usize,
    max_retries: u32,
) -> Result<BatchReader> {
    let mut last_error = None;
    let mut backoff = INITIAL_BACKOFF;

    for attempt in 0..=max_retries {
        if attempt > 0 {
            debug!(
                "Retry attempt {}/{} for file {} after {:?}",
                attempt, max_retries, path, backoff
            );
            tokio::time::sleep(backoff).await;
            backoff = std::cmp::min(backoff * 2, Duration::from_secs(5));
        }

        match create_byte_reader(&storage, &path, compression).await {
            Ok(reader) => {
                // Configure decoder with our batch size to control when it accumulates records
                let decoder = ReaderBuilder::new(schema.clone())
                    .with_batch_size(batch_size)
                    .with_strict_mode(false)
                    .build_decoder()
                    .map_err(|e| anyhow::anyhow!("Failed to build JSON decoder: {}", e))?;

                return Ok(BatchReader {
                    storage,
                    path,
                    compression,
                    schema,
                    reader,
                    decoder,
                    batch_size,
                    read_buffer: vec![0u8; READ_BUFFER_SIZE],
                    pending_bytes: Vec::new(),
                    total_records_read: 0,
                    records_to_skip: skip_records,
                    buffered_records: 0,
                    finished: false,
                    max_retries,
                });
            }
            Err(e) => {
                let error_str = e.to_string();
                if is_retryable_error(&error_str) && attempt < max_retries {
                    warn!("Retryable error opening {}: {}", path, error_str);
                    last_error = Some(e);
                    continue;
                }
                return Err(e);
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Failed to open file after retries")))
}

/// Create a byte reader for a file with decompression applied.
async fn create_byte_reader(
    storage: &StorageProvider,
    path: &str,
    compression: CompressionFormat,
) -> Result<Pin<Box<dyn AsyncRead + Send>>> {
    // Get the stream - this returns an owned reader
    let stream_reader = storage.get_as_stream(path).await?;

    // Apply decompression with buffered reading for efficiency
    let reader: Pin<Box<dyn AsyncRead + Send>> = match compression {
        CompressionFormat::Gzip => Box::pin(GzipDecoder::new(BufReader::with_capacity(
            READ_BUFFER_SIZE,
            stream_reader,
        ))),
        CompressionFormat::Zstd => Box::pin(ZstdDecoder::new(BufReader::with_capacity(
            READ_BUFFER_SIZE,
            stream_reader,
        ))),
        CompressionFormat::None => {
            Box::pin(BufReader::with_capacity(READ_BUFFER_SIZE, stream_reader))
        }
    };

    Ok(reader)
}

/// A batch reader that yields batches with record count tracking.
///
/// Uses bulk byte reading for high performance:
/// - Reads large chunks (4MB) instead of line-by-line
/// - Passes chunks directly to arrow-json Decoder which handles line boundaries
/// - Supports automatic retries for transient errors
pub struct BatchReader {
    // File metadata for recreation on retry
    storage: StorageProviderRef,
    path: String,
    compression: CompressionFormat,
    schema: SchemaRef,

    // Byte-based reader (decompressed stream)
    reader: Pin<Box<dyn AsyncRead + Send>>,
    decoder: Decoder,
    batch_size: usize,

    // Buffers for efficient reading
    read_buffer: Vec<u8>,
    pending_bytes: Vec<u8>,

    // Record tracking
    total_records_read: usize,
    records_to_skip: usize,
    buffered_records: usize, // Records in decoder waiting to be flushed
    finished: bool,
    max_retries: u32,
}

impl BatchReader {
    /// Get the total number of records read so far.
    pub fn records_read(&self) -> usize {
        self.total_records_read
    }

    /// Check if the reader is finished.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Read the next batch using bulk byte reading.
    ///
    /// This is optimized for throughput:
    /// - Reads large chunks (4MB) to minimize async overhead
    /// - Passes bytes directly to the Decoder which handles line parsing
    /// - Flushes when we have enough records for a batch
    pub async fn next_batch(&mut self) -> Result<Option<TrackedBatch>> {
        if self.finished {
            return Ok(None);
        }

        let mut retries = 0;
        let mut backoff = INITIAL_BACKOFF;

        loop {
            // Try to read a chunk of bytes
            let bytes_read = match self.reader.read(&mut self.read_buffer).await {
                Ok(0) => {
                    // EOF - process any remaining pending bytes and flush
                    if !self.pending_bytes.is_empty() {
                        let consumed = self
                            .decoder
                            .decode(&self.pending_bytes)
                            .map_err(|e| anyhow::anyhow!("Failed to decode JSON: {}", e))?;
                        if consumed < self.pending_bytes.len() {
                            // There's trailing data that couldn't be parsed
                            let remaining = &self.pending_bytes[consumed..];
                            if !remaining.iter().all(|&b| b.is_ascii_whitespace()) {
                                debug!(
                                    "Ignoring {} trailing bytes that couldn't be parsed",
                                    remaining.len()
                                );
                            }
                        }
                        self.pending_bytes.clear();
                    }

                    self.finished = true;
                    self.buffered_records = 0;
                    if let Some(batch) = self
                        .decoder
                        .flush()
                        .map_err(|e| anyhow::anyhow!("Failed to flush final batch: {}", e))?
                    {
                        let batch = self.apply_skip_if_needed(batch);
                        if let Some(batch) = batch {
                            self.total_records_read += batch.num_rows();
                            return Ok(Some(TrackedBatch {
                                batch,
                                records_read: self.total_records_read,
                            }));
                        }
                    }
                    return Ok(None);
                }
                Ok(n) => {
                    retries = 0;
                    backoff = INITIAL_BACKOFF;
                    n
                }
                Err(e) => {
                    let error_str = e.to_string();

                    if is_retryable_error(&error_str) && retries < self.max_retries {
                        retries += 1;
                        warn!(
                            "Retryable error reading {} (attempt {}/{}): {}",
                            self.path, retries, self.max_retries, error_str
                        );

                        tokio::time::sleep(backoff).await;
                        backoff = std::cmp::min(backoff * 2, Duration::from_secs(5));

                        if let Err(recreate_err) = self.recreate_reader().await {
                            return Err(anyhow::anyhow!(
                                "Failed to recreate reader after error '{}': {}",
                                error_str,
                                recreate_err
                            ));
                        }
                        continue;
                    }

                    return Err(anyhow::anyhow!("Failed to read: {}", e));
                }
            };

            // Combine pending bytes with new bytes
            self.pending_bytes
                .extend_from_slice(&self.read_buffer[..bytes_read]);

            // Decode as much as we can
            let consumed = self
                .decoder
                .decode(&self.pending_bytes)
                .map_err(|e| anyhow::anyhow!("Failed to decode JSON: {}", e))?;

            // Count newlines in consumed bytes to track buffered records
            if consumed > 0 {
                let newlines = self.pending_bytes[..consumed]
                    .iter()
                    .filter(|&&b| b == b'\n')
                    .count();
                self.buffered_records += newlines;
                self.pending_bytes.drain(..consumed);
            }

            // Flush when we have enough records for a batch
            if self.buffered_records >= self.batch_size {
                if let Some(batch) = self
                    .decoder
                    .flush()
                    .map_err(|e| anyhow::anyhow!("Failed to flush batch: {}", e))?
                {
                    self.buffered_records = 0;
                    let batch = self.apply_skip_if_needed(batch);
                    if let Some(batch) = batch {
                        self.total_records_read += batch.num_rows();
                        return Ok(Some(TrackedBatch {
                            batch,
                            records_read: self.total_records_read,
                        }));
                    }
                }
            }
        }
    }

    /// Apply skip logic if we need to skip records for checkpoint recovery.
    fn apply_skip_if_needed(&mut self, batch: RecordBatch) -> Option<RecordBatch> {
        if self.records_to_skip == 0 {
            return Some(batch);
        }

        let batch_rows = batch.num_rows();
        if self.records_to_skip >= batch_rows {
            // Skip entire batch
            self.records_to_skip -= batch_rows;
            return None;
        }

        // Skip partial batch
        let skip = self.records_to_skip;
        self.records_to_skip = 0;
        Some(batch.slice(skip, batch_rows - skip))
    }

    /// Recreate the reader for retry after transient error.
    async fn recreate_reader(&mut self) -> Result<()> {
        // Note: We lose our position in the file on recreate.
        // For full recovery, we'd need to re-read and skip to total_records_read.
        // For now, this is a best-effort retry that works for errors early in reading.
        self.reader = create_byte_reader(&self.storage, &self.path, self.compression).await?;
        self.decoder = ReaderBuilder::new(self.schema.clone())
            .with_batch_size(self.batch_size)
            .with_strict_mode(false)
            .build_decoder()
            .map_err(|e| anyhow::anyhow!("Failed to build JSON decoder: {}", e))?;
        self.pending_bytes.clear();
        self.buffered_records = 0;
        // Add current position to records_to_skip so we skip already-processed records
        self.records_to_skip += self.total_records_read;
        self.total_records_read = 0;
        Ok(())
    }
}

/// A batch result with tracking information.
pub struct TrackedBatch {
    pub batch: RecordBatch,
    pub records_read: usize,
}

/// Parse a single JSON line into a RecordBatch with the given schema.
pub fn parse_json_line(line: &str, schema: SchemaRef) -> Result<RecordBatch> {
    let mut decoder = ReaderBuilder::new(schema)
        .with_strict_mode(false)
        .build_decoder()?;

    decoder.decode(line.as_bytes())?;

    decoder
        .flush()?
        .ok_or_else(|| anyhow::anyhow!("No data in JSON line"))
}

/// Result of parallel file processing.
pub struct ParallelFileResult {
    /// All batches from the file.
    pub batches: Vec<RecordBatch>,
    /// Total records read from the file.
    pub total_records: usize,
}

/// Download a compressed file and decompress+parse using rayon.
///
/// This separates I/O (async, tokio) from CPU work (parallel, rayon):
/// 1. Download compressed bytes asynchronously
/// 2. Decompress + parse in rayon thread pool for CPU parallelism
///
/// Returns all batches from the file.
pub async fn read_file_parallel(
    storage: &StorageProviderRef,
    path: &str,
    compression: CompressionFormat,
    schema: SchemaRef,
    batch_size: usize,
    skip_records: usize,
) -> Result<ParallelFileResult> {
    // 1. Download compressed bytes (async I/O)
    let compressed = storage.get(path).await?;
    let compressed_size = compressed.len();
    debug!(
        "Downloaded {} bytes from {} (compressed)",
        compressed_size, path
    );

    // 2. Decompress + parse in rayon thread pool (CPU parallel)
    let path_owned = path.to_string();
    let result = tokio::task::spawn_blocking(move || {
        decompress_and_parse(
            compressed,
            compression,
            schema,
            batch_size,
            skip_records,
            &path_owned,
        )
    })
    .await??;

    Ok(result)
}

/// Decompress bytes and parse NDJSON to RecordBatches.
/// This runs in a rayon thread pool for CPU parallelism.
fn decompress_and_parse(
    compressed: bytes::Bytes,
    compression: CompressionFormat,
    schema: SchemaRef,
    batch_size: usize,
    skip_records: usize,
    path: &str,
) -> Result<ParallelFileResult> {
    use std::io::Read;

    // Decompress
    let decompressed = match compression {
        CompressionFormat::Gzip => {
            let mut decoder = flate2::read::GzDecoder::new(&compressed[..]);
            let mut buf = Vec::new();
            decoder
                .read_to_end(&mut buf)
                .map_err(|e| anyhow::anyhow!("Gzip decompression failed for {}: {}", path, e))?;
            buf
        }
        CompressionFormat::Zstd => zstd::decode_all(&compressed[..])
            .map_err(|e| anyhow::anyhow!("Zstd decompression failed for {}: {}", path, e))?,
        CompressionFormat::None => compressed.to_vec(),
    };

    debug!(
        "Decompressed {} -> {} bytes for {}",
        compressed.len(),
        decompressed.len(),
        path
    );

    // Parse JSON to batches
    let mut decoder = ReaderBuilder::new(schema)
        .with_batch_size(batch_size)
        .with_strict_mode(false)
        .build_decoder()
        .map_err(|e| anyhow::anyhow!("Failed to build JSON decoder: {}", e))?;

    // Decode and flush in interleaved fashion - decode() stops after batch_size records,
    // so we must flush after each decode to get all records
    let mut offset = 0;
    let mut batches = Vec::new();
    let mut total_records = 0;
    let mut records_to_skip = skip_records;

    loop {
        let consumed = decoder
            .decode(&decompressed[offset..])
            .map_err(|e| anyhow::anyhow!("Failed to decode JSON for {}: {}", path, e))?;

        // Flush any accumulated records after each decode
        if let Some(batch) = decoder
            .flush()
            .map_err(|e| anyhow::anyhow!("Failed to flush batch for {}: {}", path, e))?
        {
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

    Ok(ParallelFileResult {
        batches,
        total_records,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    #[test]
    fn test_parse_json_line() {
        let schema = test_schema();
        let line = r#"{"id": "test", "value": 42}"#;

        let batch = parse_json_line(line, schema).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_parse_json_line_missing_nullable() {
        let schema = test_schema();
        let line = r#"{"id": "test"}"#;

        let batch = parse_json_line(line, schema).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert!(batch.column(1).is_null(0));
    }
}

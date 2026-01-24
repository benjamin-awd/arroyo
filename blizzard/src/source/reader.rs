//! NDJSON.gz async reader.
//!
//! Reads gzip-compressed newline-delimited JSON files and converts them
//! to Arrow RecordBatches using a user-provided schema.

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_json::reader::{Decoder, ReaderBuilder};
use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use std::pin::Pin;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::LinesStream;
use tracing::{debug, warn};

use crate::config::CompressionFormat;
use crate::storage::{StorageProvider, StorageProviderRef};

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

        match create_stream(
            &storage,
            &path,
            compression,
            schema.clone(),
            batch_size,
            skip_records,
        )
        .await
        {
            Ok((lines, decoder)) => {
                return Ok(BatchReader {
                    storage,
                    path,
                    compression,
                    schema,
                    lines,
                    decoder,
                    batch_size,
                    buffered_count: 0,
                    total_records_read: skip_records,
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

/// Create the underlying stream for a file.
async fn create_stream(
    storage: &StorageProvider,
    path: &str,
    compression: CompressionFormat,
    schema: SchemaRef,
    _batch_size: usize,
    skip_records: usize,
) -> Result<(
    Pin<Box<dyn futures::Stream<Item = std::io::Result<String>> + Send>>,
    Decoder,
)> {
    // Get the stream - this returns an owned reader
    let stream_reader = storage.get_as_stream(path).await?;

    // Apply decompression - box everything to make it 'static
    let reader: Pin<Box<dyn AsyncRead + Send>> = match compression {
        CompressionFormat::Gzip => Box::pin(GzipDecoder::new(BufReader::new(stream_reader))),
        CompressionFormat::Zstd => Box::pin(ZstdDecoder::new(BufReader::new(stream_reader))),
        CompressionFormat::None => Box::pin(BufReader::new(stream_reader)),
    };

    let buf_reader = BufReader::new(reader);
    let lines_stream = LinesStream::new(buf_reader.lines());

    // Skip records if needed and box the stream
    let lines: Pin<Box<dyn futures::Stream<Item = std::io::Result<String>> + Send>> =
        if skip_records > 0 {
            Box::pin(lines_stream.skip(skip_records))
        } else {
            Box::pin(lines_stream)
        };

    let decoder = ReaderBuilder::new(schema)
        .with_strict_mode(false)
        .build_decoder()
        .map_err(|e| anyhow::anyhow!("Failed to build JSON decoder: {}", e))?;

    Ok((lines, decoder))
}

/// A batch reader that yields batches with record count tracking.
/// Supports automatic retries for transient errors by recreating the stream.
pub struct BatchReader {
    // File metadata for recreation on retry
    storage: StorageProviderRef,
    path: String,
    compression: CompressionFormat,
    schema: SchemaRef,

    // Current stream state
    lines: Pin<Box<dyn futures::Stream<Item = std::io::Result<String>> + Send>>,
    decoder: Decoder,
    batch_size: usize,
    buffered_count: usize,
    total_records_read: usize,
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

    /// Read the next batch with automatic retry on transient errors.
    pub async fn next_batch(&mut self) -> Result<Option<TrackedBatch>> {
        if self.finished {
            return Ok(None);
        }

        let mut retries = 0;
        let mut backoff = INITIAL_BACKOFF;

        loop {
            // Use poll_fn to poll the stream
            let next = futures::future::poll_fn(|cx| self.lines.as_mut().poll_next(cx)).await;

            match next {
                Some(Ok(line)) => {
                    // Reset retry count on successful read
                    retries = 0;
                    backoff = INITIAL_BACKOFF;

                    // Decode the line
                    self.decoder
                        .decode(line.as_bytes())
                        .map_err(|e| anyhow::anyhow!("Failed to decode JSON: {}", e))?;
                    self.buffered_count += 1;
                    self.total_records_read += 1;

                    // If we have enough records, flush
                    if self.buffered_count >= self.batch_size {
                        let records_read = self.total_records_read;
                        self.buffered_count = 0;
                        if let Some(batch) = self
                            .decoder
                            .flush()
                            .map_err(|e| anyhow::anyhow!("Failed to flush batch: {}", e))?
                        {
                            return Ok(Some(TrackedBatch {
                                batch,
                                records_read,
                            }));
                        }
                    }
                }
                Some(Err(e)) => {
                    let error_str = e.to_string();

                    // Check if error is retryable
                    if is_retryable_error(&error_str) && retries < self.max_retries {
                        retries += 1;
                        warn!(
                            "Retryable error reading {} (attempt {}/{}): {}",
                            self.path, retries, self.max_retries, error_str
                        );

                        // Wait before retry
                        tokio::time::sleep(backoff).await;
                        backoff = std::cmp::min(backoff * 2, Duration::from_secs(5));

                        // Recreate the stream from where we left off
                        // We need to skip records we've already read successfully
                        // Note: buffered_count records are in decoder but not yet flushed
                        let skip_to = self.total_records_read;

                        match self.recreate_stream(skip_to).await {
                            Ok(()) => {
                                debug!("Recreated stream for {} at record {}", self.path, skip_to);
                                continue;
                            }
                            Err(recreate_err) => {
                                return Err(anyhow::anyhow!(
                                    "Failed to recreate stream after error '{}': {}",
                                    error_str,
                                    recreate_err
                                ));
                            }
                        }
                    }

                    return Err(anyhow::anyhow!("Failed to read line: {}", e));
                }
                None => {
                    // End of stream, flush remaining
                    self.finished = true;
                    if self.buffered_count > 0 {
                        let records_read = self.total_records_read;
                        self.buffered_count = 0;
                        if let Some(batch) = self
                            .decoder
                            .flush()
                            .map_err(|e| anyhow::anyhow!("Failed to flush final batch: {}", e))?
                        {
                            return Ok(Some(TrackedBatch {
                                batch,
                                records_read,
                            }));
                        }
                    }
                    return Ok(None);
                }
            }
        }
    }

    /// Recreate the underlying stream, skipping to the given record.
    async fn recreate_stream(&mut self, skip_to: usize) -> Result<()> {
        let (lines, decoder) = create_stream(
            &self.storage,
            &self.path,
            self.compression,
            self.schema.clone(),
            self.batch_size,
            skip_to,
        )
        .await?;

        self.lines = lines;
        self.decoder = decoder;
        self.buffered_count = 0;
        // total_records_read stays the same - we're resuming from there

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

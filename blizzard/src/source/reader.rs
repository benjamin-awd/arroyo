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
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::LinesStream;

use crate::config::CompressionFormat;
use crate::storage::StorageProvider;

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

/// Create a batch reader for a file.
///
/// This is a standalone function to avoid lifetime issues with the stream.
pub async fn create_batch_reader(
    storage: &StorageProvider,
    path: &str,
    compression: CompressionFormat,
    schema: SchemaRef,
    batch_size: usize,
    skip_records: usize,
) -> Result<BatchReader> {
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

    Ok(BatchReader {
        lines,
        decoder,
        batch_size,
        buffered_count: 0,
        total_records_read: skip_records,
        finished: false,
    })
}

/// A batch reader that yields batches with record count tracking.
pub struct BatchReader {
    lines: Pin<Box<dyn futures::Stream<Item = std::io::Result<String>> + Send>>,
    decoder: Decoder,
    batch_size: usize,
    buffered_count: usize,
    total_records_read: usize,
    finished: bool,
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

    /// Read the next batch.
    pub async fn next_batch(&mut self) -> Result<Option<TrackedBatch>> {
        if self.finished {
            return Ok(None);
        }

        loop {
            // Use poll_fn to poll the stream
            let next = futures::future::poll_fn(|cx| self.lines.as_mut().poll_next(cx)).await;

            match next {
                Some(Ok(line)) => {
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

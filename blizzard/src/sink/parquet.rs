//! Parquet file writer.
//!
//! Writes Arrow RecordBatches to Parquet files with configurable
//! compression and file rolling based on size.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use bytes::{BufMut, BytesMut};
use parquet::arrow::ArrowWriter;
use parquet::basic::{GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;
use snafu::prelude::*;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use uuid::Uuid;

use super::FinishedFile;
use crate::config::{MB, ParquetCompression};
use crate::emit;
use crate::error::{ParquetError, WriteSnafu};
use crate::internal_events::ParquetWriteCompleted;

/// Statistics for tracking writer state.
#[derive(Debug, Clone, Copy)]
pub struct WriterStats {
    /// Total bytes written to the buffer (compressed).
    pub bytes_written: usize,
    /// Total records written.
    pub records_written: usize,
    /// Time of first write.
    pub first_write_at: Instant,
    /// Time of last write.
    pub last_write_at: Instant,
}

impl WriterStats {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            bytes_written: 0,
            records_written: 0,
            first_write_at: now,
            last_write_at: now,
        }
    }
}

/// Policy for when to roll files.
#[derive(Debug, Clone)]
pub enum RollingPolicy {
    /// Roll when file reaches this size in bytes.
    SizeLimit(usize),
    /// Roll after this duration of inactivity.
    InactivityDuration(Duration),
    /// Roll after file has been open this long.
    RolloverDuration(Duration),
}

impl RollingPolicy {
    /// Check if the file should be rolled based on this policy.
    pub fn should_roll(&self, stats: &WriterStats) -> bool {
        match self {
            RollingPolicy::SizeLimit(limit) => stats.bytes_written >= *limit,
            RollingPolicy::InactivityDuration(duration) => {
                stats.last_write_at.elapsed() >= *duration
            }
            RollingPolicy::RolloverDuration(duration) => {
                stats.first_write_at.elapsed() >= *duration
            }
        }
    }
}

/// A buffer with interior mutability for the ArrowWriter.
#[derive(Clone)]
struct SharedBuffer {
    buffer: Arc<Mutex<bytes::buf::Writer<BytesMut>>>,
}

impl SharedBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(BytesMut::with_capacity(capacity).writer())),
        }
    }

    fn into_inner(self) -> BytesMut {
        Arc::into_inner(self.buffer)
            .unwrap()
            .into_inner()
            .unwrap()
            .into_inner()
    }

    fn len(&self) -> usize {
        self.buffer.lock().unwrap().get_ref().len()
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut buffer = self.buffer.try_lock().unwrap();
        Write::write(&mut *buffer, buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Configuration for the Parquet writer.
#[derive(Debug, Clone)]
pub struct ParquetWriterConfig {
    /// Target file size in bytes (used as default for SizeLimit rolling policy).
    pub target_file_size: usize,
    /// Target row group size in bytes (for memory management).
    /// Row groups are flushed when in_progress_size exceeds this threshold.
    pub row_group_size_bytes: usize,
    /// Compression codec.
    pub compression: ParquetCompression,
    /// Rolling policies that determine when to roll files.
    pub rolling_policies: Vec<RollingPolicy>,
}

impl Default for ParquetWriterConfig {
    fn default() -> Self {
        let target_file_size = 128 * MB;
        Self {
            target_file_size,
            row_group_size_bytes: 128 * MB,
            compression: ParquetCompression::Snappy,
            rolling_policies: vec![RollingPolicy::SizeLimit(target_file_size)],
        }
    }
}

impl ParquetWriterConfig {
    /// Create a new config with a target file size in MB.
    pub fn with_file_size_mb(mut self, size_mb: usize) -> Self {
        self.target_file_size = size_mb * MB;
        // Update the default SizeLimit policy if present
        self.rolling_policies = vec![RollingPolicy::SizeLimit(self.target_file_size)];
        self
    }

    /// Set the compression codec.
    pub fn with_compression(mut self, compression: ParquetCompression) -> Self {
        self.compression = compression;
        self
    }

    /// Set the rolling policies.
    pub fn with_rolling_policies(mut self, policies: Vec<RollingPolicy>) -> Self {
        self.rolling_policies = policies;
        self
    }

    /// Set the row group size in bytes.
    pub fn with_row_group_size_bytes(mut self, size_bytes: usize) -> Self {
        self.row_group_size_bytes = size_bytes;
        self
    }
}

/// Parquet file writer that buffers batches and writes files.
pub struct ParquetWriter {
    schema: SchemaRef,
    config: ParquetWriterConfig,
    writer: Option<ArrowWriter<SharedBuffer>>,
    buffer: SharedBuffer,
    current_file_name: String,
    /// Writer statistics (bytes written, records, timing).
    stats: WriterStats,
    /// Records in the current row group (resets on flush).
    row_group_records: usize,
    finished_files: Vec<FinishedFile>,
}

impl ParquetWriter {
    /// Create a new Parquet writer.
    pub fn new(schema: SchemaRef, config: ParquetWriterConfig) -> Self {
        tracing::info!(
            "Creating ParquetWriter with config: target_file_size={} bytes ({:.2} MB), row_group_size_bytes={} ({:.2} MB), rolling_policies={:?}",
            config.target_file_size,
            config.target_file_size as f64 / 1024.0 / 1024.0,
            config.row_group_size_bytes,
            config.row_group_size_bytes as f64 / 1024.0 / 1024.0,
            config.rolling_policies
        );
        let buffer = SharedBuffer::new(64 * MB);
        let writer = Self::create_writer(&schema, &config, buffer.clone());
        let current_file_name = Self::generate_filename();

        Self {
            schema,
            config,
            writer: Some(writer),
            buffer,
            current_file_name,
            stats: WriterStats::new(),
            row_group_records: 0,
            finished_files: Vec::new(),
        }
    }

    fn create_writer(
        schema: &SchemaRef,
        config: &ParquetWriterConfig,
        buffer: SharedBuffer,
    ) -> ArrowWriter<SharedBuffer> {
        let writer_properties = Self::writer_properties(config);

        ArrowWriter::try_new(buffer, schema.clone(), Some(writer_properties))
            .expect("Failed to create Parquet writer")
    }

    fn writer_properties(config: &ParquetWriterConfig) -> WriterProperties {
        let mut builder = WriterProperties::builder();

        builder = builder.set_compression(match config.compression {
            ParquetCompression::Uncompressed => parquet::basic::Compression::UNCOMPRESSED,
            ParquetCompression::Snappy => parquet::basic::Compression::SNAPPY,
            ParquetCompression::Gzip => parquet::basic::Compression::GZIP(GzipLevel::default()),
            ParquetCompression::Zstd => parquet::basic::Compression::ZSTD(ZstdLevel::default()),
            ParquetCompression::Lz4 => parquet::basic::Compression::LZ4,
        });

        builder.build()
    }

    fn generate_filename() -> String {
        let uuid = Uuid::now_v7();
        format!("{}.parquet", uuid)
    }

    /// Write a batch to the current file.
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), ParquetError> {
        let writer = self.writer.as_mut().expect("Writer should be available");

        writer.write(batch).context(WriteSnafu)?;
        self.stats.records_written += batch.num_rows();
        self.stats.last_write_at = Instant::now();
        self.row_group_records += batch.num_rows();

        // Flush row group based on byte size
        let in_progress_size = writer.in_progress_size();
        if in_progress_size > self.config.row_group_size_bytes {
            tracing::info!(
                "Flushing row group: in_progress_size={} bytes ({:.2} MB), threshold={} bytes ({:.2} MB), total_records={}, row_group_records={}",
                in_progress_size,
                in_progress_size as f64 / 1024.0 / 1024.0,
                self.config.row_group_size_bytes,
                self.config.row_group_size_bytes as f64 / 1024.0 / 1024.0,
                self.stats.records_written,
                self.row_group_records,
            );
            writer.flush().context(WriteSnafu)?;
            self.row_group_records = 0;
            // Update bytes_written after flush
            self.stats.bytes_written = self.buffer.len();
            tracing::info!(
                "After flush: buffer={} bytes ({:.2} MB), bytes_written={}",
                self.buffer.len(),
                self.buffer.len() as f64 / 1024.0 / 1024.0,
                self.stats.bytes_written
            );
        }

        // Check rolling policies
        // Use current_file_size for SizeLimit (includes buffer + in-progress data)
        let current_size = self.current_file_size();
        let should_roll = self.config.rolling_policies.iter().any(|p| match p {
            RollingPolicy::SizeLimit(limit) => current_size >= *limit,
            _ => p.should_roll(&self.stats),
        });

        if should_roll {
            tracing::info!(
                "Rolling file due to policy: current_size={} ({:.2} MB), records={}, first_write={:?} ago, last_write={:?} ago",
                current_size,
                current_size as f64 / 1024.0 / 1024.0,
                self.stats.records_written,
                self.stats.first_write_at.elapsed(),
                self.stats.last_write_at.elapsed()
            );
            self.roll_file()?;
        }

        Ok(())
    }

    /// Roll the current file and start a new one.
    fn roll_file(&mut self) -> Result<(), ParquetError> {
        let start = Instant::now();
        let writer = self.writer.take().expect("Writer should be available");
        writer.close().context(WriteSnafu)?;

        // Capture the bytes before replacing the buffer
        let bytes = std::mem::replace(
            &mut self.buffer,
            SharedBuffer::new(64 * 1024 * 1024), // 64MB initial capacity
        )
        .into_inner()
        .freeze();

        emit!(ParquetWriteCompleted {
            duration: start.elapsed()
        });

        // Create finished file record with bytes
        let finished = FinishedFile {
            filename: self.current_file_name.clone(),
            size: bytes.len(),
            record_count: self.stats.records_written,
            bytes: Some(bytes),
        };
        self.finished_files.push(finished);

        // Reset for next file
        self.writer = Some(Self::create_writer(
            &self.schema,
            &self.config,
            self.buffer.clone(),
        ));
        self.current_file_name = Self::generate_filename();
        self.stats = WriterStats::new();
        self.row_group_records = 0;

        Ok(())
    }

    /// Close the current file and get all finished files.
    /// All files include their parquet bytes for uploading to storage.
    pub fn close(mut self) -> Result<Vec<FinishedFile>, ParquetError> {
        if self.stats.records_written > 0 {
            let start = Instant::now();
            let writer = self.writer.take().expect("Writer should be available");
            writer.close().context(WriteSnafu)?;

            let bytes = self.buffer.into_inner().freeze();

            emit!(ParquetWriteCompleted {
                duration: start.elapsed()
            });

            let finished = FinishedFile {
                filename: self.current_file_name.clone(),
                size: bytes.len(),
                record_count: self.stats.records_written,
                bytes: Some(bytes),
            };
            self.finished_files.push(finished);
        }

        Ok(self.finished_files)
    }

    /// Take finished files without closing.
    pub fn take_finished_files(&mut self) -> Vec<FinishedFile> {
        std::mem::take(&mut self.finished_files)
    }

    /// Get the current file size in bytes (including in-progress data).
    pub fn current_file_size(&self) -> usize {
        let buffer_size = self.buffer.len();
        let in_progress_size = self
            .writer
            .as_ref()
            .map(|w| w.in_progress_size())
            .unwrap_or(0);
        buffer_size + in_progress_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    fn test_batch(num_rows: usize) -> RecordBatch {
        let ids: Vec<String> = (0..num_rows).map(|i| format!("id_{}", i)).collect();
        let values: Vec<i64> = (0..num_rows).map(|i| i as i64).collect();

        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_parquet_writer_basic() {
        let schema = test_schema();
        let config = ParquetWriterConfig::default();
        let mut writer = ParquetWriter::new(schema, config);

        let batch = test_batch(100);
        writer.write_batch(&batch).unwrap();

        assert!(writer.current_file_size() > 0);
    }
}

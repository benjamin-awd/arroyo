//! Parquet file writer.
//!
//! Writes Arrow RecordBatches to Parquet files with configurable
//! compression and file rolling based on size.

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use bytes::{BufMut, Bytes, BytesMut};
use parquet::arrow::ArrowWriter;
use parquet::basic::{GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::io::Write;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

use super::FinishedFile;
use crate::config::ParquetCompression;

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
    /// Target file size in bytes.
    pub target_file_size: usize,
    /// Max records per row group (for memory management).
    pub max_records_per_row_group: usize,
    /// Compression codec.
    pub compression: ParquetCompression,
}

impl Default for ParquetWriterConfig {
    fn default() -> Self {
        Self {
            target_file_size: 10 * 1024 * 1024, // 10MB for testing
            max_records_per_row_group: 100_000, // 100k records per row group
            compression: ParquetCompression::Snappy,
        }
    }
}

impl ParquetWriterConfig {
    /// Create a new config with a target file size in MB.
    pub fn with_file_size_mb(mut self, size_mb: usize) -> Self {
        self.target_file_size = size_mb * 1024 * 1024;
        self
    }

    /// Set the compression codec.
    pub fn with_compression(mut self, compression: ParquetCompression) -> Self {
        self.compression = compression;
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
    record_count: usize,
    /// Records in the current row group (resets on flush).
    row_group_records: usize,
    finished_files: Vec<FinishedFile>,
}

impl ParquetWriter {
    /// Create a new Parquet writer.
    pub fn new(schema: SchemaRef, config: ParquetWriterConfig) -> Self {
        tracing::info!(
            "Creating ParquetWriter with config: target_file_size={} bytes ({:.2} MB), max_records_per_row_group={}",
            config.target_file_size,
            config.target_file_size as f64 / 1024.0 / 1024.0,
            config.max_records_per_row_group
        );
        let buffer = SharedBuffer::new(64 * 1024 * 1024); // 64MB initial capacity
        let writer = Self::create_writer(&schema, &config, buffer.clone());
        let current_file_name = Self::generate_filename();

        Self {
            schema,
            config,
            writer: Some(writer),
            buffer,
            current_file_name,
            record_count: 0,
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
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let writer = self.writer.as_mut().expect("Writer should be available");

        writer.write(batch)?;
        self.record_count += batch.num_rows();
        self.row_group_records += batch.num_rows();

        // Flush row group based on record count (more predictable than in_progress_size
        // which estimates encoded size and can be misleading for compressible data)
        if self.row_group_records >= self.config.max_records_per_row_group {
            tracing::info!(
                "Flushing row group: total_records={}, row_group_records={}, buffer_before={} bytes ({:.2} MB)",
                self.record_count,
                self.row_group_records,
                self.buffer.len(),
                self.buffer.len() as f64 / 1024.0 / 1024.0
            );
            writer.flush()?;
            self.row_group_records = 0;
            tracing::info!(
                "After flush: buffer={} bytes ({:.2} MB)",
                self.buffer.len(),
                self.buffer.len() as f64 / 1024.0 / 1024.0
            );
        }

        // Check if we need to roll the file (based on actual compressed size)
        let buffer_len = self.buffer.len();
        let target = self.config.target_file_size;
        if buffer_len > target {
            tracing::info!(
                "Rolling file: buffer={} bytes ({:.2} MB) > target={} bytes ({:.2} MB), records={}",
                buffer_len,
                buffer_len as f64 / 1024.0 / 1024.0,
                target,
                target as f64 / 1024.0 / 1024.0,
                self.record_count
            );
            self.roll_file()?;
        } else if buffer_len > 5 * 1024 * 1024 && self.row_group_records == 0 {
            // Log when buffer is getting large (right after flush)
            tracing::info!(
                "Buffer size check after flush: buffer={} bytes ({:.2} MB), target={} bytes ({:.2} MB)",
                buffer_len,
                buffer_len as f64 / 1024.0 / 1024.0,
                target,
                target as f64 / 1024.0 / 1024.0
            );
        }

        Ok(())
    }

    /// Roll the current file and start a new one.
    fn roll_file(&mut self) -> Result<()> {
        let writer = self.writer.take().expect("Writer should be available");
        writer.close()?;

        // Create finished file record
        let finished = FinishedFile {
            filename: self.current_file_name.clone(),
            size: self.buffer.len(),
            record_count: self.record_count,
        };
        self.finished_files.push(finished);

        // Reset for next file
        self.buffer = SharedBuffer::new(64 * 1024 * 1024); // 64MB initial capacity
        self.writer = Some(Self::create_writer(
            &self.schema,
            &self.config,
            self.buffer.clone(),
        ));
        self.current_file_name = Self::generate_filename();
        self.record_count = 0;
        self.row_group_records = 0;

        Ok(())
    }

    /// Get the current file's bytes for checkpointing.
    /// Returns the bytes and trailing footer that would make a valid Parquet file.
    pub fn get_checkpoint_bytes(&mut self) -> Result<(Bytes, String)> {
        let writer = self.writer.as_mut().expect("Writer should be available");
        writer.flush()?;

        // Get the current buffer content
        let current_bytes = self
            .buffer
            .buffer
            .lock()
            .unwrap()
            .get_ref()
            .clone()
            .freeze();

        Ok((current_bytes, self.current_file_name.clone()))
    }

    /// Close the current file and get all finished files.
    pub fn close(mut self) -> Result<(Vec<FinishedFile>, Option<Bytes>)> {
        if self.record_count > 0 {
            let writer = self.writer.take().expect("Writer should be available");
            writer.close()?;

            let finished = FinishedFile {
                filename: self.current_file_name.clone(),
                size: self.buffer.len(),
                record_count: self.record_count,
            };
            self.finished_files.push(finished);

            let bytes = self.buffer.into_inner().freeze();
            Ok((self.finished_files, Some(bytes)))
        } else {
            Ok((self.finished_files, None))
        }
    }

    /// Take finished files without closing.
    pub fn take_finished_files(&mut self) -> Vec<FinishedFile> {
        std::mem::take(&mut self.finished_files)
    }

    /// Get the current file name.
    pub fn current_file_name(&self) -> &str {
        &self.current_file_name
    }

    /// Get the number of records in the current file.
    pub fn current_record_count(&self) -> usize {
        self.record_count
    }

    /// Get the number of records in the current row group.
    pub fn current_row_group_records(&self) -> usize {
        self.row_group_records
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

    /// Check if there are any unflushed bytes.
    pub fn has_unflushed_data(&self) -> bool {
        self.record_count > 0
    }
}

/// Builder for creating Parquet file content.
pub struct ParquetFileBuilder {
    schema: SchemaRef,
    config: ParquetWriterConfig,
}

impl ParquetFileBuilder {
    /// Create a new builder.
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            config: ParquetWriterConfig::default(),
        }
    }

    /// Set the compression.
    pub fn with_compression(mut self, compression: ParquetCompression) -> Self {
        self.config.compression = compression;
        self
    }

    /// Build a Parquet file from batches.
    pub fn build(self, batches: &[RecordBatch]) -> Result<Bytes> {
        let buffer = SharedBuffer::new(64 * 1024 * 1024); // 64MB initial capacity
        let writer_properties = ParquetWriter::writer_properties(&self.config);

        let mut writer =
            ArrowWriter::try_new(buffer.clone(), self.schema.clone(), Some(writer_properties))?;

        for batch in batches {
            writer.write(batch)?;
        }

        writer.close()?;
        Ok(buffer.into_inner().freeze())
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

        assert_eq!(writer.current_record_count(), 100);
        assert!(writer.current_file_size() > 0);
    }

    #[test]
    fn test_parquet_file_builder() {
        let schema = test_schema();
        let batch = test_batch(10);

        let bytes = ParquetFileBuilder::new(schema)
            .with_compression(ParquetCompression::Snappy)
            .build(&[batch])
            .unwrap();

        assert!(!bytes.is_empty());
    }
}

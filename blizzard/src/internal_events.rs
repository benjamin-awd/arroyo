//! Internal events for metrics emission following Vector's pattern.
//!
//! Each event struct represents a measurable occurrence in the pipeline.
//! Events implement the `InternalEvent` trait which emits the corresponding
//! Prometheus counter metric.

use metrics::counter;
use tracing::trace;

/// Trait for internal events that can be emitted as metrics.
pub trait InternalEvent {
    /// Emit this event as a metric.
    fn emit(self);
}

/// Event emitted when records are processed through the pipeline.
pub struct RecordsProcessed {
    pub count: u64,
}

impl InternalEvent for RecordsProcessed {
    fn emit(self) {
        trace!(count = self.count, "Records processed");
        counter!("blizzard_records_processed_total").increment(self.count);
    }
}

/// Event emitted when compressed bytes are read from source.
pub struct BytesRead {
    pub bytes: u64,
}

impl InternalEvent for BytesRead {
    fn emit(self) {
        trace!(bytes = self.bytes, "Bytes read");
        counter!("blizzard_bytes_read_total").increment(self.bytes);
    }
}

/// Event emitted when bytes are written to Parquet files.
pub struct BytesWritten {
    pub bytes: u64,
}

impl InternalEvent for BytesWritten {
    fn emit(self) {
        trace!(bytes = self.bytes, "Bytes written");
        counter!("blizzard_bytes_written_total").increment(self.bytes);
    }
}

/// Status of a processed file.
#[derive(Debug, Clone, Copy)]
pub enum FileStatus {
    Success,
    Skipped,
    Failed,
}

impl FileStatus {
    fn as_str(&self) -> &'static str {
        match self {
            FileStatus::Success => "success",
            FileStatus::Skipped => "skipped",
            FileStatus::Failed => "failed",
        }
    }
}

/// Event emitted when an input file is processed.
pub struct FileProcessed {
    pub status: FileStatus,
}

impl InternalEvent for FileProcessed {
    fn emit(self) {
        trace!(status = self.status.as_str(), "File processed");
        counter!("blizzard_files_processed_total", "status" => self.status.as_str()).increment(1);
    }
}

/// Event emitted when an Arrow RecordBatch is created.
pub struct BatchesProcessed {
    pub count: u64,
}

impl InternalEvent for BatchesProcessed {
    fn emit(self) {
        trace!(count = self.count, "Batches processed");
        counter!("blizzard_batches_processed_total").increment(self.count);
    }
}

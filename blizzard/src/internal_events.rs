//! Internal events for metrics emission following Vector's pattern.
//!
//! Each event struct represents a measurable occurrence in the pipeline.
//! Events implement the `InternalEvent` trait which emits the corresponding
//! Prometheus counter metric.

use metrics::{counter, gauge, histogram};
use std::time::Duration;
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

// ============================================================================
// Histogram events for timing
// ============================================================================

/// Event emitted when a file download completes.
pub struct FileDownloadCompleted {
    pub duration: Duration,
}

impl InternalEvent for FileDownloadCompleted {
    fn emit(self) {
        trace!(
            duration_ms = self.duration.as_millis(),
            "File download completed"
        );
        histogram!("blizzard_file_download_duration_seconds").record(self.duration.as_secs_f64());
    }
}

/// Event emitted when file decompression completes.
pub struct FileDecompressionCompleted {
    pub duration: Duration,
}

impl InternalEvent for FileDecompressionCompleted {
    fn emit(self) {
        trace!(
            duration_ms = self.duration.as_millis(),
            "File decompression completed"
        );
        histogram!("blizzard_file_decompression_duration_seconds")
            .record(self.duration.as_secs_f64());
    }
}

/// Event emitted when a Parquet file write completes.
pub struct ParquetWriteCompleted {
    pub duration: Duration,
}

impl InternalEvent for ParquetWriteCompleted {
    fn emit(self) {
        trace!(
            duration_ms = self.duration.as_millis(),
            "Parquet write completed"
        );
        histogram!("blizzard_parquet_write_duration_seconds").record(self.duration.as_secs_f64());
    }
}

/// Event emitted when a Delta Lake commit completes.
pub struct DeltaCommitCompleted {
    pub duration: Duration,
}

impl InternalEvent for DeltaCommitCompleted {
    fn emit(self) {
        trace!(
            duration_ms = self.duration.as_millis(),
            "Delta commit completed"
        );
        histogram!("blizzard_delta_commit_duration_seconds").record(self.duration.as_secs_f64());
    }
}

/// Event emitted when a checkpoint save completes.
pub struct CheckpointSaveCompleted {
    pub duration: Duration,
}

impl InternalEvent for CheckpointSaveCompleted {
    fn emit(self) {
        trace!(
            duration_ms = self.duration.as_millis(),
            "Checkpoint save completed"
        );
        histogram!("blizzard_checkpoint_save_duration_seconds").record(self.duration.as_secs_f64());
    }
}

// ============================================================================
// Gauge events for concurrency and backpressure
// ============================================================================

/// Event emitted when the number of active downloads changes.
pub struct ActiveDownloads {
    pub count: usize,
}

impl InternalEvent for ActiveDownloads {
    fn emit(self) {
        trace!(count = self.count, "Active downloads");
        gauge!("blizzard_active_downloads").set(self.count as f64);
    }
}

/// Event emitted when the number of active uploads changes.
pub struct ActiveUploads {
    pub count: usize,
}

impl InternalEvent for ActiveUploads {
    fn emit(self) {
        trace!(count = self.count, "Active uploads");
        gauge!("blizzard_active_uploads").set(self.count as f64);
    }
}

/// Event emitted when the number of in-flight multipart parts changes.
pub struct ActiveMultipartParts {
    pub count: usize,
}

impl InternalEvent for ActiveMultipartParts {
    fn emit(self) {
        trace!(count = self.count, "Active multipart parts");
        gauge!("blizzard_active_multipart_parts").set(self.count as f64);
    }
}

/// Event emitted when the number of pending batches changes.
pub struct PendingBatches {
    pub count: usize,
}

impl InternalEvent for PendingBatches {
    fn emit(self) {
        trace!(count = self.count, "Pending batches");
        gauge!("blizzard_pending_batches").set(self.count as f64);
    }
}

/// Event emitted when the decompression queue depth changes.
pub struct DecompressionQueueDepth {
    pub count: usize,
}

impl InternalEvent for DecompressionQueueDepth {
    fn emit(self) {
        trace!(count = self.count, "Decompression queue depth");
        gauge!("blizzard_decompression_queue_depth").set(self.count as f64);
    }
}

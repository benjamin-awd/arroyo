//! Main processing pipeline.
//!
//! Connects the source, sink, and checkpoint components into a
//! streaming pipeline with backpressure and graceful shutdown.
//!
//! # Architecture
//!
//! Uses a producer-consumer pattern to separate I/O from CPU work:
//! - **Tokio tasks**: Download compressed files concurrently (I/O bound)
//! - **Tokio's blocking thread pool**: Decompress and parse files in parallel (CPU bound)
//!
//! This enables full CPU utilization during gzip decompression.

mod tasks;

use arrow::array::RecordBatch;
use futures::stream::{FuturesUnordered, StreamExt};
use snafu::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::checkpoint::CheckpointCoordinator;
use crate::config::{CompressionFormat, Config, MB};
use crate::emit;
use crate::error::{
    CheckpointSnafu, DeltaSnafu, ParquetSnafu, PipelineError, PipelineStorageSnafu, ReaderSnafu,
    TaskJoinSnafu,
};
use crate::internal_events::{
    BatchesProcessed, BytesWritten, DecompressionQueueDepth, FileProcessed, FileStatus,
    PendingBatches, RecordsProcessed, RecoveredFiles,
};
use crate::sink::FinishedFile;
use crate::sink::delta::DeltaSink;
use crate::sink::parquet::{ParquetWriter, ParquetWriterConfig, RollingPolicy};
use crate::source::{NdjsonReader, NdjsonReaderConfig};
use crate::storage::{StorageProvider, StorageProviderRef, list_ndjson_files};
use crate::utilization::UtilizationTimer;

use tasks::{DownloadedFile, UploaderConfig, run_downloader, run_uploader};

/// Future type for file processing operations.
type ProcessFuture = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<ProcessedFile, PipelineError>> + Send>,
>;

/// Statistics about the pipeline run.
#[derive(Debug, Clone, Default)]
pub struct PipelineStats {
    pub files_processed: usize,
    pub records_processed: usize,
    pub bytes_written: usize,
    pub parquet_files_written: usize,
    pub delta_commits: usize,
    pub checkpoints_saved: usize,
}

/// Result of processing a downloaded file.
struct ProcessedFile {
    path: String,
    batches: Vec<RecordBatch>,
    total_records: usize,
}

/// Initialized pipeline state ready for processing.
struct InitializedState {
    pending_files: Vec<String>,
    source_state: crate::source::SourceState,
    schema: Arc<arrow::datatypes::Schema>,
    writer: ParquetWriter,
    delta_sink: DeltaSink,
    max_concurrent: usize,
    compression: CompressionFormat,
    batch_size: usize,
}

/// Main processing pipeline.
pub struct Pipeline {
    config: Config,
    source_storage: StorageProviderRef,
    sink_storage: StorageProviderRef,
    checkpoint_coordinator: CheckpointCoordinator,
    stats: PipelineStats,
    shutdown: CancellationToken,
}

impl Pipeline {
    /// Create a new pipeline from configuration.
    pub async fn new(config: Config, shutdown: CancellationToken) -> Result<Self, PipelineError> {
        // Create storage providers
        let source_storage = Arc::new(
            StorageProvider::for_url_with_options(
                &config.source.path,
                config.source.storage_options.clone(),
            )
            .await
            .context(PipelineStorageSnafu)?,
        );

        let sink_storage = Arc::new(
            StorageProvider::for_url_with_options(
                &config.sink.path,
                config.sink.storage_options.clone(),
            )
            .await
            .context(PipelineStorageSnafu)?,
        );

        let checkpoint_storage = Arc::new(
            StorageProvider::for_url_with_options(
                &config.checkpoint.path,
                config.checkpoint.storage_options.clone(),
            )
            .await
            .context(PipelineStorageSnafu)?,
        );

        let checkpoint_coordinator =
            CheckpointCoordinator::new(checkpoint_storage, config.checkpoint.interval_seconds);

        Ok(Self {
            config,
            source_storage,
            sink_storage,
            checkpoint_coordinator,
            stats: PipelineStats::default(),
            shutdown,
        })
    }

    /// Run the pipeline.
    ///
    /// Uses a producer-consumer pattern to maximize parallelism:
    /// - **Producer**: Tokio tasks download compressed files concurrently (I/O bound)
    /// - **Consumer**: Tokio's blocking thread pool decompresses and parses files (CPU bound)
    pub async fn run(&mut self) -> Result<PipelineStats, PipelineError> {
        info!("Starting pipeline");

        // Race initialization against shutdown signal
        let shutdown = self.shutdown.clone();
        let state = tokio::select! {
            biased;

            _ = shutdown.cancelled() => {
                info!("Shutdown requested during initialization");
                return Ok(self.stats.clone());
            }

            result = self.prepare_pipeline() => result?,
        };

        // Handle empty work case
        let Some(state) = state else {
            info!("No files to process");
            return Ok(self.stats.clone());
        };

        // Unpack initialized state
        let InitializedState {
            pending_files,
            source_state,
            schema,
            mut writer,
            delta_sink,
            max_concurrent,
            compression,
            batch_size,
        } = state;

        // Create the NDJSON reader for decompression and parsing
        let reader_config = NdjsonReaderConfig::new(batch_size, compression);
        let reader = Arc::new(NdjsonReader::new(schema.clone(), reader_config));

        info!(
            "Processing files with max_concurrent_files={} (download-then-decompress mode)",
            max_concurrent
        );

        // Channel for downloaded files ready for CPU processing
        // Buffer size matches max_concurrent to allow downloads to stay ahead
        let (download_tx, mut download_rx) =
            mpsc::channel::<Result<DownloadedFile, crate::error::StorageError>>(max_concurrent);

        // Channel for finished parquet files ready for upload
        // Larger buffer to allow more queuing and avoid blocking the writer
        let max_concurrent_uploads = self.config.sink.max_concurrent_uploads;
        let buffer_size = max_concurrent_uploads * 4;
        let (upload_tx, upload_rx) = mpsc::channel::<FinishedFile>(buffer_size);

        // Spawn background uploader task with concurrent file uploads
        let sink_storage = self.sink_storage.clone();
        let upload_shutdown = self.shutdown.clone();
        let uploader_config = UploaderConfig {
            part_size: self.config.sink.part_size_mb * MB,
            min_multipart_size: self.config.sink.min_multipart_size_mb * MB,
            max_concurrent_uploads,
            max_concurrent_parts: self.config.sink.max_concurrent_parts,
        };

        let upload_handle = tokio::spawn(run_uploader(
            upload_rx,
            delta_sink,
            sink_storage,
            upload_shutdown,
            uploader_config,
        ));

        // Spawn download tasks concurrently using FuturesUnordered
        let storage = self.source_storage.clone();
        let source_state_clone = source_state.clone();
        let pending_files_clone = pending_files.clone();
        let shutdown = self.shutdown.clone();

        let download_handle = tokio::spawn(run_downloader(
            pending_files_clone,
            source_state_clone,
            storage,
            download_tx,
            shutdown,
            max_concurrent,
        ));

        // Consumer: Process downloaded files with parallel decompression
        // Use FuturesUnordered to process multiple files concurrently
        let mut files_remaining = pending_files.len();
        let mut processing: FuturesUnordered<ProcessFuture> = FuturesUnordered::new();
        let mut channel_open = true;
        let mut util_timer = UtilizationTimer::new("processor");

        // Helper to spawn a read task (decompress + parse)
        let spawn_read = |downloaded: DownloadedFile, reader: Arc<NdjsonReader>| {
            let path = downloaded.path.clone();
            let path_for_result = path.clone();
            let task: ProcessFuture = Box::pin(async move {
                let result = tokio::task::spawn_blocking(move || {
                    reader.read(downloaded.compressed_data, downloaded.skip_records, &path)
                })
                .await
                .context(TaskJoinSnafu)?
                .context(ReaderSnafu)?;
                Ok(ProcessedFile {
                    path: path_for_result,
                    batches: result.batches,
                    total_records: result.total_records,
                })
            });
            task
        };

        loop {
            // Update utilization state: waiting if no processing tasks
            if processing.is_empty() {
                util_timer.start_wait();
            }
            util_timer.maybe_update();

            if self.shutdown.is_cancelled() {
                info!("Shutdown requested, stopping processing");
                self.checkpoint_coordinator
                    .checkpoint()
                    .await
                    .context(CheckpointSnafu)?;
                break;
            }

            // If no processing tasks and channel closed, we're done
            if processing.is_empty() && !channel_open {
                break;
            }

            // Use select! to simultaneously:
            // 1. Receive new downloads (if we have capacity)
            // 2. Wait for processing tasks to complete
            let has_capacity = processing.len() < max_concurrent && channel_open;

            tokio::select! {
                // Only poll for new downloads if we have processing capacity
                result = download_rx.recv(), if has_capacity => {
                    match result {
                        Some(Ok(downloaded)) => {
                            // Transition to working state when we have processing tasks
                            if processing.is_empty() {
                                util_timer.stop_wait();
                            }
                            processing.push(spawn_read(downloaded, reader.clone()));
                            emit!(DecompressionQueueDepth {
                                count: processing.len()
                            });
                        }
                        Some(Err(e)) => {
                            let error_str = e.to_string();
                            if error_str.contains("not found")
                                || error_str.contains("404")
                                || error_str.contains("NoSuchKey")
                            {
                                warn!("Skipping file with download error: {}", e);
                                files_remaining -= 1;
                                emit!(FileProcessed { status: FileStatus::Skipped });
                            } else {
                                error!("Error downloading file: {}", e);
                                emit!(FileProcessed { status: FileStatus::Failed });
                                self.checkpoint_coordinator
                                    .checkpoint()
                                    .await
                                    .context(CheckpointSnafu)?;
                                return Err(e).context(PipelineStorageSnafu);
                            }
                        }
                        None => {
                            channel_open = false;
                        }
                    }
                }

                // Wait for processing tasks to complete
                result = processing.next(), if !processing.is_empty() => {
                    if let Some(result) = result {
                        emit!(DecompressionQueueDepth {
                            count: processing.len()
                        });
                        match result {
                            Ok(processed) => {
                                let short_name = processed.path.split('/').next_back().unwrap_or(&processed.path);

                                // Track pending batches before writing
                                emit!(PendingBatches {
                                    count: processed.batches.len()
                                });

                                // Write all batches from this file
                                for batch in &processed.batches {
                                    debug!("[batch] {}: {} rows", short_name, batch.num_rows());
                                    writer.write_batch(batch).context(ParquetSnafu)?;
                                    self.stats.records_processed += batch.num_rows();
                                    emit!(RecordsProcessed { count: batch.num_rows() as u64 });
                                    emit!(BatchesProcessed { count: 1 });
                                }
                                emit!(PendingBatches { count: 0 });

                                // Update checkpoint state
                                self.checkpoint_coordinator
                                    .update_source_state(&processed.path, processed.total_records, true)
                                    .await;

                                self.stats.files_processed += 1;
                                files_remaining -= 1;
                                emit!(FileProcessed { status: FileStatus::Success });
                                info!(
                                    "[-] Finished file (remaining: {}): {} ({} records)",
                                    files_remaining, short_name, processed.total_records
                                );

                                // Queue finished parquet files for background upload
                                let finished = writer.take_finished_files();
                                for file in finished {
                                    self.stats.parquet_files_written += 1;
                                    self.stats.bytes_written += file.size;
                                    emit!(BytesWritten { bytes: file.size as u64 });
                                    if upload_tx.send(file).await.is_err() {
                                        error!("Upload channel closed unexpectedly");
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                let error_str = e.to_string();
                                if error_str.contains("not found")
                                    || error_str.contains("404")
                                    || error_str.contains("NoSuchKey")
                                {
                                    warn!("Skipping file with processing error: {}", e);
                                    files_remaining -= 1;
                                    emit!(FileProcessed { status: FileStatus::Skipped });
                                } else {
                                    error!("Error processing file: {}", e);
                                    emit!(FileProcessed { status: FileStatus::Failed });
                                    self.checkpoint_coordinator
                                        .checkpoint()
                                        .await
                                        .context(CheckpointSnafu)?;
                                    return Err(e);
                                }
                            }
                        }
                    }
                }
            }

            // Check if we should checkpoint
            if self.checkpoint_coordinator.should_checkpoint().await {
                self.checkpoint_coordinator
                    .checkpoint()
                    .await
                    .context(CheckpointSnafu)?;
                self.stats.checkpoints_saved += 1;
            }
        }

        // Drop the receiver to unblock the download task (it will see channel closed)
        drop(download_rx);

        // Cancel the download task - don't wait for it since it may be blocked
        download_handle.abort();

        // Send remaining parquet files to uploader
        info!("Final flush - closing writer");
        let finished_files = writer.close().context(ParquetSnafu)?;
        for file in finished_files {
            self.stats.parquet_files_written += 1;
            self.stats.bytes_written += file.size;
            emit!(BytesWritten {
                bytes: file.size as u64
            });
            if upload_tx.send(file).await.is_err() {
                error!("Upload channel closed unexpectedly during final flush");
            }
        }

        // Close upload channel and wait for uploader to finish
        drop(upload_tx);
        info!("Waiting for uploads to complete...");
        let (delta_sink, files_uploaded, bytes_uploaded) =
            upload_handle.await.context(TaskJoinSnafu)?;
        info!(
            "All uploads complete: {} files, {} bytes",
            files_uploaded, bytes_uploaded
        );
        self.stats.delta_commits = files_uploaded;

        // Save final checkpoint
        self.checkpoint_coordinator
            .checkpoint()
            .await
            .context(CheckpointSnafu)?;
        self.stats.checkpoints_saved += 1;

        info!("Pipeline completed: {:?}", self.stats);
        info!("Final Delta table version: {}", delta_sink.version());

        Ok(self.stats.clone())
    }

    /// Prepare the pipeline for processing.
    ///
    /// Performs all initialization: checkpoint restoration, Delta sink creation,
    /// file listing, and writer setup. Returns `None` if there are no files to process.
    ///
    /// This method is designed to be cancellation-safe - it can be dropped at any
    /// `.await` point without leaving the system in an inconsistent state.
    async fn prepare_pipeline(&mut self) -> Result<Option<InitializedState>, PipelineError> {
        // Restore from checkpoint
        let checkpoint = self
            .checkpoint_coordinator
            .restore()
            .await
            .context(CheckpointSnafu)?;
        if let Some(ref cp) = checkpoint {
            info!(
                "Restored from checkpoint, delta version: {}, pending files: {}",
                cp.delta_version,
                cp.pending_files.len()
            );
        }

        // Create the Arrow schema from config
        let schema = self.config.to_arrow_schema();

        // Create Delta sink
        let mut delta_sink = DeltaSink::new(self.sink_storage.clone(), &schema)
            .await
            .context(DeltaSnafu)?;

        // Handle pending files from checkpoint
        if let Some(ref cp) = checkpoint {
            if !cp.pending_files.is_empty() {
                let pending_count = cp.pending_files.len();
                info!("Committing {} pending files from checkpoint", pending_count);
                if let Some(version) = delta_sink
                    .recover_pending_files(&cp.pending_files)
                    .await
                    .context(DeltaSnafu)?
                {
                    self.checkpoint_coordinator
                        .update_delta_version(version)
                        .await;
                    self.stats.delta_commits += 1;
                    emit!(RecoveredFiles {
                        count: pending_count as u64
                    });
                }
                self.checkpoint_coordinator.clear_pending_files().await;
            }
        }

        // List source files
        let source_files = self.list_source_files().await?;
        info!("Found {} source files", source_files.len());

        // Get current source state
        let source_state = self.checkpoint_coordinator.get_source_state().await;

        // Filter to unprocessed files
        let pending_files: Vec<String> = source_state
            .pending_files(&source_files)
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        info!("{} files remaining to process", pending_files.len());

        if pending_files.is_empty() {
            return Ok(None);
        }

        // Build writer configuration
        let rolling_policies = self.build_rolling_policies();
        let writer_config = ParquetWriterConfig::default()
            .with_file_size_mb(self.config.sink.file_size_mb)
            .with_row_group_size_bytes(self.config.sink.row_group_size_bytes)
            .with_compression(self.config.sink.compression)
            .with_rolling_policies(rolling_policies);

        let writer = ParquetWriter::new(schema.clone(), writer_config);

        Ok(Some(InitializedState {
            pending_files,
            source_state,
            schema,
            writer,
            delta_sink,
            max_concurrent: self.config.source.max_concurrent_files,
            compression: self.config.source.compression,
            batch_size: self.config.source.batch_size,
        }))
    }

    /// Build rolling policies from configuration.
    fn build_rolling_policies(&self) -> Vec<RollingPolicy> {
        let mut policies = Vec::new();

        // Always include size-based rolling
        let size_bytes = self.config.sink.file_size_mb * MB;
        policies.push(RollingPolicy::SizeLimit(size_bytes));

        // Add inactivity timeout if configured
        if let Some(secs) = self.config.sink.inactivity_timeout_secs {
            policies.push(RollingPolicy::InactivityDuration(Duration::from_secs(secs)));
            debug!("Added inactivity rolling policy: {} seconds", secs);
        }

        // Add rollover timeout if configured
        if let Some(secs) = self.config.sink.rollover_timeout_secs {
            policies.push(RollingPolicy::RolloverDuration(Duration::from_secs(secs)));
            debug!("Added rollover rolling policy: {} seconds", secs);
        }

        policies
    }

    /// List source NDJSON files.
    async fn list_source_files(&self) -> Result<Vec<String>, PipelineError> {
        list_ndjson_files(&self.source_storage)
            .await
            .context(PipelineStorageSnafu)
    }
}

/// Run the pipeline with the given configuration.
pub async fn run_pipeline(config: Config) -> Result<PipelineStats, PipelineError> {
    let shutdown = CancellationToken::new();

    // Set up signal handler for graceful shutdown
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            tokio::signal::ctrl_c().await.ok();
            info!("Received shutdown signal");
            shutdown.cancel();
        }
    });

    let mut pipeline = Pipeline::new(config, shutdown).await?;
    pipeline.run().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_stats_default() {
        let stats = PipelineStats::default();
        assert_eq!(stats.files_processed, 0);
        assert_eq!(stats.records_processed, 0);
    }
}

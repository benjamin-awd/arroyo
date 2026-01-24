//! Main processing pipeline.
//!
//! Connects the source, sink, and checkpoint components into a
//! streaming pipeline with backpressure and graceful shutdown.
//!
//! # Architecture
//!
//! Uses a producer-consumer pattern to separate I/O from CPU work:
//! - **Tokio tasks**: Download compressed files concurrently (I/O bound)
//! - **Rayon thread pool**: Decompress and parse files in parallel (CPU bound)
//!
//! This enables full CPU utilization during gzip decompression.

use anyhow::Result;
use arrow::array::RecordBatch;
use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info, warn};

use crate::checkpoint::{CheckpointCoordinator, PendingFile};
use crate::config::{CompressionFormat, Config};
use crate::sink::FinishedFile;
use crate::sink::delta::DeltaSink;
use crate::sink::parquet::{ParquetWriter, ParquetWriterConfig, RollingPolicy};
use crate::source::reader::NdjsonReader;
use crate::storage::{StorageProvider, StorageProviderRef, list_ndjson_files};

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

/// Downloaded file ready for processing.
struct DownloadedFile {
    path: String,
    compressed_data: Bytes,
    skip_records: usize,
}

/// Result of processing a downloaded file.
struct ProcessedFile {
    path: String,
    batches: Vec<RecordBatch>,
    total_records: usize,
}

/// Main processing pipeline.
pub struct Pipeline {
    config: Config,
    source_storage: StorageProviderRef,
    sink_storage: StorageProviderRef,
    checkpoint_coordinator: CheckpointCoordinator,
    stats: PipelineStats,
    shutdown_rx: watch::Receiver<bool>,
}

impl Pipeline {
    /// Create a new pipeline from configuration.
    pub async fn new(config: Config, shutdown_rx: watch::Receiver<bool>) -> Result<Self> {
        // Create storage providers
        let source_storage = Arc::new(
            StorageProvider::for_url_with_options(
                &config.source.path,
                config.source.storage_options.clone(),
            )
            .await?,
        );

        let sink_storage = Arc::new(
            StorageProvider::for_url_with_options(
                &config.sink.path,
                config.sink.storage_options.clone(),
            )
            .await?,
        );

        let checkpoint_storage = Arc::new(
            StorageProvider::for_url_with_options(
                &config.checkpoint.path,
                config.checkpoint.storage_options.clone(),
            )
            .await?,
        );

        let checkpoint_coordinator =
            CheckpointCoordinator::new(checkpoint_storage, config.checkpoint.interval_seconds);

        Ok(Self {
            config,
            source_storage,
            sink_storage,
            checkpoint_coordinator,
            stats: PipelineStats::default(),
            shutdown_rx,
        })
    }

    /// Run the pipeline.
    ///
    /// Uses a producer-consumer pattern to maximize parallelism:
    /// - **Producer**: Tokio tasks download compressed files concurrently (I/O bound)
    /// - **Consumer**: Rayon thread pool decompresses and parses files (CPU bound)
    pub async fn run(&mut self) -> Result<PipelineStats> {
        info!("Starting pipeline");

        // Try to restore from checkpoint
        let checkpoint = self.checkpoint_coordinator.restore().await?;
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
        let mut delta_sink = DeltaSink::new(self.sink_storage.clone(), &schema).await?;

        // Handle pending files from checkpoint
        if let Some(ref cp) = checkpoint {
            if !cp.pending_files.is_empty() {
                info!(
                    "Committing {} pending files from checkpoint",
                    cp.pending_files.len()
                );
                let finished_files: Vec<FinishedFile> = cp
                    .pending_files
                    .iter()
                    .map(|pf| FinishedFile {
                        filename: pf.filename.clone(),
                        size: 0, // Size is not critical for commit
                        record_count: pf.record_count,
                        bytes: None, // Files already uploaded, just need to commit
                    })
                    .collect();

                if let Some(version) = delta_sink.commit_files(&finished_files).await? {
                    self.checkpoint_coordinator
                        .update_delta_version(version)
                        .await;
                    self.stats.delta_commits += 1;
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
            info!("No files to process");
            return Ok(self.stats.clone());
        }

        // Create reader config
        let reader = NdjsonReader::new(schema.clone(), self.config.source.batch_size);

        // Build rolling policies from config
        let rolling_policies = self.build_rolling_policies();

        let writer_config = ParquetWriterConfig::default()
            .with_file_size_mb(self.config.sink.file_size_mb)
            .with_row_group_size_bytes(self.config.sink.row_group_size_bytes)
            .with_compression(self.config.sink.compression)
            .with_rolling_policies(rolling_policies);

        let mut writer = ParquetWriter::new(schema.clone(), writer_config);

        let max_concurrent = self.config.source.max_concurrent_files;
        let compression = self.config.source.compression;
        let batch_size = reader.batch_size();

        info!(
            "Processing files with max_concurrent_files={} (download-then-decompress mode)",
            max_concurrent
        );

        // Channel for downloaded files ready for CPU processing
        // Buffer size matches max_concurrent to allow downloads to stay ahead
        let (download_tx, mut download_rx) =
            mpsc::channel::<Result<DownloadedFile>>(max_concurrent);

        // Channel for finished parquet files ready for upload
        let (upload_tx, mut upload_rx) = mpsc::channel::<FinishedFile>(max_concurrent);

        // Spawn background uploader task
        let sink_storage = self.sink_storage.clone();
        let upload_handle = tokio::spawn(async move {
            let mut delta_sink = delta_sink;
            let mut files_uploaded = 0usize;
            let mut bytes_uploaded = 0usize;

            while let Some(file) = upload_rx.recv().await {
                if let Some(ref bytes) = file.bytes {
                    info!(
                        "Uploading parquet file {} ({} bytes, {} records)",
                        file.filename, file.size, file.record_count
                    );

                    // Upload to storage
                    if let Err(e) = sink_storage
                        .put(file.filename.as_str(), bytes.to_vec())
                        .await
                    {
                        error!("Failed to upload parquet file {}: {}", file.filename, e);
                        continue;
                    }

                    bytes_uploaded += file.size;
                }

                // Commit to Delta
                match delta_sink.commit_files(&[file]).await {
                    Ok(Some(version)) => {
                        files_uploaded += 1;
                        info!("Committed file to Delta Lake, version {}", version);
                    }
                    Ok(None) => {
                        debug!("No commit needed (duplicate file)");
                    }
                    Err(e) => {
                        error!("Failed to commit to Delta: {}", e);
                    }
                }
            }

            info!(
                "Uploader finished: {} files, {} bytes",
                files_uploaded, bytes_uploaded
            );
            (delta_sink, files_uploaded, bytes_uploaded)
        });

        // Spawn download tasks concurrently using FuturesUnordered
        let storage = self.source_storage.clone();
        let source_state_clone = source_state.clone();
        let pending_files_clone = pending_files.clone();
        let shutdown_rx = self.shutdown_rx.clone();

        let download_handle = tokio::spawn(async move {
            let mut downloads: FuturesUnordered<
                std::pin::Pin<Box<dyn std::future::Future<Output = Result<DownloadedFile>> + Send>>,
            > = FuturesUnordered::new();

            let mut pending_iter = pending_files_clone.into_iter();
            let mut active_downloads = 0;

            // Start initial downloads
            for file_path in pending_iter.by_ref().take(max_concurrent) {
                let skip = source_state_clone.records_to_skip(&file_path);
                let storage = storage.clone();
                active_downloads += 1;
                debug!(
                    "[download] Starting {} (active: {})",
                    file_path, active_downloads
                );
                downloads.push(Box::pin(Self::download_file(storage, file_path, skip)));
            }

            // Process downloads and start new ones as they complete
            while let Some(result) = downloads.next().await {
                if *shutdown_rx.borrow() {
                    debug!("[download] Shutdown requested, stopping downloads");
                    break;
                }

                active_downloads -= 1;

                // Send result to consumer (decompress+parse)
                let should_continue = match &result {
                    Ok(downloaded) => {
                        debug!(
                            "[download] Completed {} ({} bytes)",
                            downloaded.path,
                            downloaded.compressed_data.len()
                        );
                        true
                    }
                    Err(e) => {
                        let error_str = e.to_string();
                        // Skip 404 errors, propagate others
                        if error_str.contains("not found")
                            || error_str.contains("404")
                            || error_str.contains("NoSuchKey")
                        {
                            warn!("[download] Skipping missing file: {}", e);
                            false // Don't send error, just skip
                        } else {
                            true // Send error to consumer
                        }
                    }
                };

                if should_continue {
                    if download_tx.send(result).await.is_err() {
                        debug!("[download] Consumer closed, stopping downloads");
                        break;
                    }
                }

                // Start next download if available
                if let Some(next_file) = pending_iter.next() {
                    let skip = source_state_clone.records_to_skip(&next_file);
                    let storage = storage.clone();
                    active_downloads += 1;
                    debug!(
                        "[download] Starting {} (active: {})",
                        next_file, active_downloads
                    );
                    downloads.push(Box::pin(Self::download_file(storage, next_file, skip)));
                }
            }

            debug!("[download] All downloads complete");
        });

        // Consumer: Process downloaded files with parallel decompression
        // Use FuturesUnordered to process multiple files concurrently
        let mut files_remaining = pending_files.len();
        let mut processing: FuturesUnordered<
            std::pin::Pin<Box<dyn std::future::Future<Output = Result<ProcessedFile>> + Send>>,
        > = FuturesUnordered::new();
        let mut channel_open = true;

        // Helper to spawn a decompression task
        let spawn_decompress =
            |downloaded: DownloadedFile, schema: Arc<arrow::datatypes::Schema>| {
                let path = downloaded.path.clone();
                let task: std::pin::Pin<
                    Box<dyn std::future::Future<Output = Result<ProcessedFile>> + Send>,
                > = Box::pin(async move {
                    let result = tokio::task::spawn_blocking(move || {
                        Self::decompress_and_parse(
                            downloaded.compressed_data,
                            compression,
                            schema,
                            batch_size,
                            downloaded.skip_records,
                            &path,
                        )
                    })
                    .await??;
                    Ok(result)
                });
                task
            };

        loop {
            if *self.shutdown_rx.borrow() {
                info!("Shutdown requested, stopping processing");
                self.checkpoint_coordinator.checkpoint().await?;
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
                            processing.push(spawn_decompress(downloaded, schema.clone()));
                        }
                        Some(Err(e)) => {
                            let error_str = e.to_string();
                            if error_str.contains("not found")
                                || error_str.contains("404")
                                || error_str.contains("NoSuchKey")
                            {
                                warn!("Skipping file with download error: {}", e);
                                files_remaining -= 1;
                            } else {
                                error!("Error downloading file: {}", e);
                                self.checkpoint_coordinator.checkpoint().await?;
                                return Err(e);
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
                        match result {
                            Ok(processed) => {
                                let short_name = processed.path.split('/').last().unwrap_or(&processed.path);

                                // Write all batches from this file
                                for batch in &processed.batches {
                                    debug!("[batch] {}: {} rows", short_name, batch.num_rows());
                                    writer.write_batch(batch)?;
                                    self.stats.records_processed += batch.num_rows();
                                }

                                // Update checkpoint state
                                self.checkpoint_coordinator
                                    .update_source_state(&processed.path, processed.total_records, true)
                                    .await;

                                self.stats.files_processed += 1;
                                files_remaining -= 1;
                                info!(
                                    "[-] Finished file (remaining: {}): {} ({} records)",
                                    files_remaining, short_name, processed.total_records
                                );

                                // Queue finished parquet files for background upload
                                let finished = writer.take_finished_files();
                                for file in finished {
                                    self.stats.parquet_files_written += 1;
                                    self.stats.bytes_written += file.size;
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
                                } else {
                                    error!("Error processing file: {}", e);
                                    self.checkpoint_coordinator.checkpoint().await?;
                                    return Err(e);
                                }
                            }
                        }
                    }
                }
            }

            // Check if we should checkpoint
            if self.checkpoint_coordinator.should_checkpoint().await {
                self.checkpoint_coordinator.checkpoint().await?;
                self.stats.checkpoints_saved += 1;
            }
        }

        // Drop the receiver to unblock the download task (it will see channel closed)
        drop(download_rx);

        // Cancel the download task - don't wait for it since it may be blocked
        download_handle.abort();

        // Send remaining parquet files to uploader
        info!("Final flush - closing writer");
        let finished_files = writer.close()?;
        for file in finished_files {
            self.stats.parquet_files_written += 1;
            self.stats.bytes_written += file.size;
            if upload_tx.send(file).await.is_err() {
                error!("Upload channel closed unexpectedly during final flush");
            }
        }

        // Close upload channel and wait for uploader to finish
        drop(upload_tx);
        info!("Waiting for uploads to complete...");
        let (delta_sink, files_uploaded, bytes_uploaded) = upload_handle.await?;
        info!(
            "All uploads complete: {} files, {} bytes",
            files_uploaded, bytes_uploaded
        );
        self.stats.delta_commits = files_uploaded;

        // Save final checkpoint
        self.checkpoint_coordinator.checkpoint().await?;
        self.stats.checkpoints_saved += 1;

        info!("Pipeline completed: {:?}", self.stats);
        info!("Final Delta table version: {}", delta_sink.version());

        Ok(self.stats.clone())
    }

    /// Build rolling policies from configuration.
    fn build_rolling_policies(&self) -> Vec<RollingPolicy> {
        let mut policies = Vec::new();

        // Always include size-based rolling
        let size_bytes = self.config.sink.file_size_mb * 1024 * 1024;
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
    async fn list_source_files(&self) -> Result<Vec<String>> {
        list_ndjson_files(&self.source_storage).await
    }

    /// Download a file's compressed data asynchronously.
    async fn download_file(
        storage: StorageProviderRef,
        path: String,
        skip_records: usize,
    ) -> Result<DownloadedFile> {
        let compressed_data = storage.get(path.as_str()).await?;
        Ok(DownloadedFile {
            path,
            compressed_data,
            skip_records,
        })
    }

    /// Process a downloaded file: decompress and parse in rayon thread pool.
    async fn process_downloaded_file(
        &self,
        downloaded: DownloadedFile,
        compression: CompressionFormat,
        schema: Arc<arrow::datatypes::Schema>,
        batch_size: usize,
    ) -> Result<ProcessedFile> {
        let path = downloaded.path.clone();
        let skip_records = downloaded.skip_records;
        let compressed_data = downloaded.compressed_data;

        // Run decompression + parsing in rayon thread pool via spawn_blocking
        let result = tokio::task::spawn_blocking(move || {
            Self::decompress_and_parse(
                compressed_data,
                compression,
                schema,
                batch_size,
                skip_records,
                &path,
            )
        })
        .await??;

        Ok(result)
    }

    /// Decompress and parse a file's data (runs in rayon thread pool).
    fn decompress_and_parse(
        compressed: Bytes,
        compression: CompressionFormat,
        schema: Arc<arrow::datatypes::Schema>,
        batch_size: usize,
        skip_records: usize,
        path: &str,
    ) -> Result<ProcessedFile> {
        use arrow_json::reader::ReaderBuilder;
        use std::io::Read;

        // Decompress
        let decompressed = match compression {
            CompressionFormat::Gzip => {
                let mut decoder = flate2::read::GzDecoder::new(&compressed[..]);
                let mut buf = Vec::new();
                decoder.read_to_end(&mut buf).map_err(|e| {
                    anyhow::anyhow!("Gzip decompression failed for {}: {}", path, e)
                })?;
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

        Ok(ProcessedFile {
            path: path.to_string(),
            batches,
            total_records,
        })
    }

    /// Flush finished Parquet files to Delta Lake.
    async fn flush_finished_files(
        &mut self,
        files: &[FinishedFile],
        delta_sink: &mut DeltaSink,
    ) -> Result<()> {
        if files.is_empty() {
            return Ok(());
        }

        // Upload parquet files to storage first (only if bytes are present)
        for file in files {
            if let Some(ref bytes) = file.bytes {
                info!(
                    "Uploading parquet file {} ({} bytes, {} records)",
                    file.filename, file.size, file.record_count
                );
                self.sink_storage
                    .put(file.filename.as_str(), bytes.to_vec())
                    .await?;
            }
        }

        // Add files to pending list
        for file in files {
            self.checkpoint_coordinator
                .add_pending_file(PendingFile {
                    filename: file.filename.clone(),
                    record_count: file.record_count,
                })
                .await;
        }

        // Commit to Delta
        if let Some(version) = delta_sink.commit_files(files).await? {
            self.checkpoint_coordinator
                .update_delta_version(version)
                .await;
            self.checkpoint_coordinator.clear_pending_files().await;
            self.stats.delta_commits += 1;
        }

        self.stats.parquet_files_written += files.len();
        self.stats.bytes_written += files.iter().map(|f| f.size).sum::<usize>();

        Ok(())
    }

    /// Trigger a checkpoint.
    async fn trigger_checkpoint(
        &mut self,
        writer: &mut ParquetWriter,
        delta_sink: &mut DeltaSink,
    ) -> Result<()> {
        debug!("Triggering checkpoint");

        // Flush any finished files first
        let finished = writer.take_finished_files();
        if !finished.is_empty() {
            self.flush_finished_files(&finished, delta_sink).await?;
        }

        // Save checkpoint
        self.checkpoint_coordinator.checkpoint().await?;
        self.stats.checkpoints_saved += 1;

        info!("Checkpoint saved");

        Ok(())
    }

    /// Final flush at the end of processing.
    async fn final_flush(
        &mut self,
        writer: ParquetWriter,
        delta_sink: &mut DeltaSink,
    ) -> Result<()> {
        info!("Final flush - closing writer and committing to Delta");

        // Close the writer and get all finished files including current file
        let finished_files = writer.close()?;

        if !finished_files.is_empty() {
            info!("Flushing {} parquet files to Delta", finished_files.len());
            self.flush_finished_files(&finished_files, delta_sink)
                .await?;
        }

        // Save final checkpoint
        self.checkpoint_coordinator.checkpoint().await?;
        self.stats.checkpoints_saved += 1;

        Ok(())
    }
}

/// Run the pipeline with the given configuration.
pub async fn run_pipeline(config: Config) -> Result<PipelineStats> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Set up signal handler for graceful shutdown
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Received shutdown signal");
        shutdown_tx_clone.send(true).ok();
    });

    let mut pipeline = Pipeline::new(config, shutdown_rx).await?;
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

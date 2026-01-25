//! Background tasks for concurrent uploads and downloads.
//!
//! These tasks are spawned by the main pipeline to handle I/O-bound work
//! concurrently while the main loop processes files.

use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::emit;
use crate::error::StorageError;
use crate::internal_events::{
    ActiveDownloads, ActiveUploads, FileDownloadCompleted, RecoveredRecords,
};
use crate::sink::FinishedFile;
use crate::sink::delta::DeltaSink;
use crate::source::SourceState;
use crate::storage::{StorageProvider, StorageProviderRef};
use crate::utilization::UtilizationTimer;

/// Future type for upload operations.
type UploadFuture = Pin<Box<dyn Future<Output = Result<UploadResult, StorageError>> + Send>>;

/// Future type for download operations.
type DownloadFuture = Pin<Box<dyn Future<Output = Result<DownloadedFile, StorageError>> + Send>>;

/// Downloaded file ready for processing.
pub(super) struct DownloadedFile {
    pub path: String,
    pub compressed_data: Bytes,
    pub skip_records: usize,
}

/// Result of uploading a file.
struct UploadResult {
    filename: String,
    size: usize,
    record_count: usize,
}

/// Configuration for the uploader task.
pub(super) struct UploaderConfig {
    pub part_size: usize,
    pub min_multipart_size: usize,
    pub max_concurrent_uploads: usize,
    pub max_concurrent_parts: usize,
}

/// Upload a single file to storage with parallel part uploads.
async fn upload_file(
    storage: Arc<StorageProvider>,
    file: FinishedFile,
    part_size: usize,
    min_multipart_size: usize,
    max_concurrent_parts: usize,
) -> Result<UploadResult, StorageError> {
    let Some(bytes) = file.bytes else {
        // No bytes means the file was already uploaded (e.g., from checkpoint recovery)
        return Ok(UploadResult {
            filename: file.filename,
            size: file.size,
            record_count: file.record_count,
        });
    };

    let path = object_store::path::Path::from(file.filename.as_str());
    storage
        .put_multipart_bytes_parallel(
            &path,
            bytes,
            part_size,
            min_multipart_size,
            max_concurrent_parts,
        )
        .await?;

    Ok(UploadResult {
        filename: file.filename,
        size: file.size,
        record_count: file.record_count,
    })
}

/// Run the uploader task that handles concurrent file uploads and Delta commits.
pub(super) async fn run_uploader(
    mut upload_rx: mpsc::Receiver<FinishedFile>,
    mut delta_sink: DeltaSink,
    sink_storage: StorageProviderRef,
    shutdown: CancellationToken,
    config: UploaderConfig,
) -> (DeltaSink, usize, usize) {
    let mut uploads: FuturesUnordered<UploadFuture> = FuturesUnordered::new();

    let mut active_uploads = 0;
    let max_concurrent_uploads = config.max_concurrent_uploads;
    let mut files_uploaded = 0usize;
    let mut bytes_uploaded = 0usize;
    let mut files_to_commit: Vec<FinishedFile> = Vec::new();
    let mut channel_open = true;
    let mut util_timer = UtilizationTimer::new("uploader");

    const COMMIT_BATCH_SIZE: usize = 10;

    // Helper to commit accumulated files
    async fn commit_files(
        delta_sink: &mut DeltaSink,
        files_to_commit: &mut Vec<FinishedFile>,
    ) -> usize {
        if files_to_commit.is_empty() {
            return 0;
        }

        let commit_files: Vec<FinishedFile> = files_to_commit
            .drain(..)
            .map(|f| FinishedFile {
                filename: f.filename,
                size: f.size,
                record_count: f.record_count,
                bytes: None, // Clear bytes for commit
            })
            .collect();

        let count = commit_files.len();
        match delta_sink.commit_files(&commit_files).await {
            Ok(Some(version)) => {
                info!(
                    "Committed {} files to Delta Lake, version {}",
                    count, version
                );
            }
            Ok(None) => {
                debug!("No commit needed (duplicate files)");
            }
            Err(e) => {
                error!("Failed to commit {} files to Delta: {}", count, e);
            }
        }
        count
    }

    loop {
        // Check if we're done: channel closed, no pending uploads, no files to commit
        if !channel_open && uploads.is_empty() {
            break;
        }

        // Update utilization state: waiting if no active uploads
        if active_uploads == 0 {
            util_timer.start_wait();
        }
        util_timer.maybe_update();

        tokio::select! {
            biased;

            _ = shutdown.cancelled() => {
                info!("[upload] Shutdown requested, stopping uploads");
                break;
            }

            // Handle completed uploads
            Some(result) = uploads.next(), if !uploads.is_empty() => {
                active_uploads -= 1;
                emit!(ActiveUploads {
                    count: active_uploads
                });
                match result {
                    Ok(upload_result) => {
                        debug!(
                            "[upload] Completed {} (active: {})",
                            upload_result.filename, active_uploads
                        );
                        files_uploaded += 1;
                        bytes_uploaded += upload_result.size;
                        files_to_commit.push(FinishedFile {
                            filename: upload_result.filename,
                            size: upload_result.size,
                            record_count: upload_result.record_count,
                            bytes: None,
                        });

                        // Batch commit every N files
                        if files_to_commit.len() >= COMMIT_BATCH_SIZE {
                            commit_files(&mut delta_sink, &mut files_to_commit).await;
                        }
                    }
                    Err(e) => {
                        error!("[upload] Upload failed: {}", e);
                    }
                }
            }

            // Accept new files if under concurrency limit
            result = upload_rx.recv(), if active_uploads < max_concurrent_uploads && channel_open => {
                match result {
                    Some(file) => {
                        // Transition to working state when we have uploads
                        if active_uploads == 0 {
                            util_timer.stop_wait();
                        }
                        active_uploads += 1;
                        emit!(ActiveUploads {
                            count: active_uploads
                        });
                        info!(
                            "[upload] Starting {} ({} bytes, {} records, active: {})",
                            file.filename, file.size, file.record_count, active_uploads
                        );
                        uploads.push(Box::pin(upload_file(
                            sink_storage.clone(),
                            file,
                            config.part_size,
                            config.min_multipart_size,
                            config.max_concurrent_parts,
                        )));
                    }
                    None => {
                        // Channel closed, but continue draining uploads
                        channel_open = false;
                        debug!("[upload] Channel closed, draining {} pending uploads", uploads.len());
                    }
                }
            }
        }
    }

    // Drain any remaining pending uploads
    while let Some(result) = uploads.next().await {
        if let Ok(upload_result) = result {
            files_uploaded += 1;
            bytes_uploaded += upload_result.size;
            files_to_commit.push(FinishedFile {
                filename: upload_result.filename,
                size: upload_result.size,
                record_count: upload_result.record_count,
                bytes: None,
            });
        }
    }

    // Final commit
    commit_files(&mut delta_sink, &mut files_to_commit).await;

    // Reset gauge to 0 on completion
    emit!(ActiveUploads { count: 0 });

    info!(
        "Uploader finished: {} files, {} bytes",
        files_uploaded, bytes_uploaded
    );
    (delta_sink, files_uploaded, bytes_uploaded)
}

/// Run the downloader task that manages concurrent file downloads.
pub(super) async fn run_downloader(
    pending_files: Vec<String>,
    source_state: SourceState,
    storage: StorageProviderRef,
    download_tx: mpsc::Sender<Result<DownloadedFile, StorageError>>,
    shutdown: CancellationToken,
    max_concurrent: usize,
) {
    let mut downloads: FuturesUnordered<DownloadFuture> = FuturesUnordered::new();

    let mut pending_iter = pending_files.into_iter();
    let mut active_downloads = 0;
    let mut util_timer = UtilizationTimer::new("downloader");

    // Start initial downloads
    for file_path in pending_iter.by_ref().take(max_concurrent) {
        let skip = source_state.records_to_skip(&file_path);
        if skip > 0 {
            emit!(RecoveredRecords { count: skip as u64 });
        }
        let storage = storage.clone();
        // First download starts working state
        if active_downloads == 0 {
            util_timer.stop_wait();
        }
        active_downloads += 1;
        emit!(ActiveDownloads {
            count: active_downloads
        });
        debug!(
            "[download] Starting {} (active: {})",
            file_path, active_downloads
        );
        downloads.push(Box::pin(download_file(storage, file_path, skip)));
    }

    // Process downloads and start new ones as they complete
    while let Some(result) = downloads.next().await {
        util_timer.maybe_update();

        if shutdown.is_cancelled() {
            debug!("[download] Shutdown requested, stopping downloads");
            break;
        }

        active_downloads -= 1;
        emit!(ActiveDownloads {
            count: active_downloads
        });

        // Update utilization state: waiting if no active downloads
        if active_downloads == 0 {
            util_timer.start_wait();
        }

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

        if should_continue && download_tx.send(result).await.is_err() {
            debug!("[download] Consumer closed, stopping downloads");
            break;
        }

        // Start next download if available
        if let Some(next_file) = pending_iter.next() {
            let skip = source_state.records_to_skip(&next_file);
            if skip > 0 {
                emit!(RecoveredRecords { count: skip as u64 });
            }
            let storage = storage.clone();
            // Transition to working state when we have downloads
            if active_downloads == 0 {
                util_timer.stop_wait();
            }
            active_downloads += 1;
            emit!(ActiveDownloads {
                count: active_downloads
            });
            debug!(
                "[download] Starting {} (active: {})",
                next_file, active_downloads
            );
            downloads.push(Box::pin(download_file(storage, next_file, skip)));
        }
    }

    // Reset gauge to 0 on completion
    emit!(ActiveDownloads { count: 0 });
    debug!("[download] All downloads complete");
}

/// Download a file's compressed data asynchronously.
async fn download_file(
    storage: StorageProviderRef,
    path: String,
    skip_records: usize,
) -> Result<DownloadedFile, StorageError> {
    let start = Instant::now();
    let compressed_data = storage.get(path.as_str()).await?;
    emit!(FileDownloadCompleted {
        duration: start.elapsed()
    });
    Ok(DownloadedFile {
        path,
        compressed_data,
        skip_records,
    })
}

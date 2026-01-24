//! Multipart upload manager.
//!
//! Handles multipart uploads for large Parquet files, with checkpoint
//! support for recovery. Based on Arroyo's multipart upload pattern.

use anyhow::Result;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{MultipartUpload, PutPayloadMut, PutResult};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::storage::StorageProviderRef;

/// State of a part in a multipart upload.
#[derive(Debug, Clone)]
pub enum PartState {
    /// Part is queued for upload.
    Pending { data: Bytes },
    /// Part has been successfully uploaded.
    Completed,
}

/// A part that needs to be uploaded.
#[derive(Debug, Clone)]
pub struct PartToUpload {
    pub part_index: usize,
    pub data: Bytes,
}

/// Checkpoint data for a multipart upload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartCheckpoint {
    /// The filename being uploaded.
    pub filename: String,
    /// Number of completed parts.
    pub completed_parts_count: usize,
    /// Parts that were pending (need to be re-uploaded on recovery).
    pub pending_parts: Vec<PendingPartCheckpoint>,
    /// Total bytes pushed so far.
    pub pushed_size: usize,
    /// Whether the upload has been marked as closed.
    pub closed: bool,
}

/// Checkpoint data for a pending part.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingPartCheckpoint {
    pub part_index: usize,
    pub data: Vec<u8>,
}

/// Manager for multipart uploads.
///
/// Handles the lifecycle of a multipart upload including:
/// - Queuing and uploading parts
/// - Tracking part completion
/// - Checkpointing for recovery
/// - Finalizing the upload
pub struct MultipartManager {
    storage: StorageProviderRef,
    location: Path,
    upload: Option<Box<dyn MultipartUpload>>,
    parts: Vec<PartState>,
    completed_parts: usize,
    pushed_size: usize,
    target_part_size: usize,
    min_multipart_size: usize,
    current_buffer: PutPayloadMut,
    closed: bool,
}

impl MultipartManager {
    /// Create a new multipart manager.
    ///
    /// # Arguments
    /// * `storage` - Storage provider for uploads
    /// * `location` - Path where the file will be stored
    /// * `target_part_size` - Target size for each part in bytes
    /// * `min_multipart_size` - Minimum total size before using multipart
    pub fn new(
        storage: StorageProviderRef,
        location: Path,
        target_part_size: usize,
        min_multipart_size: usize,
    ) -> Self {
        Self {
            storage,
            location,
            upload: None,
            parts: Vec::new(),
            completed_parts: 0,
            pushed_size: 0,
            target_part_size,
            min_multipart_size,
            current_buffer: PutPayloadMut::new(),
            closed: false,
        }
    }

    /// Get the filename for this upload.
    pub fn filename(&self) -> &str {
        self.location.as_ref()
    }

    /// Get the total bytes pushed so far.
    pub fn pushed_size(&self) -> usize {
        self.pushed_size
    }

    /// Check if the upload is closed.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Check if a multipart upload has been initiated.
    pub fn is_multipart_started(&self) -> bool {
        self.upload.is_some()
    }

    /// Write data to the upload.
    ///
    /// Data is buffered until it reaches the target part size, at which point
    /// it's queued as a part to upload.
    pub fn write(&mut self, data: Bytes) {
        self.current_buffer.push(data.clone());
        self.pushed_size += data.len();

        // If we've accumulated enough data, create a part
        if self.current_buffer.content_length() >= self.target_part_size {
            self.flush_buffer_to_part();
        }
    }

    /// Flush the current buffer to a part.
    fn flush_buffer_to_part(&mut self) {
        if self.current_buffer.content_length() == 0 {
            return;
        }

        let data = std::mem::replace(&mut self.current_buffer, PutPayloadMut::new());
        let bytes = data.freeze();

        // Collect all bytes from the PutPayload into a single Bytes
        let mut collected = Vec::new();
        for chunk in bytes {
            collected.extend_from_slice(&chunk);
        }

        self.parts.push(PartState::Pending {
            data: Bytes::from(collected),
        });
    }

    /// Get parts that are ready to be uploaded.
    pub fn get_pending_parts(&self) -> Vec<PartToUpload> {
        self.parts
            .iter()
            .enumerate()
            .filter_map(|(idx, state)| {
                if let PartState::Pending { data } = state {
                    Some(PartToUpload {
                        part_index: idx,
                        data: data.clone(),
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    /// Handle completion of a part upload.
    pub fn handle_completed_part(&mut self, part_index: usize) {
        if part_index < self.parts.len() {
            self.parts[part_index] = PartState::Completed;
            self.completed_parts += 1;
            debug!(
                "Part {} completed, total completed: {}",
                part_index, self.completed_parts
            );
        }
    }

    /// Mark the upload as closed.
    ///
    /// This indicates no more data will be written. Call `finish` to complete
    /// the upload after all parts have been uploaded.
    pub fn close(&mut self) {
        self.closed = true;
        // Flush any remaining buffered data as the final part
        self.flush_buffer_to_part();
    }

    /// Check if the upload is ready to be finalized.
    ///
    /// Returns true if closed and all parts have been uploaded.
    pub fn is_ready_to_finish(&self) -> bool {
        if !self.closed {
            return false;
        }
        self.parts
            .iter()
            .all(|p| matches!(p, PartState::Completed))
    }

    /// Get the number of completed parts.
    pub fn completed_parts_count(&self) -> usize {
        self.completed_parts
    }

    /// Get the total number of parts.
    pub fn total_parts(&self) -> usize {
        self.parts.len()
    }

    /// Initialize the multipart upload with the storage provider.
    pub async fn init_multipart(&mut self) -> Result<()> {
        if self.upload.is_some() {
            return Ok(());
        }

        let upload = self.storage.start_multipart(&self.location).await?;
        self.upload = Some(upload);
        info!("Initialized multipart upload for {}", self.location);
        Ok(())
    }

    /// Upload a part to storage.
    pub async fn upload_part(&mut self, part_index: usize) -> Result<()> {
        let data = match &self.parts[part_index] {
            PartState::Pending { data } => data.clone(),
            PartState::Completed => return Ok(()),
        };

        let upload = self
            .upload
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Multipart upload not initialized"))?;

        // put_part returns an UploadPart future that we await
        upload.put_part(data.into()).await?;

        // Mark the part as completed
        self.parts[part_index] = PartState::Completed;
        self.completed_parts += 1;

        debug!(
            "Uploaded part {}, total completed: {}",
            part_index, self.completed_parts
        );
        Ok(())
    }

    /// Upload all pending parts.
    pub async fn upload_all_pending_parts(&mut self) -> Result<()> {
        let pending_indices: Vec<_> = self
            .parts
            .iter()
            .enumerate()
            .filter_map(|(idx, state)| {
                if matches!(state, PartState::Pending { .. }) {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect();

        for idx in pending_indices {
            self.upload_part(idx).await?;
        }

        Ok(())
    }

    /// Finish the multipart upload.
    ///
    /// This completes the multipart upload and makes the file available.
    pub async fn finish(mut self) -> Result<PutResult> {
        let mut upload = self
            .upload
            .take()
            .ok_or_else(|| anyhow::anyhow!("Multipart upload not initialized"))?;

        let result = upload.complete().await?;
        info!(
            "Completed multipart upload for {}, {} parts, {} bytes",
            self.location,
            self.parts.len(),
            self.pushed_size
        );
        Ok(result)
    }

    /// Abort the multipart upload.
    ///
    /// This cancels the upload and cleans up any uploaded parts.
    pub async fn abort(mut self) -> Result<()> {
        if let Some(mut upload) = self.upload.take() {
            upload.abort().await?;
            warn!("Aborted multipart upload for {}", self.location);
        }
        Ok(())
    }

    /// Get checkpoint data for recovery.
    pub fn get_checkpoint_data(&self) -> MultipartCheckpoint {
        let pending_parts: Vec<_> = self
            .parts
            .iter()
            .enumerate()
            .filter_map(|(idx, state)| {
                if let PartState::Pending { data } = state {
                    Some(PendingPartCheckpoint {
                        part_index: idx,
                        data: data.to_vec(),
                    })
                } else {
                    None
                }
            })
            .collect();

        MultipartCheckpoint {
            filename: self.location.to_string(),
            completed_parts_count: self.completed_parts,
            pending_parts,
            pushed_size: self.pushed_size,
            closed: self.closed,
        }
    }

    /// Check if we should use multipart upload based on current size.
    ///
    /// Returns true if the pushed size exceeds the minimum multipart threshold.
    pub fn should_use_multipart(&self) -> bool {
        self.pushed_size >= self.min_multipart_size
    }

    /// Perform a simple single-part upload for small files.
    ///
    /// This should only be used when the total file size is below
    /// the multipart threshold.
    pub async fn finish_single_put(mut self) -> Result<()> {
        // Flush any buffered data
        self.flush_buffer_to_part();

        // Collect all data
        let mut all_data = Vec::new();
        for part in &self.parts {
            if let PartState::Pending { data } = part {
                all_data.extend_from_slice(data);
            }
        }

        // Upload as single PUT
        let bytes = Bytes::from(all_data);
        self.storage.put_bytes(&self.location, bytes).await?;
        info!(
            "Completed single PUT upload for {}, {} bytes",
            self.location, self.pushed_size
        );
        Ok(())
    }
}

/// Builder for creating MultipartManager instances.
pub struct MultipartManagerBuilder {
    storage: StorageProviderRef,
    target_part_size: usize,
    min_multipart_size: usize,
}

impl MultipartManagerBuilder {
    /// Create a new builder with default settings.
    pub fn new(storage: StorageProviderRef) -> Self {
        Self {
            storage,
            target_part_size: 32 * 1024 * 1024,  // 32MB default
            min_multipart_size: 5 * 1024 * 1024, // 5MB default
        }
    }

    /// Set the target part size in bytes.
    pub fn with_part_size(mut self, size: usize) -> Self {
        self.target_part_size = size;
        self
    }

    /// Set the minimum file size for multipart upload in bytes.
    pub fn with_min_multipart_size(mut self, size: usize) -> Self {
        self.min_multipart_size = size;
        self
    }

    /// Build a MultipartManager for the given path.
    pub fn build(self, location: Path) -> MultipartManager {
        MultipartManager::new(
            self.storage,
            location,
            self.target_part_size,
            self.min_multipart_size,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multipart_checkpoint_serialization() {
        let checkpoint = MultipartCheckpoint {
            filename: "test.parquet".to_string(),
            completed_parts_count: 2,
            pending_parts: vec![PendingPartCheckpoint {
                part_index: 2,
                data: vec![1, 2, 3, 4],
            }],
            pushed_size: 1024,
            closed: false,
        };

        let json = serde_json::to_string(&checkpoint).unwrap();
        let restored: MultipartCheckpoint = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.filename, "test.parquet");
        assert_eq!(restored.completed_parts_count, 2);
        assert_eq!(restored.pending_parts.len(), 1);
        assert_eq!(restored.pushed_size, 1024);
        assert!(!restored.closed);
    }
}

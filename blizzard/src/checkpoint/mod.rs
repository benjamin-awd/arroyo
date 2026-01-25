//! Checkpoint coordination for exactly-once processing.
//!
//! Manages periodic checkpoints that capture source progress and
//! pending sink files to enable recovery.

pub mod state;

pub use state::{CheckpointState, PendingFile};

use snafu::prelude::*;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::emit;
use crate::error::{
    CheckpointError, CheckpointStorageSnafu, JsonSnafu, MissingCheckpointPathSnafu,
};
use crate::internal_events::{
    CheckpointAge, CheckpointSaveCompleted, CheckpointSaved, PendingFilesCount,
};
use crate::source::SourceState;
use crate::storage::StorageProviderRef;

/// Checkpoint manager that coordinates periodic checkpoints.
pub struct CheckpointManager {
    storage: StorageProviderRef,
    checkpoint_interval: Duration,
    last_checkpoint: Instant,
    checkpoint_id: u64,
}

impl CheckpointManager {
    /// Create a new checkpoint manager.
    pub fn new(storage: StorageProviderRef, interval_seconds: u64) -> Self {
        Self {
            storage,
            checkpoint_interval: Duration::from_secs(interval_seconds),
            last_checkpoint: Instant::now(),
            checkpoint_id: 0,
        }
    }

    /// Check if a checkpoint should be triggered.
    pub fn should_checkpoint(&self) -> bool {
        let age = self.last_checkpoint.elapsed().as_secs_f64();
        emit!(CheckpointAge { seconds: age });
        age >= self.checkpoint_interval.as_secs_f64()
    }

    /// Save a checkpoint.
    pub async fn save_checkpoint(
        &mut self,
        state: &CheckpointState,
    ) -> Result<(), CheckpointError> {
        let start = Instant::now();
        self.checkpoint_id += 1;

        let checkpoint_path = format!("checkpoint-{:010}.json", self.checkpoint_id);

        // Serialize the checkpoint state
        let checkpoint_json = serde_json::to_string_pretty(state).context(JsonSnafu)?;

        // Write to storage
        self.storage
            .put(checkpoint_path.clone(), checkpoint_json.into_bytes())
            .await
            .context(CheckpointStorageSnafu)?;

        // Also write a "latest" pointer
        let latest_content = serde_json::json!({
            "checkpoint_id": self.checkpoint_id,
            "checkpoint_path": checkpoint_path,
        });
        self.storage
            .put(
                "latest.json",
                serde_json::to_vec(&latest_content).context(JsonSnafu)?,
            )
            .await
            .context(CheckpointStorageSnafu)?;

        self.last_checkpoint = Instant::now();

        emit!(CheckpointSaveCompleted {
            duration: start.elapsed()
        });
        emit!(CheckpointSaved);
        emit!(CheckpointAge { seconds: 0.0 });

        info!(
            "Saved checkpoint {} with {} files tracked",
            self.checkpoint_id,
            state.source_state.files.len()
        );

        Ok(())
    }

    /// Load the latest checkpoint if available.
    /// Returns the checkpoint state and the checkpoint ID to restore.
    pub async fn load_latest_checkpoint(
        &mut self,
    ) -> Result<Option<CheckpointState>, CheckpointError> {
        // Check if checkpoint exists before attempting to read
        if !self
            .storage
            .exists("latest.json")
            .await
            .context(CheckpointStorageSnafu)?
        {
            debug!("No checkpoint found");
            return Ok(None);
        }

        // Try to read the latest pointer
        let latest_bytes = match self
            .storage
            .get_if_present("latest.json")
            .await
            .context(CheckpointStorageSnafu)?
        {
            Some(bytes) => bytes,
            None => {
                debug!("No checkpoint found");
                return Ok(None);
            }
        };

        let latest: serde_json::Value = serde_json::from_slice(&latest_bytes).context(JsonSnafu)?;
        let checkpoint_path = latest["checkpoint_path"]
            .as_str()
            .ok_or_else(|| MissingCheckpointPathSnafu.build())?;

        // Restore the checkpoint ID from the latest.json so new checkpoints continue from the correct sequence
        let checkpoint_id = latest["checkpoint_id"].as_u64().unwrap_or(0);
        self.set_checkpoint_id(checkpoint_id);

        // Load the checkpoint
        let checkpoint_bytes = self
            .storage
            .get(checkpoint_path)
            .await
            .context(CheckpointStorageSnafu)?;
        let checkpoint: CheckpointState =
            serde_json::from_slice(&checkpoint_bytes).context(JsonSnafu)?;

        info!(
            "Loaded checkpoint {} from {}, version {}",
            self.checkpoint_id(),
            checkpoint_path,
            checkpoint.delta_version
        );

        Ok(Some(checkpoint))
    }

    /// Get the current checkpoint ID.
    pub fn checkpoint_id(&self) -> u64 {
        self.checkpoint_id
    }

    /// Set the checkpoint ID (for recovery).
    pub fn set_checkpoint_id(&mut self, id: u64) {
        self.checkpoint_id = id;
    }
}

/// A coordinator that manages checkpointing with atomic semantics.
pub struct CheckpointCoordinator {
    manager: Arc<Mutex<CheckpointManager>>,
    source_state: Arc<Mutex<SourceState>>,
    pending_files: Arc<Mutex<Vec<PendingFile>>>,
    delta_version: Arc<Mutex<i64>>,
}

impl CheckpointCoordinator {
    /// Create a new checkpoint coordinator.
    pub fn new(storage: StorageProviderRef, interval_seconds: u64) -> Self {
        Self {
            manager: Arc::new(Mutex::new(CheckpointManager::new(
                storage,
                interval_seconds,
            ))),
            source_state: Arc::new(Mutex::new(SourceState::new())),
            pending_files: Arc::new(Mutex::new(Vec::new())),
            delta_version: Arc::new(Mutex::new(-1)),
        }
    }

    /// Load the latest checkpoint and restore state.
    pub async fn restore(&self) -> Result<Option<CheckpointState>, CheckpointError> {
        let mut manager = self.manager.lock().await;
        if let Some(checkpoint) = manager.load_latest_checkpoint().await? {
            drop(manager);

            // Restore state
            *self.source_state.lock().await = checkpoint.source_state.clone();
            *self.pending_files.lock().await = checkpoint.pending_files.clone();
            *self.delta_version.lock().await = checkpoint.delta_version;

            Ok(Some(checkpoint))
        } else {
            Ok(None)
        }
    }

    /// Update the source state for a file.
    pub async fn update_source_state(&self, path: &str, records_read: usize, finished: bool) {
        let mut state = self.source_state.lock().await;
        if finished {
            state.mark_finished(path);
        } else {
            state.update_records(path, records_read);
        }
    }

    /// Clear pending files (after successful commit).
    pub async fn clear_pending_files(&self) {
        let mut files = self.pending_files.lock().await;
        files.clear();
        emit!(PendingFilesCount { count: 0 });
    }

    /// Update the Delta version.
    pub async fn update_delta_version(&self, version: i64) {
        *self.delta_version.lock().await = version;
    }

    /// Check if a checkpoint should be triggered.
    pub async fn should_checkpoint(&self) -> bool {
        let manager = self.manager.lock().await;
        manager.should_checkpoint()
    }

    /// Trigger a checkpoint.
    pub async fn checkpoint(&self) -> Result<(), CheckpointError> {
        let pending_files = self.pending_files.lock().await.clone();
        emit!(PendingFilesCount {
            count: pending_files.len()
        });

        let state = CheckpointState {
            source_state: self.source_state.lock().await.clone(),
            pending_files,
            in_progress_writes: Vec::new(),
            delta_version: *self.delta_version.lock().await,
        };

        let mut manager = self.manager.lock().await;
        manager.save_checkpoint(&state).await?;

        Ok(())
    }

    /// Get the current source state.
    pub async fn get_source_state(&self) -> SourceState {
        self.source_state.lock().await.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_state_serialization() {
        let mut source_state = SourceState::new();
        source_state.update_records("file1.ndjson.gz", 100);
        source_state.mark_finished("file2.ndjson.gz");

        let state = CheckpointState {
            source_state,
            pending_files: vec![PendingFile {
                filename: "pending.parquet".to_string(),
                record_count: 50,
            }],
            in_progress_writes: Vec::new(),
            delta_version: 5,
        };

        let json = serde_json::to_string(&state).unwrap();
        let restored: CheckpointState = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.delta_version, 5);
        assert_eq!(restored.pending_files.len(), 1);
        assert!(restored.source_state.is_file_finished("file2.ndjson.gz"));
    }
}

//! Checkpoint state serialization.
//!
//! Defines the checkpoint state structure that captures
//! all information needed for recovery.

use serde::{Deserialize, Serialize};

use crate::source::SourceState;

/// A file that has been written but not yet committed to Delta.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingFile {
    /// The filename in storage.
    pub filename: String,
    /// Number of records in the file.
    pub record_count: usize,
}

/// State of a file write operation for checkpoint recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FileWriteState {
    /// Simple single-PUT upload (for small files).
    SinglePut {
        filename: String,
        record_count: usize,
    },
    /// Multipart upload not yet initialized.
    MultipartPending {
        filename: String,
        parts_data: Vec<Vec<u8>>,
        record_count: usize,
    },
    /// Multipart upload in progress.
    MultipartInFlight {
        filename: String,
        completed_parts: Vec<CompletedPart>,
        in_flight_parts: Vec<InFlightPart>,
        record_count: usize,
    },
    /// Multipart upload complete, ready for Delta commit.
    MultipartComplete {
        filename: String,
        parts: Vec<String>,
        size: usize,
        record_count: usize,
    },
}

/// A completed part in a multipart upload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletedPart {
    pub part_index: usize,
    pub content_id: String,
}

/// An in-flight part that needs to be re-uploaded on recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InFlightPart {
    pub part_index: usize,
    pub data: Vec<u8>,
}

/// Complete checkpoint state for recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointState {
    /// State of source file processing.
    pub source_state: SourceState,
    /// Files written but not yet committed to Delta.
    pub pending_files: Vec<PendingFile>,
    /// In-progress file writes (for multipart upload recovery).
    #[serde(default)]
    pub in_progress_writes: Vec<FileWriteState>,
    /// Last committed Delta version.
    pub delta_version: i64,
}

impl Default for CheckpointState {
    fn default() -> Self {
        Self {
            source_state: SourceState::new(),
            pending_files: Vec::new(),
            in_progress_writes: Vec::new(),
            delta_version: -1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_state_default() {
        let state = CheckpointState::default();
        assert_eq!(state.delta_version, -1);
        assert!(state.pending_files.is_empty());
        assert!(state.source_state.files.is_empty());
    }
}

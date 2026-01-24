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

/// Complete checkpoint state for recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointState {
    /// State of source file processing.
    pub source_state: SourceState,
    /// Files written but not yet committed to Delta.
    pub pending_files: Vec<PendingFile>,
    /// Last committed Delta version.
    pub delta_version: i64,
}

impl Default for CheckpointState {
    fn default() -> Self {
        Self {
            source_state: SourceState::new(),
            pending_files: Vec::new(),
            delta_version: -1,
        }
    }
}

impl CheckpointState {
    /// Create a new empty checkpoint state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a checkpoint state from components.
    pub fn from_parts(
        source_state: SourceState,
        pending_files: Vec<PendingFile>,
        delta_version: i64,
    ) -> Self {
        Self {
            source_state,
            pending_files,
            delta_version,
        }
    }

    /// Check if there are pending files.
    pub fn has_pending_files(&self) -> bool {
        !self.pending_files.is_empty()
    }

    /// Get the total number of pending records.
    pub fn pending_record_count(&self) -> usize {
        self.pending_files.iter().map(|f| f.record_count).sum()
    }

    /// Serialize to JSON.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialize from JSON.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Serialize to bytes (JSON).
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserialize from bytes (JSON).
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::FileReadState;

    #[test]
    fn test_checkpoint_state_default() {
        let state = CheckpointState::default();
        assert_eq!(state.delta_version, -1);
        assert!(state.pending_files.is_empty());
        assert!(state.source_state.files.is_empty());
    }

    #[test]
    fn test_checkpoint_state_json_roundtrip() {
        let mut source_state = SourceState::new();
        source_state.update_file("file1.ndjson.gz", FileReadState::RecordsRead(100));
        source_state.mark_finished("file2.ndjson.gz");

        let state = CheckpointState::from_parts(
            source_state,
            vec![
                PendingFile {
                    filename: "pending1.parquet".to_string(),
                    record_count: 50,
                },
                PendingFile {
                    filename: "pending2.parquet".to_string(),
                    record_count: 75,
                },
            ],
            10,
        );

        let json = state.to_json().unwrap();
        let restored = CheckpointState::from_json(&json).unwrap();

        assert_eq!(restored.delta_version, 10);
        assert_eq!(restored.pending_files.len(), 2);
        assert_eq!(restored.pending_record_count(), 125);
        assert!(restored.source_state.is_file_finished("file2.ndjson.gz"));
        assert_eq!(
            restored.source_state.records_to_skip("file1.ndjson.gz"),
            100
        );
    }

    #[test]
    fn test_checkpoint_state_bytes_roundtrip() {
        let state = CheckpointState::from_parts(
            SourceState::new(),
            vec![PendingFile {
                filename: "test.parquet".to_string(),
                record_count: 100,
            }],
            5,
        );

        let bytes = state.to_bytes().unwrap();
        let restored = CheckpointState::from_bytes(&bytes).unwrap();

        assert_eq!(restored.delta_version, 5);
        assert_eq!(restored.pending_files.len(), 1);
    }
}

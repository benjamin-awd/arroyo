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

impl FileWriteState {
    /// Get the filename for this write state.
    pub fn filename(&self) -> &str {
        match self {
            FileWriteState::SinglePut { filename, .. } => filename,
            FileWriteState::MultipartPending { filename, .. } => filename,
            FileWriteState::MultipartInFlight { filename, .. } => filename,
            FileWriteState::MultipartComplete { filename, .. } => filename,
        }
    }

    /// Get the record count for this write state.
    pub fn record_count(&self) -> usize {
        match self {
            FileWriteState::SinglePut { record_count, .. } => *record_count,
            FileWriteState::MultipartPending { record_count, .. } => *record_count,
            FileWriteState::MultipartInFlight { record_count, .. } => *record_count,
            FileWriteState::MultipartComplete { record_count, .. } => *record_count,
        }
    }

    /// Check if the write is complete and ready for commit.
    pub fn is_complete(&self) -> bool {
        matches!(self, FileWriteState::SinglePut { .. } | FileWriteState::MultipartComplete { .. })
    }
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
            in_progress_writes: Vec::new(),
            delta_version,
        }
    }

    /// Create a checkpoint state with in-progress writes.
    pub fn from_parts_with_writes(
        source_state: SourceState,
        pending_files: Vec<PendingFile>,
        in_progress_writes: Vec<FileWriteState>,
        delta_version: i64,
    ) -> Self {
        Self {
            source_state,
            pending_files,
            in_progress_writes,
            delta_version,
        }
    }

    /// Check if there are in-progress writes.
    pub fn has_in_progress_writes(&self) -> bool {
        !self.in_progress_writes.is_empty()
    }

    /// Add an in-progress write.
    pub fn add_in_progress_write(&mut self, write: FileWriteState) {
        self.in_progress_writes.push(write);
    }

    /// Clear in-progress writes.
    pub fn clear_in_progress_writes(&mut self) {
        self.in_progress_writes.clear();
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

    #[test]
    fn test_file_write_state_serialization() {
        let state = FileWriteState::MultipartInFlight {
            filename: "test.parquet".to_string(),
            completed_parts: vec![
                CompletedPart {
                    part_index: 0,
                    content_id: "etag1".to_string(),
                },
            ],
            in_flight_parts: vec![
                InFlightPart {
                    part_index: 1,
                    data: vec![1, 2, 3, 4],
                },
            ],
            record_count: 1000,
        };

        let json = serde_json::to_string(&state).unwrap();
        let restored: FileWriteState = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.filename(), "test.parquet");
        assert_eq!(restored.record_count(), 1000);
        assert!(!restored.is_complete());
    }

    #[test]
    fn test_checkpoint_with_in_progress_writes() {
        let state = CheckpointState::from_parts_with_writes(
            SourceState::new(),
            vec![],
            vec![
                FileWriteState::MultipartInFlight {
                    filename: "upload.parquet".to_string(),
                    completed_parts: vec![],
                    in_flight_parts: vec![],
                    record_count: 500,
                },
            ],
            3,
        );

        let json = state.to_json().unwrap();
        let restored = CheckpointState::from_json(&json).unwrap();

        assert!(restored.has_in_progress_writes());
        assert_eq!(restored.in_progress_writes.len(), 1);
        assert_eq!(restored.in_progress_writes[0].filename(), "upload.parquet");
    }
}

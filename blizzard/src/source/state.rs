//! File progress tracking for checkpoint/recovery.
//!
//! Tracks which files have been processed and how many records
//! have been read from each file to enable exactly-once processing.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// State of reading a single file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FileReadState {
    /// File has been completely processed.
    Finished,
    /// File is partially processed with this many records read.
    RecordsRead(usize),
}

/// Aggregate state for all source files.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceState {
    /// Map of file path to read state.
    pub files: HashMap<String, FileReadState>,
}

impl SourceState {
    /// Create a new empty source state.
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
        }
    }

    /// Mark a file as finished.
    pub fn mark_finished(&mut self, path: &str) {
        self.files.insert(path.to_string(), FileReadState::Finished);
    }

    /// Update the record count for a file.
    pub fn update_records(&mut self, path: &str, records_read: usize) {
        self.files
            .insert(path.to_string(), FileReadState::RecordsRead(records_read));
    }

    /// Check if a file has been completely processed.
    pub fn is_file_finished(&self, path: &str) -> bool {
        matches!(self.files.get(path), Some(FileReadState::Finished))
    }

    /// Get the number of records to skip for a file.
    pub fn records_to_skip(&self, path: &str) -> usize {
        match self.files.get(path) {
            Some(FileReadState::RecordsRead(n)) => *n,
            _ => 0,
        }
    }

    /// Get all files that need processing (not finished).
    pub fn pending_files<'a>(&'a self, all_files: &'a [String]) -> Vec<&'a str> {
        all_files
            .iter()
            .filter(|f| !self.is_file_finished(f))
            .map(|s| s.as_str())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_state() {
        let mut state = SourceState::new();

        state.update_records("file1.ndjson.gz", 100);
        state.mark_finished("file2.ndjson.gz");

        assert!(!state.is_file_finished("file1.ndjson.gz"));
        assert!(state.is_file_finished("file2.ndjson.gz"));
        assert_eq!(state.records_to_skip("file1.ndjson.gz"), 100);
        assert_eq!(state.records_to_skip("file2.ndjson.gz"), 0);
    }

    #[test]
    fn test_pending_files() {
        let mut state = SourceState::new();

        state.mark_finished("file1.ndjson.gz");
        state.update_records("file2.ndjson.gz", 50);

        let all_files = vec![
            "file1.ndjson.gz".to_string(),
            "file2.ndjson.gz".to_string(),
            "file3.ndjson.gz".to_string(),
        ];

        let pending = state.pending_files(&all_files);
        assert_eq!(pending.len(), 2);
        assert!(pending.contains(&"file2.ndjson.gz"));
        assert!(pending.contains(&"file3.ndjson.gz"));
    }
}

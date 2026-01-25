//! Integration tests for blizzard

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

mod config_tests {
    use super::*;

    #[test]
    fn test_config_yaml_parsing() {
        let yaml = r#"
source:
  path: "s3://bucket/input/*.ndjson.gz"
  compression: gzip
  batch_size: 4096

sink:
  path: "s3://bucket/output/table"
  file_size_mb: 64
  compression: zstd

checkpoint:
  path: "s3://bucket/checkpoints/"
  interval_seconds: 60

schema:
  fields:
    - name: id
      type: string
    - name: timestamp
      type: timestamp
    - name: value
      type: float64
      nullable: true
    - name: count
      type: int64
"#;
        let config: blizzard::config::Config = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(config.source.path, "s3://bucket/input/*.ndjson.gz");
        assert_eq!(config.source.batch_size, 4096);
        assert_eq!(config.sink.file_size_mb, 64);
        assert_eq!(config.checkpoint.interval_seconds, 60);
        assert_eq!(config.schema.fields.len(), 4);

        // Test schema conversion
        let arrow_schema = config.to_arrow_schema();
        assert_eq!(arrow_schema.fields().len(), 4);
        assert_eq!(arrow_schema.field(0).name(), "id");
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_config_defaults() {
        let yaml = r#"
source:
  path: "/input/*.ndjson.gz"

sink:
  path: "/output/table"

checkpoint:
  path: "/checkpoints/"

schema:
  fields:
    - name: data
      type: string
"#;
        let config: blizzard::config::Config = serde_yaml::from_str(yaml).unwrap();

        // Check defaults
        assert_eq!(config.source.batch_size, 8192);
        assert_eq!(config.sink.file_size_mb, 128);
        assert_eq!(config.checkpoint.interval_seconds, 30);
    }

    #[test]
    fn test_field_types() {
        use blizzard::config::FieldType;

        let types = vec![
            ("string", FieldType::String, DataType::Utf8),
            ("int32", FieldType::Int32, DataType::Int32),
            ("int64", FieldType::Int64, DataType::Int64),
            ("float32", FieldType::Float32, DataType::Float32),
            ("float64", FieldType::Float64, DataType::Float64),
            ("boolean", FieldType::Boolean, DataType::Boolean),
        ];

        for (name, _field_type, expected_arrow) in types {
            let yaml = format!(
                r#"
source:
  path: "/input"
sink:
  path: "/output"
checkpoint:
  path: "/checkpoint"
schema:
  fields:
    - name: test_field
      type: {}
"#,
                name
            );
            let config: blizzard::config::Config = serde_yaml::from_str(&yaml).unwrap();
            let schema = config.to_arrow_schema();
            assert_eq!(
                schema.field(0).data_type(),
                &expected_arrow,
                "Failed for type: {}",
                name
            );
        }
    }
}

mod storage_tests {
    use blizzard::storage::BackendConfig;

    #[test]
    fn test_s3_url_parsing() {
        let config = BackendConfig::parse_url("s3://mybucket/path/to/data", false).unwrap();
        match config {
            BackendConfig::S3(s3) => {
                assert_eq!(s3.bucket, "mybucket");
            }
            _ => panic!("Expected S3 config"),
        }
    }

    #[test]
    fn test_gcs_url_parsing() {
        let config = BackendConfig::parse_url("gs://mybucket/path/to/data", false).unwrap();
        match config {
            BackendConfig::Gcs(gcs) => {
                assert_eq!(gcs.bucket, "mybucket");
            }
            _ => panic!("Expected GCS config"),
        }
    }

    #[test]
    fn test_local_url_parsing() {
        let config = BackendConfig::parse_url("/local/path/to/data", false).unwrap();
        match config {
            BackendConfig::Local(local) => {
                assert_eq!(local.path, "/local/path/to/data");
            }
            _ => panic!("Expected Local config"),
        }
    }

    #[test]
    fn test_file_url_parsing() {
        let config = BackendConfig::parse_url("file:///local/path/to/data", false).unwrap();
        match config {
            BackendConfig::Local(local) => {
                assert_eq!(local.path, "/local/path/to/data");
            }
            _ => panic!("Expected Local config"),
        }
    }

    #[test]
    fn test_azure_url_parsing() {
        let config = BackendConfig::parse_url(
            "abfss://mycontainer@mystorageaccount.dfs.core.windows.net/path/to/data",
            false,
        )
        .unwrap();
        match config {
            BackendConfig::Azure(azure) => {
                assert_eq!(azure.account, "mystorageaccount");
                assert_eq!(azure.container, "mycontainer");
            }
            _ => panic!("Expected Azure config"),
        }
    }

    #[test]
    fn test_invalid_url() {
        let result = BackendConfig::parse_url("invalid://url", false);
        assert!(result.is_err());
    }
}

mod checkpoint_tests {
    use blizzard::source::SourceState;

    #[test]
    fn test_source_state_tracking() {
        let mut state = SourceState::new();

        // Track file progress
        state.update_records("file1.ndjson.gz", 100);
        assert_eq!(state.records_to_skip("file1.ndjson.gz"), 100);
        assert!(!state.is_file_finished("file1.ndjson.gz"));

        // Update progress
        state.update_records("file1.ndjson.gz", 250);
        assert_eq!(state.records_to_skip("file1.ndjson.gz"), 250);

        // Mark as finished
        state.mark_finished("file1.ndjson.gz");
        assert!(state.is_file_finished("file1.ndjson.gz"));
        assert_eq!(state.records_to_skip("file1.ndjson.gz"), 0); // Finished files skip 0
    }

    #[test]
    fn test_pending_files_filtering() {
        let mut state = SourceState::new();
        state.mark_finished("file1.ndjson.gz");
        state.update_records("file2.ndjson.gz", 50);

        let all_files = vec![
            "file1.ndjson.gz".to_string(),
            "file2.ndjson.gz".to_string(),
            "file3.ndjson.gz".to_string(),
        ];

        let pending = state.pending_files(&all_files);

        // file1 is finished, so only file2 and file3 should be pending
        assert_eq!(pending.len(), 2);
        assert!(pending.contains(&"file2.ndjson.gz"));
        assert!(pending.contains(&"file3.ndjson.gz"));
        assert!(!pending.contains(&"file1.ndjson.gz"));
    }
}

mod parquet_tests {
    use super::*;
    use blizzard::config::ParquetCompression;
    use blizzard::sink::parquet::{ParquetWriter, ParquetWriterConfig};

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    fn create_test_batch(num_rows: usize) -> RecordBatch {
        let ids: Vec<String> = (0..num_rows).map(|i| format!("id_{}", i)).collect();
        let values: Vec<i64> = (0..num_rows).map(|i| i as i64 * 10).collect();

        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_parquet_writer_basic() {
        let schema = test_schema();
        let config = ParquetWriterConfig::default();
        let mut writer = ParquetWriter::new(schema, config);

        let batch = create_test_batch(100);
        writer.write_batch(&batch).unwrap();

        assert!(writer.current_file_size() > 0);
    }

    #[test]
    fn test_parquet_writer_multiple_batches() {
        let schema = test_schema();
        let config = ParquetWriterConfig::default();
        let mut writer = ParquetWriter::new(schema, config);

        for _ in 0..5 {
            let batch = create_test_batch(100);
            writer.write_batch(&batch).unwrap();
        }

        assert!(writer.current_file_size() > 0);
    }

    #[test]
    fn test_parquet_writer_config() {
        let config = ParquetWriterConfig::default()
            .with_file_size_mb(64)
            .with_compression(ParquetCompression::Zstd);

        assert_eq!(config.target_file_size, 64 * 1024 * 1024);
    }
}

mod sink_tests {
    use blizzard::sink::FinishedFile;

    #[test]
    fn test_finished_file() {
        let file = FinishedFile {
            filename: "data-001.parquet".to_string(),
            size: 1024 * 1024,
            record_count: 10000,
            bytes: None,
        };

        assert_eq!(file.filename, "data-001.parquet");
        assert_eq!(file.size, 1024 * 1024);
        assert_eq!(file.record_count, 10000);
    }
}

mod delta_recovery_tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use blizzard::checkpoint::PendingFile;
    use blizzard::sink::delta::DeltaSink;
    use blizzard::storage::StorageProvider;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ])
    }

    #[tokio::test]
    async fn test_recover_pending_files_commits_to_delta() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        // Create storage provider for local path
        let storage = Arc::new(
            StorageProvider::for_url_with_options(table_path, HashMap::new())
                .await
                .unwrap(),
        );

        // Create Delta sink (creates the table)
        let schema = test_schema();
        let mut delta_sink = DeltaSink::new(storage, &schema).await.unwrap();

        // Initial version should be 0 (newly created table)
        let initial_version = delta_sink.version();

        // Simulate pending files from checkpoint recovery
        let pending_files = vec![
            PendingFile {
                filename: "part-00000.parquet".to_string(),
                record_count: 100,
            },
            PendingFile {
                filename: "part-00001.parquet".to_string(),
                record_count: 200,
            },
        ];

        // Recover pending files
        let result = delta_sink.recover_pending_files(&pending_files).await;
        assert!(result.is_ok(), "Recovery should succeed");

        let new_version = result.unwrap();
        assert!(new_version.is_some(), "Should return new version");
        assert!(
            new_version.unwrap() > initial_version,
            "Version should increase after commit"
        );

        // Verify delta_sink's internal version was updated
        assert_eq!(delta_sink.version(), new_version.unwrap());

        // Independently verify files were committed by opening the table directly
        let table_url = url::Url::parse(&format!("file://{}", table_path)).unwrap();
        let table = deltalake::open_table(table_url).await.unwrap();

        let committed_files: Vec<String> = table
            .get_file_uris()
            .unwrap()
            .map(|p| p.to_string())
            .collect();

        assert_eq!(
            committed_files.len(),
            2,
            "Should have exactly 2 committed files"
        );
        assert!(
            committed_files
                .iter()
                .any(|f| f.ends_with("part-00000.parquet")),
            "part-00000.parquet should be in committed files, got: {:?}",
            committed_files
        );
        assert!(
            committed_files
                .iter()
                .any(|f| f.ends_with("part-00001.parquet")),
            "part-00001.parquet should be in committed files, got: {:?}",
            committed_files
        );
    }

    #[tokio::test]
    async fn test_recover_empty_pending_files_returns_none() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        let storage = Arc::new(
            StorageProvider::for_url_with_options(table_path, HashMap::new())
                .await
                .unwrap(),
        );
        let schema = test_schema();
        let mut delta_sink = DeltaSink::new(storage, &schema).await.unwrap();

        let initial_version = delta_sink.version();

        // Recover with empty list
        let result = delta_sink.recover_pending_files(&[]).await;
        assert!(result.is_ok());
        assert!(
            result.unwrap().is_none(),
            "Empty recovery should return None"
        );

        // Version should not change
        assert_eq!(delta_sink.version(), initial_version);
    }
}

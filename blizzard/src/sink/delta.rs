//! Delta Lake commit logic.
//!
//! Handles creating/opening Delta Lake tables and committing
//! Parquet files with exactly-once semantics.

use anyhow::Result;
use arrow::datatypes::Schema;
use bytes::Bytes;
use deltalake::DeltaTable;
use deltalake::kernel::Action;
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::SaveMode;
use object_store::path::Path;
use std::collections::HashSet;
use tracing::{debug, info};
use url::Url;

use super::FinishedFile;
use crate::storage::{BackendConfig, StorageProvider, StorageProviderRef};

/// Delta Lake sink for committing Parquet files.
pub struct DeltaSink {
    table: DeltaTable,
    storage: StorageProviderRef,
    last_version: i64,
}

impl DeltaSink {
    /// Load or create a Delta Lake table.
    pub async fn new(storage: StorageProviderRef, schema: &Schema) -> Result<Self> {
        // Register Delta Lake handlers for cloud storage
        deltalake::aws::register_handlers(None);
        deltalake::gcp::register_handlers(None);

        let table = load_or_create_table(&storage, schema).await?;
        let last_version = table.version().unwrap_or(-1);

        Ok(Self {
            table,
            storage,
            last_version,
        })
    }

    /// Commit a set of finished files to the Delta Lake table.
    ///
    /// Returns the new version number if a commit was made.
    pub async fn commit_files(&mut self, files: &[FinishedFile]) -> Result<Option<i64>> {
        if files.is_empty() {
            return Ok(None);
        }

        let new_version = commit_files_to_delta(files, &mut self.table, self.last_version).await?;

        if let Some(version) = new_version {
            self.last_version = version;
            info!(
                "Committed {} files to Delta Lake, version {}",
                files.len(),
                version
            );
        }

        Ok(new_version)
    }

    /// Write Parquet bytes to storage and commit to Delta.
    pub async fn write_and_commit(
        &mut self,
        filename: &str,
        bytes: Bytes,
        record_count: usize,
    ) -> Result<i64> {
        // Write the file to storage
        let path = Path::from(filename);
        self.storage.put_bytes(&path, bytes.clone()).await?;

        // Commit to Delta
        let file = FinishedFile {
            filename: filename.to_string(),
            size: bytes.len(),
            record_count,
        };

        let version = self
            .commit_files(&[file])
            .await?
            .expect("Should have committed");

        Ok(version)
    }

    /// Get the current table version.
    pub fn version(&self) -> i64 {
        self.last_version
    }

    /// Get the storage provider.
    pub fn storage(&self) -> &StorageProvider {
        &self.storage
    }

    /// Reload the table to get the latest version.
    pub async fn reload(&mut self) -> Result<()> {
        self.table.load().await?;
        self.last_version = self.table.version().unwrap_or(-1);
        Ok(())
    }
}

/// Convert an Arrow schema to a Delta schema.
fn arrow_schema_to_delta(schema: &Schema) -> Result<deltalake::kernel::StructType> {
    use deltalake::kernel::{StructField, StructType};

    let fields: Vec<StructField> = schema
        .fields()
        .iter()
        .map(|field| {
            let delta_type = arrow_type_to_delta(field.data_type())?;
            Ok(StructField::new(
                field.name(),
                delta_type,
                field.is_nullable(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    StructType::try_new(fields).map_err(|e| anyhow::anyhow!("Failed to create struct type: {}", e))
}

/// Convert an Arrow data type to a Delta data type.
fn arrow_type_to_delta(
    arrow_type: &arrow::datatypes::DataType,
) -> Result<deltalake::kernel::DataType> {
    use arrow::datatypes::DataType as ArrowType;
    use deltalake::kernel::DataType as DeltaType;

    let delta_type = match arrow_type {
        ArrowType::Boolean => DeltaType::BOOLEAN,
        ArrowType::Int8 => DeltaType::BYTE,
        ArrowType::Int16 => DeltaType::SHORT,
        ArrowType::Int32 => DeltaType::INTEGER,
        ArrowType::Int64 => DeltaType::LONG,
        ArrowType::Float32 => DeltaType::FLOAT,
        ArrowType::Float64 => DeltaType::DOUBLE,
        ArrowType::Utf8 | ArrowType::LargeUtf8 => DeltaType::STRING,
        ArrowType::Binary | ArrowType::LargeBinary => DeltaType::BINARY,
        ArrowType::Date32 | ArrowType::Date64 => DeltaType::DATE,
        ArrowType::Timestamp(_, _) => DeltaType::TIMESTAMP,
        ArrowType::Decimal128(precision, scale) => DeltaType::decimal(*precision, *scale as u8)?,
        ArrowType::Decimal256(precision, scale) => DeltaType::decimal(*precision, *scale as u8)?,
        other => {
            return Err(anyhow::anyhow!("Unsupported Arrow type: {:?}", other));
        }
    };

    Ok(delta_type)
}

/// Load or create a Delta Lake table with the given schema.
pub async fn load_or_create_table(
    storage_provider: &StorageProvider,
    schema: &Schema,
) -> Result<DeltaTable> {
    let empty_path = &Path::parse("").unwrap();

    let table_url: String = match storage_provider.config() {
        BackendConfig::S3(s3) => {
            format!(
                "s3://{}/{}",
                s3.bucket,
                storage_provider.qualify_path(empty_path)
            )
        }
        BackendConfig::GCS(gcs) => {
            format!(
                "gs://{}/{}",
                gcs.bucket,
                storage_provider.qualify_path(empty_path)
            )
        }
        BackendConfig::Azure(azure) => {
            format!(
                "abfs://{}/{}",
                azure.container,
                storage_provider.qualify_path(empty_path)
            )
        }
        BackendConfig::Local(local) => {
            format!("file://{}", local.path)
        }
    };

    // Try to open existing table
    let parsed_url = Url::parse(&table_url)?;
    match deltalake::open_table_with_storage_options(
        parsed_url.clone(),
        storage_provider.storage_options().clone(),
    )
    .await
    {
        Ok(table) => {
            info!(
                "Loaded existing Delta table at version {}",
                table.version().unwrap_or(-1)
            );
            Ok(table)
        }
        Err(_) => {
            // Table doesn't exist, create it
            info!("Creating new Delta table at {}", table_url);

            // Convert Arrow schema to Delta schema
            let delta_schema = arrow_schema_to_delta(schema)?;

            let table = CreateBuilder::new()
                .with_location(&table_url)
                .with_columns(delta_schema.fields().cloned())
                .with_storage_options(storage_provider.storage_options().clone())
                .await?;

            Ok(table)
        }
    }
}

/// Commit files to a Delta Lake table with duplicate detection.
pub async fn commit_files_to_delta(
    finished_files: &[FinishedFile],
    table: &mut DeltaTable,
    last_version: i64,
) -> Result<Option<i64>> {
    if finished_files.is_empty() {
        return Ok(None);
    }

    // Check for duplicate files by looking at existing files in the table
    let existing_files: HashSet<String> = table
        .get_file_uris()?
        .into_iter()
        .map(|p| p.to_string())
        .collect();

    let new_files: Vec<_> = finished_files
        .iter()
        .filter(|f| !existing_files.contains(&f.filename.trim_start_matches('/').to_string()))
        .collect();

    if new_files.is_empty() {
        debug!("All files already committed, skipping");
        return Ok(Some(table.version().unwrap_or(last_version)));
    }

    // Create add actions for new files
    let add_actions: Vec<Action> = new_files
        .iter()
        .map(|file| create_add_action(file))
        .collect();

    // Commit the actions
    let new_version = commit_to_delta(table, add_actions).await?;
    Ok(Some(new_version))
}

/// Create a Delta Lake Add action for a finished file.
fn create_add_action(file: &FinishedFile) -> Action {
    use deltalake::kernel::Add;
    use std::collections::HashMap;
    use std::time::SystemTime;

    debug!("Creating add action for file {:?}", file);

    let subpath = file.filename.trim_start_matches('/');

    Action::Add(Add {
        path: subpath.to_string(),
        size: file.size as i64,
        partition_values: HashMap::new(),
        modification_time: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
        data_change: true,
        ..Default::default()
    })
}

/// Commit add actions to the Delta table.
async fn commit_to_delta(table: &mut DeltaTable, add_actions: Vec<Action>) -> Result<i64> {
    use deltalake::kernel::transaction::CommitBuilder;

    let version = CommitBuilder::default()
        .with_actions(add_actions)
        .build(
            Some(table.snapshot()?),
            table.log_store(),
            deltalake::protocol::DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by: None,
                predicate: None,
            },
        )
        .await?
        .version;

    // Reload table to get new state
    table.load().await?;

    Ok(version)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_add_action() {
        let file = FinishedFile {
            filename: "test-file.parquet".to_string(),
            size: 1024,
            record_count: 100,
        };

        let action = create_add_action(&file);

        match action {
            Action::Add(add) => {
                assert_eq!(add.path, "test-file.parquet");
                assert_eq!(add.size, 1024);
                assert!(add.data_change);
            }
            _ => panic!("Expected Add action"),
        }
    }

    #[test]
    fn test_create_add_action_strips_leading_slash() {
        let file = FinishedFile {
            filename: "/path/to/file.parquet".to_string(),
            size: 2048,
            record_count: 200,
        };

        let action = create_add_action(&file);

        match action {
            Action::Add(add) => {
                assert_eq!(add.path, "path/to/file.parquet");
            }
            _ => panic!("Expected Add action"),
        }
    }
}

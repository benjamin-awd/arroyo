//! blizzard: A standalone tool for streaming NDJSON.gz files to Delta Lake.
//!
//! This tool reads compressed NDJSON files from various storage backends (S3, GCS,
//! Azure, local filesystem) and writes them to Delta Lake tables with exactly-once
//! semantics using checkpoint-based recovery.

mod checkpoint;
mod config;
mod pipeline;
mod sink;
mod source;
mod storage;

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use tracing::info;
use tracing_subscriber::EnvFilter;

use config::Config;
use pipeline::run_pipeline;

/// NDJSON.gz to Delta Lake streaming tool.
#[derive(Parser, Debug)]
#[command(name = "blizzard")]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the configuration file.
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Source path (overrides config file).
    #[arg(long)]
    source: Option<String>,

    /// Sink path (overrides config file).
    #[arg(long)]
    sink: Option<String>,

    /// Schema file path (used when config is not provided).
    #[arg(long)]
    schema: Option<PathBuf>,

    /// Checkpoint path (overrides config file).
    #[arg(long)]
    checkpoint: Option<String>,

    /// Checkpoint interval in seconds.
    #[arg(long, default_value = "30")]
    checkpoint_interval: u64,

    /// Target file size in MB.
    #[arg(long, default_value = "128")]
    file_size_mb: usize,

    /// Batch size for reading records.
    #[arg(long, default_value = "8192")]
    batch_size: usize,

    /// Maximum number of files to process concurrently.
    #[arg(long, default_value = "4")]
    max_concurrent_files: usize,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Dry run - validate configuration without processing.
    #[arg(long)]
    dry_run: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    info!("blizzard starting");

    // Load or build configuration
    let config = build_config(&args)?;

    if args.dry_run {
        info!("Dry run mode - validating configuration");
        info!("Source: {}", config.source.path);
        info!("Sink: {}", config.sink.path);
        info!("Checkpoint: {}", config.checkpoint.path);
        info!("Schema fields: {}", config.schema.fields.len());
        for field in &config.schema.fields {
            info!("  - {}: {:?}", field.name, field.field_type);
        }
        info!("Configuration is valid");
        return Ok(());
    }

    // Run the pipeline
    let stats = run_pipeline(config).await?;

    info!("Pipeline completed successfully");
    info!("  Files processed: {}", stats.files_processed);
    info!("  Records processed: {}", stats.records_processed);
    info!("  Parquet files written: {}", stats.parquet_files_written);
    info!("  Bytes written: {}", stats.bytes_written);
    info!("  Delta commits: {}", stats.delta_commits);
    info!("  Checkpoints saved: {}", stats.checkpoints_saved);

    Ok(())
}

/// Build configuration from arguments.
fn build_config(args: &Args) -> Result<Config> {
    // If a config file is provided, load it and apply overrides
    if let Some(config_path) = &args.config {
        let mut config = Config::from_file(config_path)?;

        // Apply command-line overrides
        if let Some(source) = &args.source {
            config.source.path = source.clone();
        }
        if let Some(sink) = &args.sink {
            config.sink.path = sink.clone();
        }
        if let Some(checkpoint) = &args.checkpoint {
            config.checkpoint.path = checkpoint.clone();
        }

        config.checkpoint.interval_seconds = args.checkpoint_interval;
        config.sink.file_size_mb = args.file_size_mb;
        config.source.batch_size = args.batch_size;

        return Ok(config);
    }

    // Build config from command-line arguments
    let source = args
        .source
        .clone()
        .ok_or_else(|| anyhow::anyhow!("Source path is required (--source or --config)"))?;

    let sink = args
        .sink
        .clone()
        .ok_or_else(|| anyhow::anyhow!("Sink path is required (--sink or --config)"))?;

    let checkpoint = args.checkpoint.clone().unwrap_or_else(|| {
        // Default checkpoint path based on sink
        format!("{}/_checkpoints", sink)
    });

    let schema = if let Some(schema_path) = &args.schema {
        config::SchemaConfig::from_file(schema_path)?
    } else {
        return Err(anyhow::anyhow!("Schema is required (--schema or --config)"));
    };

    Ok(Config {
        source: config::SourceConfig {
            path: source,
            compression: config::CompressionFormat::Gzip,
            storage_options: std::collections::HashMap::new(),
            batch_size: args.batch_size,
            max_concurrent_files: args.max_concurrent_files,
        },
        sink: config::SinkConfig {
            path: sink,
            file_size_mb: args.file_size_mb,
            row_group_size_bytes: 128 * 1024 * 1024, // 128MB default
            inactivity_timeout_secs: None,
            rollover_timeout_secs: None,
            part_size_mb: 32,
            min_multipart_size_mb: 5,
            storage_options: std::collections::HashMap::new(),
            compression: config::ParquetCompression::Snappy,
        },
        checkpoint: config::CheckpointConfig {
            path: checkpoint,
            interval_seconds: args.checkpoint_interval,
            storage_options: std::collections::HashMap::new(),
        },
        schema,
    })
}

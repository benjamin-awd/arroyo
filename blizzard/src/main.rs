//! blizzard: A standalone tool for streaming NDJSON.gz files to Delta Lake.
//!
//! This tool reads compressed NDJSON files from various storage backends (S3, GCS,
//! Azure, local filesystem) and writes them to Delta Lake tables with exactly-once
//! semantics using checkpoint-based recovery.

mod checkpoint;
mod config;
mod error;
mod internal_events;
mod metrics;
mod pipeline;
mod sink;
mod source;
mod storage;
mod utilization;

use clap::Parser;
use snafu::prelude::*;
use std::path::PathBuf;
use tracing::info;
use tracing_subscriber::EnvFilter;

use config::Config;
use error::{AddressParseSnafu, ConfigSnafu, MetricsSnafu, PipelineError};
use pipeline::run_pipeline;

/// NDJSON.gz to Delta Lake streaming tool.
#[derive(Parser, Debug)]
#[command(name = "blizzard")]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the configuration file.
    #[arg(short, long)]
    config: PathBuf,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Dry run - validate configuration without processing.
    #[arg(long)]
    dry_run: bool,
}

#[snafu::report]
#[tokio::main]
async fn main() -> Result<(), PipelineError> {
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

    // Initialize metrics if enabled
    if config.metrics.enabled {
        let addr = config.metrics.address.parse().context(AddressParseSnafu)?;
        metrics::init(addr).context(MetricsSnafu)?;
        info!(
            "Metrics endpoint listening on http://{}/metrics",
            config.metrics.address
        );
    }

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
fn build_config(args: &Args) -> Result<Config, PipelineError> {
    Config::from_file(&args.config).context(ConfigSnafu)
}

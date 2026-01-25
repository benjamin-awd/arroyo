//! Prometheus metrics infrastructure for Blizzard.
//!
//! This module provides metrics collection and exposure via HTTP.
//! It also provides health endpoints for Kubernetes liveness/readiness probes.

use axum::{Extension, Router, routing::get};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use snafu::prelude::*;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::error;

use crate::error::{MetricsError, PrometheusInitSnafu};

/// Initialize the Prometheus metrics exporter with an HTTP endpoint.
///
/// This starts an HTTP server on the given address that exposes:
/// - `/metrics` - Prometheus metrics in text format
/// - `/health` - Health check endpoint (returns 200 OK)
///
/// # Arguments
///
/// * `addr` - The socket address to bind the HTTP server to
///
/// # Example
///
/// ```ignore
/// use std::net::SocketAddr;
/// use blizzard::metrics;
///
/// let addr: SocketAddr = "0.0.0.0:9090".parse().unwrap();
/// metrics::init(addr).expect("Failed to initialize metrics");
/// ```
pub fn init(addr: SocketAddr) -> Result<(), MetricsError> {
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .context(PrometheusInitSnafu)?;

    // Spawn the HTTP server in the background
    tokio::spawn(run_server(addr, handle));

    Ok(())
}

/// Run the HTTP server for metrics and health endpoints.
async fn run_server(addr: SocketAddr, handle: PrometheusHandle) {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .layer(Extension(handle));

    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to bind metrics server to {}: {}", addr, e);
            return;
        }
    };

    if let Err(e) = axum::serve(listener, app).await {
        error!("Metrics server error: {}", e);
    }
}

/// Handler for `/metrics` endpoint.
async fn metrics_handler(Extension(handle): Extension<PrometheusHandle>) -> String {
    handle.render()
}

/// Handler for `/health` endpoint.
async fn health_handler() -> &'static str {
    "ok\n"
}

/// Macro for emitting metric events (Vector-style pattern).
///
/// This macro calls the `InternalEvent::emit()` method on the given event,
/// which records the corresponding Prometheus counter metric.
///
/// # Example
///
/// ```ignore
/// use blizzard::internal_events::{RecordsProcessed, BytesWritten};
///
/// emit!(RecordsProcessed { count: 100 });
/// emit!(BytesWritten { bytes: 1024 });
/// ```
#[macro_export]
macro_rules! emit {
    ($event:expr) => {
        $crate::internal_events::InternalEvent::emit($event)
    };
}

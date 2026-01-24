//! Benchmark utilities for generating test data.

use arrow::array::{BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use rand::Rng;
use std::io::Write;
use std::sync::Arc;

/// Returns a realistic schema with mixed types for benchmarking.
///
/// Matches a typical orderbook-style schema:
/// - id: String (non-nullable)
/// - timestamp: Int64 (non-nullable)
/// - price: Float64 (nullable)
/// - quantity: Float64 (nullable)
/// - side: String (nullable)
/// - active: Boolean (nullable)
pub fn benchmark_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("price", DataType::Float64, true),
        Field::new("quantity", DataType::Float64, true),
        Field::new("side", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, true),
    ]))
}

/// Generate NDJSON lines for parsing benchmarks.
///
/// Each line is a valid JSON object matching the benchmark schema.
pub fn generate_json_lines(count: usize) -> Vec<String> {
    let mut rng = rand::thread_rng();
    let sides = ["buy", "sell"];

    (0..count)
        .map(|i| {
            let side = sides[rng.gen_range(0..2)];
            let price: f64 = rng.gen_range(100.0..10000.0);
            let quantity: f64 = rng.gen_range(0.01..100.0);
            let timestamp: i64 = 1700000000000 + (i as i64);
            let active: bool = rng.gen_bool(0.9);

            format!(
                r#"{{"id":"order_{}","timestamp":{},"price":{:.2},"quantity":{:.4},"side":"{}","active":{}}}"#,
                i, timestamp, price, quantity, side, active
            )
        })
        .collect()
}

/// Generate a gzip-compressed NDJSON file for I/O benchmarks.
/// Returns the path to the temporary file.
pub fn generate_ndjson_gz_file(count: usize) -> tempfile::NamedTempFile {
    use flate2::Compression;
    use flate2::write::GzEncoder;

    let lines = generate_json_lines(count);
    let file = tempfile::NamedTempFile::new().expect("Failed to create temp file");

    let mut encoder = GzEncoder::new(
        std::fs::File::create(file.path()).expect("Failed to create file"),
        Compression::fast(),
    );

    for line in &lines {
        writeln!(encoder, "{}", line).expect("Failed to write line");
    }

    encoder.finish().expect("Failed to finish compression");
    file
}

/// Generate Arrow RecordBatches for Parquet benchmarks.
///
/// Creates batches with the benchmark schema containing realistic data.
pub fn generate_record_batches(batch_size: usize, num_batches: usize) -> Vec<RecordBatch> {
    let schema = benchmark_schema();
    let mut rng = rand::thread_rng();

    (0..num_batches)
        .map(|batch_idx| {
            let base_idx = batch_idx * batch_size;

            // Generate ids
            let ids: Vec<String> = (0..batch_size)
                .map(|i| format!("order_{}", base_idx + i))
                .collect();

            // Generate timestamps
            let timestamps: Vec<i64> = (0..batch_size)
                .map(|i| 1700000000000 + ((base_idx + i) as i64))
                .collect();

            // Generate prices (some nulls)
            let prices: Vec<Option<f64>> = (0..batch_size)
                .map(|_| {
                    if rng.gen_bool(0.95) {
                        Some(rng.gen_range(100.0..10000.0))
                    } else {
                        None
                    }
                })
                .collect();

            // Generate quantities (some nulls)
            let quantities: Vec<Option<f64>> = (0..batch_size)
                .map(|_| {
                    if rng.gen_bool(0.95) {
                        Some(rng.gen_range(0.01..100.0))
                    } else {
                        None
                    }
                })
                .collect();

            // Generate sides
            let sides: Vec<Option<&str>> = (0..batch_size)
                .map(|_| {
                    if rng.gen_bool(0.5) {
                        Some("buy")
                    } else {
                        Some("sell")
                    }
                })
                .collect();

            // Generate active flags
            let active: Vec<Option<bool>> = (0..batch_size)
                .map(|_| {
                    if rng.gen_bool(0.98) {
                        Some(rng.gen_bool(0.9))
                    } else {
                        None
                    }
                })
                .collect();

            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(ids)),
                    Arc::new(Int64Array::from(timestamps)),
                    Arc::new(Float64Array::from(prices)),
                    Arc::new(Float64Array::from(quantities)),
                    Arc::new(StringArray::from(sides)),
                    Arc::new(BooleanArray::from(active)),
                ],
            )
            .expect("Failed to create RecordBatch")
        })
        .collect()
}

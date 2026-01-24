//! Blizzard benchmark suite.
//!
//! Benchmarks for key operations:
//! - JSON parsing throughput
//! - Parquet writing throughput
//! - Checkpoint serialization/deserialization

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

mod bench_utils;

use blizzard::checkpoint::CheckpointState;
use blizzard::config::ParquetCompression;
use blizzard::sink::parquet::{ParquetFileBuilder, ParquetWriter, ParquetWriterConfig};
use blizzard::source::reader::parse_json_line;

/// Benchmarks for JSON line parsing.
///
/// Tests the throughput of parsing NDJSON lines into Arrow RecordBatches.
fn json_parsing_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_parsing");

    for size in [100, 1000, 10000] {
        let lines = bench_utils::generate_json_lines(size);
        let schema = bench_utils::benchmark_schema();

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("parse", size), &lines, |b, lines| {
            b.iter(|| {
                for line in lines {
                    parse_json_line(line, schema.clone()).unwrap();
                }
            });
        });
    }

    group.finish();
}

/// Benchmarks for Parquet writing.
///
/// Tests the throughput of writing Arrow RecordBatches to Parquet format.
fn parquet_writing_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("parquet_writing");

    for batch_size in [1000, 8192, 32768] {
        let batches = bench_utils::generate_record_batches(batch_size, 10);
        let total_records = batch_size * 10;

        group.throughput(Throughput::Elements(total_records as u64));
        group.bench_with_input(
            BenchmarkId::new("write_batch", batch_size),
            &batches,
            |b, batches| {
                b.iter(|| {
                    let schema = bench_utils::benchmark_schema();
                    let config = ParquetWriterConfig::default();
                    let mut writer = ParquetWriter::new(schema, config);
                    for batch in batches {
                        writer.write_batch(batch).unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmarks comparing different Parquet compression codecs.
///
/// Tests Snappy, Zstd, Gzip, and uncompressed performance.
fn compression_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression");

    // Use a moderate batch size with enough batches for meaningful compression comparison
    let batches = bench_utils::generate_record_batches(8192, 20);
    let total_records = 8192 * 20;

    group.throughput(Throughput::Elements(total_records as u64));

    let compressions = [
        ("snappy", ParquetCompression::Snappy),
        ("zstd", ParquetCompression::Zstd),
        ("gzip", ParquetCompression::Gzip),
        ("none", ParquetCompression::Uncompressed),
    ];

    for (name, compression) in compressions {
        group.bench_with_input(BenchmarkId::from_parameter(name), &batches, |b, batches| {
            b.iter(|| {
                let schema = bench_utils::benchmark_schema();
                ParquetFileBuilder::new(schema)
                    .with_compression(compression)
                    .build(batches)
                    .unwrap()
            });
        });
    }

    group.finish();
}

/// Benchmarks for checkpoint serialization.
///
/// Tests the performance of serializing checkpoint state to JSON.
fn checkpoint_serialization_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint");

    // Test with different checkpoint sizes
    for (files, records_per_file) in [(10, 10000), (100, 50000), (1000, 100000)] {
        let state = bench_utils::generate_checkpoint_state(files, records_per_file);

        // Serialization benchmark
        group.bench_with_input(BenchmarkId::new("serialize", files), &state, |b, state| {
            b.iter(|| state.to_json().unwrap());
        });

        // Deserialization benchmark - serialize once, then benchmark deserialize
        let json = state.to_json().unwrap();
        group.bench_with_input(BenchmarkId::new("deserialize", files), &json, |b, json| {
            b.iter(|| CheckpointState::from_json(json).unwrap());
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    json_parsing_benchmarks,
    parquet_writing_benchmarks,
    compression_benchmarks,
    checkpoint_serialization_benchmarks,
);
criterion_main!(benches);

//! Blizzard benchmark suite.
//!
//! Benchmarks for key operations:
//! - JSON parsing throughput
//! - Parquet writing throughput
//! - Parallel decompression

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

mod bench_utils;

use arrow_json::reader::ReaderBuilder;
use blizzard::sink::parquet::{ParquetWriter, ParquetWriterConfig};

/// Benchmarks for JSON line parsing.
///
/// Tests the throughput of parsing NDJSON lines into Arrow RecordBatches.
fn json_parsing_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_parsing");

    for size in [100, 1000, 10000] {
        let lines = bench_utils::generate_json_lines(size);
        let schema = bench_utils::benchmark_schema();

        group.throughput(Throughput::Elements(size as u64));

        // Reuse decoder, decode line by line
        group.bench_with_input(
            BenchmarkId::new("decode_reuse", size),
            &lines,
            |b, lines| {
                b.iter(|| {
                    let mut decoder = ReaderBuilder::new(schema.clone())
                        .with_strict_mode(false)
                        .build_decoder()
                        .unwrap();
                    for line in lines {
                        decoder.decode(line.as_bytes()).unwrap();
                    }
                    decoder.flush().unwrap()
                });
            },
        );

        // Bulk decode with newline-separated bytes
        let bulk_data: String = lines.join("\n");
        group.bench_with_input(
            BenchmarkId::new("decode_bulk", size),
            &bulk_data,
            |b, data| {
                b.iter(|| {
                    let mut decoder = ReaderBuilder::new(schema.clone())
                        .with_strict_mode(false)
                        .build_decoder()
                        .unwrap();
                    decoder.decode(data.as_bytes()).unwrap();
                    decoder.flush().unwrap()
                });
            },
        );
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

/// Benchmarks comparing sequential vs parallel gzip decompression.
///
/// This benchmark verifies that parallel decompression utilizes multiple CPU cores
/// by comparing the throughput of processing multiple files sequentially vs in parallel.
fn parallel_decompression_benchmarks(c: &mut Criterion) {
    use bytes::Bytes;
    use flate2::read::GzDecoder;
    use std::io::Read;
    use tokio::runtime::Runtime;

    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("parallel_decompression");

    // Generate multiple compressed files for parallel processing test
    let num_files = 8; // Test with 8 files to show parallelism
    let records_per_file = 50_000;

    // Pre-generate compressed data for all files
    let compressed_files: Vec<Bytes> = (0..num_files)
        .map(|_| {
            let temp_file = bench_utils::generate_ndjson_gz_file(records_per_file);
            let data = std::fs::read(temp_file.path()).expect("Failed to read temp file");
            Bytes::from(data)
        })
        .collect();

    let total_records = num_files * records_per_file;
    group.throughput(Throughput::Elements(total_records as u64));

    let schema = bench_utils::benchmark_schema();

    // Sequential decompression (one at a time)
    group.bench_function("sequential", |b| {
        b.iter(|| {
            let mut total = 0;
            for compressed in &compressed_files {
                // Decompress
                let mut decoder = GzDecoder::new(&compressed[..]);
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed).unwrap();

                // Parse
                let mut json_decoder = ReaderBuilder::new(schema.clone())
                    .with_batch_size(8192)
                    .with_strict_mode(false)
                    .build_decoder()
                    .unwrap();

                let mut offset = 0;
                while offset < decompressed.len() {
                    let consumed = json_decoder.decode(&decompressed[offset..]).unwrap();
                    if let Some(batch) = json_decoder.flush().unwrap() {
                        total += batch.num_rows();
                    }
                    if consumed == 0 {
                        break;
                    }
                    offset += consumed;
                }
            }
            total
        });
    });

    // Parallel decompression using spawn_blocking (simulates pipeline behavior)
    group.bench_function("parallel_spawn_blocking", |b| {
        b.to_async(&rt).iter(|| {
            let files = compressed_files.clone();
            let schema = schema.clone();
            async move {
                use futures::stream::{FuturesUnordered, StreamExt};

                let mut futures: FuturesUnordered<_> = files
                    .into_iter()
                    .map(|compressed| {
                        let schema = schema.clone();
                        tokio::task::spawn_blocking(move || {
                            // Decompress
                            let mut decoder = GzDecoder::new(&compressed[..]);
                            let mut decompressed = Vec::new();
                            decoder.read_to_end(&mut decompressed).unwrap();

                            // Parse
                            let mut json_decoder = ReaderBuilder::new(schema)
                                .with_batch_size(8192)
                                .with_strict_mode(false)
                                .build_decoder()
                                .unwrap();

                            let mut count = 0;
                            let mut offset = 0;
                            while offset < decompressed.len() {
                                let consumed =
                                    json_decoder.decode(&decompressed[offset..]).unwrap();
                                if let Some(batch) = json_decoder.flush().unwrap() {
                                    count += batch.num_rows();
                                }
                                if consumed == 0 {
                                    break;
                                }
                                offset += consumed;
                            }
                            count
                        })
                    })
                    .collect();

                let mut total = 0;
                while let Some(result) = futures.next().await {
                    total += result.unwrap();
                }
                total
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    json_parsing_benchmarks,
    parquet_writing_benchmarks,
    parallel_decompression_benchmarks,
);
criterion_main!(benches);

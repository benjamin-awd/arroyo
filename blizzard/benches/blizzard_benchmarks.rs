//! Blizzard benchmark suite.
//!
//! Benchmarks for key operations:
//! - JSON parsing throughput
//! - Parquet writing throughput
//! - Checkpoint serialization/deserialization

use criterion::async_executor::AsyncExecutor;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::sync::Arc;

mod bench_utils;

use arrow_json::reader::ReaderBuilder;
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

        // Old approach: parse each line separately (creates decoder per line)
        group.bench_with_input(
            BenchmarkId::new("parse_single", size),
            &lines,
            |b, lines| {
                b.iter(|| {
                    for line in lines {
                        parse_json_line(line, schema.clone()).unwrap();
                    }
                });
            },
        );

        // Better approach: reuse decoder, decode line by line
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

        // Best approach: bulk decode with newline-separated bytes
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

/// Benchmarks for end-to-end file reading with BatchReader.
///
/// Tests actual throughput including gzip decompression and file I/O.
fn reader_throughput_benchmarks(c: &mut Criterion) {
    use blizzard::config::CompressionFormat;
    use blizzard::source::reader::create_batch_reader;
    use blizzard::storage::StorageProvider;
    use std::path::Path;
    use tokio::runtime::Runtime;

    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("reader_throughput");

    for record_count in [10_000, 100_000, 500_000] {
        // Generate test file
        let temp_file = bench_utils::generate_ndjson_gz_file(record_count);
        let file_path = temp_file.path().to_path_buf();
        let parent_dir = file_path.parent().unwrap().to_str().unwrap().to_string();
        let file_name = file_path.file_name().unwrap().to_str().unwrap().to_string();
        let schema = bench_utils::benchmark_schema();

        group.throughput(Throughput::Elements(record_count as u64));
        group.bench_with_input(
            BenchmarkId::new("batch_reader", record_count),
            &(parent_dir.clone(), file_name.clone()),
            |b, (dir, name)| {
                b.to_async(&rt).iter(|| {
                    let dir = dir.clone();
                    let name = name.clone();
                    let schema = schema.clone();
                    async move {
                        let storage = Arc::new(
                            StorageProvider::for_url(&dir)
                                .await
                                .expect("Failed to create storage"),
                        );

                        let mut reader = create_batch_reader(
                            &storage,
                            &name,
                            CompressionFormat::Gzip,
                            schema,
                            8192,
                            0,
                        )
                        .await
                        .expect("Failed to create reader");

                        let mut total_records = 0;
                        let mut batch_count = 0;
                        while let Some(batch) =
                            reader.next_batch().await.expect("Failed to read batch")
                        {
                            total_records += batch.batch.num_rows();
                            batch_count += 1;
                        }
                        if total_records != record_count {
                            eprintln!(
                                "WARNING: Expected {} records, got {} in {} batches",
                                record_count, total_records, batch_count
                            );
                        }
                        total_records
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
    use blizzard::config::CompressionFormat;
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

    // Parallel using rayon directly (for comparison)
    group.bench_function("parallel_rayon", |b| {
        use rayon::prelude::*;

        b.iter(|| {
            let total: usize = compressed_files
                .par_iter()
                .map(|compressed| {
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

                    let mut count = 0;
                    let mut offset = 0;
                    while offset < decompressed.len() {
                        let consumed = json_decoder.decode(&decompressed[offset..]).unwrap();
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
                .sum();
            total
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    json_parsing_benchmarks,
    parquet_writing_benchmarks,
    compression_benchmarks,
    checkpoint_serialization_benchmarks,
    reader_throughput_benchmarks,
    parallel_decompression_benchmarks,
);
criterion_main!(benches);

# RustyCSV Benchmarks

This document presents benchmark results comparing RustyCSV's parsing strategies against NimbleCSV.

## Test Environment

- **Elixir**: 1.19.4
- **OTP**: 28
- **Hardware**: Apple Silicon M1 Pro (10 cores)
- **RustyCSV**: 0.2.0
- **NimbleCSV**: 1.3.0
- **Test date**: January 25, 2026

## Strategies Compared

| Strategy | Description | Best For |
|----------|-------------|----------|
| `:simd` | SIMD-accelerated via memchr (default) | General use |
| `:basic` | Byte-by-byte parsing | Debugging, baseline |
| `:indexed` | Two-phase index-then-extract | Row range extraction |
| `:parallel` | Multi-threaded via rayon | Very large files (500MB+) |
| `:zero_copy` | Sub-binary references | Maximum speed |
| `:streaming` | Bounded-memory chunks | Unbounded files |

## Throughput Benchmark Results

### Simple CSV (333 KB, 10K rows, no quotes)

| Strategy | Throughput | Latency | vs NimbleCSV |
|----------|------------|---------|--------------|
| RustyCSV (zero_copy) | 719 ips | 1.39ms | **3.5x faster** |
| RustyCSV (simd) | 597 ips | 1.68ms | 2.9x faster |
| RustyCSV (basic) | 596 ips | 1.68ms | 2.9x faster |
| RustyCSV (indexed) | 562 ips | 1.78ms | 2.7x faster |
| NimbleCSV | 209 ips | 4.80ms | baseline |
| RustyCSV (parallel) | 149 ips | 6.73ms | 0.71x (overhead) |

### Quoted CSV (947 KB, 10K rows, all fields quoted with escapes)

| Strategy | Throughput | Latency | vs NimbleCSV |
|----------|------------|---------|--------------|
| RustyCSV (simd) | 375 ips | 2.66ms | **17.9x faster** |
| RustyCSV (zero_copy) | 370 ips | 2.70ms | 17.6x faster |
| RustyCSV (basic) | 349 ips | 2.87ms | 16.6x faster |
| RustyCSV (indexed) | 325 ips | 3.07ms | 15.5x faster |
| RustyCSV (parallel) | 119 ips | 8.40ms | 5.7x faster |
| NimbleCSV | 21 ips | 47.65ms | baseline |

### Mixed/Realistic CSV (652 KB, 10K rows)

| Strategy | Throughput | Latency | vs NimbleCSV |
|----------|------------|---------|--------------|
| RustyCSV (zero_copy) | 424 ips | 2.36ms | **4.3x faster** |
| RustyCSV (basic) | 372 ips | 2.69ms | 3.8x faster |
| RustyCSV (simd) | 372 ips | 2.69ms | 3.8x faster |
| RustyCSV (indexed) | 354 ips | 2.83ms | 3.6x faster |
| RustyCSV (parallel) | 119 ips | 8.38ms | 1.2x faster |
| NimbleCSV | 99 ips | 10.13ms | baseline |

### Large CSV (6.82 MB, 100K rows)

| Strategy | Throughput | Latency | vs NimbleCSV |
|----------|------------|---------|--------------|
| RustyCSV (zero_copy) | 41.9 ips | 23.85ms | **9.2x faster** |
| RustyCSV (basic) | 35.1 ips | 28.50ms | 7.7x faster |
| RustyCSV (simd) | 34.9 ips | 28.68ms | 7.6x faster |
| RustyCSV (indexed) | 33.9 ips | 29.46ms | 7.4x faster |
| RustyCSV (parallel) | 13.5 ips | 74.28ms | 2.9x faster |
| NimbleCSV | 4.6 ips | 219.13ms | baseline |

### Very Large CSV (108 MB, 1.5M rows)

| Strategy | Throughput | Latency | vs NimbleCSV |
|----------|------------|---------|--------------|
| RustyCSV (zero_copy) | 1.93 ips | 0.52s | **8.6x faster** |
| RustyCSV (simd) | 1.75 ips | 0.57s | 7.8x faster |
| RustyCSV (parallel) | 0.80 ips | 1.25s | 3.6x faster |
| NimbleCSV | 0.23 ips | 4.44s | baseline |

**Note:** Even at 108 MB, `:parallel` is slower than single-threaded strategies due to coordination overhead. The crossover point where `:parallel` becomes beneficial appears to be 500MB+ with highly complex (quoted) data.

## Memory Comparison

**Important:** Memory measurement for NIFs is complex. RustyCSV allocates on both the Rust side and BEAM side, while NimbleCSV allocates entirely on the BEAM.

### Methodology

We measure three metrics:
1. **Process Heap**: Memory delta in the calling process (what Benchee measures)
2. **Rust NIF**: Peak allocation on the Rust side during parsing
3. **BEAM Allocation**: Memory allocated on the BEAM during parsing (includes binaries)

### Mixed CSV (652 KB input)

| Strategy | Rust NIF Peak | Notes |
|----------|---------------|-------|
| RustyCSV (zero_copy) | 1.67 MB | Lowest - sub-binary refs avoid copies |
| RustyCSV (basic) | 2.44 MB | Copies all field data |
| RustyCSV (simd) | 2.44 MB | Same as basic |
| RustyCSV (parallel) | 3.40 MB | Extra buffers for coordination |
| RustyCSV (indexed) | 3.74 MB | Index structure + field data |

| Parser | BEAM Allocation (Benchee) |
|--------|---------------------------|
| RustyCSV (all strategies) | 1.55 KB |
| NimbleCSV | 9.41 MB |

**Key insight:** Benchee's "1.55 KB" for RustyCSV measures process heap delta only, not the actual data. The parsed data exists in BEAM binaries (created by the NIF). NimbleCSV's 9.41 MB includes all list/tuple allocations.

**Bottom line:** Both parsers use memory proportional to the data. RustyCSV's memory is split between Rust and BEAM; NimbleCSV's is entirely on BEAM. Neither is dramatically more efficient.

## BEAM Reductions (Scheduler Work)

| Strategy | Reductions | vs NimbleCSV |
|----------|------------|--------------|
| RustyCSV (zero_copy) | 18 | 14,228x fewer |
| RustyCSV (indexed) | 18 | 14,228x fewer |
| RustyCSV (basic) | 2,800 | 91x fewer |
| RustyCSV (simd) | 3,400 | 75x fewer |
| RustyCSV (parallel) | 35,500 | 7x fewer |
| NimbleCSV | 256,100 | baseline |

**What this means:**
- Low reductions = less scheduler overhead
- NIFs run outside BEAM's reduction counting
- Trade-off: NIFs can't be preempted mid-execution

## Streaming Comparison

**File:** 6.5 MB (100K rows)

### `File.stream!` Input (`parse_stream/2`)

|                       | p10     | median  | p90     | min     | max     |
|-----------------------|---------|---------|---------|---------|---------|
| RustyCSV streaming    | 46.8ms  | 47.7ms  | 48.9ms  | 46.6ms  | 49.0ms  |
| NimbleCSV streaming   | 69.3ms  | 69.9ms  | 70.5ms  | 68.3ms  | 71.0ms  |

**Result:** RustyCSV is **1.5x faster** (22ms saved at median).

RustyCSV automatically detects `File.Stream` in line mode and switches to 64KB binary chunk reads, reducing stream iterations from ~100K (one per line) to ~100. The Rust NIF handles arbitrary chunk boundaries internally, so it can operate on raw binary chunks rather than pre-split lines.

### Arbitrary Binary Chunks

RustyCSV can also process arbitrary binary chunks directly (useful for network streams, compressed data, etc.). NimbleCSV's `parse_stream` operates on line-delimited input, which is the standard approach when using `File.stream!/1`.

## Real-World Benchmark: Amazon Settlement Reports

This section presents results from parsing Amazon SP-API settlement reports in TSV format.

### Test Data

- **Data source**: Amazon Seller Central settlement reports (TSV format)
- **Report sizes**: 1KB to 2.6MB (20 to 15,820 rows)

### Small Files (<200 rows)

| Rows | RustyCSV | NimbleCSV | String.split |
|-----:|---------:|----------:|-------------:|
| 20 | 2ms | 2ms | 2ms |
| 24 | 2ms | 2ms | 2ms |
| 36 | 2ms | 2ms | 2ms |
| 93 | 2ms | 2ms | 2ms |
| 100 | 2ms | 2ms | 2ms |
| 141 | 2ms | 3ms | 3ms |

**Conclusion**: For small files, all approaches perform equivalently (~2ms).

### Large Files (10K+ rows)

| Rows | RustyCSV | NimbleCSV | vs NimbleCSV |
|-----:|---------:|----------:|-------------:|
| 9,985 | **46ms** | 64ms | 28% faster |
| 10,961 | **54ms** | 68ms | 21% faster |
| 11,246 | **60ms** | 69ms | 13% faster |
| 11,754 | **56ms** | 78ms | 28% faster |
| 13,073 | **84ms** | 96ms | 13% faster |

**Conclusion**: RustyCSV is consistently 13-28% faster than NimbleCSV for large real-world files.

## Summary

### Speed Rankings by File Type

| File Type | Best Strategy | Speedup vs NimbleCSV |
|-----------|---------------|----------------------|
| Simple CSV | `:zero_copy` | 3.5x |
| Quoted CSV | `:simd` | 17.9x |
| Mixed CSV | `:zero_copy` | 4.3x |
| Large CSV (7MB) | `:zero_copy` | 9.2x |
| Very Large CSV (108MB) | `:zero_copy` | 8.6x |
| Streaming (6.5MB) | `parse_stream/2` | 1.5x |
| Real-world TSV | `:simd` | 1.1-1.3x |

### Strategy Selection Guide

| Use Case | Recommended Strategy |
|----------|---------------------|
| Default / General use | `:simd` |
| Maximum speed | `:zero_copy` |
| Very large files (500MB+) with complex quoting | `:parallel` |
| Streaming / Unbounded | `parse_stream/2` |
| Memory-constrained | `:simd` (copies data, frees input) |
| Debugging | `:basic` |

### Key Findings

1. **`:zero_copy` is fastest** for most workloads (up to 9.2x faster than NimbleCSV)

2. **Quoted fields show largest gains** - 17.9x faster due to efficient escape handling

3. **Memory usage is comparable** - RustyCSV allocates on Rust side, NimbleCSV on BEAM. Neither is dramatically more efficient.

4. **BEAM reductions are minimal** - Up to 14,228x fewer reductions, reducing scheduler load (but NIFs can't be preempted)

5. **`:parallel` has significant overhead** - Not beneficial until 500MB+ files with complex data

6. **Streaming is 1.5x faster** - RustyCSV auto-optimizes `File.Stream` to binary chunk mode, reducing iterations from ~100K lines to ~100 chunks. Also supports arbitrary binary chunks for non-file streams.

7. **Real-world vs synthetic** - Synthetic benchmarks show 3-18x gains; real-world TSV shows 13-28% gains due to simpler data patterns.

## Running the Benchmarks

```bash
# Comprehensive benchmark (all strategies)
mix run bench/comprehensive_bench.exs

# Quick benchmark
mix run bench/csv_bench.exs
```

For memory tracking details, enable the `memory_tracking` feature:

```toml
# In native/rustycsv/Cargo.toml
[features]
default = ["mimalloc", "memory_tracking"]
```

Then rebuild: `FORCE_RUSTYCSV_BUILD=true mix compile --force`

# RustyCSV Benchmarks

This document presents benchmark results comparing RustyCSV's parsing strategies against NimbleCSV.

## Test Environment

- **Elixir**: 1.19.4
- **OTP**: 28
- **Hardware**: Apple Silicon M1 Pro (10 cores)
- **RustyCSV**: 0.3.4
- **NimbleCSV**: 1.3.0
- **Test date**: February 1, 2026

## Strategies Compared

| Strategy | Description | Best For |
|----------|-------------|----------|
| `:simd` | SIMD structural scanner (default) | General use |
| `:basic` | SIMD scan + basic field extraction | Debugging, baseline |
| `:indexed` | SIMD scan + two-phase index-then-extract | Row range extraction |
| `:parallel` | SIMD scan + flat index + parallel field extraction via rayon | Large files |
| `:zero_copy` | SIMD scan + sub-binary references | Maximum speed |
| `:streaming` | Bounded-memory chunks | Unbounded files |

All batch strategies share a single-pass SIMD structural scanner that finds every unquoted separator and row ending, then diverge only in how they extract field data.

## Throughput Benchmark Results

### Simple CSV (334 KB, 10K rows, no quotes)

| Strategy | Throughput | Latency | vs NimbleCSV |
|----------|------------|---------|--------------|
| RustyCSV (zero_copy) | 829 ips | 1.02ms | **3.7x faster** |
| RustyCSV (basic) | 758 ips | 1.13ms | 3.3x faster |
| RustyCSV (simd) | 744 ips | 1.14ms | 3.3x faster |
| RustyCSV (indexed) | 688 ips | 1.26ms | 3.0x faster |
| RustyCSV (parallel) | 567 ips | 1.58ms | 2.5x faster |
| NimbleCSV | 227 ips | 4.32ms | baseline |

### Quoted CSV (947 KB, 10K rows, all fields quoted with escapes)

| Strategy | Throughput | Latency | vs NimbleCSV |
|----------|------------|---------|--------------|
| RustyCSV (zero_copy) | 444 ips | 2.04ms | **17.9x faster** |
| RustyCSV (basic) | 418 ips | 2.18ms | 16.9x faster |
| RustyCSV (simd) | 418 ips | 2.19ms | 16.8x faster |
| RustyCSV (parallel) | 400 ips | 2.33ms | 16.1x faster |
| RustyCSV (indexed) | 390 ips | 2.35ms | 15.7x faster |
| NimbleCSV | 25 ips | 40.20ms | baseline |

### Mixed/Realistic CSV (652 KB, 10K rows)

| Strategy | Throughput | Latency | vs NimbleCSV |
|----------|------------|---------|--------------|
| RustyCSV (zero_copy) | 564 ips | 1.53ms | **5.8x faster** |
| RustyCSV (simd) | 504 ips | 1.71ms | 5.2x faster |
| RustyCSV (basic) | 473 ips | 1.85ms | 4.9x faster |
| RustyCSV (indexed) | 437 ips | 2.03ms | 4.5x faster |
| RustyCSV (parallel) | 362 ips | 2.52ms | 3.7x faster |
| NimbleCSV | 98 ips | 10.14ms | baseline |

### Large CSV (6.82 MB, 100K rows)

| Strategy | Throughput | Latency | vs NimbleCSV |
|----------|------------|---------|--------------|
| RustyCSV (zero_copy) | 49.5 ips | 17.37ms | **10.7x faster** |
| RustyCSV (basic) | 45.0 ips | 18.58ms | 9.7x faster |
| RustyCSV (simd) | 45.0 ips | 18.65ms | 9.6x faster |
| RustyCSV (indexed) | 40.8 ips | 20.21ms | 8.8x faster |
| RustyCSV (parallel) | 40.6 ips | 20.46ms | 8.7x faster |
| NimbleCSV | 4.6 ips | 211ms | baseline |

### Very Large CSV (108 MB, 1.5M rows)

| Strategy | Throughput | Latency | vs NimbleCSV |
|----------|------------|---------|--------------|
| RustyCSV (zero_copy) | 2.53 ips | 0.29s | **12.5x faster** |
| RustyCSV (simd) | 2.28 ips | 0.30s | 11.2x faster |
| RustyCSV (parallel) | 1.99 ips | 0.49s | 9.7x faster |
| NimbleCSV | 0.20 ips | 4.93s | baseline |

## Memory Comparison

**Important:** Memory measurement for NIFs is complex. RustyCSV allocates on both the Rust side and BEAM side, while NimbleCSV allocates entirely on the BEAM.

### Methodology

We measure three metrics:
1. **Process Heap**: Memory delta in the calling process (what Benchee measures)
2. **Rust NIF**: Peak allocation on the Rust side during parsing
3. **BEAM Allocation**: Memory allocated on the BEAM during parsing (includes binaries)

### Mixed CSV (652 KB input)

| Parser | BEAM Allocation (Benchee) |
|--------|---------------------------|
| RustyCSV (all strategies) | 1.55 KB |
| NimbleCSV | 9.40 MB |

**Key insight:** Benchee's "1.55 KB" for RustyCSV measures process heap delta only, not the actual data. The parsed data exists in BEAM binaries (created by the NIF). NimbleCSV's 9.40 MB includes all list/tuple allocations.

**Bottom line:** Both parsers use memory proportional to the data. RustyCSV's memory is split between Rust and BEAM; NimbleCSV's is entirely on BEAM. Neither is dramatically more efficient.

## BEAM Reductions (Scheduler Work)

| Strategy | Reductions | vs NimbleCSV |
|----------|------------|--------------|
| RustyCSV (zero_copy) | 7,100 | 36x fewer |
| RustyCSV (indexed) | 7,700 | 33x fewer |
| RustyCSV (basic) | 11,500 | 22x fewer |
| RustyCSV (simd) | 11,100 | 23x fewer |
| RustyCSV (parallel) | 16,200 | 16x fewer |
| NimbleCSV | 254,800 | baseline |

**What this means:**
- Low reductions = less scheduler overhead
- NIFs run outside BEAM's reduction counting
- Trade-off: NIFs can't be preempted mid-execution

## Streaming Comparison

**File:** 6.8 MB (100K rows)

### `File.stream!` Input (`parse_stream/2`)

| Parser | Mode | Time | Speedup |
|--------|------|------|---------|
| RustyCSV | line-based | 54ms | **2.2x faster** |
| NimbleCSV | line-based | 117ms | baseline |
| RustyCSV | 64KB binary chunks | 244ms | unique capability |

**Result:** RustyCSV is **2.2x faster** for line-based streaming.

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
| Simple CSV | `:zero_copy` | 3.7x |
| Quoted CSV | `:zero_copy` | 17.9x |
| Mixed CSV | `:zero_copy` | 5.8x |
| Large CSV (7MB) | `:zero_copy` | 10.7x |
| Very Large CSV (108MB) | `:zero_copy` | 12.5x |
| Streaming (6.8MB) | `parse_stream/2` | 2.2x |
| Real-world TSV | `:simd` | 1.1-1.3x |

### Strategy Selection Guide

| Use Case | Recommended Strategy |
|----------|---------------------|
| Default / General use | `:simd` |
| Maximum speed | `:zero_copy` |
| Large files with many cores | `:parallel` |
| Streaming / Unbounded | `parse_stream/2` |
| Memory-constrained | `:simd` (copies data, frees input) |
| Debugging | `:basic` |

### Key Findings

1. **`:zero_copy` is fastest** for all workloads (up to 12.5x faster than NimbleCSV on 108MB)

2. **Quoted fields show largest gains** — 17.9x faster due to SIMD prefix-XOR quote detection handling all escapes in a single pass

3. **`:parallel` is now competitive at all file sizes** — the shared SIMD structural scan eliminated the coordination overhead that previously made it slower than single-threaded strategies on small/medium files

4. **Memory usage is comparable** — RustyCSV allocates on Rust side, NimbleCSV on BEAM. Neither is dramatically more efficient.

5. **BEAM reductions are minimal** — 16-36x fewer reductions than NimbleCSV, reducing scheduler load (but NIFs can't be preempted)

6. **Streaming is 2.2x faster** — RustyCSV auto-optimizes `File.Stream` to binary chunk mode. Also supports arbitrary binary chunks for non-file streams.

7. **Real-world vs synthetic** — Synthetic benchmarks show 3.7-18x gains; real-world TSV shows 13-28% gains due to simpler data patterns.

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

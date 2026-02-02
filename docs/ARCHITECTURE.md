# RustyCSV Architecture

A purpose-built Rust NIF for ultra-fast CSV parsing in Elixir. Not a wrapper around an existing library—custom-built from the ground up for optimal BEAM integration.

## Key Innovations

### Purpose-Built, Not Wrapped

Unlike projects that wrap existing Rust crates (like the excellent `csv` crate), RustyCSV is **designed specifically for Elixir**:

- **Direct BEAM term construction** - Results go straight to Erlang terms, no intermediate serialization
- **ResourceArc integration** - Streaming parser state managed by BEAM's garbage collector
- **Dirty scheduler awareness** - Long operations run on dirty CPU schedulers
- **Zero-copy where possible** - `Cow<[u8]>` borrows data, only allocates for quote unescaping
- **Sub-binary support** - Optional zero-copy mode using BEAM sub-binary references

### Six Parsing Strategies

RustyCSV offers unmatched flexibility with six parsing strategies:

| Strategy | Innovation |
|----------|------------|
| `:simd` | Shared SIMD structural scan + `Cow`-based field extraction (default) |
| `:parallel` | Shared SIMD scan + multi-threaded field extraction via `rayon` |
| `:streaming` | Stateful parser with bounded memory, handles multi-GB files |
| `:indexed` | Shared SIMD scan + two-phase index-then-extract for row range access |
| `:zero_copy` | Shared SIMD scan + sub-binary references for maximum speed |
| `:basic` | Shared SIMD scan + basic field extraction (debugging, baseline) |

### Memory Efficiency

- **Configurable memory model** - Choose between copying (frees input early) or sub-binaries (zero-copy)
- **Streaming bounded memory** - Process 10GB+ files with ~64KB memory footprint
- **mimalloc allocator** - High-performance allocator for reduced fragmentation
- **Optional memory tracking** - Opt-in profiling with zero overhead when disabled

### Validated Correctness

- **367 tests** covering RFC 4180, industry test suites, edge cases, encodings, multi-byte separators/escapes, and headers-to-maps
- **Cross-strategy validation** - All 6 strategies produce identical output
- **NimbleCSV compatibility** - Verified identical behavior for all API functions

## Quick Start

```elixir
# Use the pre-defined RFC4180 parser (like NimbleCSV.RFC4180)
alias RustyCSV.RFC4180, as: CSV

# Parse CSV (skips headers by default, like NimbleCSV)
CSV.parse_string("name,age\njohn,27\n")
#=> [["john", "27"]]

# Include headers
CSV.parse_string("name,age\njohn,27\n", skip_headers: false)
#=> [["name", "age"], ["john", "27"]]

# Choose strategy for large files
CSV.parse_string(huge_csv, strategy: :parallel)

# Use zero-copy for maximum speed (keeps parent binary alive)
CSV.parse_string(data, strategy: :zero_copy)

# Stream large files (uses bounded-memory streaming parser)
"huge.csv"
|> File.stream!()
|> CSV.parse_stream()
|> Stream.each(&process/1)
|> Stream.run()

# Dump back to CSV
CSV.dump_to_iodata([["a", "b"], ["1", "2"]])
#=> "a,b\n1,2\n"
```

## NimbleCSV API Compatibility

RustyCSV implements the complete NimbleCSV API:

| Function | Description | Status |
|----------|-------------|--------|
| `parse_string/2` | Parse CSV string to list of rows | ✅ |
| `parse_stream/2` | Lazily parse a stream | ✅ |
| `parse_enumerable/2` | Parse any enumerable | ✅ |
| `dump_to_iodata/1` | Convert rows to iodata | ✅ |
| `dump_to_stream/1` | Lazily convert rows to iodata stream | ✅ |
| `to_line_stream/1` | Convert arbitrary chunks to lines | ✅ |
| `options/0` | Return module configuration | ✅ |

### `define/2` Options

| Option | NimbleCSV | RustyCSV | Status |
|--------|-----------|----------|--------|
| `:separator` | ✅ Any | ✅ Any (single or multi-byte) | ✅ |
| `:escape` | ✅ Any | ✅ Any (single or multi-byte) | ✅ |
| `:line_separator` | ✅ | ✅ | ✅ |
| `:newlines` | ✅ | ✅ | ✅ |
| `:trim_bom` | ✅ | ✅ | ✅ |
| `:dump_bom` | ✅ | ✅ | ✅ |
| `:reserved` | ✅ | ✅ | ✅ |
| `:escape_formula` | ✅ | ✅ | ✅ |
| `:moduledoc` | ✅ | ✅ | ✅ |
| `:encoding` | ✅ | ✅ | Full support |

### Migration from NimbleCSV

```elixir
# Before
alias NimbleCSV.RFC4180, as: CSV

# After
alias RustyCSV.RFC4180, as: CSV

# That's it! All function calls work identically.
```

### RustyCSV Extensions

RustyCSV adds options not in NimbleCSV:

```elixir
# Choose parsing strategy (RustyCSV only)
CSV.parse_string(data, strategy: :parallel)
CSV.parse_string(data, strategy: :zero_copy)

# Return maps instead of lists (RustyCSV only)
CSV.parse_string(data, headers: true)
#=> [%{"name" => "john", "age" => "27"}]

CSV.parse_string(data, headers: [:name, :age])
#=> [%{name: "john", age: "27"}]
```

## Parsing Strategies

RustyCSV implements six parsing strategies, each optimized for different use cases:

All five batch strategies (`:simd`, `:basic`, `:indexed`, `:parallel`, `:zero_copy`) share a single-pass SIMD structural scanner (`scan_structural`) that finds every unquoted separator and row ending in one sweep, producing a `StructuralIndex`. The strategies differ only in how they extract field data from the index.

| Strategy | Description | Best For |
|----------|-------------|----------|
| `:simd` | SIMD scan + `Cow`-based field extraction (default) | Most files - fastest general purpose |
| `:basic` | SIMD scan + basic field extraction | Debugging, baseline comparison |
| `:indexed` | SIMD scan + two-phase index-then-extract | When you need to re-extract rows |
| `:parallel` | SIMD scan + rayon parallel field extraction | Large files with many cores |
| `:zero_copy` | SIMD scan + sub-binary references | Maximum speed, controlled memory lifetime |
| `:streaming` | Stateful chunked parser | Unbounded files, bounded memory |

### Strategy Selection Guide

```
File Size        Recommended Strategy
─────────────────────────────────────────────────────────────
< 1 MB           :simd (default) or :zero_copy
1-500 MB         :simd or :zero_copy
Large files      :parallel or :zero_copy
Unbounded        streaming (parse_stream)
Memory-sensitive :simd (copies data, frees input immediately)
Speed-sensitive  :zero_copy (sub-binaries, keeps input alive)
```

### Memory Model Trade-offs

| Strategy | Memory Model | Input Binary | Best When |
|----------|--------------|--------------|-----------|
| `:simd`, `:basic`, `:indexed`, `:parallel` | Copy | Freed immediately | Processing subsets, memory-constrained |
| `:zero_copy` | Sub-binary | Kept alive | Speed-critical, short-lived results |
| `:streaming` | Copy (chunked) | Freed per chunk | Unbounded files |

## Project Structure

```
native/rustycsv/src/
├── lib.rs                 # NIF entry points, separator/escape decoding, dispatch
├── core/
│   ├── mod.rs            # Re-exports
│   ├── simd_scanner.rs   # Single-pass SIMD structural scanner (prefix-XOR quote detection)
│   ├── simd_index.rs     # StructuralIndex, RowIter, RowFieldIter, FieldIter
│   ├── scanner.rs        # Byte-level helpers (separator matching)
│   ├── field.rs          # Field extraction, quote handling
│   └── newlines.rs       # Custom newline support
├── strategy/
│   ├── mod.rs            # Strategy exports
│   ├── direct.rs         # A/B: Basic and SIMD strategies (consume StructuralIndex)
│   ├── two_phase.rs      # C: Index-then-extract (StructuralIndex → CsvIndex bridge)
│   ├── streaming.rs      # D: Stateful chunked parser (single-byte fast path)
│   ├── parallel.rs       # E: Rayon-based parallel (StructuralIndex for row ranges)
│   ├── zero_copy.rs      # F: Sub-binary boundary parsing (consume StructuralIndex)
│   └── general.rs        # Multi-byte separator/escape support (all strategies)
├── term.rs               # Term building (lists + maps, copy + sub-binary, multi-byte escape)
└── resource.rs           # ResourceArc for streaming parser (single-byte + general)

lib/
├── rusty_csv.ex          # Main module with define/2 macro, types, specs
├── rusty_csv/
│   ├── native.ex         # NIF stubs with full documentation
│   └── streaming.ex      # Elixir streaming interface
```

## Implementation Details

### SIMD Structural Scanner

All batch strategies share a single-pass SIMD structural scanner (`scan_structural` in `simd_scanner.rs`) inspired by simdjson's approach. It processes the entire input once and produces a `StructuralIndex` containing the positions of all unquoted separators and row endings.

**How it works:**

1. Load 16-byte chunks (or 32-byte on AVX2) into SIMD registers
2. Compare against separator, quote, `\n`, and `\r` characters simultaneously
3. Use **prefix-XOR** on the quote bitmask to determine which positions are inside quoted regions — a cumulative XOR where bit *i* is set if there's an odd number of quotes before position *i*
4. Mask out quoted positions, then extract the remaining separator and newline positions into `Vec<u32>` arrays
5. A `quote_carry` bit tracks quote parity across chunk boundaries

The prefix-XOR uses a portable shift-and-xor cascade on all targets (6 XOR+shift ops on a u64). Architecture-specific intrinsics (CLMUL, PMULL) were evaluated but removed — benchmarks showed no measurable difference for the 16/32-bit masks used in CSV scanning, and removing them keeps the entire scanner free of `unsafe` code.

**`std::simd` API surface:** The scanner uses only the stabilization-safe subset of `portable_simd`: `Simd::from_slice`, `splat`, `simd_eq`, `to_bitmask`, and bitwise ops. It avoids the APIs [blocking stabilization](https://github.com/rust-lang/portable-simd/issues/364) (swizzle, scatter/gather, lane-count generics). No `std::arch` intrinsics are used.

**Output — `StructuralIndex`:**

```rust
pub struct StructuralIndex {
    pub field_seps: Vec<u32>,   // positions of unquoted separators
    pub row_ends: Vec<RowEnd>,  // positions of unquoted row terminators
    pub input_len: u32,
}
```

Positions use `u32` (4 GB cap) to halve memory vs `usize` on 64-bit. Strategies consume the index via `rows_with_fields()`, a cursor-based iterator that yields `(row_start, row_content_end, FieldIter)` tuples by advancing a linear cursor through `field_seps` — O(total_seps) across all rows instead of O(rows × log(seps)) with binary search.

### Quote Handling with Cow

All strategies properly handle CSV quote escaping (doubled quotes `""` → `"`). This is achieved using `Cow<'_, [u8]>` which:
- Returns borrowed slices when no unescaping needed (zero-copy)
- Allocates owned data only when escaped quotes must be processed

```rust
pub fn extract_field_cow(input: &[u8], start: usize, end: usize) -> Cow<'_, [u8]> {
    // Fast path: not quoted or no escaped quotes inside
    if !needs_unescaping(field) {
        return Cow::Borrowed(field);
    }
    // Slow path: unescape "" to "
    Cow::Owned(unescape_quotes(field))
}
```

### Strategy A/B: Direct Parsing

Both basic and SIMD strategies call `scan_structural` to build a `StructuralIndex`, then iterate rows via `rows_with_fields()` extracting fields with `extract_field_cow_with_escape`. The two strategies are now functionally identical (both use the SIMD scanner); `:basic` is retained as a named alias for debugging.

### Strategy C: Two-Phase Index-then-Extract

1. **Phase 1**: `scan_structural` → `StructuralIndex` (shared SIMD scan)
2. **Bridge**: `structural_to_csv_index` converts to the legacy `CsvIndex` (row bounds + field bounds)
3. **Phase 2**: Extract fields using the index with `Cow`-based unescaping

Benefits: Better cache utilization, can skip rows via `extract_rows` range queries, predictable memory.

### Strategy D: Streaming Parser

Stateful parser wrapped in `ResourceArc` for NIF resource management. The resource
holds a `StreamingParserEnum` that dispatches between the single-byte fast path
and the general multi-byte path:

```rust
pub enum StreamingParserEnum {
    SingleByte(StreamingParser),
    General(GeneralStreamingParser),
}
```

Both variants share the same interface (feed, take_rows, finalize, etc.) and are
selected at creation time based on separator/escape lengths.

Key features:
- Owns data (`Vec<u8>`) because input chunks are temporary
- Tracks `scan_pos` to resume parsing where it left off
- Preserves quote state across chunks
- Enforces a configurable maximum buffer size (default 256 MB) to prevent unbounded
  memory growth; raises `:buffer_overflow` if exceeded
- Mutex-protected access with poisoning recovery (raises `:mutex_poisoned` instead
  of panicking the VM)

### Strategy E: Parallel Parser

Uses rayon for multi-threaded row parsing on a **dedicated thread pool** (`rustycsv-*`
threads, capped at 8) to avoid contention with other Rayon users in the same VM:

1. **Single-threaded**: `scan_structural` → `StructuralIndex` (row boundaries + field separator positions)
2. **O(n) cursor walk**: Collect `(row_start, content_end, sep_lo, sep_hi)` into a flat `Vec`, mapping each row to its slice of `field_seps`
3. **Parallel**: Each worker indexes directly into the shared `&[u32]` field_seps slice — no re-scanning, no per-row allocation

Uses `DirtyCpu` scheduler to avoid blocking normal BEAM schedulers.

**Evolution of field-position reuse from the structural index:**

The SIMD structural scanner already finds every separator position. Three approaches were benchmarked for reusing those positions instead of re-scanning with memchr:

- **Approach A** — Pre-collect `Vec<Vec<(u32, u32)>>` field bounds: 10K+ inner Vec allocations cost more than the memchr scan (-18% on simple CSV).
- **Approach B** — Binary search via `fields_in_row()`: Two `partition_point` calls per row add O(log n) overhead (-11% on simple CSV).
- **Approach C** (current) — Flat index + direct slice: O(n) cursor walk builds a single flat Vec mapping each row to its slice of `field_seps`. Each worker indexes into the shared `&[u32]` with O(1) lookup. This avoids A's allocation overhead and B's binary search overhead, improving performance +1-12% vs the memchr baseline depending on workload.

**Note**: Parallel mode involves a double-copy (Rust Vec → BEAM binary) because BEAM terms cannot be constructed on worker threads. The SIMD structural scan makes phase 1 fast enough that `:parallel` is now competitive at all file sizes, not just 500MB+.

### Strategy F: Zero-Copy Parser

Returns BEAM sub-binary references instead of copying data:

```rust
fn field_to_term_hybrid(env, input: &Binary, start, end, escape) -> Term {
    let field = &input.as_slice()[start..end];

    // Check if quoted with escapes
    if needs_unescaping(field) {
        // Must copy and unescape: "val""ue" -> val"ue
        return copy_and_unescape(field);
    }

    // Zero-copy: create sub-binary reference (safe API)
    input.make_subbinary(start, len).unwrap().into()
}
```

**Hybrid approach**:
- Unquoted fields → sub-binary (zero-copy)
- Quoted without escapes → sub-binary of inner content (zero-copy)
- Quoted with escapes → copy and unescape (must allocate)

**Trade-off**: Sub-binaries keep the parent binary alive until all field references are garbage collected.

### Headers-to-Maps

Two dedicated NIFs (`parse_to_maps`, `parse_to_maps_parallel`) return rows as Elixir maps instead of lists. They reuse all existing parsing strategy code — only the term conversion layer differs.

**Architecture:**
```
headers: false (default)  →  existing NIFs  →  cow_rows_to_term (list of lists)
headers: true/[...]       →  new NIFs       →  cow_rows_to_maps (list of maps)
```

**Key interning**: Header terms are allocated once in Rust and reused for every row, avoiding repeated binary allocation for map keys.

**Header modes**:
- `headers: true` — first CSV row parsed as string keys, remaining rows become maps
- `headers: [atoms/strings]` — explicit key terms passed from Elixir, optionally skipping the first row

**Streaming**: Uses Elixir-side `Stream.transform` for map conversion rather than a new NIF, since the streaming parser already yields one row at a time and the map wrapping overhead is negligible.

**Edge case handling** (all in Rust):
- Fewer columns than keys → `nil` fill
- More columns than keys → extra columns ignored
- Duplicate keys → last value wins (incremental map building fallback)

---

## Performance Optimizations

### mimalloc Allocator

RustyCSV uses [mimalloc](https://github.com/microsoft/mimalloc) as the default allocator:

```rust
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
```

Benefits:
- 10-20% faster allocation for many small objects
- Reduced fragmentation
- No tracking overhead in default configuration

To disable mimalloc (for exotic build targets):
```toml
[dependencies]
rusty_csv = { version = "0.1", default-features = false }
```

### Optional Memory Tracking (Benchmarking Only)

For profiling Rust-side memory during development and benchmarking, enable the `memory_tracking` feature in `native/rustycsv/Cargo.toml`. This is not intended for production — it wraps every allocation/deallocation with atomic counter updates, adding overhead. It is also the only source of `unsafe` in the codebase (required by the `GlobalAlloc` trait).

```toml
[features]
default = ["mimalloc", "memory_tracking"]
```

This wraps the allocator with tracking overhead:

```rust
#[cfg(feature = "memory_tracking")]
#[global_allocator]
static GLOBAL: tracking::TrackingAllocator = tracking::TrackingAllocator;
```

When enabled, these functions return actual values:
- `RustyCSV.Native.get_rust_memory/0` - Current allocation
- `RustyCSV.Native.get_rust_memory_peak/0` - Peak allocation
- `RustyCSV.Native.reset_rust_memory_stats/0` - Reset and get stats

When disabled (default), they return `0` with zero overhead.

### Pre-allocated Vectors

The structural scanner pre-allocates vectors with capacity estimates based on input size:

```rust
let est_seps = input.len() / 10 + 16;  // ~1 separator per 10 bytes
let est_rows = input.len() / 50 + 4;   // ~1 row per 50 bytes
```

This reduces reallocation overhead during the scan pass.

---

## Background: Why a Rust NIF?

### NimbleCSV Strengths

NimbleCSV is remarkably fast for pure Elixir:
- Binary pattern matching is highly optimized
- Sub-binary references provide zero-copy field extraction
- Match context optimization for continuous parsing

### RustyCSV Advantages

1. **Multiple strategies** - Choose the right tool for each workload
2. **Streaming support** - Process arbitrarily large files with bounded memory
3. **Reduced scheduler load** - Offload parsing to native code
4. **Competitive speed** - 3.7x-12.5x faster on typical workloads, up to 18x on quoted data
5. **Flexible memory model** - Copy or sub-binary, your choice
6. **NIF safety** - Dirty schedulers for long operations

## NIF Safety

### The 1ms Rule

NIFs should complete in under 1ms to avoid blocking schedulers.

| Approach | Used By | Description |
|----------|---------|-------------|
| Dirty Schedulers | All batch NIFs, `streaming_feed`, `streaming_next_rows`, `streaming_finalize` | Separate from normal schedulers |
| Chunked Processing | streaming | Return control between chunks |
| Stateful Resource | streaming | Let Elixir control iteration |
| Fast SIMD | all others | Complete quickly via hardware acceleration |

All 12 NIFs that process unbounded input run on dirty CPU schedulers. Only trivial O(1)
NIFs (`streaming_new`, `streaming_status`, `streaming_set_max_buffer`, memory tracking)
remain on the normal scheduler.

### Memory Safety

All application code is **zero `unsafe`**. The only `unsafe` in the codebase is the `GlobalAlloc` trait impl behind the opt-in `memory_tracking` feature flag — this is a development-only benchmarking tool for profiling Rust-side allocations, not intended for production use. The `unsafe` is required by the `GlobalAlloc` trait definition and cannot be avoided. It is disabled by default and adds measurable overhead when enabled.

- Copy-based strategies copy data to BEAM terms, then free Rust memory
- Zero-copy strategy creates sub-binary references via rustler's safe `Binary::make_subbinary` API (bounds-checked, returns `NifResult`)
- SIMD scanner uses `std::simd` portable SIMD with no `std::arch` intrinsics
- Streaming parser uses `ResourceArc` with proper cleanup
- Streaming buffer is capped at 256 MB by default (configurable via `:max_buffer_size`)
- Mutex poisoning on streaming resources raises `:mutex_poisoned` instead of panicking
- mimalloc wrapped in tracking allocator for observability
- Dedicated rayon thread pool avoids contention with other Rayon users

## Benchmark Results

- **Synthetic benchmarks**: 3.7x-12.5x faster than NimbleCSV for typical data, up to 18x for heavily quoted CSVs
- **Real-world TSV**: 13-28% faster than NimbleCSV (10K+ rows)
- **Streaming**: 2.2x faster than NimbleCSV for line-based streams; RustyCSV uniquely handles binary chunks

The speedup varies by data complexity—quoted fields with escapes show the largest gains.

See [BENCHMARK.md](BENCHMARK.md) for detailed methodology, real-world results, and raw data.

## Documentation

RustyCSV includes comprehensive documentation for hexdocs:

- **Module docs**: Detailed guides with examples
- **Type specs**: `@type`, `@typedoc` for all public types
- **Function specs**: `@spec` for all public functions
- **Examples**: Runnable examples in docstrings
- **Callbacks**: Full behaviour definition for generated modules

## Compliance & Validation

RustyCSV is validated against industry-standard CSV test suites to ensure correctness:

- **RFC 4180 Compliance** - Full compliance with the CSV specification
- **csv-spectrum** - Industry "acid test" for CSV parsers (12 test cases)
- **csv-test-data** - Comprehensive RFC 4180 test suite (17+ test cases)
- **Cross-strategy validation** - All strategies produce identical output

See [COMPLIANCE.md](COMPLIANCE.md) for full details on test suites and validation methodology.

## Future Work

- **Error positions**: Line/column numbers in ParseError

## References

- [RFC 4180](https://tools.ietf.org/html/rfc4180) - CSV specification
- [csv-spectrum](https://github.com/max-mapper/csv-spectrum) - CSV acid test suite
- [csv-test-data](https://github.com/sineemore/csv-test-data) - RFC 4180 test data
- [NimbleCSV Source](https://github.com/dashbitco/nimble_csv)
- [BEAM Binary Handling](https://www.erlang.org/doc/efficiency_guide/binaryhandling.html)
- [simdjson](https://github.com/simdjson/simdjson) - Inspiration for structural index and prefix-XOR quote detection
- [std::simd](https://doc.rust-lang.org/std/simd/index.html) - Portable SIMD (used in structural scanner)
- [rayon crate](https://docs.rs/rayon/latest/rayon/) - Parallel iteration
- [mimalloc](https://github.com/microsoft/mimalloc) - High-performance allocator

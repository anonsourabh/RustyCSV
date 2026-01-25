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
| `:simd` | Hardware-accelerated delimiter scanning via `memchr` crate |
| `:parallel` | Multi-threaded row parsing via `rayon`, runs on dirty schedulers |
| `:streaming` | Stateful parser with bounded memory, handles multi-GB files |
| `:indexed` | Two-phase approach enables row range extraction without full parse |
| `:zero_copy` | Sub-binary references for maximum speed (like NimbleCSV's memory model) |
| `:basic` | Reference implementation for correctness validation |

### Memory Efficiency

- **Configurable memory model** - Choose between copying (frees input early) or sub-binaries (zero-copy)
- **Streaming bounded memory** - Process 10GB+ files with ~64KB memory footprint
- **mimalloc allocator** - High-performance allocator for reduced fragmentation
- **Optional memory tracking** - Opt-in profiling with zero overhead when disabled

### Validated Correctness

- **147 tests** covering RFC 4180, industry test suites, edge cases, and encodings
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
| `:separator` | ✅ Any | ✅ Any single-byte | ✅ |
| `:escape` | ✅ Any | ✅ Any single-byte | ✅ |
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

RustyCSV adds one additional option not in NimbleCSV:

```elixir
# Choose parsing strategy (RustyCSV only)
CSV.parse_string(data, strategy: :parallel)
CSV.parse_string(data, strategy: :zero_copy)
```

## Parsing Strategies

RustyCSV implements six parsing strategies, each optimized for different use cases:

| Strategy | Description | Best For |
|----------|-------------|----------|
| `:simd` | SIMD-accelerated via memchr (default) | Most files - fastest general purpose |
| `:basic` | Simple byte-by-byte parsing | Debugging, baseline comparison |
| `:indexed` | Two-phase index-then-extract | When you need to re-extract rows |
| `:parallel` | Multi-threaded via rayon | Very large files (500MB+) with complex quoting |
| `:zero_copy` | Sub-binary references | Maximum speed, controlled memory lifetime |
| `:streaming` | Stateful chunked parser | Unbounded files, bounded memory |

### Strategy Selection Guide

```
File Size        Recommended Strategy
─────────────────────────────────────────────────────────────
< 1 MB           :simd (default) or :zero_copy
1-500 MB         :simd or :zero_copy
500 MB+          :parallel (with complex quoted data) or :zero_copy
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
├── lib.rs                 # NIF entry points, memory tracking, mimalloc
├── core/
│   ├── mod.rs            # Re-exports
│   ├── scanner.rs        # SIMD row/field boundary detection (memchr3)
│   └── field.rs          # Field extraction, quote handling
├── strategy/
│   ├── mod.rs            # Strategy exports
│   ├── direct.rs         # A/B: Basic and SIMD strategies
│   ├── two_phase.rs      # C: Index-then-extract
│   ├── streaming.rs      # D: Stateful chunked parser
│   ├── parallel.rs       # E: Rayon-based parallel
│   └── zero_copy.rs      # F: Sub-binary boundary parsing
├── term.rs               # Term building (copy + sub-binary)
└── resource.rs           # ResourceArc for streaming parser

lib/
├── rusty_csv.ex          # Main module with define/2 macro, types, specs
├── rusty_csv/
│   ├── native.ex         # NIF stubs with full documentation
│   └── streaming.ex      # Elixir streaming interface
```

## Implementation Details

### SIMD-Accelerated Row Scanning

Row boundary detection uses `memchr3` for hardware-accelerated scanning:

```rust
// Outside quotes: SIMD jump to next interesting byte
match memchr3(escape, b'\n', b'\r', &input[pos..]) {
    Some(offset) => {
        let found = pos + offset;
        match input[found] {
            b if b == escape => { in_quotes = true; pos = found + 1; }
            b'\n' => { starts.push(pos + 1); pos = found + 1; }
            b'\r' => { /* handle CRLF */ }
        }
    }
    None => break,
}

// Inside quotes: SIMD jump to next escape char only
match memchr(escape, &input[pos..]) {
    Some(offset) => {
        // Handle escaped quote "" (RFC 4180)
        if input[found + 1] == escape {
            pos = found + 2; // Skip both, stay in quotes
        } else {
            in_quotes = false;
        }
    }
    None => break,
}
```

This approach skips over non-interesting bytes using SIMD, only examining positions where quotes or newlines appear.

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

Both basic and SIMD strategies use the same quote-aware row parser with `Cow`-based field extraction. The SIMD version uses `memchr` for faster delimiter scanning.

### Strategy C: Two-Phase Index-then-Extract

1. **Phase 1**: Build index of row/field boundaries (fast scan)
2. **Phase 2**: Extract fields using the index with `Cow`-based unescaping

Benefits: Better cache utilization, can skip rows, predictable memory.

### Strategy D: Streaming Parser

Stateful parser wrapped in `ResourceArc` for NIF resource management:

```rust
pub struct StreamingParser {
    buffer: Vec<u8>,           // Holds incoming chunks
    complete_rows: Vec<...>,   // Parsed rows ready to take
    partial_row_start: usize,  // Where incomplete row begins
    scan_pos: usize,           // Resume position for scanning
    in_quotes: bool,           // Quote state across chunks
}
```

Key features:
- Owns data (`Vec<u8>`) because input chunks are temporary
- Tracks `scan_pos` to resume parsing where it left off
- Preserves `in_quotes` state for fields spanning chunks

### Strategy E: Parallel Parser

Uses rayon for multi-threaded row parsing:

1. **Single-threaded**: Find all row boundaries (SIMD-accelerated, quote-aware)
2. **Parallel**: Parse each row independently

Uses `DirtyCpu` scheduler to avoid blocking normal BEAM schedulers.

**Note**: Parallel mode involves a double-copy (Rust Vec → BEAM binary) because BEAM terms cannot be constructed on worker threads. This overhead is offset by CPU savings on large files with complex quoted fields.

### Strategy F: Zero-Copy Parser

Returns BEAM sub-binary references instead of copying data:

```rust
fn field_to_term_hybrid(env, input_term, start, end, escape) -> Term {
    let field = &input_bytes[start..end];

    // Check if quoted with escapes
    if needs_unescaping(field) {
        // Must copy and unescape: "val""ue" -> val"ue
        return copy_and_unescape(field);
    }

    // Zero-copy: create sub-binary reference
    unsafe { enif_make_sub_binary(env, input_term, start, len) }
}
```

**Hybrid approach**:
- Unquoted fields → sub-binary (zero-copy)
- Quoted without escapes → sub-binary of inner content (zero-copy)
- Quoted with escapes → copy and unescape (must allocate)

**Trade-off**: Sub-binaries keep the parent binary alive until all field references are garbage collected.

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

### Optional Memory Tracking

For profiling, enable the `memory_tracking` feature in `native/rustycsv/Cargo.toml`:

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

All parsing paths pre-allocate vectors with capacity estimates:

```rust
// Row starts: ~50 bytes per row estimate
let mut starts = Vec::with_capacity(input.len() / 50 + 1);

// Fields per row: ~8 fields estimate
let mut fields = Vec::with_capacity(8);
```

This reduces reallocation overhead during parsing.

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
4. **Competitive speed** - 3.5x-9x faster on typical workloads, up to 18x on quoted data
5. **Flexible memory model** - Copy or sub-binary, your choice
6. **NIF safety** - Dirty schedulers for long operations

## NIF Safety

### The 1ms Rule

NIFs should complete in under 1ms to avoid blocking schedulers.

| Approach | Used By | Description |
|----------|---------|-------------|
| Dirty Schedulers | `:parallel` | Separate from normal schedulers |
| Chunked Processing | streaming | Return control between chunks |
| Stateful Resource | streaming | Let Elixir control iteration |
| Fast SIMD | all others | Complete quickly via hardware acceleration |

### Memory Safety

- Copy-based strategies copy data to BEAM terms, then free Rust memory
- Zero-copy strategy creates sub-binary references (no Rust allocation)
- Streaming parser uses `ResourceArc` with proper cleanup
- mimalloc wrapped in tracking allocator for observability

## Benchmark Results

- **Synthetic benchmarks**: 3.5x-9x faster than NimbleCSV for typical data, up to 18x for heavily quoted CSVs
- **Real-world TSV**: 13-28% faster than NimbleCSV (10K+ rows)
- **Streaming**: Comparable speed to NimbleCSV for line-based streams; RustyCSV uniquely handles binary chunks

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
- [memchr crate](https://docs.rs/memchr/latest/memchr/) - SIMD byte searching
- [rayon crate](https://docs.rs/rayon/latest/rayon/) - Parallel iteration
- [mimalloc](https://github.com/microsoft/mimalloc) - High-performance allocator

# RustyCSV Architecture

A purpose-built Rust NIF for high-performance CSV parsing in Elixir. Not a wrapper around an existing library—custom-built from the ground up for optimal BEAM integration.

## Key Innovations

### Purpose-Built, Not Wrapped

Unlike projects that wrap existing Rust crates (like the excellent `csv` crate), RustyCSV is **designed specifically for Elixir**:

- **Direct BEAM term construction** - Results go straight to Erlang terms, no intermediate serialization
- **ResourceArc integration** - Streaming parser state managed by BEAM's garbage collector
- **Dirty scheduler awareness** - Long operations run on dirty CPU schedulers
- **Zero-copy where possible** - `Cow<[u8]>` borrows data, only allocates for quote unescaping

### Five Parsing Strategies

No other CSV library offers this level of flexibility:

| Strategy | Innovation |
|----------|------------|
| `:simd` | Hardware-accelerated delimiter scanning via `memchr` crate |
| `:parallel` | Multi-threaded row parsing via `rayon`, runs on dirty schedulers |
| `:streaming` | Stateful parser with bounded memory, handles multi-GB files |
| `:indexed` | Two-phase approach enables row range extraction without full parse |
| `:basic` | Reference implementation for correctness validation |

### Memory Efficiency

- **No parent binary retention** - Unlike NimbleCSV's sub-binaries, RustyCSV copies to BEAM terms then frees Rust memory
- **Built-in memory tracking** - `get_rust_memory/0` and `get_rust_memory_peak/0` for profiling
- **Streaming bounded memory** - Process 10GB+ files with ~64KB memory footprint

### Validated Correctness

- **127 tests** covering RFC 4180, industry test suites, and edge cases
- **Cross-strategy validation** - All 5 strategies produce identical output
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
| `:encoding` | ✅ | ❌ | Not yet |

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
```

## Parsing Strategies

RustyCSV implements five parsing strategies, each optimized for different use cases:

| Strategy | Description | Best For |
|----------|-------------|----------|
| `:simd` | SIMD-accelerated via memchr (default) | Most files - fastest general purpose |
| `:basic` | Simple byte-by-byte parsing | Debugging, baseline comparison |
| `:indexed` | Two-phase index-then-extract | When you need to re-extract rows |
| `:parallel` | Multi-threaded via rayon | Very large files (100MB+) |
| `:streaming` | Stateful chunked parser | Unbounded files, bounded memory |

### Strategy Selection Guide

```
File Size        Recommended Strategy
─────────────────────────────────────
< 1 MB           :simd (default)
1-100 MB         :simd or :parallel
100 MB+          :parallel or streaming
Unbounded        streaming (parse_stream)
```

## Project Structure

```
native/rustycsv/src/
├── lib.rs                 # NIF entry points, memory tracking
├── core/
│   ├── mod.rs            # Re-exports
│   ├── scanner.rs        # Row/field boundary detection (memchr)
│   └── field.rs          # Field extraction, quote handling
├── strategy/
│   ├── mod.rs            # Strategy exports
│   ├── direct.rs         # A/B: Basic and SIMD strategies
│   ├── two_phase.rs      # C: Index-then-extract
│   ├── streaming.rs      # D: Stateful chunked parser
│   └── parallel.rs       # E: Rayon-based parallel
├── term.rs               # Shared term building utilities
└── resource.rs           # ResourceArc for streaming parser

lib/
├── rusty_csv.ex          # Main module with define/2 macro, types, specs
├── rusty_csv/
│   ├── native.ex         # NIF stubs with full documentation
│   └── streaming.ex      # Elixir streaming interface
```

## Implementation Details

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

1. **Single-threaded**: Find all row boundaries (quote-aware)
2. **Parallel**: Parse each row independently

Uses `DirtyCpu` scheduler to avoid blocking normal BEAM schedulers.

---

## Background: Why a Rust NIF?

### NimbleCSV Strengths

NimbleCSV is remarkably fast for pure Elixir:
- Binary pattern matching is highly optimized
- Sub-binary references provide zero-copy field extraction
- Match context optimization for continuous parsing

### NimbleCSV Limitations

- **Memory retention**: Sub-binaries keep parent binary alive
- **No streaming**: Requires entire CSV in memory
- **Scheduler load**: All work on BEAM schedulers

### RustyCSV Advantages

1. **Streaming support** - Process arbitrarily large files with bounded memory
2. **Reduced scheduler load** - Offload parsing to native code
3. **Competitive speed** - 4-5x faster than NimbleCSV
4. **Memory efficiency** - No parent binary retention
5. **NIF safety** - Dirty schedulers for long operations

## NIF Safety

### The 1ms Rule

NIFs should complete in under 1ms to avoid blocking schedulers.

| Approach | Used By | Description |
|----------|---------|-------------|
| Dirty Schedulers | `:parallel` | Separate from normal schedulers |
| Chunked Processing | streaming | Return control between chunks |
| Stateful Resource | streaming | Let Elixir control iteration |

### Memory Safety

- All strategies copy data to BEAM terms, then free Rust memory
- Streaming parser uses `ResourceArc` with proper cleanup
- No sub-binary retention issues

## Benchmark Results

**Environment**: Apple Silicon, 15 MB CSV (100K rows, 10 columns)

| Parser | Speed | vs NimbleCSV |
|--------|-------|--------------|
| RustyCSV (:simd) | ~42ms | 4.5x faster |
| RustyCSV (:basic) | ~50ms | 3.8x faster |
| RustyCSV (:indexed) | ~45ms | 4.2x faster |
| RustyCSV (:parallel) | ~35ms | 5.4x faster |
| NimbleCSV | ~190ms | baseline |

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

- **Encoding support**: UTF-16, other encodings
- **Error positions**: Line/column numbers in ParseError

## References

- [RFC 4180](https://tools.ietf.org/html/rfc4180) - CSV specification
- [csv-spectrum](https://github.com/max-mapper/csv-spectrum) - CSV acid test suite
- [csv-test-data](https://github.com/sineemore/csv-test-data) - RFC 4180 test data
- [NimbleCSV Source](https://github.com/dashbitco/nimble_csv)
- [BEAM Binary Handling](https://www.erlang.org/doc/efficiency_guide/binaryhandling.html)
- [memchr crate](https://docs.rs/memchr/latest/memchr/) - SIMD byte searching
- [rayon crate](https://docs.rs/rayon/latest/rayon/) - Parallel iteration

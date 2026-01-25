# RustyCSV

**Ultra-fast CSV parsing for Elixir.** A purpose-built Rust NIF with six parsing strategies, SIMD acceleration, and bounded-memory streaming. Drop-in replacement for NimbleCSV.

[![Hex.pm](https://img.shields.io/hexpm/v/rusty_csv.svg)](https://hex.pm/packages/rusty_csv)
[![Tests](https://img.shields.io/badge/tests-147%20passed-brightgreen.svg)]()
[![RFC 4180](https://img.shields.io/badge/RFC%204180-compliant-blue.svg)]()

## Why RustyCSV?

**The Problem**: CSV parsing in Elixir can be optimized further:

1. **Speed**: Pure Elixir parsing, while well-optimized, can't match native code with SIMD acceleration for large files.

2. **Flexibility**: NimbleCSV offers one parsing approach. Different workloads benefit from different strategies—parallel processing for huge files, streaming for unbounded data, zero-copy for maximum speed.

3. **Binary chunk streaming**: NimbleCSV's streaming requires line-delimited input. RustyCSV can process arbitrary binary chunks (useful for network streams, compressed data, etc.).

**Why not wrap an existing Rust CSV library?** The excellent [csv](https://docs.rs/csv) crate is designed for Rust workflows, not BEAM integration. Wrapping it would require serializing data between Rust and Erlang formats—adding overhead and losing the benefits of direct term construction.

**RustyCSV's approach**: The Rust NIF is purpose-built for BEAM integration—no wrapped CSV libraries, no unnecessary abstractions, and resource-efficient at runtime with modular features you opt into—focusing on:

1. **Bounded memory streaming** - Process multi-GB files with ~64KB memory footprint
2. **No parent binary retention** - Data copied to BEAM terms, Rust memory freed immediately
3. **Multiple strategies** - Choose SIMD, parallel, streaming, or indexed based on your workload
4. **Reduced scheduler load** - Heavy parsing runs on dirty schedulers
5. **Full NimbleCSV compatibility** - Same API, drop-in replacement

## Feature Comparison

| Feature | RustyCSV | NimbleCSV |
|---------|----------|-----------|
| **Parsing strategies** | 6 (SIMD, parallel, streaming, indexed, zero_copy, basic) | 1 |
| **SIMD acceleration** | ✅ via memchr | ❌ |
| **Parallel parsing** | ✅ via rayon | ❌ |
| **Streaming (bounded memory)** | ✅ | ❌ (requires full file in memory) |
| **Encoding support** | ✅ UTF-8, UTF-16, Latin-1, UTF-32 | ✅ |
| **Memory model** | ✅ Choice of copy or sub-binary | Sub-binary only |
| **High-performance allocator** | ✅ mimalloc | System |
| **Drop-in replacement** | ✅ Same API | - |
| **RFC 4180 compliant** | ✅ 147 tests | ✅ |
| **Benchmark (7MB CSV)** | ~24ms | ~219ms |

## Purpose-Built for Elixir

RustyCSV isn't a wrapper around an existing Rust CSV library. It's **custom-built from the ground up** for optimal Elixir/BEAM integration:

- **Zero-copy field extraction** using Rust's `Cow<[u8]>` - borrows data when possible, only allocates when quote unescaping is needed
- **Dirty scheduler aware** - long-running parallel parses run on dirty CPU schedulers, never blocking your BEAM schedulers
- **ResourceArc-based streaming** - stateful parser properly integrated with BEAM's garbage collector
- **Direct term building** - results go straight to BEAM terms, no intermediate allocations

### Six Parsing Strategies

Choose the right tool for the job:

| Strategy | Use Case | How It Works |
|----------|----------|--------------|
| `:simd` | **Default.** Fastest for most files | SIMD-accelerated delimiter scanning via `memchr` |
| `:parallel` | Files 500MB+ with complex quoting | Multi-threaded row parsing via `rayon` |
| `:streaming` | Unbounded/huge files | Bounded-memory chunk processing |
| `:indexed` | Re-extracting row ranges | Two-phase index-then-extract |
| `:zero_copy` | Maximum speed, short-lived data | Sub-binary references (like NimbleCSV) |
| `:basic` | Debugging, baselines | Simple byte-by-byte parsing |

**Memory Model Trade-offs:**

| Strategy | Memory Model | Input Binary | Best When |
|----------|--------------|--------------|-----------|
| `:simd`, `:parallel`, `:indexed`, `:basic` | Copy | Freed immediately | Default, memory-constrained |
| `:zero_copy` | Sub-binary | Kept alive | Speed-critical, short-lived results |
| `:streaming` | Copy (chunked) | Freed per chunk | Unbounded files |

```elixir
# Automatic strategy selection
CSV.parse_string(data)                           # Uses :simd (default)
CSV.parse_string(data, strategy: :zero_copy)     # Maximum speed
CSV.parse_string(huge_data, strategy: :parallel) # 500MB+ files with complex quoting
File.stream!("huge.csv") |> CSV.parse_stream()   # Bounded memory
```

## Installation

```elixir
def deps do
  [{:rusty_csv, "~> 0.2.0"}]
end
```

Requires Rust 1.70+ (automatically compiled via Rustler).

## Quick Start

```elixir
alias RustyCSV.RFC4180, as: CSV

# Parse CSV (skips headers by default, like NimbleCSV)
CSV.parse_string("name,age\njohn,27\njane,30\n")
#=> [["john", "27"], ["jane", "30"]]

# Include headers
CSV.parse_string(csv, skip_headers: false)
#=> [["name", "age"], ["john", "27"], ["jane", "30"]]

# Stream large files with bounded memory
"huge.csv"
|> File.stream!()
|> CSV.parse_stream()
|> Stream.each(&process_row/1)
|> Stream.run()

# Dump back to CSV
CSV.dump_to_iodata([["name", "age"], ["john", "27"]])
#=> "name,age\r\njohn,27\r\n"
```

## Drop-in NimbleCSV Replacement

```elixir
# Before
alias NimbleCSV.RFC4180, as: CSV

# After
alias RustyCSV.RFC4180, as: CSV

# That's it. Same API, 3-9x faster on typical workloads.
```

All NimbleCSV functions are supported:

| Function | Description |
|----------|-------------|
| `parse_string/2` | Parse CSV string to list of rows |
| `parse_stream/2` | Lazily parse a stream (bounded memory) |
| `parse_enumerable/2` | Parse any enumerable |
| `dump_to_iodata/1` | Convert rows to iodata |
| `dump_to_stream/1` | Lazily convert rows to iodata stream |
| `to_line_stream/1` | Convert arbitrary chunks to lines |
| `options/0` | Return module configuration |

## Benchmarks

**3.5x-9x faster than NimbleCSV** on synthetic benchmarks for typical data. Up to **18x faster** on heavily quoted CSVs.

**13-28% faster than NimbleCSV** on real-world TSV files (10K+ rows). Speedup varies by data complexity—quoted fields with escapes show the largest gains.

```bash
mix run bench/csv_bench.exs
```

See [docs/BENCHMARK.md](docs/BENCHMARK.md) for detailed methodology and results.

### When to Use RustyCSV

| Scenario | Recommendation |
|----------|----------------|
| **Large files (1-500MB)** | ✅ Use `:zero_copy` or `:simd` - biggest wins |
| **Very large files (500MB+)** | ✅ Use `:parallel` with complex quoted data |
| **Huge/unbounded files** | ✅ Use `parse_stream/2` - bounded memory |
| **Memory-constrained** | ✅ Use default `:simd` - copies data, frees input |
| **Maximum speed** | ✅ Use `:zero_copy` - sub-binary refs |
| **High-throughput APIs** | ✅ Reduced scheduler load |
| **Small files (<100KB)** | Either works - NIF overhead negligible |
| **Need pure Elixir** | Use NimbleCSV |

## Custom Parsers

Define parsers with custom separators and options:

```elixir
# TSV parser
RustyCSV.define(MyApp.TSV,
  separator: "\t",
  escape: "\"",
  line_separator: "\n"
)

# Pipe-separated
RustyCSV.define(MyApp.PSV,
  separator: "|",
  escape: "\"",
  line_separator: "\n"
)

MyApp.TSV.parse_string("a\tb\tc\n1\t2\t3\n")
#=> [["1", "2", "3"]]
```

### Define Options

| Option | Description | Default |
|--------|-------------|---------|
| `:separator` | Field separator (any single byte) | `","` |
| `:escape` | Quote character | `"\""` |
| `:line_separator` | Line ending for dumps | `"\r\n"` |
| `:newlines` | Accepted line endings | `["\r\n", "\n"]` |
| `:encoding` | Character encoding (see below) | `:utf8` |
| `:trim_bom` | Remove BOM when parsing | `false` |
| `:dump_bom` | Add BOM when dumping | `false` |
| `:escape_formula` | Escape formula injection | `nil` |
| `:strategy` | Default parsing strategy | `:simd` |

### Encoding Support

RustyCSV supports character encoding conversion, matching NimbleCSV's encoding options:

```elixir
# UTF-16 Little Endian (Excel/Windows exports)
RustyCSV.define(MyApp.Spreadsheet,
  separator: "\t",
  encoding: {:utf16, :little},
  trim_bom: true,
  dump_bom: true
)

# Or use the pre-defined spreadsheet parser
alias RustyCSV.Spreadsheet
Spreadsheet.parse_string(utf16_data)
```

| Encoding | Description |
|----------|-------------|
| `:utf8` | UTF-8 (default, no conversion overhead) |
| `:latin1` | ISO-8859-1 / Latin-1 |
| `{:utf16, :little}` | UTF-16 Little Endian |
| `{:utf16, :big}` | UTF-16 Big Endian |
| `{:utf32, :little}` | UTF-32 Little Endian |
| `{:utf32, :big}` | UTF-32 Big Endian |

## RFC 4180 Compliance

RustyCSV is **fully RFC 4180 compliant** and validated against industry-standard test suites:

| Test Suite | Tests | Status |
|------------|-------|--------|
| [csv-spectrum](https://github.com/max-mapper/csv-spectrum) | 12 | ✅ All pass |
| [csv-test-data](https://github.com/sineemore/csv-test-data) | 17 | ✅ All pass |
| Edge cases (PapaParse-inspired) | 53 | ✅ All pass |
| Core + NimbleCSV compat | 36 | ✅ All pass |
| Encoding (UTF-16, Latin-1, etc.) | 20 | ✅ All pass |
| **Total** | **147** | ✅ |

See [docs/COMPLIANCE.md](docs/COMPLIANCE.md) for full compliance details.

## How It Works

### Why Not Wrap the Rust `csv` Crate?

The Rust ecosystem has excellent CSV libraries like [csv](https://docs.rs/csv) and [polars](https://docs.rs/polars). But wrapping them for Elixir has overhead:

1. Parse CSV → Rust data structures (allocation)
2. Convert Rust structs → Erlang terms (allocation + serialization)
3. Return to BEAM

RustyCSV eliminates the middle step by parsing directly into BEAM terms:

1. Parse CSV → Erlang terms directly (single pass)
2. Return to BEAM

### Strategy Implementations

Each strategy takes a different approach. All share direct term building (no intermediate Rust structs), but differ in how they scan and parse:

| Strategy | Scanning | Parsing | Memory | Best For |
|----------|----------|---------|--------|----------|
| `:basic` | Byte-by-byte | Sequential | O(n) | Debugging, correctness reference |
| `:simd` | SIMD via [memchr](https://docs.rs/memchr) | Sequential | O(n) | Default, fastest for most files |
| `:indexed` | SIMD | Two-phase (index, then extract) | O(n) + index | Re-extracting row ranges |
| `:parallel` | SIMD via [memchr3](https://docs.rs/memchr) | Multi-threaded via [rayon](https://docs.rs/rayon) | O(n) | Very large files (500MB+) with complex quoting |
| `:zero_copy` | SIMD | Sub-binary references | O(n) | Maximum speed, short-lived data |
| `:streaming` | Byte-by-byte | Stateful chunks | O(chunk) | Unbounded/huge files |

**Shared across all strategies:**
- `Cow<[u8]>` for zero-copy field extraction when no unescaping needed
- Direct Erlang term construction via Rustler (no serde)
- [mimalloc](https://github.com/microsoft/mimalloc) high-performance allocator

**`:parallel` specifics:**
- Runs on dirty CPU schedulers to avoid blocking BEAM
- Row boundaries found via SIMD-accelerated `memchr3` (quote-aware), then rows parsed in parallel

**`:zero_copy` specifics:**
- Returns BEAM sub-binary references instead of copying data
- Hybrid approach: sub-binaries for clean fields, copies only when unescaping `""` → `"`
- Trade-off: keeps parent binary alive until all field references are GC'd

**`:streaming` specifics:**
- `ResourceArc` integrates parser state with BEAM GC
- Tracks quote state across chunk boundaries
- Copies field data (since input chunks are temporary)

## Architecture

RustyCSV is built with a modular Rust architecture:

```
native/rustycsv/src/
├── lib.rs              # NIF entry points, mimalloc allocator
├── core/
│   ├── scanner.rs      # SIMD row/field scanning (memchr, memchr3)
│   └── field.rs        # Zero-copy field extraction (Cow)
├── strategy/
│   ├── direct.rs       # Basic + SIMD strategies
│   ├── two_phase.rs    # Indexed strategy
│   ├── streaming.rs    # Stateful streaming parser
│   ├── parallel.rs     # Rayon-based parallel parsing
│   └── zero_copy.rs    # Sub-binary reference parsing
├── term.rs             # BEAM term building (copy + sub-binary)
└── resource.rs         # ResourceArc for streaming state
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed implementation notes.

## Memory Efficiency

The streaming parser uses bounded memory regardless of file size:

```elixir
# Process a 10GB file with ~64KB memory
File.stream!("huge.csv", [], 65_536)
|> CSV.parse_stream()
|> Stream.each(&process/1)
|> Stream.run()
```

### High-Performance Allocator

RustyCSV uses [mimalloc](https://github.com/microsoft/mimalloc) as the default allocator, providing:
- 10-20% faster allocation for many small objects
- Reduced memory fragmentation
- Zero tracking overhead in default configuration

To disable mimalloc (for exotic build targets):

```elixir
# In mix.exs
def project do
  [
    # Force local build without mimalloc
    compilers: [:rustler] ++ Mix.compilers(),
    rustler_crates: [rustycsv: [features: []]]
  ]
end
```

### Optional Memory Tracking

For profiling Rust memory usage, enable the `memory_tracking` feature:

```toml
# In native/rustycsv/Cargo.toml
[features]
default = ["mimalloc", "memory_tracking"]
```

Then use:

```elixir
RustyCSV.Native.reset_rust_memory_stats()
result = CSV.parse_string(large_csv)
peak = RustyCSV.Native.get_rust_memory_peak()
IO.puts("Peak Rust memory: #{peak / 1_000_000} MB")
```

When disabled (default), these functions return `0` with zero overhead.

## Development

```bash
# Install dependencies
mix deps.get

# Compile (includes Rust NIF)
mix compile

# Run tests (147 tests)
mix test

# Run benchmarks
mix run bench/csv_bench.exs

# Code quality
mix credo --strict
mix dialyzer
```

## License

MIT License - see LICENSE file for details.

---

**RustyCSV** - Purpose-built Rust NIF for ultra-fast CSV parsing in Elixir.

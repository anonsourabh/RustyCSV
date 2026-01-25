# RustyCSV

**High-performance CSV parsing for Elixir.** A purpose-built Rust NIF with five parsing strategies, SIMD acceleration, and bounded-memory streaming. Drop-in replacement for NimbleCSV.

[![Hex.pm](https://img.shields.io/hexpm/v/rusty_csv.svg)](https://hex.pm/packages/rusty_csv)
[![Tests](https://img.shields.io/badge/tests-147%20passed-brightgreen.svg)]()
[![RFC 4180](https://img.shields.io/badge/RFC%204180-compliant-blue.svg)]()

## Why RustyCSV?

**The Problem**: CSV parsing in Elixir faces two challenges at scale:

1. **Memory**: NimbleCSV uses sub-binary references for zero-copy parsing, which is fast—but those references keep the entire original binary alive. Parse a 100MB CSV, extract a few fields, and you're still holding 100MB in memory until GC runs.

2. **Streaming**: NimbleCSV requires the entire CSV in memory before parsing. For multi-gigabyte files or unbounded streams, this isn't feasible.

**Why not wrap an existing Rust CSV library?** The excellent [csv](https://docs.rs/csv) crate is designed for Rust workflows, not BEAM integration. Wrapping it would require serializing data between Rust and Erlang formats—adding overhead and losing the benefits of direct term construction.

**RustyCSV's approach**: Built from scratch for Elixir, focusing on:

1. **Bounded memory streaming** - Process multi-GB files with ~64KB memory footprint
2. **No parent binary retention** - Data copied to BEAM terms, Rust memory freed immediately
3. **Multiple strategies** - Choose SIMD, parallel, streaming, or indexed based on your workload
4. **Reduced scheduler load** - Heavy parsing runs on dirty schedulers
5. **Full NimbleCSV compatibility** - Same API, drop-in replacement

## Feature Comparison

| Feature | RustyCSV | NimbleCSV |
|---------|----------|-----------|
| **Parsing strategies** | 5 (SIMD, parallel, streaming, indexed, basic) | 1 |
| **SIMD acceleration** | ✅ via memchr | ❌ |
| **Parallel parsing** | ✅ via rayon | ❌ |
| **Streaming (bounded memory)** | ✅ | ❌ (requires full file in memory) |
| **Encoding support** | ✅ UTF-8, UTF-16, Latin-1, UTF-32 | ✅ |
| **Parent binary retention** | ❌ (copies to terms) | ✅ (sub-binary refs) |
| **Drop-in replacement** | ✅ Same API | - |
| **RFC 4180 compliant** | ✅ 147 tests | ✅ |
| **Benchmark (15MB CSV)** | ~42ms | ~190ms |

## Purpose-Built for Elixir

RustyCSV isn't a wrapper around an existing Rust CSV library. It's **custom-built from the ground up** for optimal Elixir/BEAM integration:

- **Zero-copy field extraction** using Rust's `Cow<[u8]>` - borrows data when possible, only allocates when quote unescaping is needed
- **Dirty scheduler aware** - long-running parallel parses run on dirty CPU schedulers, never blocking your BEAM schedulers
- **ResourceArc-based streaming** - stateful parser properly integrated with BEAM's garbage collector
- **Direct term building** - results go straight to BEAM terms, no intermediate allocations

### Five Parsing Strategies

Choose the right tool for the job:

| Strategy | Use Case | How It Works |
|----------|----------|--------------|
| `:simd` | **Default.** Fastest for most files | SIMD-accelerated delimiter scanning via `memchr` |
| `:parallel` | Files 100MB+ | Multi-threaded row parsing via `rayon` |
| `:streaming` | Unbounded/huge files | Bounded-memory chunk processing |
| `:indexed` | Re-extracting row ranges | Two-phase index-then-extract |
| `:basic` | Debugging, baselines | Simple byte-by-byte parsing |

```elixir
# Automatic strategy selection
CSV.parse_string(data)                           # Uses :simd (default)
CSV.parse_string(huge_data, strategy: :parallel) # 100MB+ files
File.stream!("huge.csv") |> CSV.parse_stream()   # Bounded memory
```

## Installation

```elixir
def deps do
  [{:rusty_csv, "~> 0.1.0"}]
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

# That's it. Same API, 4-5x faster.
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

**4-5x faster than NimbleCSV** on synthetic benchmarks (15MB CSV, 100K rows).

**13-28% faster than NimbleCSV** on real-world TSV files (10K+ rows). The larger the file, the greater the performance gap.

```bash
mix run bench/csv_bench.exs
```

See [docs/BENCHMARK.md](docs/BENCHMARK.md) for detailed methodology and results.

### When to Use RustyCSV

| Scenario | Recommendation |
|----------|----------------|
| **Large files (10MB+)** | ✅ Use `:parallel` strategy - biggest wins |
| **Huge/unbounded files** | ✅ Use `parse_stream/2` - bounded memory |
| **Memory-constrained** | ✅ No parent binary retention |
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
| `:parallel` | SIMD | Multi-threaded via [rayon](https://docs.rs/rayon) | O(n) | Large files (100MB+) |
| `:streaming` | Byte-by-byte | Stateful chunks | O(chunk) | Unbounded/huge files |

**Shared across all strategies:**
- `Cow<[u8]>` for zero-copy field extraction when no unescaping needed
- Direct Erlang term construction via Rustler (no serde)

**`:parallel` specifics:**
- Runs on dirty CPU schedulers to avoid blocking BEAM
- Row boundaries found single-threaded (quote-aware), then rows parsed in parallel

**`:streaming` specifics:**
- `ResourceArc` integrates parser state with BEAM GC
- Tracks quote state across chunk boundaries
- Copies field data (since input chunks are temporary)

## Architecture

RustyCSV is built with a modular Rust architecture:

```
native/rustycsv/src/
├── lib.rs              # NIF entry points, memory tracking
├── core/
│   ├── scanner.rs      # SIMD delimiter scanning (memchr)
│   └── field.rs        # Zero-copy field extraction (Cow)
├── strategy/
│   ├── direct.rs       # Basic + SIMD strategies
│   ├── two_phase.rs    # Indexed strategy
│   ├── streaming.rs    # Stateful streaming parser
│   └── parallel.rs     # Rayon-based parallel parsing
├── term.rs             # BEAM term building
└── resource.rs         # ResourceArc for streaming state
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed implementation notes.

## Memory Efficiency

RustyCSV includes built-in memory tracking for benchmarking:

```elixir
RustyCSV.Native.reset_rust_memory_stats()
result = CSV.parse_string(large_csv)
peak = RustyCSV.Native.get_rust_memory_peak()
IO.puts("Peak Rust memory: #{peak / 1_000_000} MB")
```

The streaming parser uses bounded memory regardless of file size:

```elixir
# Process a 10GB file with ~64KB memory
File.stream!("huge.csv", [], 65_536)
|> CSV.parse_stream()
|> Stream.each(&process/1)
|> Stream.run()
```

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

**RustyCSV** - Purpose-built Rust NIF for high-performance CSV parsing in Elixir.

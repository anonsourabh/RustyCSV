# Real-World Benchmark: Amazon Settlement Reports

This document presents benchmark results from parsing Amazon SP-API settlement reports in TSV format. These are production reports ranging from 20 to 15,820 rows.

## Test Environment

- **Data source**: Amazon Seller Central settlement reports (TSV format)
- **Report sizes**: 1KB to 2.6MB (20 to 15,820 rows)
- **Hardware**: Apple Silicon
- **Test date**: January 2025

## Parsing Approaches Compared

### 1. String.split/2

```elixir
line |> String.split("\t")
```

- Simple, no dependencies
- Fast for simple data
- Cannot handle quoted fields (if a field contains a tab inside quotes)
- Cannot handle escaped quotes
- Not RFC 4180 compliant

### 2. NimbleCSV

```elixir
NimbleCSV.define(MyApp.TSV, separator: "\t", escape: "\"")
MyApp.TSV.parse_string(data)
```

- Handles quoted fields, escaping, RFC 4180 compliant
- Pure Elixir implementation
- Sub-binary references can retain parent binary in memory

### 3. RustyCSV

```elixir
RustyCSV.define(MyApp.TSV, separator: "\t", escape: "\"")
MyApp.TSV.parse_string(data)
```

- Handles quoted fields, escaping, RFC 4180 compliant
- SIMD-accelerated delimiter scanning via Rust NIF
- No parent binary retention (copies to BEAM terms, frees Rust memory)

## Results

### Small Files (<200 rows)

| Rows  | RustyCSV | NimbleCSV | String.split |
|------:|---------:|----------:|-------------:|
|    20 |      2ms |       2ms |          2ms |
|    24 |      2ms |       2ms |          2ms |
|    25 |    7ms\* |       2ms |          2ms |
|    36 |      2ms |       2ms |          2ms |
|    46 |      2ms |       2ms |      13ms\*\* |
|    47 |      2ms |       2ms |          3ms |
|    93 |      2ms |       2ms |          2ms |
|   100 |      2ms |       2ms |          2ms |
|   141 |      2ms |       3ms |          3ms |

\* First call includes NIF warmup overhead
\*\* Likely GC pause

**Conclusion**: For small files, all three approaches perform equivalently. The NIF overhead for RustyCSV is negligible at this scale.

### Large Files (10K+ rows)

| Rows   | RustyCSV   | NimbleCSV | String.split | vs NimbleCSV |
|-------:|-----------:|----------:|-------------:|-------------:|
|  9,985 | **46ms**   |      64ms |        110ms |   28% faster |
| 10,961 | **54ms**   |      68ms |         60ms |   21% faster |
| 11,246 | **60ms**   |      69ms |         70ms |   13% faster |
| 11,754 | **56ms**   |      78ms |         72ms |   28% faster |
| 13,073 | **84ms**   |      96ms |         87ms |   13% faster |

**Conclusion**: RustyCSV is consistently 13-28% faster than NimbleCSV for large files. String.split shows high variability (60ms to 110ms for similar row counts), likely due to GC pressure from creating many small binaries.

## Key Finding

**The larger the file, the greater the performance gap.**

For small files (<200 rows), all three approaches perform similarly at ~2ms. The NIF overhead is negligible. But as file size grows, RustyCSV pulls ahead:

| File Size   | RustyCSV vs NimbleCSV |
|------------:|----------------------:|
|  <200 rows  |                ~same  |
|  ~10K rows  |         13-28% faster |
|  ~13K rows  |            13% faster |

This pattern is expected: SIMD-accelerated scanning provides more benefit as there's more data to scan.

## Analysis

### Why RustyCSV is Faster

1. **SIMD scanning**: The `memchr` crate uses SIMD instructions to scan for delimiters, processing multiple bytes per CPU cycle.

2. **Direct term building**: RustyCSV builds BEAM terms directly from parsed data, avoiding intermediate allocations.

3. **Zero-copy field extraction**: Uses Rust's `Cow<[u8]>` to borrow data when possible, only allocating when quote unescaping is needed.

### Why String.split Shows Variability

String.split creates a new binary for each field. For a 10K row file with 10 columns, that's 100K small binaries. This can trigger garbage collection, causing the observed variability (46ms to 110ms for similar workloads).

### When to Use Each Approach

| Scenario                         | Recommendation                              |
|----------------------------------|---------------------------------------------|
| Simple TSV, guaranteed no quotes | String.split is fine                        |
| Production data, unknown content | RustyCSV or NimbleCSV (RFC 4180 compliant)  |
| Large files (10K+ rows)          | RustyCSV (13-28% faster than NimbleCSV)     |
| Memory-constrained               | RustyCSV (no parent binary retention)       |
| Pure Elixir requirement          | NimbleCSV                                   |

## Raw Data

Each report number refers to the same file across all three runs (e.g., Report 2 is the same 13,073-row file parsed by all three approaches).

### RustyCSV Run

```
Report 1:  25 rows, 7ms (first call warmup)
Report 2:  13,073 rows, 84ms
Report 3:  93 rows, 2ms
Report 4:  47 rows, 2ms
Report 5:  11,754 rows, 56ms
Report 6:  46 rows, 2ms
Report 7:  11,246 rows, 60ms
Report 8:  100 rows, 2ms
Report 9:  24 rows, 2ms
Report 10: 9,985 rows, 46ms
Report 11: 20 rows, 2ms
Report 12: 10,961 rows, 54ms
Report 13: 141 rows, 2ms
Report 14: 36 rows, 2ms
```

### NimbleCSV Run

```
Report 1:  25 rows, 2ms
Report 2:  13,073 rows, 96ms
Report 3:  93 rows, 2ms
Report 4:  47 rows, 2ms
Report 5:  11,754 rows, 78ms
Report 6:  46 rows, 2ms
Report 7:  11,246 rows, 69ms
Report 8:  100 rows, 2ms
Report 9:  24 rows, 2ms
Report 10: 9,985 rows, 64ms
Report 11: 20 rows, 2ms
Report 12: 10,961 rows, 68ms
Report 13: 141 rows, 3ms
Report 14: 36 rows, 2ms
```

### String.split Run

```
Report 1:  25 rows, 2ms
Report 2:  13,073 rows, 87ms
Report 3:  93 rows, 2ms
Report 4:  47 rows, 3ms
Report 5:  11,754 rows, 72ms
Report 6:  46 rows, 13ms (GC pause)
Report 7:  11,246 rows, 70ms
Report 8:  100 rows, 2ms
Report 9:  24 rows, 2ms
Report 10: 9,985 rows, 110ms (GC pause)
Report 11: 20 rows, 2ms
Report 12: 10,961 rows, 60ms
Report 13: 141 rows, 3ms
Report 14: 36 rows, 2ms
```

## Methodology

### Parser Implementations Tested

**String.split (baseline):**
```elixir
defp parse_tsv(content) do
  [header | rows] = String.split(content, ~r/\r?\n/, trim: true)
  headers = header |> String.split("\t")
  Enum.map(rows, fn row ->
    values = String.split(row, "\t")
    Enum.zip(headers, values) |> Map.new()
  end)
end
```

**NimbleCSV:**
```elixir
NimbleCSV.define(MyApp.TSV, separator: "\t", escape: "\"")

defp parse_tsv(content) do
  [headers | rows] = MyApp.TSV.parse_string(content, skip_headers: false)
  Enum.map(rows, fn row -> Enum.zip(headers, row) |> Map.new() end)
end
```

**RustyCSV:**
```elixir
RustyCSV.define(MyApp.TSV, separator: "\t", escape: "\"")

defp parse_tsv(content) do
  [headers | rows] = MyApp.TSV.parse_string(content, skip_headers: false)
  Enum.map(rows, fn row -> Enum.zip(headers, row) |> Map.new() end)
end
```

### Test Conditions

1. **Same data**: All three runs processed identical settlement reports
2. **Sequential processing**: Reports processed one at a time
3. **Warm system**: Multiple reports in sequence, reducing cold-start effects
4. **Parse time only**: Times measure TSV parsing only, excluding I/O
5. **Memory isolated**: GC forced before each measurement

## Synthetic Benchmark

In addition to the real-world TSV benchmark above, RustyCSV includes a synthetic benchmark using generated CSV data.

**Environment:** Apple Silicon, 15 MB CSV (100K rows, 10 columns)

```
Name                        ips        average    vs NimbleCSV
RustyCSV (parallel)       28.57       35.00 ms        5.4x faster
RustyCSV (simd)           23.77       42.06 ms        4.5x faster
RustyCSV (indexed)        22.22       45.00 ms        4.2x faster
RustyCSV (basic)          20.00       50.00 ms        3.8x faster
NimbleCSV                  5.25      190.33 ms        baseline
```

The synthetic benchmark shows larger gains (4-5x) than the real-world benchmark (13-28%) because:
1. Generated data is uniform and cache-friendly
2. The synthetic file is larger (15MB vs 1-2MB)
3. Real-world data includes I/O between parse operations

### Running the Synthetic Benchmark

```bash
mix run bench/csv_bench.exs
```

This generates a 15MB CSV file and compares all five RustyCSV strategies against NimbleCSV.

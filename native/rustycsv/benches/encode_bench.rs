// Standalone Rust benchmark for CSV encoding strategies
//
// Run: cargo bench --bench encode_bench
//
// Compares scalar vs SWAR vs SIMD encoding across:
//   - Clean data (no quoting needed)
//   - Mixed data (some fields need quoting/escaping)
//   - Various sizes (1K, 10K, 100K rows)

#![feature(portable_simd)]

use std::time::{Duration, Instant};

// Import the encode module from the library
use rustycsv::strategy::encode::{
    encode_csv_scalar, encode_csv_simd, encode_csv_swar,
};

// ==========================================================================
// "Elixir-style" naive encoder — simulates what the BEAM does
// ==========================================================================
// This mirrors the Elixir dump_to_iodata logic:
//   1. For each field: String.contains?(field, escape_chars) — linear scan
//   2. If needs escaping: String.replace(field, escape, escape <> escape) — allocates new string
//   3. Wrap in escape chars
//   4. Enum.intersperse(fields, separator) — builds list
//   5. Concat everything
//
// In Rust we simulate this with per-field String allocations and Vec<String>
// joins. This is a LOWER BOUND on Elixir's cost since we don't pay for:
//   - BEAM term construction overhead
//   - Process heap allocation / GC pressure
//   - List cell allocation for iodata
//   - Copying from Rust-managed memory to BEAM binaries

fn encode_csv_naive(rows: &[&[&[u8]]], separator: u8, escape: u8, line_sep: &[u8]) -> Vec<u8> {
    let escape_chars: &[u8] = &[separator, escape, b'\n', b'\r'];
    let mut parts: Vec<Vec<u8>> = Vec::with_capacity(rows.len());

    for row in rows {
        let mut field_strings: Vec<Vec<u8>> = Vec::with_capacity(row.len());

        for field in *row {
            // Step 1: String.contains?(field, @escape_chars) — linear scan
            let needs_quoting = field.iter().any(|b| escape_chars.contains(b));

            if needs_quoting {
                // Step 2: String.replace(field, escape, escape <> escape) — new allocation
                let mut escaped = Vec::with_capacity(field.len() + 10);
                for &b in *field {
                    if b == escape {
                        escaped.push(escape);
                        escaped.push(escape);
                    } else {
                        escaped.push(b);
                    }
                }
                // Step 3: Wrap in escape chars
                let mut quoted = Vec::with_capacity(escaped.len() + 2);
                quoted.push(escape);
                quoted.extend_from_slice(&escaped);
                quoted.push(escape);
                field_strings.push(quoted);
            } else {
                // Copy the field (Elixir's maybe_to_encoding still touches it)
                field_strings.push(field.to_vec());
            }
        }

        // Step 4: Enum.intersperse + join — builds iodata list then flattens
        let mut row_out = Vec::new();
        for (i, f) in field_strings.iter().enumerate() {
            if i > 0 {
                row_out.push(separator);
            }
            row_out.extend_from_slice(f);
        }
        row_out.extend_from_slice(line_sep);
        parts.push(row_out);
    }

    // IO.iodata_to_binary — final concatenation
    let total_len: usize = parts.iter().map(|p| p.len()).sum();
    let mut out = Vec::with_capacity(total_len);
    for part in &parts {
        out.extend_from_slice(part);
    }
    out
}

/// Generate clean rows (no fields need quoting)
fn generate_clean_rows(num_rows: usize, fields_per_row: usize) -> Vec<Vec<Vec<u8>>> {
    (0..num_rows)
        .map(|i| {
            (0..fields_per_row)
                .map(|j| format!("field_{}_{}_value", i, j).into_bytes())
                .collect()
        })
        .collect()
}

/// Generate mixed rows (some fields need quoting)
fn generate_mixed_rows(num_rows: usize, fields_per_row: usize) -> Vec<Vec<Vec<u8>>> {
    (0..num_rows)
        .map(|i| {
            (0..fields_per_row)
                .map(|j| {
                    match j % 5 {
                        0 => format!("plain_value_{}", i).into_bytes(),
                        1 => format!("has,comma_{}", i).into_bytes(),
                        2 => format!("has\"quote_{}", i).into_bytes(),
                        3 => format!("has\nnewline_{}", i).into_bytes(),
                        _ => format!("normal_field_{}_{}", i, j).into_bytes(),
                    }
                })
                .collect()
        })
        .collect()
}

/// Generate rows with long fields (to exercise SIMD paths)
fn generate_long_field_rows(num_rows: usize) -> Vec<Vec<Vec<u8>>> {
    (0..num_rows)
        .map(|i| {
            vec![
                // 100-byte clean field
                format!("{:0>100}", i).into_bytes(),
                // 200-byte field with comma near the end
                {
                    let mut f = format!("{:a>198}", i).into_bytes();
                    f[195] = b',';
                    f
                },
                // 50-byte clean field
                format!("{:x>50}", i).into_bytes(),
            ]
        })
        .collect()
}

struct BenchResult {
    name: String,
    iterations: u64,
    total_time: Duration,
    output_size: usize,
}

impl BenchResult {
    fn avg_ns(&self) -> f64 {
        self.total_time.as_nanos() as f64 / self.iterations as f64
    }

    fn throughput_mb_s(&self) -> f64 {
        let bytes_per_iter = self.output_size as f64;
        let secs_per_iter = self.avg_ns() / 1_000_000_000.0;
        bytes_per_iter / secs_per_iter / 1_000_000.0
    }
}

fn bench_fn<F: Fn() -> Vec<u8>>(name: &str, f: F, warmup_secs: f64, bench_secs: f64) -> BenchResult {
    // Warmup
    let warmup_deadline = Instant::now() + Duration::from_secs_f64(warmup_secs);
    let mut output_size = 0;
    while Instant::now() < warmup_deadline {
        let out = f();
        output_size = out.len();
    }

    // Benchmark
    let mut iterations: u64 = 0;
    let start = Instant::now();
    let deadline = start + Duration::from_secs_f64(bench_secs);
    while Instant::now() < deadline {
        let _ = f();
        iterations += 1;
    }
    let total_time = start.elapsed();

    BenchResult {
        name: name.to_string(),
        iterations,
        total_time,
        output_size,
    }
}

fn print_results(results: &[BenchResult]) {
    let max_name_len = results.iter().map(|r| r.name.len()).max().unwrap_or(0);

    // Find fastest for comparison
    let fastest_ns = results
        .iter()
        .map(|r| r.avg_ns())
        .fold(f64::MAX, f64::min);

    for r in results {
        let avg = r.avg_ns();
        let speedup = avg / fastest_ns;
        let marker = if (speedup - 1.0).abs() < 0.01 { " (fastest)" } else { "" };
        println!(
            "  {:<width$}  {:>10.2} µs/iter  {:>8.1} MB/s  {:>6.2}x{}",
            r.name,
            avg / 1000.0,
            r.throughput_mb_s(),
            speedup,
            marker,
            width = max_name_len,
        );
    }
}

fn run_benchmark_suite(
    label: &str,
    rows_owned: &[Vec<Vec<u8>>],
    warmup: f64,
    time: f64,
) {
    // Convert owned data to slice references matching the encode API
    let row_fields: Vec<Vec<&[u8]>> = rows_owned
        .iter()
        .map(|row| row.iter().map(|f| f.as_slice()).collect())
        .collect();
    let row_slices: Vec<&[&[u8]]> = row_fields.iter().map(|r| r.as_slice()).collect();

    println!("\n--- {} ---", label);

    let results = vec![
        bench_fn("Naive (Elixir-like)", || {
            encode_csv_naive(&row_slices, b',', b'"', b"\n")
        }, warmup, time),
        bench_fn("Scalar", || {
            encode_csv_scalar(&row_slices, b',', b'"', b"\n")
        }, warmup, time),
        bench_fn("SWAR", || {
            encode_csv_swar(&row_slices, b',', b'"', b"\n")
        }, warmup, time),
        bench_fn("SIMD", || {
            encode_csv_simd(&row_slices, b',', b'"', b"\n")
        }, warmup, time),
    ];

    // Verify all produce identical output
    let naive_out = encode_csv_naive(&row_slices, b',', b'"', b"\n");
    let scalar_out = encode_csv_scalar(&row_slices, b',', b'"', b"\n");
    let swar_out = encode_csv_swar(&row_slices, b',', b'"', b"\n");
    let simd_out = encode_csv_simd(&row_slices, b',', b'"', b"\n");
    assert_eq!(naive_out, scalar_out, "Scalar output differs from naive!");
    assert_eq!(scalar_out, swar_out, "SWAR output differs from scalar!");
    assert_eq!(scalar_out, simd_out, "SIMD output differs from scalar!");
    println!("  Output: {} bytes (all strategies match)", scalar_out.len());

    print_results(&results);
}

fn main() {
    println!("=== RustyCSV Encoding Benchmark ===");
    println!("Strategies: Naive (Elixir-like), Scalar, SWAR (8-byte Mycroft), SIMD (16/32-byte vectorized)");
    println!("Note: Naive simulates Elixir's per-field alloc pattern. Real Elixir is SLOWER due to GC/term overhead.");

    let warmup = 1.0;
    let time = 3.0;

    // 1K rows, clean
    let rows = generate_clean_rows(1_000, 10);
    run_benchmark_suite("1K rows x 10 fields (clean, no quoting)", &rows, warmup, time);

    // 10K rows, clean
    let rows = generate_clean_rows(10_000, 10);
    run_benchmark_suite("10K rows x 10 fields (clean, no quoting)", &rows, warmup, time);

    // 10K rows, mixed (quoting needed)
    let rows = generate_mixed_rows(10_000, 10);
    run_benchmark_suite("10K rows x 10 fields (mixed, with quoting)", &rows, warmup, time);

    // 100K rows, clean
    let rows = generate_clean_rows(100_000, 10);
    run_benchmark_suite("100K rows x 10 fields (clean, no quoting)", &rows, warmup, time);

    // 100K rows, mixed
    let rows = generate_mixed_rows(100_000, 10);
    run_benchmark_suite("100K rows x 10 fields (mixed, with quoting)", &rows, warmup, time);

    // 10K rows, long fields (exercise SIMD more)
    let rows = generate_long_field_rows(10_000);
    run_benchmark_suite("10K rows x 3 long fields (50-200 bytes each)", &rows, warmup, time);

    println!("\n=== Done ===");
}

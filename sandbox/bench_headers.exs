# Benchmark: Headers-to-Maps Performance
#
# Run with: mix run bench_headers.exs
#
# This compares different approaches to converting CSV rows to maps:
# 1. RustyCSV + manual Enum.zip (baseline)
# 2. NimbleCSV + manual Enum.zip
# 3. CSV library with headers: true
#
# The goal is to establish baselines before implementing native Rust support.

alias RustyCSV.RFC4180, as: RustyCSV

IO.puts("\n=== Generating test data ===\n")

# Different dataset sizes to test
# 100 rows, 10 columns
small_csv = Sandbox.generate_csv(100, 10)
# 1,000 rows, 10 columns
medium_csv = Sandbox.generate_csv(1_000, 10)
# 10,000 rows, 10 columns
large_csv = Sandbox.generate_csv(10_000, 10)
# 1,000 rows, 50 columns
wide_csv = Sandbox.generate_csv(1_000, 50)

IO.puts("Small CSV:  #{byte_size(small_csv)} bytes (100 rows x 10 cols)")
IO.puts("Medium CSV: #{byte_size(medium_csv)} bytes (1,000 rows x 10 cols)")
IO.puts("Large CSV:  #{byte_size(large_csv)} bytes (10,000 rows x 10 cols)")
IO.puts("Wide CSV:   #{byte_size(wide_csv)} bytes (1,000 rows x 50 cols)")

# Verify correctness first
IO.puts("\n=== Verifying correctness ===\n")

test_csv = "name,age,city\njohn,27,nyc\njane,32,sf\n"

rusty_result = Sandbox.baseline_to_maps(test_csv)
nimble_result = Sandbox.nimble_to_maps(test_csv)
csv_lib_result = Sandbox.csv_library_to_maps(test_csv)

IO.puts("RustyCSV baseline: #{inspect(rusty_result)}")
IO.puts("NimbleCSV baseline: #{inspect(nimble_result)}")
IO.puts("CSV library:        #{inspect(csv_lib_result)}")

# Check they all produce the same output
expected = [
  %{"name" => "john", "age" => "27", "city" => "nyc"},
  %{"name" => "jane", "age" => "32", "city" => "sf"}
]

if rusty_result == expected and nimble_result == expected and csv_lib_result == expected do
  IO.puts("\nAll implementations produce correct output!")
else
  IO.puts("\nWARNING: Output mismatch!")
  IO.puts("Expected: #{inspect(expected)}")
end

# Run benchmarks
IO.puts("\n=== Running benchmarks ===\n")

Benchee.run(
  %{
    # Parse only (no map conversion) - to isolate parsing performance
    "RustyCSV parse only" => fn input -> RustyCSV.parse_string(input, skip_headers: false) end,
    "NimbleCSV parse only" => fn input ->
      NimbleCSV.RFC4180.parse_string(input, skip_headers: false)
    end,

    # Full pipeline: parse + convert to maps
    "RustyCSV + Enum.zip" => fn input -> Sandbox.baseline_to_maps(input) end,
    "NimbleCSV + Enum.zip" => fn input -> Sandbox.nimble_to_maps(input) end,
    "CSV library (headers: true)" => fn input -> Sandbox.csv_library_to_maps(input) end
  },
  inputs: %{
    "small (100 rows)" => small_csv,
    "medium (1K rows)" => medium_csv,
    "large (10K rows)" => large_csv,
    "wide (1K x 50 cols)" => wide_csv
  },
  memory_time: 2,
  reduction_time: 2,
  formatters: [
    Benchee.Formatters.Console
  ]
)

IO.puts("\n=== Key metrics to watch ===\n")

IO.puts("""
1. "parse only" shows raw parsing speed (should not regress)
2. "RustyCSV + Enum.zip" is the current user experience
3. "CSV library" shows what users get with headers: true today

If we implement headers: true in Rust:
- It should be faster than "RustyCSV + Enum.zip"
- It should ideally match or beat "CSV library"
- Memory usage should be lower due to key interning
""")

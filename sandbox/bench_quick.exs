# Quick benchmark to establish baselines
#
# Run with: mix run bench_quick.exs

alias RustyCSV.RFC4180, as: RustyCSV

IO.puts("\n=== Generating test data ===\n")

# Focus on the most relevant size: 10K rows (common for API responses, exports)
csv_10k = Sandbox.generate_csv(10_000, 10)
IO.puts("Test CSV: #{byte_size(csv_10k)} bytes (10,000 rows x 10 cols)")

# Verify correctness
IO.puts("\n=== Verifying correctness ===\n")

test_csv = "name,age,city\njohn,27,nyc\njane,32,sf\n"

rusty_result = Sandbox.baseline_to_maps(test_csv)
nimble_result = Sandbox.nimble_to_maps(test_csv)
csv_lib_result = Sandbox.csv_library_to_maps(test_csv)

expected = [
  %{"name" => "john", "age" => "27", "city" => "nyc"},
  %{"name" => "jane", "age" => "32", "city" => "sf"}
]

IO.puts("RustyCSV: #{if rusty_result == expected, do: "OK", else: "FAIL"}")
IO.puts("NimbleCSV: #{if nimble_result == expected, do: "OK", else: "FAIL"}")
IO.puts("CSV lib: #{if csv_lib_result == expected, do: "OK", else: "FAIL"}")

IO.puts("\n=== Benchmarks (10K rows) ===\n")

Benchee.run(
  %{
    "1. RustyCSV parse only" => fn -> RustyCSV.parse_string(csv_10k, skip_headers: false) end,
    "2. NimbleCSV parse only" => fn ->
      NimbleCSV.RFC4180.parse_string(csv_10k, skip_headers: false)
    end,
    "3. RustyCSV + Enum.zip (maps)" => fn -> Sandbox.baseline_to_maps(csv_10k) end,
    "4. NimbleCSV + Enum.zip (maps)" => fn -> Sandbox.nimble_to_maps(csv_10k) end,
    "5. CSV lib headers:true (maps)" => fn -> Sandbox.csv_library_to_maps(csv_10k) end
  },
  warmup: 1,
  time: 3,
  memory_time: 1,
  formatters: [Benchee.Formatters.Console]
)

IO.puts("""

=== Analysis ===

Compare rows 1 vs 3: How much overhead does Enum.zip add?
Compare rows 3 vs 5: Can we beat the CSV library?

If row 3 is much slower than row 1, there's opportunity for optimization
by doing the map conversion in Rust with interned keys.
""")

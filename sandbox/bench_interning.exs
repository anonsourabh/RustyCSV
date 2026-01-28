# Benchmark: Key Interning Performance
#
# Run with: mix run bench_interning.exs
#
# Compares:
# 1. RustyCSV + Enum.zip (current approach)
# 2. Rust NIF with interned binary keys
# 3. Rust NIF without interning (baseline)
# 4. Rust NIF with atom keys

alias RustyCSV.RFC4180, as: RustyCSV

IO.puts("\n=== Generating test data ===\n")

csv_10k = Sandbox.generate_csv(10_000, 10)
IO.puts("Test CSV: #{byte_size(csv_10k)} bytes (10,000 rows x 10 cols)")

# Verify correctness
IO.puts("\n=== Verifying correctness ===\n")

test_csv = "name,age,city\njohn,27,nyc\njane,32,sf\n"

elixir_result = Sandbox.baseline_to_maps(test_csv)
interned_result = Sandbox.Native.parse_to_maps_interned(test_csv, ?,, ?")
no_intern_result = Sandbox.Native.parse_to_maps_no_intern(test_csv, ?,, ?")
atoms_result = Sandbox.Native.parse_to_maps_atoms(test_csv, ?,, ?")

expected_string_keys = [
  %{"name" => "john", "age" => "27", "city" => "nyc"},
  %{"name" => "jane", "age" => "32", "city" => "sf"}
]

expected_atom_keys = [
  %{name: "john", age: "27", city: "nyc"},
  %{name: "jane", age: "32", city: "sf"}
]

IO.puts(
  "Elixir baseline:   #{if elixir_result == expected_string_keys, do: "OK", else: "FAIL - #{inspect(elixir_result)}"}"
)

IO.puts(
  "Rust interned:     #{if interned_result == expected_string_keys, do: "OK", else: "FAIL - #{inspect(interned_result)}"}"
)

IO.puts(
  "Rust no-intern:    #{if no_intern_result == expected_string_keys, do: "OK", else: "FAIL - #{inspect(no_intern_result)}"}"
)

IO.puts(
  "Rust atoms:        #{if atoms_result == expected_atom_keys, do: "OK", else: "FAIL - #{inspect(atoms_result)}"}"
)

IO.puts("\n=== Benchmarks (10K rows x 10 cols) ===\n")

Benchee.run(
  %{
    "1. RustyCSV parse only (lists)" => fn ->
      RustyCSV.parse_string(csv_10k, skip_headers: false)
    end,
    "2. RustyCSV + Enum.zip (maps)" => fn ->
      Sandbox.baseline_to_maps(csv_10k)
    end,
    "3. Rust maps (interned keys)" => fn ->
      Sandbox.Native.parse_to_maps_interned(csv_10k, ?,, ?")
    end,
    "4. Rust maps (no interning)" => fn ->
      Sandbox.Native.parse_to_maps_no_intern(csv_10k, ?,, ?")
    end,
    "5. Rust maps (atom keys)" => fn ->
      Sandbox.Native.parse_to_maps_atoms(csv_10k, ?,, ?")
    end
  },
  warmup: 1,
  time: 3,
  memory_time: 2,
  formatters: [Benchee.Formatters.Console]
)

IO.puts("""

=== Analysis ===

Key comparisons:
- Row 1 vs 2: Overhead of Enum.zip in Elixir
- Row 2 vs 3: Benefit of doing maps in Rust with interning
- Row 3 vs 4: Benefit of key interning specifically
- Row 3 vs 5: Atom keys vs binary keys (atoms use BEAM's interning)

Success criteria:
- Row 3 should be faster than Row 2 (Rust maps beat Elixir Enum.zip)
- Row 3 should be faster than Row 4 (interning helps)
- Memory for Row 3 should be lower than Row 4
""")

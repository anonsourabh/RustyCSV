# Benchmark: Scaling behavior
#
# Test how interning benefits scale with data size

alias RustyCSV.RFC4180, as: RustyCSV

IO.puts("\n=== Testing at different scales ===\n")

sizes = [
  {1_000, 10, "1K rows x 10 cols"},
  {10_000, 10, "10K rows x 10 cols"},
  {50_000, 10, "50K rows x 10 cols"},
  {10_000, 50, "10K rows x 50 cols (wide)"}
]

for {rows, cols, label} <- sizes do
  csv = Sandbox.generate_csv(rows, cols)
  IO.puts("\n--- #{label} (#{div(byte_size(csv), 1024)} KB) ---\n")

  # Warm up
  _ = RustyCSV.parse_string(csv, skip_headers: false)
  _ = Sandbox.baseline_to_maps(csv)
  _ = Sandbox.Native.parse_to_maps_interned(csv, ?,, ?")
  _ = Sandbox.Native.parse_to_maps_atoms(csv, ?,, ?")

  # Time each approach (simple timing, not full benchee)
  {parse_time, _} =
    :timer.tc(fn ->
      for _ <- 1..10, do: RustyCSV.parse_string(csv, skip_headers: false)
    end)

  {enum_zip_time, _} =
    :timer.tc(fn ->
      for _ <- 1..10, do: Sandbox.baseline_to_maps(csv)
    end)

  {interned_time, _} =
    :timer.tc(fn ->
      for _ <- 1..10, do: Sandbox.Native.parse_to_maps_interned(csv, ?,, ?")
    end)

  {atoms_time, _} =
    :timer.tc(fn ->
      for _ <- 1..10, do: Sandbox.Native.parse_to_maps_atoms(csv, ?,, ?")
    end)

  parse_avg = parse_time / 10 / 1000
  enum_zip_avg = enum_zip_time / 10 / 1000
  interned_avg = interned_time / 10 / 1000
  atoms_avg = atoms_time / 10 / 1000

  IO.puts("Parse only:      #{Float.round(parse_avg, 2)}ms")
  IO.puts("Enum.zip:        #{Float.round(enum_zip_avg, 2)}ms")

  IO.puts(
    "Rust interned:   #{Float.round(interned_avg, 2)}ms (#{Float.round(enum_zip_avg / interned_avg, 2)}x faster than Enum.zip)"
  )

  IO.puts(
    "Rust atoms:      #{Float.round(atoms_avg, 2)}ms (#{Float.round(enum_zip_avg / atoms_avg, 2)}x faster than Enum.zip)"
  )
end

IO.puts("\n=== Summary ===\n")

IO.puts("""
The interning benefit should increase with:
- More rows (more key reuse)
- More columns (more keys to intern per row)

Atom keys should be fastest when headers are valid atoms.
""")

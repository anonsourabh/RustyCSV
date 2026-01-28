# Benchmark: Find the crossover point where Rust interning beats Elixir
#
# We need to find the byte size threshold where Rust becomes faster

alias RustyCSV.RFC4180, as: RustyCSV

IO.puts("\n=== Finding the crossover point ===\n")

# Test various sizes to find where Rust interning starts winning
sizes = [
  {100, 10},
  {250, 10},
  {500, 10},
  {750, 10},
  {1000, 10},
  {1500, 10},
  {2000, 10},
  {3000, 10},
  {5000, 10}
]

IO.puts("Rows | Bytes    | Elixir   | Rust     | Winner")
IO.puts("-----|----------|----------|----------|-------")

for {rows, cols} <- sizes do
  csv = Sandbox.generate_csv(rows, cols)
  bytes = byte_size(csv)

  # Warm up
  _ = Sandbox.baseline_to_maps(csv)
  _ = Sandbox.Native.parse_to_maps_interned(csv, ?,, ?")

  # Time each (10 iterations)
  {elixir_time, _} =
    :timer.tc(fn ->
      for _ <- 1..10, do: Sandbox.baseline_to_maps(csv)
    end)

  {rust_time, _} =
    :timer.tc(fn ->
      for _ <- 1..10, do: Sandbox.Native.parse_to_maps_interned(csv, ?,, ?")
    end)

  elixir_avg = elixir_time / 10 / 1000
  rust_avg = rust_time / 10 / 1000

  winner = if rust_avg < elixir_avg, do: "Rust", else: "Elixir"
  ratio = Float.round(elixir_avg / rust_avg, 2)

  bytes_str = if bytes > 1024, do: "#{div(bytes, 1024)}KB", else: "#{bytes}B"

  IO.puts(
    "#{String.pad_leading(Integer.to_string(rows), 4)} | #{String.pad_leading(bytes_str, 8)} | #{String.pad_leading(Float.to_string(Float.round(elixir_avg, 2)), 6)}ms | #{String.pad_leading(Float.to_string(Float.round(rust_avg, 2)), 6)}ms | #{winner} (#{ratio}x)"
  )
end

IO.puts("""

=== Recommendation ===

Look for where "Winner" switches from Elixir to Rust.
That byte size is our threshold for using interning.

The threshold should be set slightly below the crossover point
to ensure Rust is always faster when we choose it.
""")

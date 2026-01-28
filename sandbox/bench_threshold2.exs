# More rigorous crossover test with better warmup

IO.puts("\n=== Finding the crossover point (rigorous) ===\n")

sizes = [
  {100, 10},
  {500, 10},
  {1000, 10},
  {2000, 10},
  {5000, 10},
  {10000, 10}
]

# Pre-generate all CSVs
csvs =
  for {rows, cols} <- sizes do
    csv = Sandbox.generate_csv(rows, cols)
    {rows, cols, csv, byte_size(csv)}
  end

# Heavy warmup - run each 50 times
IO.puts("Warming up...")

for {_rows, _cols, csv, _bytes} <- csvs do
  for _ <- 1..50 do
    _ = Sandbox.baseline_to_maps(csv)
    _ = Sandbox.Native.parse_to_maps_interned(csv, ?,, ?")
  end
end

IO.puts("\nRows  | Bytes    | Elixir    | Rust      | Speedup")
IO.puts("------|----------|-----------|-----------|--------")

for {rows, _cols, csv, bytes} <- csvs do
  # 50 iterations for stable measurement
  {elixir_time, _} =
    :timer.tc(fn ->
      for _ <- 1..50, do: Sandbox.baseline_to_maps(csv)
    end)

  {rust_time, _} =
    :timer.tc(fn ->
      for _ <- 1..50, do: Sandbox.Native.parse_to_maps_interned(csv, ?,, ?")
    end)

  elixir_avg = elixir_time / 50 / 1000
  rust_avg = rust_time / 50 / 1000

  speedup = elixir_avg / rust_avg

  winner =
    if speedup > 1.0,
      do: "Rust #{Float.round(speedup, 2)}x",
      else: "Elixir #{Float.round(1 / speedup, 2)}x"

  bytes_str = if bytes > 1024, do: "#{div(bytes, 1024)}KB", else: "#{bytes}B"

  IO.puts(
    "#{String.pad_leading(Integer.to_string(rows), 5)} | #{String.pad_leading(bytes_str, 8)} | #{String.pad_leading(Float.to_string(Float.round(elixir_avg, 3)), 7)}ms | #{String.pad_leading(Float.to_string(Float.round(rust_avg, 3)), 7)}ms | #{winner}"
  )
end

IO.puts("""

=== Analysis ===

If Rust is faster at all tested sizes, we can always use Rust.
If there's a crossover, we should use byte_size threshold.
""")

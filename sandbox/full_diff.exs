path = "bench/data/large_bench.csv"

rusty = File.stream!(path) |> RustyCSV.RFC4180.parse_stream(skip_headers: false) |> Enum.to_list()
nimble = File.stream!(path) |> NimbleCSV.RFC4180.parse_stream(skip_headers: false) |> Enum.to_list()

IO.puts("Rusty rows:  #{length(rusty)}")
IO.puts("Nimble rows: #{length(nimble)}")
IO.puts("Identical:   #{rusty == nimble}")

if rusty != nimble do
  # Find first differing rows
  Enum.zip(rusty, nimble)
  |> Enum.with_index()
  |> Enum.reject(fn {{r, n}, _} -> r == n end)
  |> Enum.take(5)
  |> Enum.each(fn {{r, n}, i} ->
    IO.puts("\nRow #{i} differs:")
    IO.puts("  Rusty:  #{inspect(r)}")
    IO.puts("  Nimble: #{inspect(n)}")
  end)

  if length(rusty) != length(nimble) do
    IO.puts("\nRow count mismatch: #{length(rusty)} vs #{length(nimble)}")
  end
end

#!/usr/bin/env elixir
# Standalone encoding benchmark — no deps required
# Compares pure Elixir CSV encoding vs the approach NIF would use
#
# Usage: elixir bench/standalone_encode_bench.exs

defmodule PureElixirEncoder do
  @separator ","
  @escape "\""
  @escape_chars [@separator, @escape, "\n", "\r"]

  def encode(rows) do
    rows
    |> Enum.map(&encode_row/1)
    |> IO.iodata_to_binary()
  end

  defp encode_row(row) do
    fields = Enum.map(row, &escape_field/1)
    [Enum.intersperse(fields, @separator), "\n"]
  end

  defp escape_field(field) when is_binary(field) do
    if String.contains?(field, @escape_chars) do
      escaped = String.replace(field, @escape, @escape <> @escape)
      [@escape, escaped, @escape]
    else
      field
    end
  end
end

defmodule Bench do
  def measure(name, fun, opts \\ []) do
    warmup_ms = Keyword.get(opts, :warmup, 2_000)
    bench_ms = Keyword.get(opts, :time, 5_000)

    # Warmup
    deadline = System.monotonic_time(:millisecond) + warmup_ms
    warmup_loop(fun, deadline)

    # Benchmark
    start = System.monotonic_time(:nanosecond)
    bench_deadline = System.monotonic_time(:millisecond) + bench_ms
    iterations = bench_loop(fun, bench_deadline, 0)
    elapsed_ns = System.monotonic_time(:nanosecond) - start

    avg_us = elapsed_ns / iterations / 1_000
    {name, iterations, avg_us}
  end

  defp warmup_loop(fun, deadline) do
    if System.monotonic_time(:millisecond) < deadline do
      fun.()
      warmup_loop(fun, deadline)
    end
  end

  defp bench_loop(fun, deadline, count) do
    if System.monotonic_time(:millisecond) < deadline do
      fun.()
      bench_loop(fun, deadline, count + 1)
    else
      count
    end
  end

  def print_results(results, output_size) do
    fastest = results |> Enum.map(fn {_, _, avg} -> avg end) |> Enum.min()

    IO.puts("  Output: #{output_size} bytes")

    for {name, iters, avg_us} <- results do
      mb_s = output_size / avg_us  # bytes/µs = MB/s
      ratio = Float.round(avg_us / fastest, 2)
      marker = if ratio == 1.0, do: " (fastest)", else: ""
      IO.puts(
        "  #{String.pad_trailing(name, 25)} #{String.pad_leading(Float.to_string(Float.round(avg_us, 1)), 10)} µs/iter  " <>
        "#{String.pad_leading(Float.to_string(Float.round(mb_s, 1)), 8)} MB/s  " <>
        "#{String.pad_leading(Float.to_string(ratio), 6)}x#{marker}"
      )
    end
  end
end

defmodule DataGen do
  def clean_rows(count, fields) do
    for i <- 1..count do
      for j <- 1..fields do
        "field_#{i}_#{j}_value"
      end
    end
  end

  def mixed_rows(count, fields) do
    for i <- 1..count do
      for j <- 1..fields do
        case rem(j, 5) do
          0 -> "plain_value_#{i}"
          1 -> "has,comma_#{i}"
          2 -> "has\"quote_#{i}"
          3 -> "has\nnewline_#{i}"
          4 -> "normal_field_#{i}_#{j}"
        end
      end
    end
  end
end

IO.puts("=== RustyCSV Encoding Benchmark (Pure Elixir) ===")
IO.puts("Erlang/OTP #{System.otp_release()}, Elixir #{System.version()}")
IO.puts("")

# 10K clean
IO.puts("--- 10K rows x 10 fields (clean, no quoting) ---")
rows = DataGen.clean_rows(10_000, 10)
output = PureElixirEncoder.encode(rows)
output_size = byte_size(output)

results = [
  Bench.measure("Elixir (iodata)", fn -> PureElixirEncoder.encode(rows) end),
]
Bench.print_results(results, output_size)
IO.puts("")

# 10K mixed
IO.puts("--- 10K rows x 10 fields (mixed, with quoting) ---")
rows = DataGen.mixed_rows(10_000, 10)
output = PureElixirEncoder.encode(rows)
output_size = byte_size(output)

results = [
  Bench.measure("Elixir (iodata)", fn -> PureElixirEncoder.encode(rows) end),
]
Bench.print_results(results, output_size)
IO.puts("")

# 100K clean
IO.puts("--- 100K rows x 10 fields (clean, no quoting) ---")
rows = DataGen.clean_rows(100_000, 10)
output = PureElixirEncoder.encode(rows)
output_size = byte_size(output)

results = [
  Bench.measure("Elixir (iodata)", fn -> PureElixirEncoder.encode(rows) end, warmup: 2_000, time: 5_000),
]
Bench.print_results(results, output_size)
IO.puts("")

# 100K mixed
IO.puts("--- 100K rows x 10 fields (mixed, with quoting) ---")
rows = DataGen.mixed_rows(100_000, 10)
output = PureElixirEncoder.encode(rows)
output_size = byte_size(output)

results = [
  Bench.measure("Elixir (iodata)", fn -> PureElixirEncoder.encode(rows) end, warmup: 2_000, time: 5_000),
]
Bench.print_results(results, output_size)

IO.puts("")
IO.puts("=== Compare these numbers against the Rust benchmark results ===")
IO.puts("Rust SIMD times for reference (from cargo bench):")
IO.puts("  10K clean:   729 µs")
IO.puts("  10K mixed:  1584 µs")
IO.puts("  100K clean: 8519 µs")
IO.puts("  100K mixed: 18692 µs")

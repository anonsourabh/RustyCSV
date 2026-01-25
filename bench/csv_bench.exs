# CSV Benchmark: RustyCSV Strategies vs NimbleCSV
#
# Usage: mix run bench/csv_bench.exs
#
# Strategies benchmarked:
#   A: Basic byte-by-byte parsing
#   B: SIMD-accelerated via memchr
#   C: Two-phase index-then-extract
#   D: Streaming (separate memory test)
#   E: Parallel via rayon

defmodule CsvBench do
  def run do
    # Check for test file
    test_file = "bench/data/large.csv"

    unless File.exists?(test_file) do
      IO.puts("Generating test CSV file...")
      generate_test_csv(test_file, 100_000)
    end

    csv = File.read!(test_file)
    file_size = byte_size(csv)
    IO.puts("\n=== CSV Benchmark ===")
    IO.puts("File: #{test_file}")
    IO.puts("Size: #{Float.round(file_size / 1_000_000, 2)} MB")
    IO.puts("")

    # Warm up all strategies
    IO.puts("Warming up...")
    _ = RustyCSV.parse_string(csv, strategy: :basic)
    _ = RustyCSV.parse_string(csv, strategy: :simd)
    _ = RustyCSV.parse_string(csv, strategy: :indexed)
    _ = RustyCSV.parse_string(csv, strategy: :parallel)
    _ = RustyCSV.Nimble.parse_string(csv)

    # Main benchmark
    Benchee.run(
      %{
        "RustyCSV (basic)" => fn -> RustyCSV.parse_string(csv, strategy: :basic) end,
        "RustyCSV (SIMD)" => fn -> RustyCSV.parse_string(csv, strategy: :simd) end,
        "RustyCSV (indexed)" => fn -> RustyCSV.parse_string(csv, strategy: :indexed) end,
        "RustyCSV (parallel)" => fn -> RustyCSV.parse_string(csv, strategy: :parallel) end,
        "NimbleCSV" => fn -> RustyCSV.Nimble.parse_string(csv) end
      },
      warmup: 2,
      time: 5,
      memory_time: 2,
      print: [configuration: false]
    )

    # BEAM Reductions comparison
    IO.puts("\n=== BEAM Reductions ===")
    measure_reductions("RustyCSV (SIMD)", fn -> RustyCSV.parse_string(csv, strategy: :simd) end)
    measure_reductions("RustyCSV (parallel)", fn -> RustyCSV.parse_string(csv, strategy: :parallel) end)
    measure_reductions("NimbleCSV", fn -> RustyCSV.Nimble.parse_string(csv) end)

    # Rust memory usage for each strategy
    IO.puts("\n=== Rust Memory (NIF-side) ===")
    measure_rust_memory("RustyCSV (basic)", fn -> RustyCSV.parse_string(csv, strategy: :basic) end)
    measure_rust_memory("RustyCSV (SIMD)", fn -> RustyCSV.parse_string(csv, strategy: :simd) end)
    measure_rust_memory("RustyCSV (indexed)", fn -> RustyCSV.parse_string(csv, strategy: :indexed) end)
    measure_rust_memory("RustyCSV (parallel)", fn -> RustyCSV.parse_string(csv, strategy: :parallel) end)

    # Streaming memory test
    IO.puts("\n=== Streaming Memory Test ===")
    measure_streaming_memory(test_file)

    # Correctness verification
    IO.puts("\n=== Correctness Verification ===")
    verify_strategies(csv)
  end

  defp measure_reductions(name, fun) do
    {:reductions, reductions_before} = Process.info(self(), :reductions)
    _ = fun.()
    {:reductions, reductions_after} = Process.info(self(), :reductions)
    reductions = reductions_after - reductions_before
    IO.puts("#{name}: #{format_number(reductions)} reductions")
  end

  defp measure_rust_memory(name, fun) do
    # Reset stats before measurement
    RustyCSV.Native.reset_rust_memory_stats()

    before = RustyCSV.Native.get_rust_memory()
    _ = fun.()
    after_mem = RustyCSV.Native.get_rust_memory()
    peak = RustyCSV.Native.get_rust_memory_peak()

    delta = after_mem - before
    peak_delta = peak - before

    IO.puts("#{name}:")
    IO.puts("  Peak allocation: #{format_bytes(peak_delta)}")
    IO.puts("  Retained after:  #{format_bytes(delta)}")
  end

  defp measure_streaming_memory(path) do
    # Reset stats
    RustyCSV.Native.reset_rust_memory_stats()

    before = RustyCSV.Native.get_rust_memory()

    # Stream the file and count rows
    row_count =
      RustyCSV.stream_file(path, chunk_size: 64 * 1024)
      |> Enum.count()

    after_mem = RustyCSV.Native.get_rust_memory()
    peak = RustyCSV.Native.get_rust_memory_peak()

    delta = after_mem - before
    peak_delta = peak - before

    IO.puts("RustyCSV (streaming):")
    IO.puts("  Rows processed:  #{format_number(row_count)}")
    IO.puts("  Peak allocation: #{format_bytes(peak_delta)}")
    IO.puts("  Retained after:  #{format_bytes(delta)}")
  end

  defp verify_strategies(csv) do
    # Parse with each strategy
    basic = RustyCSV.parse_string(csv, strategy: :basic)
    simd = RustyCSV.parse_string(csv, strategy: :simd)
    indexed = RustyCSV.parse_string(csv, strategy: :indexed)
    parallel = RustyCSV.parse_string(csv, strategy: :parallel)

    # Compare results
    all_equal =
      basic == simd and
      simd == indexed and
      indexed == parallel

    if all_equal do
      IO.puts("All strategies produce identical results (#{length(basic)} rows)")
    else
      IO.puts("WARNING: Strategy results differ!")
      IO.puts("  basic: #{length(basic)} rows")
      IO.puts("  simd: #{length(simd)} rows")
      IO.puts("  indexed: #{length(indexed)} rows")
      IO.puts("  parallel: #{length(parallel)} rows")

      # Find first difference
      find_first_difference(basic, simd, "basic", "simd")
      find_first_difference(simd, indexed, "simd", "indexed")
      find_first_difference(indexed, parallel, "indexed", "parallel")
    end
  end

  defp find_first_difference(list1, list2, name1, name2) do
    list1
    |> Enum.zip(list2)
    |> Enum.with_index()
    |> Enum.find(fn {{a, b}, _} -> a != b end)
    |> case do
      nil -> :ok
      {{a, b}, idx} ->
        IO.puts("  First diff at row #{idx}:")
        IO.puts("    #{name1}: #{inspect(a)}")
        IO.puts("    #{name2}: #{inspect(b)}")
    end
  end

  defp format_bytes(n) when n >= 1_000_000, do: "#{Float.round(n / 1_000_000, 2)} MB"
  defp format_bytes(n) when n >= 1_000, do: "#{Float.round(n / 1_000, 1)} KB"
  defp format_bytes(n), do: "#{n} bytes"

  defp format_number(n) when n >= 1_000_000, do: "#{Float.round(n / 1_000_000, 2)}M"
  defp format_number(n) when n >= 1_000, do: "#{Float.round(n / 1_000, 1)}K"
  defp format_number(n), do: "#{n}"

  defp generate_test_csv(path, rows) do
    File.mkdir_p!(Path.dirname(path))

    headers = "id,name,email,amount,date,description,status,category,notes,extra\n"

    data =
      1..rows
      |> Enum.map(fn i ->
        [
          Integer.to_string(i),
          "User #{i}",
          "user#{i}@example.com",
          Float.to_string(:rand.uniform() * 1000),
          "2024-01-#{rem(i, 28) + 1}",
          "Description for row #{i} with some longer text",
          Enum.random(["active", "pending", "completed", "cancelled"]),
          Enum.random(["A", "B", "C", "D"]),
          "Notes #{i}",
          "Extra data #{i}"
        ]
        |> Enum.join(",")
      end)
      |> Enum.join("\n")

    File.write!(path, headers <> data)
    IO.puts("Generated #{path} with #{rows} rows")
  end
end

CsvBench.run()

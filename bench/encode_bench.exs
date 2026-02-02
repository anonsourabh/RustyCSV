# CSV Encoding Benchmark: NIF strategies vs Pure Elixir
#
# Usage: mix run bench/encode_bench.exs
#
# Compares:
#   - Pure Elixir encoding (dump_to_iodata with :elixir strategy)
#   - NIF Scalar encoding (byte-by-byte scanning in Rust)
#   - NIF SWAR encoding (8-byte Mycroft's trick)
#   - NIF SIMD encoding (16/32-byte vectorized scanning)

defmodule EncodeBench do
  def run do
    IO.puts("\n=== CSV Encoding Benchmark ===\n")

    # Generate test data of different sizes
    small_rows = generate_rows(1_000, 10)
    medium_rows = generate_rows(10_000, 10)
    large_rows = generate_rows(100_000, 10)

    # Also generate data with fields that need quoting
    mixed_rows = generate_mixed_rows(10_000, 10)

    IO.puts("Test datasets:")
    IO.puts("  small:  #{length(small_rows)} rows x 10 fields")
    IO.puts("  medium: #{length(medium_rows)} rows x 10 fields")
    IO.puts("  large:  #{length(large_rows)} rows x 10 fields")
    IO.puts("  mixed:  #{length(mixed_rows)} rows x 10 fields (with quoting)")
    IO.puts("")

    # Verify correctness first
    IO.puts("=== Correctness Verification ===")
    verify_correctness(small_rows)
    verify_correctness(mixed_rows)
    IO.puts("")

    # Warm up
    IO.puts("Warming up...")
    for strategy <- [:elixir, :scalar, :swar, :simd] do
      _ = RustyCSV.RFC4180.dump_to_iodata(small_rows, encode_strategy: strategy)
    end

    # Benchmark: Medium dataset (10K rows) — clean data
    IO.puts("\n--- Medium dataset (10K rows, clean data) ---")
    Benchee.run(
      %{
        "Elixir (pure)" => fn ->
          RustyCSV.RFC4180.dump_to_iodata(medium_rows, encode_strategy: :elixir)
        end,
        "NIF (scalar)" => fn ->
          RustyCSV.RFC4180.dump_to_iodata(medium_rows, encode_strategy: :scalar)
        end,
        "NIF (SWAR)" => fn ->
          RustyCSV.RFC4180.dump_to_iodata(medium_rows, encode_strategy: :swar)
        end,
        "NIF (SIMD)" => fn ->
          RustyCSV.RFC4180.dump_to_iodata(medium_rows, encode_strategy: :simd)
        end
      },
      warmup: 2,
      time: 5,
      memory_time: 2,
      print: [configuration: false]
    )

    # Benchmark: Medium dataset (10K rows) — mixed data with quoting
    IO.puts("\n--- Medium dataset (10K rows, mixed data with quoting) ---")
    Benchee.run(
      %{
        "Elixir (pure)" => fn ->
          RustyCSV.RFC4180.dump_to_iodata(mixed_rows, encode_strategy: :elixir)
        end,
        "NIF (scalar)" => fn ->
          RustyCSV.RFC4180.dump_to_iodata(mixed_rows, encode_strategy: :scalar)
        end,
        "NIF (SWAR)" => fn ->
          RustyCSV.RFC4180.dump_to_iodata(mixed_rows, encode_strategy: :swar)
        end,
        "NIF (SIMD)" => fn ->
          RustyCSV.RFC4180.dump_to_iodata(mixed_rows, encode_strategy: :simd)
        end
      },
      warmup: 2,
      time: 5,
      memory_time: 2,
      print: [configuration: false]
    )

    # Benchmark: Large dataset (100K rows)
    IO.puts("\n--- Large dataset (100K rows, clean data) ---")
    Benchee.run(
      %{
        "Elixir (pure)" => fn ->
          RustyCSV.RFC4180.dump_to_iodata(large_rows, encode_strategy: :elixir)
        end,
        "NIF (scalar)" => fn ->
          RustyCSV.RFC4180.dump_to_iodata(large_rows, encode_strategy: :scalar)
        end,
        "NIF (SWAR)" => fn ->
          RustyCSV.RFC4180.dump_to_iodata(large_rows, encode_strategy: :swar)
        end,
        "NIF (SIMD)" => fn ->
          RustyCSV.RFC4180.dump_to_iodata(large_rows, encode_strategy: :simd)
        end
      },
      warmup: 2,
      time: 5,
      memory_time: 2,
      print: [configuration: false]
    )

    # Direct NIF benchmark (bypass Elixir wrapper overhead)
    IO.puts("\n--- Direct NIF calls (10K rows, measuring NIF overhead) ---")
    Benchee.run(
      %{
        "NIF scalar (direct)" => fn ->
          RustyCSV.Native.encode_string_scalar(medium_rows, ",", "\"", :default)
        end,
        "NIF SWAR (direct)" => fn ->
          RustyCSV.Native.encode_string_swar(medium_rows, ",", "\"", :default)
        end,
        "NIF SIMD (direct)" => fn ->
          RustyCSV.Native.encode_string_simd(medium_rows, ",", "\"", :default)
        end
      },
      warmup: 2,
      time: 5,
      memory_time: 2,
      print: [configuration: false]
    )

    # Output size comparison
    IO.puts("\n=== Output Size ===")
    elixir_out = RustyCSV.RFC4180.dump_to_iodata(medium_rows, encode_strategy: :elixir)
    nif_out = RustyCSV.RFC4180.dump_to_iodata(medium_rows, encode_strategy: :simd)
    IO.puts("  Elixir iodata size: #{IO.iodata_length(elixir_out)} bytes")
    IO.puts("  NIF binary size:    #{byte_size(nif_out)} bytes")
    IO.puts("  Match: #{IO.iodata_to_binary(elixir_out) == nif_out}")
  end

  defp generate_rows(count, fields_per_row) do
    for i <- 1..count do
      for j <- 1..fields_per_row do
        "field_#{i}_#{j}_value"
      end
    end
  end

  defp generate_mixed_rows(count, fields_per_row) do
    for i <- 1..count do
      for j <- 1..fields_per_row do
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

  defp verify_correctness(rows) do
    elixir_result =
      RustyCSV.RFC4180.dump_to_iodata(rows, encode_strategy: :elixir)
      |> IO.iodata_to_binary()

    for strategy <- [:scalar, :swar, :simd] do
      nif_result =
        RustyCSV.RFC4180.dump_to_iodata(rows, encode_strategy: strategy)
        |> IO.iodata_to_binary()

      if elixir_result == nif_result do
        IO.puts("  #{strategy}: PASS (#{byte_size(nif_result)} bytes)")
      else
        IO.puts("  #{strategy}: FAIL")
        # Find first difference
        elixir_bytes = :binary.bin_to_list(elixir_result)
        nif_bytes = :binary.bin_to_list(nif_result)

        diff_pos =
          elixir_bytes
          |> Enum.zip(nif_bytes)
          |> Enum.with_index()
          |> Enum.find(fn {{a, b}, _} -> a != b end)

        case diff_pos do
          nil ->
            IO.puts("    Lengths differ: elixir=#{byte_size(elixir_result)} nif=#{byte_size(nif_result)}")

          {{a, b}, idx} ->
            IO.puts("    First diff at byte #{idx}: elixir=#{a} nif=#{b}")
            context_start = max(0, idx - 20)
            IO.puts(
              "    Elixir context: #{inspect(binary_part(elixir_result, context_start, min(40, byte_size(elixir_result) - context_start)))}"
            )
            IO.puts(
              "    NIF context:    #{inspect(binary_part(nif_result, context_start, min(40, byte_size(nif_result) - context_start)))}"
            )
        end
      end
    end
  end
end

EncodeBench.run()

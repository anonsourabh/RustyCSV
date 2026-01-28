defmodule Sandbox do
  @moduledoc """
  Performance testing utilities for headers-to-maps feature.
  """

  @doc """
  Generate CSV data with specified dimensions.
  """
  def generate_csv(rows, cols) do
    headers = Enum.map(1..cols, fn i -> "column_#{i}" end)
    header_line = Enum.join(headers, ",")

    data_lines =
      for row <- 1..rows do
        Enum.map(1..cols, fn col -> "value_#{row}_#{col}" end)
        |> Enum.join(",")
      end

    [header_line | data_lines]
    |> Enum.join("\n")
    |> Kernel.<>("\n")
  end

  @doc """
  Generate CSV with realistic field patterns (varying lengths, some quoted).
  """
  def generate_realistic_csv(rows, cols) do
    headers =
      Enum.map(1..cols, fn i ->
        Enum.random([
          "id",
          "name",
          "email",
          "status",
          "created_at",
          "updated_at",
          "description",
          "amount",
          "quantity",
          "price",
          "total",
          "user_id",
          "order_id",
          "product_id",
          "category",
          "tags"
        ])
        |> Kernel.<>("_#{i}")
      end)

    header_line = Enum.join(headers, ",")

    data_lines =
      for _row <- 1..rows do
        Enum.map(1..cols, fn _col ->
          case :rand.uniform(5) do
            1 -> Integer.to_string(:rand.uniform(100_000))
            2 -> "user_#{:rand.uniform(1000)}@example.com"
            3 -> "\"Value with, comma\""
            4 -> Date.utc_today() |> Date.to_string()
            5 -> random_string(10 + :rand.uniform(20))
          end
        end)
        |> Enum.join(",")
      end

    [header_line | data_lines]
    |> Enum.join("\n")
    |> Kernel.<>("\n")
  end

  defp random_string(length) do
    for _ <- 1..length, into: "", do: <<Enum.random(?a..?z)>>
  end

  @doc """
  Baseline approach: parse to lists, then zip with headers in Elixir.
  """
  def baseline_to_maps(csv_string) do
    alias RustyCSV.RFC4180, as: CSV

    [headers | rows] = CSV.parse_string(csv_string, skip_headers: false)

    Enum.map(rows, fn row ->
      Enum.zip(headers, row) |> Map.new()
    end)
  end

  @doc """
  Optimized Elixir approach: pre-compute headers once.
  """
  def optimized_elixir_to_maps(csv_string) do
    alias RustyCSV.RFC4180, as: CSV

    [headers | rows] = CSV.parse_string(csv_string, skip_headers: false)

    # Pre-allocate the keys list once
    keys = headers

    Enum.map(rows, fn row ->
      keys
      |> Enum.zip(row)
      |> Map.new()
    end)
  end

  @doc """
  Using the CSV library's built-in headers support for comparison.
  """
  def csv_library_to_maps(csv_string) do
    csv_string
    |> String.split("\n", trim: true)
    |> Stream.map(&(&1 <> "\n"))
    |> CSV.decode!(headers: true)
    |> Enum.to_list()
  end

  @doc """
  NimbleCSV baseline (what users would do manually).
  """
  def nimble_to_maps(csv_string) do
    [headers | rows] = NimbleCSV.RFC4180.parse_string(csv_string, skip_headers: false)

    Enum.map(rows, fn row ->
      Enum.zip(headers, row) |> Map.new()
    end)
  end
end

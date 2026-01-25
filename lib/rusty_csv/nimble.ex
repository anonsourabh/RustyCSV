defmodule RustyCSV.Nimble do
  @moduledoc """
  NimbleCSV parser for benchmarking comparison.
  """

  NimbleCSV.define(Parser, separator: ",", escape: "\"")

  def parse_string(csv) do
    Parser.parse_string(csv, skip_headers: false)
  end

  def parse_file(path) do
    path
    |> File.read!()
    |> Parser.parse_string(skip_headers: false)
  end
end

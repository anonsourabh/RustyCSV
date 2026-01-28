alias RustyCSV.RFC4180, as: R
alias NimbleCSV.RFC4180, as: N

defmodule Compare do
  def check(label, rusty, nimble) do
    if rusty == nimble do
      IO.puts("  OK  #{label}")
    else
      IO.puts("  FAIL #{label}")
      IO.puts("       Rusty:  #{inspect(rusty)}")
      IO.puts("       Nimble: #{inspect(nimble)}")
    end
  end
end

IO.puts("=== parse_string ===")

csv = "name,age,city\njohn,27,nyc\njane,32,sf\n"
Compare.check("basic (skip_headers: true)",
  R.parse_string(csv),
  N.parse_string(csv))

Compare.check("basic (skip_headers: false)",
  R.parse_string(csv, skip_headers: false),
  N.parse_string(csv, skip_headers: false))

csv2 = "a,b\n\"hello \"\"world\"\"\",\"line1\nline2\"\n"
Compare.check("quoted + escaped + multiline",
  R.parse_string(csv2, skip_headers: false),
  N.parse_string(csv2, skip_headers: false))

csv3 = "a,,c\n\"\",b,\n"
Compare.check("empty fields",
  R.parse_string(csv3, skip_headers: false),
  N.parse_string(csv3, skip_headers: false))

csv4 = "a,b\r\nc,d\ne,f\r\n"
Compare.check("mixed CRLF/LF",
  R.parse_string(csv4, skip_headers: false),
  N.parse_string(csv4, skip_headers: false))

csv5 = "a,b\nc,d"
Compare.check("no trailing newline",
  R.parse_string(csv5, skip_headers: false),
  N.parse_string(csv5, skip_headers: false))

csv6 = ""
Compare.check("empty string",
  R.parse_string(csv6, skip_headers: false),
  N.parse_string(csv6, skip_headers: false))

csv7 = "a,b\n"
Compare.check("single header row only (skip: true)",
  R.parse_string(csv7),
  N.parse_string(csv7))

csv8 = "a,\" b \",c\n"
Compare.check("whitespace in quoted field",
  R.parse_string(csv8, skip_headers: false),
  N.parse_string(csv8, skip_headers: false))

csv9 = "a,b\n1,2\n3,4\n5,6\n"
Compare.check("multi-row",
  R.parse_string(csv9, skip_headers: false),
  N.parse_string(csv9, skip_headers: false))

IO.puts("\n=== dump_to_iodata ===")

rows = [["john", "27", "nyc"], ["jane", "32", "sf"]]
Compare.check("basic dump",
  IO.iodata_to_binary(R.dump_to_iodata(rows)),
  IO.iodata_to_binary(N.dump_to_iodata(rows)))

rows2 = [["hello \"world\"", "line1\nline2", "normal"]]
Compare.check("dump with escaping",
  IO.iodata_to_binary(R.dump_to_iodata(rows2)),
  IO.iodata_to_binary(N.dump_to_iodata(rows2)))

rows3 = [["has,comma", "has\"quote", "plain"]]
Compare.check("dump separator+quote in field",
  IO.iodata_to_binary(R.dump_to_iodata(rows3)),
  IO.iodata_to_binary(N.dump_to_iodata(rows3)))

rows4 = [["", "", ""]]
Compare.check("dump empty fields",
  IO.iodata_to_binary(R.dump_to_iodata(rows4)),
  IO.iodata_to_binary(N.dump_to_iodata(rows4)))

rows5 = [[123, :atom, 45.6]]
Compare.check("dump non-binary types",
  IO.iodata_to_binary(R.dump_to_iodata(rows5)),
  IO.iodata_to_binary(N.dump_to_iodata(rows5)))

IO.puts("\n=== dump_to_stream ===")

rows = [["a", "b"], ["c", "d"]]
Compare.check("dump_to_stream",
  R.dump_to_stream(rows) |> Enum.map(&IO.iodata_to_binary/1),
  N.dump_to_stream(rows) |> Enum.map(&IO.iodata_to_binary/1))

IO.puts("\n=== parse_stream ===")

path = "bench/data/large_bench.csv"
Compare.check("parse_stream row count",
  File.stream!(path) |> R.parse_stream() |> Enum.count(),
  File.stream!(path) |> N.parse_stream() |> Enum.count())

Compare.check("parse_stream first 5 rows",
  File.stream!(path) |> R.parse_stream() |> Enum.take(5),
  File.stream!(path) |> N.parse_stream() |> Enum.take(5))

Compare.check("parse_stream last 5 rows",
  File.stream!(path) |> R.parse_stream() |> Enum.slice(-5..-1//1),
  File.stream!(path) |> N.parse_stream() |> Enum.slice(-5..-1//1))

Compare.check("parse_stream skip_headers: false first 3",
  File.stream!(path) |> R.parse_stream(skip_headers: false) |> Enum.take(3),
  File.stream!(path) |> N.parse_stream(skip_headers: false) |> Enum.take(3))

IO.puts("\n=== to_line_stream ===")

chunks = ["a,b\nc,", "d\ne,f\n"]
Compare.check("to_line_stream",
  R.to_line_stream(chunks) |> Enum.to_list(),
  N.to_line_stream(chunks) |> Enum.to_list())

IO.puts("\n=== parse_enumerable ===")

lines = ["name,age\n", "john,27\n", "jane,32\n"]
Compare.check("parse_enumerable (skip: true)",
  R.parse_enumerable(lines),
  N.parse_enumerable(lines))

Compare.check("parse_enumerable (skip: false)",
  R.parse_enumerable(lines, skip_headers: false),
  N.parse_enumerable(lines, skip_headers: false))

IO.puts("\n=== roundtrip ===")

original = [["hello \"world\"", "line1\nline2", "has,comma"], ["normal", "", "  spaces  "]]
Compare.check("parse(dump(rows)) roundtrip",
  R.dump_to_iodata(original) |> IO.iodata_to_binary() |> R.parse_string(skip_headers: false),
  N.dump_to_iodata(original) |> IO.iodata_to_binary() |> N.parse_string(skip_headers: false))

IO.puts("\n=== options ===")

r_opts = R.options()
n_opts = N.options()
Compare.check("separator", r_opts[:separator], n_opts[:separator])
Compare.check("escape", r_opts[:escape], n_opts[:escape])

IO.puts("\n=== Spreadsheet module ===")

tsv = "name\tage\n\"john\"\t27\njane\t32\n"
Compare.check("Spreadsheet parse_string",
  RustyCSV.Spreadsheet.parse_string(tsv, skip_headers: false),
  NimbleCSV.Spreadsheet.parse_string(tsv, skip_headers: false))

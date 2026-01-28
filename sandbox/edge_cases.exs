alias RustyCSV.RFC4180, as: R
alias NimbleCSV.RFC4180, as: N

defmodule Edge do
  def check(label, rusty_fn, nimble_fn) do
    try do
      rusty = rusty_fn.()
      nimble = nimble_fn.()

      if rusty == nimble do
        IO.puts("  OK    #{label}")
      else
        IO.puts("  DIFF  #{label}")
        IO.puts("        Rusty:  #{inspect(rusty)}")
        IO.puts("        Nimble: #{inspect(nimble)}")
      end
    rescue
      e ->
        IO.puts("  ERR   #{label}: #{inspect(e)}")
    end
  end

  def check_both_raise(label, rusty_fn, nimble_fn) do
    r_err = try do rusty_fn.(); :no_error rescue e -> e end
    n_err = try do nimble_fn.(); :no_error rescue e -> e end

    cond do
      r_err != :no_error and n_err != :no_error ->
        IO.puts("  OK    #{label} (both raise)")
      r_err == :no_error and n_err == :no_error ->
        IO.puts("  OK    #{label} (neither raises)")
      r_err == :no_error ->
        IO.puts("  DIFF  #{label} — Rusty succeeds, Nimble raises: #{inspect(n_err)}")
      true ->
        IO.puts("  DIFF  #{label} — Rusty raises: #{inspect(r_err)}, Nimble succeeds")
    end
  end
end

opts = [skip_headers: false]

IO.puts("=== Quoted field edge cases ===")

Edge.check("escaped quotes inside quoted field",
  fn -> R.parse_string("\"he said \"\"hi\"\"\"\n", opts) end,
  fn -> N.parse_string("\"he said \"\"hi\"\"\"\n", opts) end)

Edge.check("newline inside quoted field",
  fn -> R.parse_string("\"line1\nline2\"\n", opts) end,
  fn -> N.parse_string("\"line1\nline2\"\n", opts) end)

Edge.check("CRLF inside quoted field",
  fn -> R.parse_string("\"line1\r\nline2\"\r\n", opts) end,
  fn -> N.parse_string("\"line1\r\nline2\"\r\n", opts) end)

Edge.check("separator inside quoted field",
  fn -> R.parse_string("\"has,comma\",b\n", opts) end,
  fn -> N.parse_string("\"has,comma\",b\n", opts) end)

Edge.check("empty quoted field",
  fn -> R.parse_string("\"\",b\n", opts) end,
  fn -> N.parse_string("\"\",b\n", opts) end)

Edge.check("quoted field with only spaces",
  fn -> R.parse_string("\"   \",b\n", opts) end,
  fn -> N.parse_string("\"   \",b\n", opts) end)

Edge.check("quoted field with only escape chars",
  fn -> R.parse_string("\"\"\"\"\"\"\n", opts) end,
  fn -> N.parse_string("\"\"\"\"\"\"\n", opts) end)

Edge.check("adjacent quoted fields",
  fn -> R.parse_string("\"a\",\"b\",\"c\"\n", opts) end,
  fn -> N.parse_string("\"a\",\"b\",\"c\"\n", opts) end)

IO.puts("\n=== Empty / minimal edge cases ===")

Edge.check("empty string",
  fn -> R.parse_string("", opts) end,
  fn -> N.parse_string("", opts) end)

Edge.check("only newline",
  fn -> R.parse_string("\n", opts) end,
  fn -> N.parse_string("\n", opts) end)

Edge.check("only CRLF",
  fn -> R.parse_string("\r\n", opts) end,
  fn -> N.parse_string("\r\n", opts) end)

Edge.check("multiple empty lines",
  fn -> R.parse_string("\n\n\n", opts) end,
  fn -> N.parse_string("\n\n\n", opts) end)

Edge.check("single field",
  fn -> R.parse_string("hello\n", opts) end,
  fn -> N.parse_string("hello\n", opts) end)

Edge.check("single empty field",
  fn -> R.parse_string(",\n", opts) end,
  fn -> N.parse_string(",\n", opts) end)

Edge.check("trailing comma (extra empty field)",
  fn -> R.parse_string("a,b,\n", opts) end,
  fn -> N.parse_string("a,b,\n", opts) end)

Edge.check("leading comma",
  fn -> R.parse_string(",a,b\n", opts) end,
  fn -> N.parse_string(",a,b\n", opts) end)

Edge.check("all empty fields",
  fn -> R.parse_string(",,\n,,\n", opts) end,
  fn -> N.parse_string(",,\n,,\n", opts) end)

IO.puts("\n=== No trailing newline ===")

Edge.check("no trailing newline single row",
  fn -> R.parse_string("a,b", opts) end,
  fn -> N.parse_string("a,b", opts) end)

Edge.check("no trailing newline multi row",
  fn -> R.parse_string("a,b\nc,d", opts) end,
  fn -> N.parse_string("a,b\nc,d", opts) end)

Edge.check("no trailing newline quoted",
  fn -> R.parse_string("\"a\",\"b\"", opts) end,
  fn -> N.parse_string("\"a\",\"b\"", opts) end)

IO.puts("\n=== Newline variations ===")

Edge.check("LF only",
  fn -> R.parse_string("a,b\nc,d\n", opts) end,
  fn -> N.parse_string("a,b\nc,d\n", opts) end)

Edge.check("CRLF only",
  fn -> R.parse_string("a,b\r\nc,d\r\n", opts) end,
  fn -> N.parse_string("a,b\r\nc,d\r\n", opts) end)

Edge.check("mixed LF and CRLF",
  fn -> R.parse_string("a,b\nc,d\r\ne,f\n", opts) end,
  fn -> N.parse_string("a,b\nc,d\r\ne,f\n", opts) end)

Edge.check("lone CR (not a newline in RFC4180)",
  fn -> R.parse_string("a\rb\n", opts) end,
  fn -> N.parse_string("a\rb\n", opts) end)

IO.puts("\n=== Unicode ===")

Edge.check("unicode in fields",
  fn -> R.parse_string("café,naïve\n日本語,中文\n", opts) end,
  fn -> N.parse_string("café,naïve\n日本語,中文\n", opts) end)

Edge.check("unicode in quoted fields",
  fn -> R.parse_string("\"café\",\"naïve\"\n", opts) end,
  fn -> N.parse_string("\"café\",\"naïve\"\n", opts) end)

Edge.check("emoji",
  fn -> R.parse_string("👋,🌍\n", opts) end,
  fn -> N.parse_string("👋,🌍\n", opts) end)

IO.puts("\n=== Large / stress fields ===")

long_field = String.duplicate("x", 100_000)
Edge.check("100KB single field",
  fn -> R.parse_string(long_field <> "\n", opts) end,
  fn -> N.parse_string(long_field <> "\n", opts) end)

long_quoted = "\"" <> String.duplicate("x", 100_000) <> "\"\n"
Edge.check("100KB quoted field",
  fn -> R.parse_string(long_quoted, opts) end,
  fn -> N.parse_string(long_quoted, opts) end)

many_fields = Enum.join(1..1000, ",") <> "\n"
Edge.check("1000 fields in one row",
  fn -> R.parse_string(many_fields, opts) end,
  fn -> N.parse_string(many_fields, opts) end)

IO.puts("\n=== Whitespace handling ===")

Edge.check("spaces around fields (unquoted)",
  fn -> R.parse_string(" a , b , c \n", opts) end,
  fn -> N.parse_string(" a , b , c \n", opts) end)

Edge.check("tabs in fields",
  fn -> R.parse_string("a\tb,c\n", opts) end,
  fn -> N.parse_string("a\tb,c\n", opts) end)

Edge.check("space before quote",
  fn -> R.parse_string(" \"a\",b\n", opts) end,
  fn -> N.parse_string(" \"a\",b\n", opts) end)

IO.puts("\n=== Dump edge cases ===")

Edge.check("dump empty list",
  fn -> IO.iodata_to_binary(R.dump_to_iodata([])) end,
  fn -> IO.iodata_to_binary(N.dump_to_iodata([])) end)

Edge.check("dump single empty row",
  fn -> IO.iodata_to_binary(R.dump_to_iodata([[""]])) end,
  fn -> IO.iodata_to_binary(N.dump_to_iodata([[""]])) end)

Edge.check("dump field with newline",
  fn -> IO.iodata_to_binary(R.dump_to_iodata([["a\nb"]])) end,
  fn -> IO.iodata_to_binary(N.dump_to_iodata([["a\nb"]])) end)

Edge.check("dump field with CRLF",
  fn -> IO.iodata_to_binary(R.dump_to_iodata([["a\r\nb"]])) end,
  fn -> IO.iodata_to_binary(N.dump_to_iodata([["a\r\nb"]])) end)

Edge.check("dump field with quote",
  fn -> IO.iodata_to_binary(R.dump_to_iodata([["say \"hi\""]])) end,
  fn -> IO.iodata_to_binary(N.dump_to_iodata([["say \"hi\""]])) end)

Edge.check("dump unicode",
  fn -> IO.iodata_to_binary(R.dump_to_iodata([["café", "日本"]])) end,
  fn -> IO.iodata_to_binary(N.dump_to_iodata([["café", "日本"]])) end)

Edge.check("dump integer and float",
  fn -> IO.iodata_to_binary(R.dump_to_iodata([[42, 3.14]])) end,
  fn -> IO.iodata_to_binary(N.dump_to_iodata([[42, 3.14]])) end)

Edge.check("dump nil",
  fn -> IO.iodata_to_binary(R.dump_to_iodata([[nil]])) end,
  fn -> IO.iodata_to_binary(N.dump_to_iodata([[nil]])) end)

IO.puts("\n=== Roundtrip edge cases ===")

roundtrip_cases = [
  {"quotes in field", [["say \"hi\"", "ok"]]},
  {"newline in field", [["line1\nline2", "ok"]]},
  {"CRLF in field", [["line1\r\nline2", "ok"]]},
  {"comma in field", [["a,b", "ok"]]},
  {"all special chars", [["\"hello\",\nworld\r\n", "a,b"]]},
  {"empty fields", [["", "", ""]]},
  {"unicode", [["café", "日本語"]]},
]

for {label, rows} <- roundtrip_cases do
  Edge.check("roundtrip: #{label}",
    fn ->
      R.dump_to_iodata(rows) |> IO.iodata_to_binary() |> R.parse_string(opts)
    end,
    fn ->
      N.dump_to_iodata(rows) |> IO.iodata_to_binary() |> N.parse_string(opts)
    end)
end

IO.puts("\n=== Streaming edge cases ===")

Edge.check("stream: single chunk",
  fn -> ["a,b\nc,d\n"] |> R.parse_stream(opts) |> Enum.to_list() end,
  fn -> ["a,b\nc,d\n"] |> N.parse_stream(opts) |> Enum.to_list() end)

Edge.check("stream: char-by-char",
  fn -> String.graphemes("a,b\nc,d\n") |> R.parse_stream(opts) |> Enum.to_list() end,
  fn -> String.graphemes("a,b\nc,d\n") |> N.parse_stream(opts) |> Enum.to_list() end)

Edge.check("stream: split mid-field",
  fn -> ["a,", "b\nc", ",d\n"] |> R.parse_stream(opts) |> Enum.to_list() end,
  fn -> ["a,", "b\nc", ",d\n"] |> N.parse_stream(opts) |> Enum.to_list() end)

Edge.check("stream: split mid-quoted-field",
  fn -> ["\"hel", "lo\",b\n"] |> R.parse_stream(opts) |> Enum.to_list() end,
  fn -> ["\"hel", "lo\",b\n"] |> N.parse_stream(opts) |> Enum.to_list() end)

Edge.check("stream: split mid-escape",
  fn -> ["\"say \"", "\"hi\"\"\",b\n"] |> R.parse_stream(opts) |> Enum.to_list() end,
  fn -> ["\"say \"", "\"hi\"\"\",b\n"] |> N.parse_stream(opts) |> Enum.to_list() end)

Edge.check("stream: quoted field spans chunks with newline",
  fn -> ["\"line1\n", "line2\",b\n"] |> R.parse_stream(opts) |> Enum.to_list() end,
  fn -> ["\"line1\n", "line2\",b\n"] |> N.parse_stream(opts) |> Enum.to_list() end)

Edge.check("stream: empty chunks interspersed",
  fn -> ["", "a,b\n", "", "", "c,d\n", ""] |> R.parse_stream(opts) |> Enum.to_list() end,
  fn -> ["", "a,b\n", "", "", "c,d\n", ""] |> N.parse_stream(opts) |> Enum.to_list() end)

Edge.check("stream: Enum.take early termination",
  fn -> ["a,b\nc,d\ne,f\n"] |> R.parse_stream(opts) |> Enum.take(1) end,
  fn -> ["a,b\nc,d\ne,f\n"] |> N.parse_stream(opts) |> Enum.take(1) end)

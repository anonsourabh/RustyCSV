defmodule RustyCSV do
  @moduledoc ~S"""
  RustyCSV is a high-performance CSV parsing and dumping library powered by Rust NIFs.

  It provides a drop-in replacement for NimbleCSV with the same API, while offering
  multiple parsing strategies optimized for different use cases.

  ## Quick Start

  Use the pre-defined `RustyCSV.RFC4180` parser:

      alias RustyCSV.RFC4180, as: CSV

      CSV.parse_string("name,age\njohn,27\n")
      #=> [["john", "27"]]

      CSV.parse_string("name,age\njohn,27\n", skip_headers: false)
      #=> [["name", "age"], ["john", "27"]]

  ## Defining Custom Parsers

  You can define custom CSV parsers with `define/2`:

      RustyCSV.define(MyParser,
        separator: ",",
        escape: "\"",
        line_separator: "\n"
      )

      MyParser.parse_string("a,b\n1,2\n")
      #=> [["1", "2"]]

  ## Parsing Strategies

  RustyCSV supports multiple parsing strategies via the `:strategy` option:

    * `:simd` - SIMD-accelerated scanning via memchr (default, fastest for most files)
    * `:basic` - Simple byte-by-byte parsing (good for debugging)
    * `:indexed` - Two-phase index-then-extract (good for re-extracting rows)
    * `:parallel` - Multi-threaded via rayon (best for very large files 100MB+)

  Example:

      CSV.parse_string(large_csv, strategy: :parallel)

  ## Streaming

  For large files, use `parse_stream/2` which uses a bounded-memory streaming parser:

      "huge.csv"
      |> File.stream!()
      |> CSV.parse_stream()
      |> Stream.each(&process_row/1)
      |> Stream.run()

  ## Dumping

  Convert rows back to CSV format:

      CSV.dump_to_iodata([["name", "age"], ["john", "27"]])
      #=> "name,age\njohn,27\n"

  ## NimbleCSV Compatibility

  RustyCSV is designed as a drop-in replacement for NimbleCSV. The API is identical:

    * `parse_string/2` - Parse CSV string to list of rows
    * `parse_stream/2` - Lazily parse a stream
    * `parse_enumerable/2` - Parse any enumerable
    * `dump_to_iodata/1` - Convert rows to iodata
    * `dump_to_stream/1` - Lazily convert rows to iodata stream
    * `to_line_stream/1` - Convert arbitrary chunks to lines
    * `options/0` - Return module configuration

  The only behavioral difference is that RustyCSV adds the `:strategy` option
  for selecting the parsing approach.

  ## Differences from NimbleCSV

  While RustyCSV aims for full compatibility, there are some current limitations:

    * Encoding conversion (`:encoding` option) is not yet supported

  Configurable separators (TSV, custom delimiters) and escape characters are fully supported.

  """

  # ==========================================================================
  # Types
  # ==========================================================================

  @typedoc """
  A single row of CSV data, represented as a list of field binaries.
  """
  @type row :: [binary()]

  @typedoc """
  Multiple rows of CSV data.
  """
  @type rows :: [row()]

  @typedoc """
  Options for parsing functions.

  ## Common Options

    * `:skip_headers` - When `true`, skips the first row. Defaults to `true`.
    * `:strategy` - The parsing strategy to use. One of:
      * `:simd` - SIMD-accelerated (default)
      * `:basic` - Simple byte-by-byte
      * `:indexed` - Two-phase index-then-extract
      * `:parallel` - Multi-threaded via rayon

  ## Streaming Options

    * `:chunk_size` - Bytes per IO read for streaming. Defaults to `65536`.
    * `:batch_size` - Rows per batch for streaming. Defaults to `1000`.

  """
  @type parse_options :: [
          skip_headers: boolean(),
          strategy: :simd | :basic | :indexed | :parallel,
          chunk_size: pos_integer(),
          batch_size: pos_integer()
        ]

  @typedoc """
  Options for `define/2`.

  ## Parsing Options

    * `:separator` - Field separator character. Defaults to `","`.
    * `:escape` - Escape/quote character. Defaults to `"\""`.
    * `:newlines` - List of recognized line endings. Defaults to `["\r\n", "\n"]`.
    * `:trim_bom` - Remove UTF-8 BOM when parsing strings. Defaults to `false`.

  ## Dumping Options

    * `:line_separator` - Line separator for output. Defaults to `"\n"`.
    * `:dump_bom` - Include UTF-8 BOM in output. Defaults to `false`.
    * `:reserved` - Additional characters requiring escaping.
    * `:escape_formula` - Map for formula injection prevention. Defaults to `nil`.

  ## Other Options

    * `:strategy` - Default parsing strategy. Defaults to `:simd`.
    * `:moduledoc` - Documentation for the generated module.

  """
  @type define_options :: [
          separator: String.t(),
          escape: String.t(),
          newlines: [String.t()],
          line_separator: String.t(),
          trim_bom: boolean(),
          dump_bom: boolean(),
          reserved: [String.t()],
          escape_formula: map() | nil,
          strategy: :simd | :basic | :indexed | :parallel,
          moduledoc: String.t() | false | nil
        ]

  # ==========================================================================
  # Exceptions
  # ==========================================================================

  defmodule ParseError do
    @moduledoc """
    Exception raised when CSV parsing fails.

    ## Fields

      * `:message` - Human-readable error description

    """
    defexception [:message]

    @impl true
    def message(%{message: message}), do: message
  end

  # ==========================================================================
  # Callbacks (Behaviour)
  # ==========================================================================

  @doc """
  Returns the options used to define this CSV module.
  """
  @callback options() :: keyword()

  @doc """
  Parses a CSV string into a list of rows.
  """
  @callback parse_string(binary()) :: rows()

  @doc """
  Parses a CSV string into a list of rows with options.
  """
  @callback parse_string(binary(), parse_options()) :: rows()

  @doc """
  Lazily parses a stream of CSV data into a stream of rows.
  """
  @callback parse_stream(Enumerable.t()) :: Enumerable.t()

  @doc """
  Lazily parses a stream of CSV data into a stream of rows with options.
  """
  @callback parse_stream(Enumerable.t(), parse_options()) :: Enumerable.t()

  @doc """
  Eagerly parses an enumerable of CSV data into a list of rows.
  """
  @callback parse_enumerable(Enumerable.t()) :: rows()

  @doc """
  Eagerly parses an enumerable of CSV data into a list of rows with options.
  """
  @callback parse_enumerable(Enumerable.t(), parse_options()) :: rows()

  @doc """
  Converts rows to iodata in CSV format.
  """
  @callback dump_to_iodata(Enumerable.t()) :: iodata()

  @doc """
  Lazily converts rows to a stream of iodata in CSV format.
  """
  @callback dump_to_stream(Enumerable.t()) :: Enumerable.t()

  @doc """
  Converts a stream of arbitrary binary chunks into a line-oriented stream.
  """
  @callback to_line_stream(Enumerable.t()) :: Enumerable.t()

  # ==========================================================================
  # Module Definition
  # ==========================================================================

  @doc ~S"""
  Defines a new CSV parser/dumper module.

  ## Options

  ### Parsing Options

    * `:separator` - The field separator character. Defaults to `","`.
      Currently only comma is supported; other separators will raise an error.

    * `:escape` - The escape/quote character. Defaults to `"\""`.
      Currently only double-quote is supported.

    * `:newlines` - List of recognized line endings for parsing.
      Defaults to `["\r\n", "\n"]`. Both CRLF and LF are always recognized.

    * `:trim_bom` - When `true`, removes the UTF-8 BOM (byte order marker)
      from the beginning of strings before parsing. Defaults to `false`.

  ### Dumping Options

    * `:line_separator` - The line separator for dumped output.
      Defaults to `"\n"`.

    * `:dump_bom` - When `true`, includes a UTF-8 BOM at the start of
      dumped output. Defaults to `false`.

    * `:reserved` - Additional characters that should trigger field escaping
      when dumping. By default, fields containing the separator, escape
      character, or newlines are escaped.

    * `:escape_formula` - A map of characters to their escaped versions
      for preventing CSV formula injection. When set, fields starting with
      these characters will be prefixed with a tab. Defaults to `nil`.

      Example: `%{"=" => true, "+" => true, "-" => true, "@" => true}`

  ### Strategy Options

    * `:strategy` - The default parsing strategy. One of:
      * `:simd` - SIMD-accelerated via memchr (default, fastest)
      * `:basic` - Simple byte-by-byte parsing
      * `:indexed` - Two-phase index-then-extract
      * `:parallel` - Multi-threaded via rayon

  ### Documentation

    * `:moduledoc` - The `@moduledoc` for the generated module.
      Set to `false` to disable documentation.

  ## Examples

      # Define a standard CSV parser
      RustyCSV.define(MyApp.CSV,
        separator: ",",
        escape: "\"",
        line_separator: "\n"
      )

      # Use it
      MyApp.CSV.parse_string("a,b\n1,2\n")
      #=> [["1", "2"]]

      # Get the configuration
      MyApp.CSV.options()
      #=> [separator: ",", escape: "\"", ...]

  """
  @spec define(module(), define_options()) :: :ok
  def define(module, options \\ []) do
    config = extract_and_validate_options(options)
    compile_module(module, config)
    :ok
  end

  # ==========================================================================
  # Private: Option Extraction and Validation
  # ==========================================================================

  defp extract_and_validate_options(options) do
    separator = Keyword.get(options, :separator, ",")
    escape = Keyword.get(options, :escape, "\"")

    validate_single_byte!(:separator, separator)
    validate_single_byte!(:escape, escape)

    <<separator_byte>> = separator
    <<escape_byte>> = escape

    line_separator = Keyword.get(options, :line_separator, "\n")
    newlines = Keyword.get(options, :newlines, ["\r\n", "\n"])
    trim_bom = Keyword.get(options, :trim_bom, false)
    dump_bom = Keyword.get(options, :dump_bom, false)
    reserved = Keyword.get(options, :reserved, [])
    escape_formula = Keyword.get(options, :escape_formula, nil)
    default_strategy = Keyword.get(options, :strategy, :simd)
    moduledoc = Keyword.get(options, :moduledoc)

    escape_chars = [separator, escape, "\n", "\r"] ++ reserved

    stored_options = [
      separator: separator,
      escape: escape,
      line_separator: line_separator,
      newlines: newlines,
      trim_bom: trim_bom,
      dump_bom: dump_bom,
      reserved: reserved,
      escape_formula: escape_formula,
      strategy: default_strategy
    ]

    %{
      separator: separator,
      separator_byte: separator_byte,
      escape: escape,
      escape_byte: escape_byte,
      line_separator: line_separator,
      newlines: newlines,
      trim_bom: trim_bom,
      dump_bom: dump_bom,
      escape_chars: escape_chars,
      escape_formula: escape_formula,
      default_strategy: default_strategy,
      stored_options: stored_options,
      moduledoc: moduledoc
    }
  end

  defp validate_single_byte!(name, value) do
    unless is_binary(value) and byte_size(value) == 1 do
      raise ArgumentError,
            "RustyCSV requires a single-byte #{name}, got: #{inspect(value)}"
    end
  end

  # ==========================================================================
  # Private: Module Compilation
  # ==========================================================================

  defp compile_module(module, config) do
    quoted_ast =
      quote do
        defmodule unquote(module) do
          unquote(quoted_module_header(config))
          unquote(quoted_config_function(config))
          unquote_splicing(quoted_parsing_functions(config))
          unquote(quoted_dumping_functions(config))
        end
      end

    Code.compile_quoted(quoted_ast)
  end

  # ==========================================================================
  # Private: AST Generation Helpers
  # ==========================================================================

  defp quoted_module_header(config) do
    quote do
      @moduledoc unquote(Macro.escape(config.moduledoc))
      @behaviour RustyCSV

      @separator unquote(Macro.escape(config.separator))
      @separator_byte unquote(Macro.escape(config.separator_byte))
      @escape unquote(Macro.escape(config.escape))
      @escape_byte unquote(Macro.escape(config.escape_byte))
      @line_separator unquote(Macro.escape(config.line_separator))
      @newlines unquote(Macro.escape(config.newlines))
      @trim_bom unquote(Macro.escape(config.trim_bom))
      @dump_bom unquote(Macro.escape(config.dump_bom))
      @escape_chars unquote(Macro.escape(config.escape_chars))
      @escape_formula unquote(Macro.escape(config.escape_formula))
      @default_strategy unquote(Macro.escape(config.default_strategy))
      @stored_options unquote(Macro.escape(config.stored_options))
      @bom <<0xEF, 0xBB, 0xBF>>
    end
  end

  defp quoted_config_function(config) do
    quote do
      @doc """
      Returns the options used to define this CSV module.
      """
      @impl RustyCSV
      @spec options() :: keyword()
      def options, do: unquote(Macro.escape(config.stored_options))
    end
  end

  defp quoted_parsing_functions(config) do
    List.flatten([
      quoted_parse_string_function(config),
      quoted_parse_stream_function(),
      quoted_parse_enumerable_function(),
      quoted_to_line_stream_function()
    ])
  end

  defp quoted_parse_string_function(config) do
    [
      quoted_parse_string_main(),
      quoted_maybe_trim_bom(config.trim_bom),
      quoted_do_parse_string_clauses()
    ]
  end

  defp quoted_parse_string_main do
    quote do
      @doc """
      Parses a CSV string into a list of rows.

      ## Options

        * `:skip_headers` - When `true`, skips the first row. Defaults to `true`.
        * `:strategy` - The parsing strategy. Defaults to `#{inspect(@default_strategy)}`.

      """
      @impl RustyCSV
      @spec parse_string(binary(), RustyCSV.parse_options()) :: RustyCSV.rows()
      def parse_string(string, opts \\ [])

      def parse_string(string, opts) when is_binary(string) and is_list(opts) do
        strategy = Keyword.get(opts, :strategy, @default_strategy)
        skip_headers = Keyword.get(opts, :skip_headers, true)

        string = maybe_trim_bom(string)
        rows = do_parse_string(string, strategy)

        case {skip_headers, rows} do
          {true, [_ | tail]} -> tail
          _ -> rows
        end
      end
    end
  end

  defp quoted_maybe_trim_bom(true) do
    quote do
      defp maybe_trim_bom(<<@bom, rest::binary>>), do: rest
      defp maybe_trim_bom(string), do: string
    end
  end

  defp quoted_maybe_trim_bom(false) do
    quote do
      defp maybe_trim_bom(string), do: string
    end
  end

  defp quoted_do_parse_string_clauses do
    quote do
      defp do_parse_string(string, :basic) do
        RustyCSV.Native.parse_string_with_config(string, @separator_byte, @escape_byte)
      end

      defp do_parse_string(string, :simd) do
        RustyCSV.Native.parse_string_fast_with_config(string, @separator_byte, @escape_byte)
      end

      defp do_parse_string(string, :indexed) do
        RustyCSV.Native.parse_string_indexed_with_config(string, @separator_byte, @escape_byte)
      end

      defp do_parse_string(string, :parallel) do
        RustyCSV.Native.parse_string_parallel_with_config(string, @separator_byte, @escape_byte)
      end
    end
  end

  defp quoted_parse_stream_function do
    quote do
      @doc """
      Lazily parses a stream of CSV data into a stream of rows.
      """
      @impl RustyCSV
      @spec parse_stream(Enumerable.t(), RustyCSV.parse_options()) :: Enumerable.t()
      def parse_stream(stream, opts \\ [])

      def parse_stream(stream, opts) when is_list(opts) do
        skip_headers = Keyword.get(opts, :skip_headers, true)
        chunk_size = Keyword.get(opts, :chunk_size, 64 * 1024)
        batch_size = Keyword.get(opts, :batch_size, 1000)

        result_stream =
          RustyCSV.Streaming.stream_enumerable(stream,
            chunk_size: chunk_size,
            batch_size: batch_size,
            separator: @separator_byte,
            escape: @escape_byte
          )

        if skip_headers do
          Stream.drop(result_stream, 1)
        else
          result_stream
        end
      end
    end
  end

  defp quoted_parse_enumerable_function do
    quote do
      @doc """
      Eagerly parses an enumerable of CSV data into a list of rows.
      """
      @impl RustyCSV
      @spec parse_enumerable(Enumerable.t(), RustyCSV.parse_options()) :: RustyCSV.rows()
      def parse_enumerable(enumerable, opts \\ [])

      def parse_enumerable(enumerable, opts) when is_list(opts) do
        string = Enum.join(enumerable, "")
        parse_string(string, opts)
      end
    end
  end

  defp quoted_to_line_stream_function do
    quote do
      @doc """
      Converts a stream of arbitrary binary chunks into a line-oriented stream.
      """
      @impl RustyCSV
      @spec to_line_stream(Enumerable.t()) :: Enumerable.t()
      def to_line_stream(stream) do
        Stream.transform(
          stream,
          fn -> "" end,
          fn chunk, acc ->
            combined = acc <> chunk

            case String.split(combined, @newlines, trim: false) do
              [] ->
                {[], ""}

              [partial] ->
                {[], partial}

              parts ->
                {complete, [last]} = Enum.split(parts, -1)
                {complete, last}
            end
          end,
          fn
            "" -> {[], ""}
            acc -> {[acc], ""}
          end,
          fn _acc -> :ok end
        )
      end
    end
  end

  defp quoted_dumping_functions(config) do
    escape_formula_ast = quoted_escape_formula_function(config.escape_formula)

    quote do
      @doc """
      Converts an enumerable of rows to iodata in CSV format.
      """
      @impl RustyCSV
      @spec dump_to_iodata(Enumerable.t()) :: iodata()
      def dump_to_iodata(enumerable) do
        iodata = Enum.map(enumerable, &dump_row/1)

        if @dump_bom do
          [@bom | iodata]
        else
          iodata
        end
      end

      @doc """
      Lazily converts an enumerable of rows to a stream of iodata.
      """
      @impl RustyCSV
      @spec dump_to_stream(Enumerable.t()) :: Enumerable.t()
      def dump_to_stream(enumerable) do
        Stream.map(enumerable, &dump_row/1)
      end

      defp dump_row(row) do
        fields = Enum.map(row, &escape_field/1)
        [Enum.intersperse(fields, @separator), @line_separator]
      end

      defp escape_field(field) when is_binary(field) do
        field = maybe_escape_formula(field)

        if String.contains?(field, @escape_chars) do
          escaped = String.replace(field, @escape, @escape <> @escape)
          [@escape, escaped, @escape]
        else
          field
        end
      end

      defp escape_field(field), do: escape_field(to_string(field))

      unquote(escape_formula_ast)
    end
  end

  defp quoted_escape_formula_function(nil) do
    quote do
      defp maybe_escape_formula(field), do: field
    end
  end

  defp quoted_escape_formula_function(escape_formula) do
    quote do
      defp maybe_escape_formula(<<char, _rest::binary>> = field) do
        if Map.has_key?(unquote(Macro.escape(escape_formula)), <<char>>) do
          "\t" <> field
        else
          field
        end
      end

      defp maybe_escape_formula(field), do: field
    end
  end
end

# ==========================================================================
# Pre-defined Parsers
# ==========================================================================

RustyCSV.define(RustyCSV.RFC4180,
  separator: ",",
  escape: "\"",
  line_separator: "\r\n",
  newlines: ["\r\n", "\n"],
  strategy: :simd,
  moduledoc: ~S"""
  A CSV parser/dumper following RFC 4180 conventions.

  This module uses comma (`,`) as the field separator and double-quote (`"`)
  as the escape character. It recognizes both CRLF and LF line endings.

  This is a drop-in replacement for `NimbleCSV.RFC4180`.

  ## Quick Start

      alias RustyCSV.RFC4180, as: CSV

      # Parse CSV (skips headers by default)
      CSV.parse_string("name,age\njohn,27\n")
      #=> [["john", "27"]]

      # Include headers
      CSV.parse_string("name,age\njohn,27\n", skip_headers: false)
      #=> [["name", "age"], ["john", "27"]]

      # Use parallel parsing for large files
      CSV.parse_string(large_csv, strategy: :parallel)

      # Stream large files with bounded memory
      "huge.csv"
      |> File.stream!()
      |> CSV.parse_stream()
      |> Enum.each(&process/1)

  ## Dumping

      CSV.dump_to_iodata([["name", "age"], ["john", "27"]])
      |> IO.iodata_to_binary()
      #=> "name,age\njohn,27\n"

  ## Configuration

  This module was defined with:

      RustyCSV.define(RustyCSV.RFC4180,
        separator: ",",
        escape: "\"",
        line_separator: "\n",
        newlines: ["\r\n", "\n"],
        strategy: :simd
      )

  To customize these options, define your own parser with `RustyCSV.define/2`.

  """
)

defmodule RustyCSV.Native do
  @moduledoc """
  Low-level NIF bindings for CSV parsing.

  This module provides direct access to the Rust NIF functions. For normal use,
  prefer the higher-level `RustyCSV.RFC4180` or custom parsers defined with
  `RustyCSV.define/2`.

  ## Strategies

  The module exposes six parsing strategies:

    * `parse_string/1` - Basic byte-by-byte parsing (Strategy A)
    * `parse_string_fast/1` - SIMD-accelerated via memchr (Strategy B)
    * `parse_string_indexed/1` - Two-phase index-then-extract (Strategy C)
    * `parse_string_parallel/1` - Multi-threaded via rayon (Strategy E)
    * `parse_string_zero_copy/1` - Sub-binary references (Strategy F)
    * `streaming_*` functions - Stateful streaming parser (Strategy D)

  ## Strategy Selection

  | Strategy | Use Case | Memory Model |
  |----------|----------|--------------|
  | `parse_string_fast/1` | Default, most files | Copy (frees input) |
  | `parse_string_parallel/1` | Large files 100MB+ | Copy (frees input) |
  | `parse_string_zero_copy/1` | Maximum speed | Sub-binary (keeps input) |
  | `parse_string_indexed/1` | Row range extraction | Copy (frees input) |
  | `streaming_*` | Unbounded files | Copy (per chunk) |
  | `parse_string/1` | Debugging | Copy (frees input) |

  ## Memory Tracking (Optional)

  Memory tracking functions are available but require the `memory_tracking`
  Cargo feature to be enabled. Without the feature, they return `0` with
  no runtime overhead.

  See `get_rust_memory/0`, `get_rust_memory_peak/0`, `reset_rust_memory_stats/0`.

  """

  version = Mix.Project.config()[:version]

  use RustlerPrecompiled,
    otp_app: :rusty_csv,
    crate: "rustycsv",
    base_url: "https://github.com/jeffhuen/rustycsv/releases/download/v#{version}",
    force_build: System.get_env("FORCE_RUSTYCSV_BUILD") in ["1", "true"],
    nif_versions: ["2.15", "2.16", "2.17"],
    targets:
      Enum.uniq(
        ["aarch64-apple-darwin", "x86_64-apple-darwin"] ++
          RustlerPrecompiled.Config.default_targets()
      ),
    version: version

  # ==========================================================================
  # Types
  # ==========================================================================

  @typedoc "Opaque reference to a streaming parser"
  @opaque parser_ref :: reference()

  @typedoc "A parsed row (list of field binaries)"
  @type row :: [binary()]

  @typedoc "Multiple parsed rows"
  @type rows :: [row()]

  @typedoc "Field separator byte (e.g., `,` = 44, `\\t` = 9)"
  @type separator :: non_neg_integer()

  @typedoc "Quote/escape byte (e.g., `\"` = 34)"
  @type escape :: non_neg_integer()

  # ==========================================================================
  # Strategy A: Basic Parsing
  # ==========================================================================

  @doc """
  Parse CSV using basic byte-by-byte scanning.

  This is the simplest implementation, processing one byte at a time.
  Use `parse_string_fast/1` for better performance in most cases.

  ## Examples

      iex> RustyCSV.Native.parse_string("a,b\\n1,2\\n")
      [["a", "b"], ["1", "2"]]

  """
  @spec parse_string(binary()) :: rows()
  def parse_string(_csv), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Parse CSV with configurable separator and escape characters.

  ## Parameters

    * `csv` - The CSV binary to parse
    * `separator` - The field separator byte (e.g., `,` = 44, `\\t` = 9)
    * `escape` - The quote/escape byte (e.g., `"` = 34)

  ## Examples

      # TSV parsing (tab-separated)
      iex> RustyCSV.Native.parse_string_with_config("a\\tb\\n1\\t2\\n", 9, 34)
      [["a", "b"], ["1", "2"]]

  """
  @spec parse_string_with_config(binary(), non_neg_integer(), non_neg_integer()) :: rows()
  def parse_string_with_config(_csv, _separator, _escape), do: :erlang.nif_error(:nif_not_loaded)

  # ==========================================================================
  # Strategy B: SIMD-Accelerated Parsing
  # ==========================================================================

  @doc """
  Parse CSV using SIMD-accelerated delimiter scanning.

  Uses the `memchr` crate for fast delimiter detection on supported
  architectures. This is the recommended strategy for most use cases.

  ## Examples

      iex> RustyCSV.Native.parse_string_fast("a,b\\n1,2\\n")
      [["a", "b"], ["1", "2"]]

  """
  @spec parse_string_fast(binary()) :: rows()
  def parse_string_fast(_csv), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Parse CSV using SIMD with configurable separator and escape.

  ## Examples

      # TSV parsing (tab-separated)
      iex> RustyCSV.Native.parse_string_fast_with_config("a\\tb\\n1\\t2\\n", 9, 34)
      [["a", "b"], ["1", "2"]]

  """
  @spec parse_string_fast_with_config(binary(), non_neg_integer(), non_neg_integer()) :: rows()
  def parse_string_fast_with_config(_csv, _separator, _escape),
    do: :erlang.nif_error(:nif_not_loaded)

  # ==========================================================================
  # Strategy C: Two-Phase Index-then-Extract
  # ==========================================================================

  @doc """
  Parse CSV using two-phase index-then-extract approach.

  First builds an index of row/field boundaries, then extracts fields.
  This approach has better cache utilization and enables advanced use
  cases like extracting specific row ranges.

  ## Examples

      iex> RustyCSV.Native.parse_string_indexed("a,b\\n1,2\\n")
      [["a", "b"], ["1", "2"]]

  """
  @spec parse_string_indexed(binary()) :: rows()
  def parse_string_indexed(_csv), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Parse CSV using two-phase approach with configurable separator and escape.

  ## Examples

      # TSV parsing (tab-separated)
      iex> RustyCSV.Native.parse_string_indexed_with_config("a\\tb\\n1\\t2\\n", 9, 34)
      [["a", "b"], ["1", "2"]]

  """
  @spec parse_string_indexed_with_config(binary(), non_neg_integer(), non_neg_integer()) :: rows()
  def parse_string_indexed_with_config(_csv, _separator, _escape),
    do: :erlang.nif_error(:nif_not_loaded)

  # ==========================================================================
  # Strategy D: Streaming Parser
  # ==========================================================================

  @doc """
  Create a new streaming parser instance.

  The streaming parser maintains internal state and can process CSV data
  in chunks, making it suitable for large files with bounded memory usage.

  ## Examples

      parser = RustyCSV.Native.streaming_new()
      RustyCSV.Native.streaming_feed(parser, "a,b\\n")
      RustyCSV.Native.streaming_feed(parser, "1,2\\n")
      rows = RustyCSV.Native.streaming_next_rows(parser, 100)

  """
  @spec streaming_new() :: parser_ref()
  def streaming_new, do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Create a new streaming parser with configurable separator and escape.

  ## Parameters

    * `separator` - The field separator byte (e.g., `,` = 44, `\\t` = 9)
    * `escape` - The quote/escape byte (e.g., `"` = 34)

  ## Examples

      # TSV streaming parser
      parser = RustyCSV.Native.streaming_new_with_config(9, 34)

  """
  @spec streaming_new_with_config(non_neg_integer(), non_neg_integer()) :: parser_ref()
  def streaming_new_with_config(_separator, _escape), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Feed a chunk of CSV data to the streaming parser.

  Returns `{available_rows, buffer_size}` indicating the number of complete
  rows ready to be taken and the current buffer size.

  ## Examples

      {available, buffer_size} = RustyCSV.Native.streaming_feed(parser, chunk)

  """
  @spec streaming_feed(parser_ref(), binary()) :: {non_neg_integer(), non_neg_integer()}
  def streaming_feed(_parser, _chunk), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Take up to `max` complete rows from the streaming parser.

  Returns the rows as a list of lists of binaries.

  ## Examples

      rows = RustyCSV.Native.streaming_next_rows(parser, 100)

  """
  @spec streaming_next_rows(parser_ref(), non_neg_integer()) :: rows()
  def streaming_next_rows(_parser, _max), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Finalize the streaming parser and get any remaining rows.

  This should be called after all data has been fed to get any partial
  row that was waiting for a terminating newline.

  ## Examples

      final_rows = RustyCSV.Native.streaming_finalize(parser)

  """
  @spec streaming_finalize(parser_ref()) :: rows()
  def streaming_finalize(_parser), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Get the current status of the streaming parser.

  Returns `{available_rows, buffer_size, has_partial}`:
    * `available_rows` - Number of complete rows ready to be taken
    * `buffer_size` - Current size of the internal buffer in bytes
    * `has_partial` - Whether there's an incomplete row in the buffer

  ## Examples

      {available, buffer, has_partial} = RustyCSV.Native.streaming_status(parser)

  """
  @spec streaming_status(parser_ref()) :: {non_neg_integer(), non_neg_integer(), boolean()}
  def streaming_status(_parser), do: :erlang.nif_error(:nif_not_loaded)

  # ==========================================================================
  # Strategy E: Parallel Parsing
  # ==========================================================================

  @doc """
  Parse CSV in parallel using multiple threads.

  Uses the rayon thread pool to parse rows in parallel. This is most
  beneficial for very large files (100MB+) where the parallelization
  overhead is outweighed by the parsing speedup.

  This function runs on a dirty CPU scheduler to avoid blocking the
  normal BEAM schedulers.

  ## Examples

      iex> RustyCSV.Native.parse_string_parallel("a,b\\n1,2\\n")
      [["a", "b"], ["1", "2"]]

  """
  @spec parse_string_parallel(binary()) :: rows()
  def parse_string_parallel(_csv), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Parse CSV in parallel with configurable separator and escape.

  ## Examples

      # TSV parallel parsing (tab-separated)
      iex> RustyCSV.Native.parse_string_parallel_with_config("a\\tb\\n1\\t2\\n", 9, 34)
      [["a", "b"], ["1", "2"]]

  """
  @spec parse_string_parallel_with_config(binary(), non_neg_integer(), non_neg_integer()) ::
          rows()
  def parse_string_parallel_with_config(_csv, _separator, _escape),
    do: :erlang.nif_error(:nif_not_loaded)

  # ==========================================================================
  # Strategy F: Zero-Copy Parsing (Sub-binary references)
  # ==========================================================================

  @doc """
  Parse CSV using zero-copy sub-binary references where possible.

  Uses BEAM sub-binary references for unquoted and simply-quoted fields,
  only copying when quote unescaping is needed (hybrid Cow approach).

  **Trade-off**: Sub-binaries keep the parent binary alive until all
  references are garbage collected. Use this when you want maximum speed
  and control memory lifetime yourself.

  ## Examples

      iex> RustyCSV.Native.parse_string_zero_copy("a,b\\n1,2\\n")
      [["a", "b"], ["1", "2"]]

  """
  @spec parse_string_zero_copy(binary()) :: rows()
  def parse_string_zero_copy(_csv), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Parse CSV using zero-copy with configurable separator and escape.

  ## Examples

      # TSV zero-copy parsing (tab-separated)
      iex> RustyCSV.Native.parse_string_zero_copy_with_config("a\\tb\\n1\\t2\\n", 9, 34)
      [["a", "b"], ["1", "2"]]

  """
  @spec parse_string_zero_copy_with_config(binary(), non_neg_integer(), non_neg_integer()) ::
          rows()
  def parse_string_zero_copy_with_config(_csv, _separator, _escape),
    do: :erlang.nif_error(:nif_not_loaded)

  # ==========================================================================
  # Memory Tracking (requires `memory_tracking` feature flag)
  # ==========================================================================

  @doc """
  Get current Rust heap allocation in bytes.

  **Note**: Requires the `memory_tracking` Cargo feature to be enabled.
  Without the feature, this returns `0` with no overhead.

  To enable memory tracking, set the feature in `native/rustycsv/Cargo.toml`:

      [features]
      default = ["mimalloc", "memory_tracking"]

  ## Examples

      bytes = RustyCSV.Native.get_rust_memory()

  """
  @spec get_rust_memory() :: non_neg_integer()
  def get_rust_memory, do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Get peak Rust heap allocation since last reset, in bytes.

  **Note**: Requires the `memory_tracking` Cargo feature. Returns `0` otherwise.

  ## Examples

      peak_bytes = RustyCSV.Native.get_rust_memory_peak()

  """
  @spec get_rust_memory_peak() :: non_neg_integer()
  def get_rust_memory_peak, do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Reset memory tracking statistics.

  **Note**: Requires the `memory_tracking` Cargo feature. Returns `{0, 0}` otherwise.

  Returns `{current_bytes, previous_peak_bytes}`.

  ## Examples

      {current, peak} = RustyCSV.Native.reset_rust_memory_stats()

  """
  @spec reset_rust_memory_stats() :: {non_neg_integer(), non_neg_integer()}
  def reset_rust_memory_stats, do: :erlang.nif_error(:nif_not_loaded)
end

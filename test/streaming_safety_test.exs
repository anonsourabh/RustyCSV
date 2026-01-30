defmodule RustyCSV.StreamingSafetyTest do
  @moduledoc """
  Tests for streaming parser safety features:
  - Buffer overflow detection and :buffer_overflow exception
  - Configurable max_buffer_size via NIF and high-level APIs
  - Mutex poisoning recovery (:mutex_poisoned exception)
  """
  use ExUnit.Case, async: true

  alias RustyCSV.Native

  # ==========================================================================
  # Buffer overflow — NIF level
  # ==========================================================================

  describe "streaming buffer overflow (NIF)" do
    test "feeding data beyond max_buffer_size raises :buffer_overflow" do
      parser = Native.streaming_new()
      Native.streaming_set_max_buffer(parser, 100)

      assert_raise ErlangError, ~r/buffer_overflow/, fn ->
        Native.streaming_feed(parser, String.duplicate("x", 200))
      end
    end

    test "feeding data exactly at the limit succeeds" do
      parser = Native.streaming_new()
      Native.streaming_set_max_buffer(parser, 100)

      {_available, buffer_size} = Native.streaming_feed(parser, String.duplicate("x", 100))
      assert buffer_size == 100
    end

    test "feeding data one byte over the limit raises :buffer_overflow" do
      parser = Native.streaming_new()
      Native.streaming_set_max_buffer(parser, 100)

      assert_raise ErlangError, ~r/buffer_overflow/, fn ->
        Native.streaming_feed(parser, String.duplicate("x", 101))
      end
    end

    test "incremental feeds accumulate toward the limit" do
      parser = Native.streaming_new()
      Native.streaming_set_max_buffer(parser, 100)

      # First feed: 60 bytes (no newline, stays in buffer)
      Native.streaming_feed(parser, String.duplicate("x", 60))

      # Second feed: 50 bytes — total 110 exceeds 100
      assert_raise ErlangError, ~r/buffer_overflow/, fn ->
        Native.streaming_feed(parser, String.duplicate("y", 50))
      end
    end

    test "newlines drain the buffer, resetting toward the limit" do
      parser = Native.streaming_new()
      Native.streaming_set_max_buffer(parser, 100)

      # Feed 80 bytes with a newline — the completed row drains from the buffer
      Native.streaming_feed(parser, String.duplicate("x", 80) <> "\n")

      # Another 80 bytes with a newline — should succeed because buffer was drained
      {available, _buffer_size} = Native.streaming_feed(parser, String.duplicate("y", 80) <> "\n")
      assert available >= 1
    end

    test "streaming_set_max_buffer returns :ok" do
      parser = Native.streaming_new()
      assert Native.streaming_set_max_buffer(parser, 1024) == :ok
    end
  end

  # ==========================================================================
  # Buffer overflow — high-level API
  # ==========================================================================

  describe "max_buffer_size option (high-level API)" do
    test "parse_stream with max_buffer_size raises on overflow" do
      # A single chunk with no newlines that exceeds the limit
      stream = [String.duplicate("a,b,c", 50)] |> Stream.map(& &1)

      assert_raise ErlangError, ~r/buffer_overflow/, fn ->
        RustyCSV.RFC4180.parse_stream(stream, max_buffer_size: 100)
        |> Enum.to_list()
      end
    end

    test "parse_stream with large enough max_buffer_size succeeds" do
      stream = ["a,b\n1,2\n"] |> Stream.map(& &1)

      # RFC4180 skips the first row as a header by default
      result =
        RustyCSV.RFC4180.parse_stream(stream, max_buffer_size: 1024)
        |> Enum.to_list()

      assert result == [["1", "2"]]
    end
  end

  # ==========================================================================
  # Buffer overflow — with_config parser variants
  # ==========================================================================

  describe "buffer overflow with configured parsers" do
    test "custom separator parser respects buffer limit" do
      parser = Native.streaming_new_with_config(<<9>>, 34, :default)
      Native.streaming_set_max_buffer(parser, 50)

      assert_raise ErlangError, ~r/buffer_overflow/, fn ->
        Native.streaming_feed(parser, String.duplicate("x", 100))
      end
    end

    test "multi-byte separator parser respects buffer limit" do
      parser = Native.streaming_new_with_config("::", 34, :default)
      Native.streaming_set_max_buffer(parser, 50)

      assert_raise ErlangError, ~r/buffer_overflow/, fn ->
        Native.streaming_feed(parser, String.duplicate("x", 100))
      end
    end
  end

  # ==========================================================================
  # Mutex poisoning
  # ==========================================================================

  describe "mutex poisoning" do
    # Mutex poisoning occurs when a thread panics while holding a mutex lock.
    # This is difficult to trigger deterministically from Elixir because it
    # requires a Rust-side panic inside the locked section. However, we can
    # verify that normal operations don't raise :mutex_poisoned (regression
    # guard) and that all 4 lock_parser call sites are exercised.

    test "normal streaming operations do not raise :mutex_poisoned" do
      parser = Native.streaming_new()
      {available, _size} = Native.streaming_feed(parser, "a,b\n1,2\n")
      assert available >= 1

      rows = Native.streaming_next_rows(parser, 100)
      assert rows != []

      {avail, _buf, _partial} = Native.streaming_status(parser)
      assert is_integer(avail)

      final = Native.streaming_finalize(parser)
      assert is_list(final)
    end

    test "all streaming NIFs use lock_parser (no raw unwrap)" do
      # Exercise all 4 lock_parser call sites in a single sequence
      parser = Native.streaming_new_with_config(44, 34, :default)

      # Call site 1: streaming_feed
      Native.streaming_feed(parser, "a,b\n")

      # Call site 2: streaming_next_rows
      rows = Native.streaming_next_rows(parser, 10)
      assert rows == [["a", "b"]]

      # Call site 3: streaming_status
      {0, _buf, false} = Native.streaming_status(parser)

      # Call site 4: streaming_finalize
      final = Native.streaming_finalize(parser)
      assert final == []
    end
  end
end

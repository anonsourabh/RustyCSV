defmodule MultiByteEscapeTest do
  @moduledoc """
  Tests for multi-byte escape support.
  """
  use ExUnit.Case

  # Define parser with multi-byte escape
  RustyCSV.define(TestDollarEscape,
    separator: ",",
    escape: "$$",
    line_separator: "\n"
  )

  RustyCSV.define(TestDoubleColonSepDollarEscape,
    separator: "::",
    escape: "$$",
    line_separator: "\n"
  )

  describe "multi-byte escape parsing" do
    test "parses quoted fields with $$ escape" do
      result = TestDollarEscape.parse_string("$$hello$$,world\n", skip_headers: false)
      assert result == [["hello", "world"]]
    end

    test "handles doubled escape (escaped $$)" do
      result = TestDollarEscape.parse_string("$$val$$$$ue$$,other\n", skip_headers: false)
      assert result == [["val$$ue", "other"]]
    end

    test "handles unquoted fields" do
      result = TestDollarEscape.parse_string("hello,world\n", skip_headers: false)
      assert result == [["hello", "world"]]
    end

    test "handles empty quoted fields" do
      result = TestDollarEscape.parse_string("$$$$,world\n", skip_headers: false)
      assert result == [["", "world"]]
    end

    test "handles separator inside quoted field" do
      result = TestDollarEscape.parse_string("$$a,b$$,c\n", skip_headers: false)
      assert result == [["a,b", "c"]]
    end

    test "handles newline inside quoted field" do
      result = TestDollarEscape.parse_string("$$line1\nline2$$,c\n", skip_headers: false)
      assert result == [["line1\nline2", "c"]]
    end
  end

  describe "multi-byte escape with multi-byte separator" do
    test "both separator and escape are multi-byte" do
      result =
        TestDoubleColonSepDollarEscape.parse_string(
          "$$hello$$::world\n",
          skip_headers: false
        )

      assert result == [["hello", "world"]]
    end

    test "separator inside quoted field" do
      result =
        TestDoubleColonSepDollarEscape.parse_string(
          "$$a::b$$::c\n",
          skip_headers: false
        )

      assert result == [["a::b", "c"]]
    end

    test "doubled escape with multi-byte separator" do
      result =
        TestDoubleColonSepDollarEscape.parse_string(
          "$$val$$$$ue$$::other\n",
          skip_headers: false
        )

      assert result == [["val$$ue", "other"]]
    end
  end

  describe "strategy compatibility with multi-byte escape" do
    test "all strategies produce identical output" do
      csv = "$$hello$$,world\n$$val$$$$ue$$,other\n"

      basic = TestDollarEscape.parse_string(csv, strategy: :basic, skip_headers: false)
      simd = TestDollarEscape.parse_string(csv, strategy: :simd, skip_headers: false)
      indexed = TestDollarEscape.parse_string(csv, strategy: :indexed, skip_headers: false)
      parallel = TestDollarEscape.parse_string(csv, strategy: :parallel, skip_headers: false)
      zero_copy = TestDollarEscape.parse_string(csv, strategy: :zero_copy, skip_headers: false)

      expected = [
        ["hello", "world"],
        ["val$$ue", "other"]
      ]

      assert basic == expected
      assert simd == expected
      assert indexed == expected
      assert parallel == expected
      assert zero_copy == expected
    end
  end

  describe "streaming with multi-byte escape" do
    test "streams correctly" do
      chunks = ["$$hello$$,world\n", "$$val$$$$ue$$,other\n"]

      result =
        chunks
        |> TestDollarEscape.parse_stream(skip_headers: false)
        |> Enum.to_list()

      assert result == [["hello", "world"], ["val$$ue", "other"]]
    end
  end

  describe "round-trip with multi-byte escape" do
    test "parse then dump produces consistent output" do
      parsed = TestDollarEscape.parse_string("$$hello$$,world\n", skip_headers: false)
      dumped = TestDollarEscape.dump_to_iodata(parsed) |> IO.iodata_to_binary()
      reparsed = TestDollarEscape.parse_string(dumped, skip_headers: false)
      assert reparsed == parsed
    end
  end
end

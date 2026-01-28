defmodule Sandbox.Native do
  @moduledoc """
  Native bindings for sandbox experiments.
  """

  use Rustler,
    otp_app: :sandbox,
    crate: :sandbox_nif,
    path: "native/sandbox_nif"

  @doc """
  Parse CSV to list of maps with interned binary keys.
  Header strings are allocated once and reused as keys for all rows.
  """
  @spec parse_to_maps_interned(binary(), non_neg_integer(), non_neg_integer()) :: [map()]
  def parse_to_maps_interned(_input, _separator, _escape),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Parse CSV to list of maps WITHOUT key interning.
  Each row gets fresh key binaries (simulates Enum.zip behavior).
  """
  @spec parse_to_maps_no_intern(binary(), non_neg_integer(), non_neg_integer()) :: [map()]
  def parse_to_maps_no_intern(_input, _separator, _escape),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Parse CSV to list of maps with atom keys.
  Uses BEAM's atom table for automatic interning.
  """
  @spec parse_to_maps_atoms(binary(), non_neg_integer(), non_neg_integer()) :: [map()]
  def parse_to_maps_atoms(_input, _separator, _escape),
    do: :erlang.nif_error(:nif_not_loaded)
end

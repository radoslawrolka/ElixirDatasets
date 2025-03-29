defmodule ElixirDatasets do
  @moduledoc """
  Documentation for `ElixirDatasets`.
  """

  @doc """
  Hello world.

  ## Examples

      iex> ElixirDatasets.hello()
      :world

  """
  def hello do
    :world
  end

  def unformmated(data) do
    data
      |> Enum.map(fn {k,   v}  -> {k, v}
    end)
    |> Enum.map(fn {k, v} -> {k, v} end)
  end
end

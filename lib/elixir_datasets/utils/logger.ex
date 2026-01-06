defmodule ElixirDatasets.Utils.Logger do
  @moduledoc """
  Simple debug logger for ElixirDatasets that prints to IO when HF_DEBUG environment variable is set to "true".

  Usage:
    ElixirDatasets.Utils.Logger.debug("This is a debug message")
  """

  @doc """
  Prints a debug message to IO if HF_DEBUG environment variable is set to "true".

  ## Examples
    iex> ElixirDatasets.Utils.Logger.debug("Starting dataset download")
    :ok
  """
  def debug(message) do
    if debug_enabled?() do
      IO.puts("[HF_DEBUG] #{message}")
    end

    :ok
  end

  @doc """
  Returns true if debug logging is enabled (HF_DEBUG environment variable is set to "true").

  ## Examples
    iex> System.put_env("HF_DEBUG", "true")
    iex> ElixirDatasets.Utils.Logger.debug_enabled?()
    true

    iex> System.delete_env("HF_DEBUG")
    iex> ElixirDatasets.Utils.Logger.debug_enabled?()
    false
  """
  def debug_enabled? do
    System.get_env("HF_DEBUG", "false")
    |> String.downcase()
    |> String.trim()
    |> Kernel.==("true")
  end
end

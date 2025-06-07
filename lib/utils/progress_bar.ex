defmodule ElixirDatasets.Utils.ProgressBar do
  @moduledoc false

  @doc """
  Checks if the progress bar is enabled globally.
  """
  @spec progress_bar_enabled? :: boolean()
  def progress_bar_enabled?() do
    Application.get_env(:elixirDatasets, :progress_bar_enabled, true)
  end
end

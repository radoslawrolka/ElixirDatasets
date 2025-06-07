# This file, part of the ElixirDatasets project, has been adapted from code originally under Apache License 2.0.
# The original code can be found at:
# https://github.com/elixir-nx/bumblebee/blob/710a645222948f80208c348d3a2589cbd3ab8e7d/lib/bumblebee/utils.ex

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

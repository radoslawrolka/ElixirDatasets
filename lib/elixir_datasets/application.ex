# This file, part of the ElixirDatasets project, has been adapted from code originally under Apache License 2.0.
# The original code can be found at:
# https://github.com/elixir-nx/bumblebee/blob/710a645222948f80208c348d3a2589cbd3ab8e7d/lib/bumblebee/application.ex

defmodule ElixirDatasets.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    ElixirDatasets.Utils.HTTP.start_inets_profile()

    children = []
    opts = [strategy: :one_for_one, name: ElixirDatasets.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @impl true
  def stop(_state) do
    ElixirDatasets.Utils.HTTP.stop_inets_profile()
  end
end

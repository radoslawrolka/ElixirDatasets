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

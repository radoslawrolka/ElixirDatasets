defmodule ElixirDatasets.ApplicationTest do
  use ExUnit.Case, async: true

  describe "Application lifecycle" do
    test "starts and stops successfully" do
      # Stop the application if it's already started
      assert :ok = ElixirDatasets.Application.stop(nil), "Application should stop without errors"

      # Start the application
      assert {_, {:already_started, _pid}} = ElixirDatasets.Application.start(:normal, []),
             "Application should start without errors"

      # Ensure the application is running
      assert {:ok, _} = Application.ensure_all_started(:elixirDatasets),
             "Application should be ensured to start without errors"

      # Stop the application again
      assert :ok = ElixirDatasets.Application.stop(nil), "Application should stop without errors"
    end
  end
end

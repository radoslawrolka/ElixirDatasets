defmodule ElixirDatasets.Utils.ProgressBarTest do
  use ExUnit.Case, async: true

  alias ElixirDatasets.Utils.ProgressBar

  describe "progress_bar_enabled?/0" do
    test "returns true when :progress_bar_enabled is not set in config" do
      Application.delete_env(:elixirDatasets, :progress_bar_enabled)
      assert ProgressBar.progress_bar_enabled?() == true
    end

    test "returns true when :progress_bar_enabled is set to true" do
      Application.put_env(:elixirDatasets, :progress_bar_enabled, true)
      assert ProgressBar.progress_bar_enabled?() == true
    end

    test "returns false when :progress_bar_enabled is set to false" do
      Application.put_env(:elixirDatasets, :progress_bar_enabled, false)
      assert ProgressBar.progress_bar_enabled?() == false
    end
  end
end

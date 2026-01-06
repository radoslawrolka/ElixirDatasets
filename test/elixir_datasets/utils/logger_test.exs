defmodule ElixirDatasets.Utils.LoggerTest do
  use ExUnit.Case

  alias ElixirDatasets.Utils.Logger

  describe "debug_enabled?/0" do
    setup do
      original_debug = System.get_env("HF_DEBUG")

      on_exit(fn ->
        if original_debug,
          do: System.put_env("HF_DEBUG", original_debug),
          else: System.delete_env("HF_DEBUG")
      end)

      :ok
    end

    test "returns true when HF_DEBUG is set to 'true'" do
      System.put_env("HF_DEBUG", "true")
      assert Logger.debug_enabled?() == true
    end

    test "returns true when HF_DEBUG is set to 'TRUE' (case insensitive)" do
      System.put_env("HF_DEBUG", "TRUE")
      assert Logger.debug_enabled?() == true
    end

    test "returns false when HF_DEBUG is set to 'false'" do
      System.put_env("HF_DEBUG", "false")
      assert Logger.debug_enabled?() == false
    end

    test "returns false when HF_DEBUG is not set" do
      System.delete_env("HF_DEBUG")
      assert Logger.debug_enabled?() == false
    end

    test "returns false when HF_DEBUG is set to any other value" do
      System.put_env("HF_DEBUG", "yes")
      assert Logger.debug_enabled?() == false
    end

    test "handles whitespace in HF_DEBUG value" do
      System.put_env("HF_DEBUG", "  true  ")
      assert Logger.debug_enabled?() == true
    end
  end

  describe "debug/1" do
    setup do
      original_debug = System.get_env("HF_DEBUG")

      on_exit(fn ->
        if original_debug,
          do: System.put_env("HF_DEBUG", original_debug),
          else: System.delete_env("HF_DEBUG")
      end)

      :ok
    end

    test "prints debug message when HF_DEBUG is enabled" do
      System.put_env("HF_DEBUG", "true")
      assert Logger.debug("Test debug message") == :ok
    end

    test "does not print debug message when HF_DEBUG is disabled" do
      System.put_env("HF_DEBUG", "false")
      assert Logger.debug("Test debug message") == :ok
    end
  end
end

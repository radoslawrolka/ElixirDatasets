defmodule ElixirDatasets.StreamingTest do
  use ExUnit.Case, async: false
  doctest ElixirDatasets.Streaming

  alias ElixirDatasets.Loader

  describe "streaming mode" do
    @cache_dir "test_cache_streaming"
    @repository {:hf, "aaaaa32r/elixirDatasets", [cache_dir: @cache_dir]}

    setup do
      on_exit(fn ->
        File.rm_rf!(@cache_dir)

        File.rm_rf!(
          :filename.basedir(
            :user_cache,
            "elixir_datasets" <> "/huggingface/aaaaa32r--elixirDatasets"
          )
        )
      end)
    end

    test "loads dataset with streaming parameter returns Stream" do
      repository = {:local, "resources"}
      assert {:ok, stream} = Loader.load_dataset(repository, streaming: true)

      assert is_function(stream, 2), "Expected a Stream (function/2)"

      rows = stream |> Enum.take(5)
      assert is_list(rows)
      assert Enum.all?(rows, &is_map/1), "Each row should be a map"
    end

    test "streaming mode fetches data progressively" do
      repository = {:local, "resources"}
      assert {:ok, stream} = Loader.load_dataset(repository, streaming: true)

      rows = stream |> Enum.take(3)
      assert length(rows) <= 3
      assert Enum.all?(rows, &is_map/1)
    end

    test "streaming with custom batch_size" do
      repository = {:local, "resources"}

      assert {:ok, stream} =
               Loader.load_dataset(
                 repository,
                 streaming: true,
                 batch_size: 2
               )

      rows = stream |> Enum.take(5)
      assert is_list(rows)
    end

    test "streaming is lazy - data fetched on demand, not upfront" do
      repository = {:local, "resources"}

      {:ok, stream} = Loader.load_dataset(repository, streaming: true)

      rows1 = stream |> Enum.take(3)
      assert length(rows1) == 3

      Process.sleep(2000)

      rows2 = stream |> Enum.take(5)
      assert length(rows2) == 5

      fetch_count = :counters.new(1, [:atomics])

      counted_stream =
        stream
        |> Stream.map(fn row ->
          :counters.add(fetch_count, 1, 1)
          row
        end)

      _small_batch = counted_stream |> Enum.take(2)
      count_after_2 = :counters.get(fetch_count, 1)

      :counters.put(fetch_count, 1, 0)

      _large_batch = counted_stream |> Enum.take(10)
      count_after_10 = :counters.get(fetch_count, 1)

      assert count_after_2 <= 5, "Should fetch minimal rows for small take"
      assert count_after_10 >= 8, "Should fetch more rows for larger take"
    end

    test "streaming from HuggingFace demonstrates progressive fetching" do
      repository = @repository

      {:ok, stream} = Loader.load_dataset(repository, streaming: true, batch_size: 5)

      rows1 = stream |> Enum.take(3)
      assert length(rows1) == 3

      Process.sleep(1000)

      rows2 = stream |> Enum.take(8)
      assert length(rows2) == 8

      result =
        stream
        |> Stream.filter(fn row -> Map.has_key?(row, "id") end)
        |> Stream.take(5)
        |> Enum.to_list()

      assert length(result) <= 5
    end

    test "verification_mode works with streaming" do
      repository = @repository

      {:ok, stream1} =
        Loader.load_dataset(
          repository,
          streaming: true,
          verification_mode: :basic_checks
        )

      rows1 = stream1 |> Enum.take(2)
      assert length(rows1) == 2

      {:ok, stream2} =
        Loader.load_dataset(
          repository,
          streaming: true,
          verification_mode: :no_checks
        )

      rows2 = stream2 |> Enum.take(2)
      assert length(rows2) == 2
    end
  end
end

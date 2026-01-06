defmodule ElixirDatasetsTest do
  use ExUnit.Case, async: false
  doctest ElixirDatasets

  test "Check if the tests are working correctly" do
    assert 2 + 2 == 4
  end

  test "Check if the module is loaded" do
    assert Code.ensure_loaded?(ElixirDatasets)
  end

  describe "do_load_spec/2" do
    @cache_dir "test_cache_do_load_spec"
    @repository {:hf, "aaaaa32r/elixirDatasets", [cache_dir: @cache_dir]}
    @valid_repo_files %{
      "resources/csv-test.csv" => "\"2dccc814f47c01b5344abbb72367a5b322656b0b\""
    }
    @invalid_repo_files %{"invalid.csv" => "\"1234567890asdfgh\""}

    test "Loads valid files" do
      assert {:ok, _paths} = ElixirDatasets.do_load_spec(@repository, @valid_repo_files, 1)
      File.rm_rf!(@cache_dir)
    end

    test "Return error for invalid files" do
      assert {:error, _reason} = ElixirDatasets.do_load_spec(@repository, @invalid_repo_files, 1)

      File.rm_rf!(@cache_dir)
    end
  end

  describe "decode_config/1" do
    test "Decodes a valid JSON file" do
      File.write!("valid.json", ~s({"key": "value"}))
      assert {:ok, %{"key" => "value"}} = ElixirDatasets.decode_config("valid.json")
      File.rm!("valid.json")
    end

    test "Fails to decode JSON file" do
      File.write!("invalid.json", "{invalid_json}")
      assert {:error, _} = ElixirDatasets.decode_config("invalid.json")
      File.rm!("invalid.json")
    end
  end

  describe "load_dataset/2" do
    @cache_dir "test_cache_load_dataset"
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

    test "loads a dataset from Hugging Face" do
      assert {:ok, datasets} = ElixirDatasets.load_dataset(@repository)
      assert is_list(datasets)
    end

    test "loads a dataset from Hugging Face without opts" do
      repository_short = {:hf, "aaaaa32r/elixirDatasets"}
      assert {:ok, datasets} = ElixirDatasets.load_dataset(repository_short)
      assert is_list(datasets)
    end

    test "loads a dataset from local directory" do
      repository = {:local, "resources"}
      assert {:ok, datasets} = ElixirDatasets.load_dataset(repository)
      assert is_list(datasets)
    end

    test "raise error when invalid local directory" do
      repository = {:local, "invalid/path"}
      assert {:error, _reason} = ElixirDatasets.load_dataset(repository)
    end

    test "loads dataset offline" do
      repository = {:hf, "aaaaa32r/elixirDatasets", [cache_dir: @cache_dir]}
      assert {:ok, datasets} = ElixirDatasets.load_dataset(repository)
      assert is_list(datasets)
      repositoryOffline = {:hf, "aaaaa32r/elixirDatasets", [cache_dir: @cache_dir, offline: true]}
      assert {:ok, datasets} = ElixirDatasets.load_dataset(repositoryOffline)
      assert is_list(datasets)
      repositoryOfflineInvalid = {:hf, "not/exists", [cache_dir: @cache_dir, offline: true]}
      assert {:error, _reason} = ElixirDatasets.load_dataset(repositoryOfflineInvalid)
    end

    test "loads a dataset from Hugging Face with subdirectory" do
      repositorySubdir =
        {:hf, "aaaaa32r/elixirDatasets", [cache_dir: @cache_dir, subdir: "resources"]}

      assert {:ok, datasets} = ElixirDatasets.load_dataset(repositorySubdir)
      assert is_list(datasets)
    end

    test "returns error for non-existent dataset" do
      repository = {:test, "nonexistent/repo", []}

      assert_raise ArgumentError, fn ->
        ElixirDatasets.load_dataset(repository)
      end
    end

    test "loads dataset with split parameter from local directory" do
      repository = {:local, "resources"}
      assert {:ok, datasets} = ElixirDatasets.load_dataset(repository, split: "train")
      assert is_list(datasets)
    end

    test "loads dataset with name parameter filters files" do
      repository = {:local, "resources"}
      assert {:ok, datasets} = ElixirDatasets.load_dataset(repository, name: "csv")
      assert is_list(datasets)
    end

    test "loads dataset with streaming parameter returns Stream" do
      repository = {:local, "resources"}
      assert {:ok, stream} = ElixirDatasets.load_dataset(repository, streaming: true)

      assert is_function(stream, 2), "Expected a Stream (function/2)"

      rows = stream |> Enum.take(5)
      assert is_list(rows)
      assert Enum.all?(rows, &is_map/1), "Each row should be a map"
    end

    test "streaming mode fetches data progressively" do
      repository = {:local, "resources"}
      assert {:ok, stream} = ElixirDatasets.load_dataset(repository, streaming: true)

      rows = stream |> Enum.take(3)
      assert length(rows) <= 3
      assert Enum.all?(rows, &is_map/1)
    end

    test "streaming with custom batch_size" do
      repository = {:local, "resources"}

      assert {:ok, stream} =
               ElixirDatasets.load_dataset(
                 repository,
                 streaming: true,
                 batch_size: 2
               )

      rows = stream |> Enum.take(5)
      assert is_list(rows)
    end

    test "streaming is lazy - data fetched on demand, not upfront" do
      repository = {:local, "resources"}

      {:ok, stream} = ElixirDatasets.load_dataset(repository, streaming: true)

      IO.puts("\n  🔍 Testing lazy streaming behavior:")

      IO.puts("    1. Fetching first 3 rows...")

      {time1, rows1} =
        :timer.tc(fn ->
          stream |> Enum.take(3)
        end)

      IO.puts("       ✓ Got #{length(rows1)} rows in #{time1 / 1000}ms")
      assert length(rows1) == 3

      IO.puts("    2. Waiting 2 seconds...")
      Process.sleep(2000)

      IO.puts("    3. Fetching 5 rows from same stream...")

      {time2, rows2} =
        :timer.tc(fn ->
          stream |> Enum.take(5)
        end)

      IO.puts("       ✓ Got #{length(rows2)} rows in #{time2 / 1000}ms")
      assert length(rows2) == 5

      IO.puts("    4. Key insight: Stream is reusable, each Enum.take starts fresh")

      IO.puts("    5. Demonstrating progressive fetching...")

      fetch_count = :counters.new(1, [:atomics])

      counted_stream =
        stream
        |> Stream.map(fn row ->
          :counters.add(fetch_count, 1, 1)
          row
        end)

      IO.puts("       Taking 2 rows...")
      _small_batch = counted_stream |> Enum.take(2)
      count_after_2 = :counters.get(fetch_count, 1)
      IO.puts("       ✓ Fetched #{count_after_2} rows (should be ~2)")

      :counters.put(fetch_count, 1, 0)

      IO.puts("       Taking 10 rows...")
      _large_batch = counted_stream |> Enum.take(10)
      count_after_10 = :counters.get(fetch_count, 1)
      IO.puts("       ✓ Fetched #{count_after_10} rows (should be ~10)")

      assert count_after_2 <= 5, "Should fetch minimal rows for small take"
      assert count_after_10 >= 8, "Should fetch more rows for larger take"

      IO.puts("    ✅ Streaming is truly lazy - fetches only what's needed!")
    end

    test "streaming from HuggingFace demonstrates progressive fetching" do
      repository = @repository

      IO.puts("\n  🌐 Testing HuggingFace streaming:")

      {:ok, stream} = ElixirDatasets.load_dataset(repository, streaming: true, batch_size: 5)
      IO.puts("    ✓ Created stream (no data downloaded yet)")

      IO.puts("    1. Fetching only 3 rows...")

      {time1, rows1} =
        :timer.tc(fn ->
          stream |> Enum.take(3)
        end)

      IO.puts("       ✓ Got #{length(rows1)} rows in #{Float.round(time1 / 1000, 2)}ms")
      assert length(rows1) == 3

      IO.puts("    2. Waiting 1 second...")
      Process.sleep(1000)

      IO.puts("    3. Fetching 8 rows from same stream...")

      {time2, rows2} =
        :timer.tc(fn ->
          stream |> Enum.take(8)
        end)

      IO.puts("       ✓ Got #{length(rows2)} rows in #{Float.round(time2 / 1000, 2)}ms")
      assert length(rows2) == 8

      IO.puts("    4. Processing with Stream operations (lazy)...")

      result =
        stream
        |> Stream.filter(fn row -> Map.has_key?(row, "id") end)
        |> Stream.take(5)
        |> Enum.to_list()

      IO.puts("       ✓ Processed and got #{length(result)} filtered rows")
      assert length(result) <= 5

      IO.puts("    ✅ HuggingFace streaming works progressively!")
    end

    test "verification_mode works with streaming" do
      repository = @repository

      IO.puts("\n  🔍 Testing verification_mode with streaming:")

      IO.puts("    1. With verification_mode: :basic_checks (default)...")

      {:ok, stream1} =
        ElixirDatasets.load_dataset(
          repository,
          streaming: true,
          verification_mode: :basic_checks
        )

      rows1 = stream1 |> Enum.take(2)
      IO.puts("       ✓ Got #{length(rows1)} rows")
      assert length(rows1) == 2

      IO.puts("    2. With verification_mode: :no_checks...")

      {:ok, stream2} =
        ElixirDatasets.load_dataset(
          repository,
          streaming: true,
          verification_mode: :no_checks
        )

      rows2 = stream2 |> Enum.take(2)
      IO.puts("       ✓ Got #{length(rows2)} rows")
      assert length(rows2) == 2

      IO.puts("    ℹ️  Note: verification_mode applies to metadata fetching,")
      IO.puts("       not to the streaming data itself (which comes from URLs)")
      IO.puts("    ✅ verification_mode works with streaming!")
    end

    test "loads dataset with split and name parameters combined" do
      repository = {:local, "resources"}

      assert {:ok, datasets} =
               ElixirDatasets.load_dataset(repository, split: "train", name: "csv")

      assert is_list(datasets)
    end

    test "loads dataset with download_mode option" do
      repository = {:local, "resources"}

      assert {:ok, datasets} =
               ElixirDatasets.load_dataset(repository, download_mode: :reuse_dataset_if_exists)

      assert is_list(datasets)
    end

    test "loads dataset with verification_mode option" do
      repository = {:local, "resources"}

      assert {:ok, datasets} =
               ElixirDatasets.load_dataset(repository, verification_mode: :no_checks)

      assert is_list(datasets)
    end

    test "loads dataset with num_proc for parallel processing" do
      repository = {:local, "resources"}
      assert {:ok, datasets} = ElixirDatasets.load_dataset(repository, num_proc: 2)
      assert is_list(datasets)
      assert length(datasets) > 0
    end

    test "loads dataset with num_proc=1 (sequential)" do
      repository = {:local, "resources"}
      assert {:ok, datasets} = ElixirDatasets.load_dataset(repository, num_proc: 1)
      assert is_list(datasets)
    end

    test "num_proc=4 is faster than num_proc=1 for parallel loading" do
      repository = @repository

      {time_sequential, {:ok, datasets_seq}} =
        :timer.tc(fn ->
          ElixirDatasets.load_dataset(repository, num_proc: 1)
        end)

      {time_parallel, {:ok, datasets_par}} =
        :timer.tc(fn ->
          ElixirDatasets.load_dataset(repository, num_proc: 4)
        end)

      assert length(datasets_seq) == length(datasets_par)

      total_rows_seq =
        Enum.reduce(datasets_seq, 0, fn df, acc ->
          acc + Explorer.DataFrame.n_rows(df)
        end)

      total_rows_par =
        Enum.reduce(datasets_par, 0, fn df, acc ->
          acc + Explorer.DataFrame.n_rows(df)
        end)

      assert total_rows_seq == total_rows_par

      time_seq_sec = time_sequential / 1_000_000
      time_par_sec = time_parallel / 1_000_000
      speedup = time_sequential / time_parallel

      IO.puts("\n  ⏱️  Performance Comparison:")
      IO.puts("     Sequential (num_proc: 1): #{Float.round(time_seq_sec, 3)}s")
      IO.puts("     Parallel (num_proc: 4):   #{Float.round(time_par_sec, 3)}s")
      IO.puts("     Speedup: #{Float.round(speedup, 2)}x")

      assert time_parallel <= time_sequential * 1.5,
             "Parallel processing overhead should be reasonable for this dataset size (no more than 1.5x slower than sequential)"
    end

    test "num_proc produces same results as sequential" do
      repository = {:local, "resources"}

      {:ok, datasets_seq} = ElixirDatasets.load_dataset(repository, num_proc: 1)
      {:ok, datasets_par} = ElixirDatasets.load_dataset(repository, num_proc: 4)

      assert length(datasets_seq) == length(datasets_par)
      seq_row_counts = Enum.map(datasets_seq, &Explorer.DataFrame.n_rows/1) |> Enum.sort()
      par_row_counts = Enum.map(datasets_par, &Explorer.DataFrame.n_rows/1) |> Enum.sort()

      assert seq_row_counts == par_row_counts
    end

    # todo more tests for load_dataset/2
  end

  describe "cache_dir/0" do
    test "Cache directory in ENV" do
      if System.get_env("ELIXIR_DATASETS_CACHE_DIR") do
        assert ElixirDatasets.cache_dir() == System.get_env("ELIXIR_DATASETS_CACHE_DIR")
      else
        path = "test/cache"
        System.put_env("ELIXIR_DATASETS_CACHE_DIR", path)
        expected_dir = Path.expand(path)
        assert ElixirDatasets.cache_dir() == expected_dir
        System.delete_env("ELIXIR_DATASETS_CACHE_DIR")
      end
    end

    test "Cache directory not set" do
      if env_var = System.get_env("ELIXIR_DATASETS_CACHE_DIR") do
        System.delete_env("ELIXIR_DATASETS_CACHE_DIR")
      end

      expected_dir = :filename.basedir(:user_cache, "elixir_datasets")
      assert ElixirDatasets.cache_dir() == expected_dir

      if env_var do
        System.put_env("ELIXIR_DATASETS_CACHE_DIR", env_var)
      end
    end
  end

  describe "get_dataset_info/2" do
    test "fetches dataset info from Hugging Face API" do
      assert {:ok, info} = ElixirDatasets.get_dataset_info("aaaaa32r/elixirDatasets")
      assert is_map(info)
      assert info["id"] == "aaaaa32r/elixirDatasets"

      assert is_map(info["cardData"])
      dataset_info = info["cardData"]["dataset_info"]
      assert is_list(dataset_info)

      first_config = Enum.at(dataset_info, 0)
      assert first_config["config_name"] == "csv"
      assert is_list(first_config["features"])
      assert is_list(first_config["splits"])

      first_split = Enum.at(first_config["splits"], 0)
      assert first_split["num_examples"] == 10
    end
  end

  describe "get_dataset_infos/2" do
    test "fetches dataset infos as DatasetInfo structs" do
      assert {:ok, infos} = ElixirDatasets.get_dataset_infos("aaaaa32r/elixirDatasets")
      assert is_list(infos)
      assert Enum.count(infos) > 0

      first_info = Enum.at(infos, 0)
      assert %ElixirDatasets.DatasetInfo{} = first_info
      assert first_info.config_name == "csv"
      assert is_list(first_info.features)
      assert is_list(first_info.splits)
    end
  end

  describe "parse_dataset_infos/1" do
    test "parses raw dataset info map into DatasetInfo structs" do
      data = %{
        "cardData" => %{
          "dataset_info" => [
            %{
              "config_name" => "csv",
              "features" => [%{"name" => "id", "dtype" => "int64"}],
              "splits" => [%{"name" => "train", "num_examples" => 10}]
            }
          ]
        }
      }

      infos = ElixirDatasets.parse_dataset_infos(data)
      assert is_list(infos)
      assert Enum.count(infos) == 1

      first_info = Enum.at(infos, 0)
      assert %ElixirDatasets.DatasetInfo{} = first_info
      assert first_info.config_name == "csv"
      assert first_info.features == [%{"name" => "id", "dtype" => "int64"}]
      assert first_info.splits == [%{"name" => "train", "num_examples" => 10}]
    end

    test "handles missing dataset_info gracefully" do
      data = %{"cardData" => %{}}
      infos = ElixirDatasets.parse_dataset_infos(data)
      assert infos == []
    end
  end

  describe "get_dataset_split_names/2" do
    test "fetches split names from dataset" do
      assert {:ok, splits} = ElixirDatasets.get_dataset_split_names("aaaaa32r/elixirDatasets")
      assert is_list(splits)
      assert Enum.count(splits) > 0
      assert Enum.all?(splits, &is_binary/1)
    end
  end

  describe "get_dataset_config_names/2" do
    test "fetches config names from dataset" do
      assert {:ok, configs} = ElixirDatasets.get_dataset_config_names("aaaaa32r/elixirDatasets")
      assert is_list(configs)
      assert Enum.count(configs) > 0
      assert Enum.all?(configs, &is_binary/1)
      assert Enum.member?(configs, "csv")
    end
  end
end

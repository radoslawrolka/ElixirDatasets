defmodule ElixirDatasetsTest do
  use ExUnit.Case, async: false
  doctest ElixirDatasets

  test "Check if the tests are working correctly" do
    assert 2 + 2 == 4
  end

  test "Check if the module is loaded" do
    assert Code.ensure_loaded?(ElixirDatasets)
  end

  # Integration tests for public API
  describe "load_dataset/2 - Public API Integration Tests" do
    @cache_dir "test_cache_integration"
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

    test "loads a dataset from local directory" do
      repository = {:local, "resources"}
      assert {:ok, datasets} = ElixirDatasets.load_dataset(repository)
      assert is_list(datasets)
    end
  end

  describe "load_dataset!/2 - Public API" do
    test "loads dataset successfully" do
      repository = {:local, "resources"}
      datasets = ElixirDatasets.load_dataset!(repository)
      assert is_list(datasets)
    end

    test "raises error on failure" do
      repository = {:local, "invalid/path"}

      assert_raise RuntimeError, fn ->
        ElixirDatasets.load_dataset!(repository)
      end
    end
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

  # Public API tests for dataset info functions
  describe "get_dataset_info/2 - Public API" do
    test "fetches dataset info from Hugging Face API" do
      assert {:ok, info} = ElixirDatasets.get_dataset_info("aaaaa32r/elixirDatasets")
      assert is_map(info)
      assert info["id"] == "aaaaa32r/elixirDatasets"
    end
  end

  describe "get_dataset_infos/2 - Public API" do
    test "fetches dataset infos as DatasetInfo structs" do
      assert {:ok, infos} = ElixirDatasets.get_dataset_infos("aaaaa32r/elixirDatasets")
      assert is_list(infos)
      assert Enum.count(infos) > 0
    end
  end

  describe "get_dataset_split_names/2 - Public API" do
    test "fetches split names from dataset" do
      assert {:ok, splits} = ElixirDatasets.get_dataset_split_names("aaaaa32r/elixirDatasets")
      assert is_list(splits)
      assert Enum.count(splits) > 0
    end
  end

  describe "get_dataset_config_names/2 - Public API" do
    test "fetches config names from dataset" do
      assert {:ok, configs} = ElixirDatasets.get_dataset_config_names("aaaaa32r/elixirDatasets")
      assert is_list(configs)
      assert Enum.count(configs) > 0
      assert Enum.member?(configs, "csv")
    end
  end
end

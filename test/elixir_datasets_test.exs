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
      assert {:ok, _paths} = ElixirDatasets.do_load_spec(@repository, @valid_repo_files)
      File.rm_rf!(@cache_dir)
    end

    test "Return error for invalid files" do
      assert {:error, _reason} = ElixirDatasets.do_load_spec(@repository, @invalid_repo_files)

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
      # Loads a dataset from Hugging Face in online mode
      repository = {:hf, "aaaaa32r/elixirDatasets", [cache_dir: @cache_dir]}
      assert {:ok, datasets} = ElixirDatasets.load_dataset(repository)
      assert is_list(datasets)
      # Loads the same dataset in offline mode
      repositoryOffline = {:hf, "aaaaa32r/elixirDatasets", [cache_dir: @cache_dir, offline: true]}
      assert {:ok, datasets} = ElixirDatasets.load_dataset(repositoryOffline)
      assert is_list(datasets)
      # Loads not existing dataset in offline mode
      repositoryOfflineInvalid = {:hf, "not/exists", [cache_dir: @cache_dir, offline: true]}
      assert {:error, _reason} = ElixirDatasets.load_dataset(repositoryOfflineInvalid)
    end

    test "loads a dataset from Hugging Face with subdirectory" do
      repositorySubdir =
        {:hf, "aaaaa32r/elixirDatasets", [cache_dir: @cache_dir, subdir: "resources"]}

      assert {:ok, datasets} = ElixirDatasets.load_dataset(repositorySubdir)
      assert is_list(datasets)
    end

    # might be not exactly what we want?
    # test "loads a dataset from Hugging Face with spec" do
    #   repositorySpec = {:hf, "aaaaa32r/elixirDatasets", [cache_dir: @cache_dir]}

    #   assert {:ok, datasets} =
    #            ElixirDatasets.load_dataset(repositorySpec, spec: ["csv-test.csv"])
    #   assert is_list(datasets)
    # end

    test "returns error for non-existent dataset" do
      repository = {:test, "nonexistent/repo", []}

      assert_raise ArgumentError, fn ->
        ElixirDatasets.load_dataset(repository)
      end
    end

    # todo more tests for load_dataset/2
  end

  # describe "maybe_load_model_spec/3" do
  #   #   defp maybe_load_model_spec(opts, repository, repo_files) do
  #   # spec_result =
  #   #   if spec = opts[:spec] do
  #   #     {:ok, spec}
  #   #   else
  #   #     do_load_spec(repository, repo_files)
  #   #   end

  #   test ""
  # end

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

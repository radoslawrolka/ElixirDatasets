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
    @valid_repo_files %{"csv-test.csv" => "\"2dccc814f47c01b5344abbb72367a5b322656b0b\""}
    @invalid_repo_files %{"invalid.csv" => "\"1234567890asdfgh\""}

    test "Loads valid files" do
      assert {:ok, _paths} = ElixirDatasets.do_load_spec_TEST(@repository, @valid_repo_files)
      File.rm_rf!(@cache_dir)
    end

    test "Raise error for invalid files" do
      assert_raise ArgumentError, fn ->
        ElixirDatasets.do_load_spec_TEST(@repository, @invalid_repo_files)
      end

      File.rm_rf!(@cache_dir)
    end
  end

  describe "decode_config/1" do
    test "Decodes a valid JSON file" do
      File.write!("valid.json", ~s({"key": "value"}))
      assert {:ok, %{"key" => "value"}} = ElixirDatasets.decode_config_TEST("valid.json")
      File.rm!("valid.json")
    end

    test "Fails to decode JSON file" do
      File.write!("invalid.json", "{invalid_json}")
      assert {:error, _} = ElixirDatasets.decode_config_TEST("invalid.json")
      File.rm!("invalid.json")
    end
  end

  describe "load_dataset/2" do
    @cache_dir "test_cache_load_dataset"
    @repository {:hf, "aaaaa32r/elixirDatasets", [cache_dir: @cache_dir]}

    test "loads a dataset from Hugging Face" do
      assert {:ok, %{dataset: _paths}} = ElixirDatasets.load_dataset(@repository)
      File.rm_rf!(@cache_dir)
    end

    test "loads a dataset from Hugging Face without opts" do
      repositoryShort = {:hf, "aaaaa32r/elixirDatasets"}
      assert {:ok, %{dataset: _paths}} = ElixirDatasets.load_dataset(repositoryShort)

      File.rm_rf!(
        :filename.basedir(
          :user_cache,
          "elixirDatasets" <> "/huggingface/aaaaa32r--elixirDatasets"
        )
      )
    end

    # test "loads a dataset from local directory" do
    #   repository = {:local, "test/fixtures/datasets/local_dataset"}
    #   assert {:ok, %{dataset: _paths}} = ElixirDatasets.load_dataset(repository)
    # end

    test "loads a dataset from Hugging Face with subdirectory" do
      repositorySubdir =
        {:hf, "aaaaa32r/elixirDatasets", [cache_dir: @cache_dir, subdir: "resources"]}

      assert {:ok, %{dataset: _paths}} = ElixirDatasets.load_dataset(repositorySubdir)
      File.rm_rf!(@cache_dir)
    end

    # might be not exaclty what we want?
    test "loads a dataset from Hugging Face with spec" do
      repositorySpec = {:hf, "aaaaa32r/elixirDatasets", [cache_dir: @cache_dir]}

      assert {:ok, %{dataset: _paths}} =
               ElixirDatasets.load_dataset(repositorySpec, spec: ["csv-test.csv"])

      File.rm_rf!(@cache_dir)
    end

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
      if System.get_env("ELIXIRDATASETS_CACHE_DIR") do
        assert ElixirDatasets.cache_dir() == System.get_env("ELIXIRDATASETS_CACHE_DIR")
      else
        path = "test/cache"
        System.put_env("ELIXIRDATASETS_CACHE_DIR", path)
        expected_dir = Path.expand(path)
        assert ElixirDatasets.cache_dir() == expected_dir
        System.delete_env("ELIXIRDATASETS_CACHE_DIR")
      end
    end

    test "Cache directory not set" do
      if env_var = System.get_env("ELIXIRDATASETS_CACHE_DIR") do
        System.delete_env("ELIXIRDATASETS_CACHE_DIR")
      end

      expected_dir = :filename.basedir(:user_cache, "elixirDatasets")
      assert ElixirDatasets.cache_dir() == expected_dir

      if env_var do
        System.put_env("ELIXIRDATASETS_CACHE_DIR", env_var)
      end
    end
  end
end

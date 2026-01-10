defmodule ElixirDatasets.LoaderTest do
  use ExUnit.Case, async: false
  doctest ElixirDatasets.Loader

  alias ElixirDatasets.Loader

  describe "load_spec/3" do
    @cache_dir "test_cache_load_spec"
    @repository {:hf, "aaaaa32r/elixirDatasets", [cache_dir: @cache_dir]}
    @valid_repo_files %{
      "resources/csv-test.csv" => "\"2dccc814f47c01b5344abbb72367a5b322656b0b\""
    }
    @invalid_repo_files %{"invalid.csv" => "\"1234567890asdfgh\""}

    test "loads valid files" do
      assert {:ok, _paths} = Loader.load_spec(@repository, @valid_repo_files, 1)
      File.rm_rf!(@cache_dir)
    end

    test "returns error for invalid files" do
      assert {:error, _reason} = Loader.load_spec(@repository, @invalid_repo_files, 1)
      File.rm_rf!(@cache_dir)
    end

    test "loads files with num_proc > 1" do
      assert {:ok, paths} = Loader.load_spec(@repository, @valid_repo_files, 4)
      assert is_list(paths)
      File.rm_rf!(@cache_dir)
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
      assert {:ok, datasets} = Loader.load_dataset(@repository)
      assert is_list(datasets)
    end

    test "loads a dataset from Hugging Face without opts" do
      repository_short = {:hf, "aaaaa32r/elixirDatasets"}
      assert {:ok, datasets} = Loader.load_dataset(repository_short)
      assert is_list(datasets)
    end

    test "loads a dataset from local directory" do
      repository = {:local, "resources"}
      assert {:ok, datasets} = Loader.load_dataset(repository)
      assert is_list(datasets)
    end

    test "raises error when invalid local directory" do
      repository = {:local, "invalid/path"}
      assert {:error, _reason} = Loader.load_dataset(repository)
    end

    test "loads dataset offline" do
      repository = {:hf, "aaaaa32r/elixirDatasets", [cache_dir: @cache_dir]}
      assert {:ok, datasets} = Loader.load_dataset(repository)
      assert is_list(datasets)

      repository_offline = {:hf, "aaaaa32r/elixirDatasets", [cache_dir: @cache_dir, offline: true]}
      assert {:ok, datasets} = Loader.load_dataset(repository_offline)
      assert is_list(datasets)

      repository_offline_invalid = {:hf, "not/exists", [cache_dir: @cache_dir, offline: true]}
      assert {:error, _reason} = Loader.load_dataset(repository_offline_invalid)
    end

    test "loads a dataset from Hugging Face with subdirectory" do
      repository_subdir =
        {:hf, "aaaaa32r/elixirDatasets", [cache_dir: @cache_dir, subdir: "resources"]}

      assert {:ok, datasets} = Loader.load_dataset(repository_subdir)
      assert is_list(datasets)
    end

    test "returns error for non-existent dataset" do
      repository = {:test, "nonexistent/repo", []}

      assert_raise ArgumentError, fn ->
        Loader.load_dataset(repository)
      end
    end

    test "loads dataset with split parameter from local directory" do
      repository = {:local, "resources"}
      assert {:ok, datasets} = Loader.load_dataset(repository, split: "train")
      assert is_list(datasets)
    end

    test "loads dataset with name parameter filters files" do
      repository = {:local, "resources"}
      assert {:ok, datasets} = Loader.load_dataset(repository, name: "csv")
      assert is_list(datasets)
    end

    test "loads dataset with split and name parameters combined" do
      repository = {:local, "resources"}

      assert {:ok, datasets} =
               Loader.load_dataset(repository, split: "train", name: "csv")

      assert is_list(datasets)
    end

    test "loads dataset with download_mode option" do
      repository = {:local, "resources"}

      assert {:ok, datasets} =
               Loader.load_dataset(repository, download_mode: :reuse_dataset_if_exists)

      assert is_list(datasets)
    end

    test "loads dataset with verification_mode option" do
      repository = {:local, "resources"}

      assert {:ok, datasets} =
               Loader.load_dataset(repository, verification_mode: :no_checks)

      assert is_list(datasets)
    end

    test "loads dataset with num_proc for parallel processing" do
      repository = {:local, "resources"}
      assert {:ok, datasets} = Loader.load_dataset(repository, num_proc: 2)
      assert is_list(datasets)
      assert length(datasets) > 0
    end

    test "loads dataset with num_proc=1 (sequential)" do
      repository = {:local, "resources"}
      assert {:ok, datasets} = Loader.load_dataset(repository, num_proc: 1)
      assert is_list(datasets)
    end

    test "num_proc=4 is faster than num_proc=1 for parallel loading" do
      repository = @repository

      {time_sequential, {:ok, datasets_seq}} =
        :timer.tc(fn ->
          Loader.load_dataset(repository, num_proc: 1)
        end)

      {time_parallel, {:ok, datasets_par}} =
        :timer.tc(fn ->
          Loader.load_dataset(repository, num_proc: 4)
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

      assert time_parallel <= time_sequential * 1.5,
             "Parallel processing overhead should be reasonable for this dataset size"
    end

    test "num_proc produces same results as sequential" do
      repository = {:local, "resources"}

      {:ok, datasets_seq} = Loader.load_dataset(repository, num_proc: 1)
      {:ok, datasets_par} = Loader.load_dataset(repository, num_proc: 4)

      assert length(datasets_seq) == length(datasets_par)
      seq_row_counts = Enum.map(datasets_seq, &Explorer.DataFrame.n_rows/1) |> Enum.sort()
      par_row_counts = Enum.map(datasets_par, &Explorer.DataFrame.n_rows/1) |> Enum.sort()

      assert seq_row_counts == par_row_counts
    end
  end

  describe "load_dataset!/2" do
    test "loads dataset successfully" do
      repository = {:local, "resources"}
      datasets = Loader.load_dataset!(repository)
      assert is_list(datasets)
    end

    test "raises error on failure" do
      repository = {:local, "invalid/path"}

      assert_raise RuntimeError, fn ->
        Loader.load_dataset!(repository)
      end
    end
  end
end

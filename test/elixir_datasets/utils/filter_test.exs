defmodule ElixirDatasets.FilterTest do
  use ExUnit.Case, async: true
  doctest ElixirDatasets.Filter

  alias ElixirDatasets.Filter

  describe "by_config_and_split/3" do
    @sample_files %{
      "train.csv" => "etag1",
      "test.csv" => "etag2",
      "validation.csv" => "etag3",
      "sst2/train.parquet" => "etag4",
      "sst2/test.parquet" => "etag5",
      "cola/train.parquet" => "etag6"
    }

    test "returns all files when no filters applied" do
      assert {:ok, filtered} = Filter.by_config_and_split(@sample_files, nil, nil)
      assert filtered == @sample_files
    end

    test "filters by split name" do
      assert {:ok, filtered} = Filter.by_config_and_split(@sample_files, nil, "train")
      assert map_size(filtered) == 3
      assert Map.has_key?(filtered, "train.csv")
      assert Map.has_key?(filtered, "sst2/train.parquet")
      assert Map.has_key?(filtered, "cola/train.parquet")
    end

    test "filters by config name" do
      assert {:ok, filtered} = Filter.by_config_and_split(@sample_files, "sst2", nil)
      assert map_size(filtered) == 2
      assert Map.has_key?(filtered, "sst2/train.parquet")
      assert Map.has_key?(filtered, "sst2/test.parquet")
    end

    test "filters by both config and split" do
      assert {:ok, filtered} = Filter.by_config_and_split(@sample_files, "sst2", "train")
      assert map_size(filtered) == 1
      assert Map.has_key?(filtered, "sst2/train.parquet")
    end

    test "returns empty map when no matches" do
      assert {:ok, filtered} = Filter.by_config_and_split(@sample_files, "nonexistent", nil)
      assert map_size(filtered) == 0
    end
  end

  describe "by_config_name/2" do
    @sample_files %{
      "train.csv" => "etag1",
      "sst2/train.parquet" => "etag2",
      "cola/train.parquet" => "etag3"
    }

    test "returns all files when config is nil" do
      filtered = Filter.by_config_name(@sample_files, nil)
      assert filtered == @sample_files
    end

    test "filters files by config name" do
      filtered = Filter.by_config_name(@sample_files, "sst2")
      assert is_map(filtered)
      assert map_size(filtered) == 1
      assert Map.has_key?(filtered, "sst2/train.parquet")
    end

    test "works with list input" do
      files_list = [{"train.csv", "etag1"}, {"sst2/train.parquet", "etag2"}]
      filtered = Filter.by_config_name(files_list, "sst2")
      assert is_list(filtered)
      assert length(filtered) == 1
      assert {"sst2/train.parquet", "etag2"} in filtered
    end
  end

  describe "by_split/2" do
    @sample_files %{
      "train.csv" => "etag1",
      "test.csv" => "etag2",
      "validation.csv" => "etag3"
    }

    test "returns all files when split is nil" do
      filtered = Filter.by_split(@sample_files, nil)
      assert filtered == @sample_files
    end

    test "filters files by split name" do
      filtered = Filter.by_split(@sample_files, "train")
      assert is_map(filtered)
      assert map_size(filtered) == 1
      assert Map.has_key?(filtered, "train.csv")
    end

    test "filters files with split in basename" do
      files = %{
        "train-00000.parquet" => "etag1",
        "test-00000.parquet" => "etag2",
        "train-00001.parquet" => "etag3"
      }

      filtered = Filter.by_split(files, "train")
      assert map_size(filtered) == 2
      assert Map.has_key?(filtered, "train-00000.parquet")
      assert Map.has_key?(filtered, "train-00001.parquet")
    end

    test "works with list input" do
      files_list = [{"train.csv", "etag1"}, {"test.csv", "etag2"}]
      filtered = Filter.by_split(files_list, "train")
      assert is_list(filtered)
      assert length(filtered) == 1
      assert {"train.csv", "etag1"} in filtered
    end
  end
end


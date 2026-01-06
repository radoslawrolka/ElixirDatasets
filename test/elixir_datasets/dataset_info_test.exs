defmodule ElixirDatasets.DatasetInfoTest do
  use ExUnit.Case, async: true
  doctest ElixirDatasets.DatasetInfo

  alias ElixirDatasets.DatasetInfo

  describe "from_map/1" do
    test "creates DatasetInfo struct from map with all fields" do
      data = %{
        "config_name" => "csv",
        "features" => [%{"name" => "id", "dtype" => "int64"}],
        "splits" => [%{"name" => "train", "num_examples" => 10}],
        "description" => "A test dataset",
        "homepage" => "https://example.com",
        "license" => "MIT",
        "citation" => "@article{test, ...}"
      }

      info = DatasetInfo.from_map(data)

      assert info.config_name == "csv"
      assert info.features == [%{"name" => "id", "dtype" => "int64"}]
      assert info.splits == [%{"name" => "train", "num_examples" => 10}]
      assert info.description == "A test dataset"
      assert info.homepage == "https://example.com"
      assert info.license == "MIT"
      assert info.citation == "@article{test, ...}"
    end

    test "creates DatasetInfo struct with nil for missing fields" do
      data = %{"config_name" => "csv"}

      info = DatasetInfo.from_map(data)

      assert info.config_name == "csv"
      assert info.features == nil
      assert info.splits == nil
      assert info.description == nil
      assert info.homepage == nil
      assert info.license == nil
      assert info.citation == nil
    end

    test "creates DatasetInfo struct from empty map" do
      data = %{}

      info = DatasetInfo.from_map(data)

      assert info.config_name == nil
      assert info.features == nil
      assert info.splits == nil
    end
  end

  describe "to_map/1" do
    test "converts DatasetInfo struct to map" do
      info = %DatasetInfo{
        config_name: "csv",
        features: [%{"name" => "id", "dtype" => "int64"}],
        splits: [%{"name" => "train", "num_examples" => 10}],
        description: "A test dataset",
        homepage: "https://example.com",
        license: "MIT",
        citation: "@article{test, ...}"
      }

      map = DatasetInfo.to_map(info)

      assert map["config_name"] == "csv"
      assert map["features"] == [%{"name" => "id", "dtype" => "int64"}]
      assert map["splits"] == [%{"name" => "train", "num_examples" => 10}]
      assert map["description"] == "A test dataset"
      assert map["homepage"] == "https://example.com"
      assert map["license"] == "MIT"
      assert map["citation"] == "@article{test, ...}"
    end

    test "converts DatasetInfo struct with nil values to map" do
      info = %DatasetInfo{
        config_name: "csv",
        features: nil,
        splits: nil,
        description: nil,
        homepage: nil,
        license: nil,
        citation: nil
      }

      map = DatasetInfo.to_map(info)

      assert map["config_name"] == "csv"
      assert map["features"] == nil
      assert map["splits"] == nil
      assert map["description"] == nil
      assert map["homepage"] == nil
      assert map["license"] == nil
      assert map["citation"] == nil
    end
  end

  describe "from_map/1 and to_map/1 roundtrip" do
    test "roundtrip conversion preserves data" do
      original_data = %{
        "config_name" => "parquet",
        "features" => [
          %{"name" => "text", "dtype" => "string"},
          %{"name" => "label", "dtype" => "int64"}
        ],
        "splits" => [
          %{"name" => "train", "num_examples" => 1000},
          %{"name" => "test", "num_examples" => 200}
        ],
        "description" => "Multi-label classification dataset",
        "homepage" => "https://datasets.example.com",
        "license" => "Apache 2.0",
        "citation" => "@dataset{example, ...}"
      }

      info = DatasetInfo.from_map(original_data)
      result_map = DatasetInfo.to_map(info)

      assert result_map == original_data
    end
  end
end

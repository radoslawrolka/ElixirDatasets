defmodule ElixirDatasets.InfoTest do
  use ExUnit.Case, async: true
  doctest ElixirDatasets.Info

  alias ElixirDatasets.Info

  describe "get_dataset_info/2" do
    test "fetches dataset info from Hugging Face API" do
      assert {:ok, info} = Info.get_dataset_info("aaaaa32r/elixirDatasets")
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
      assert {:ok, infos} = Info.get_dataset_infos("aaaaa32r/elixirDatasets")
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

      infos = Info.parse_dataset_infos(data)
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
      infos = Info.parse_dataset_infos(data)
      assert infos == []
    end
  end

  describe "get_dataset_split_names/2" do
    test "fetches split names from dataset" do
      assert {:ok, splits} = Info.get_dataset_split_names("aaaaa32r/elixirDatasets")
      assert is_list(splits)
      assert Enum.count(splits) > 0
      assert Enum.all?(splits, &is_binary/1)
    end
  end

  describe "get_dataset_config_names/2" do
    test "fetches config names from dataset" do
      assert {:ok, configs} = Info.get_dataset_config_names("aaaaa32r/elixirDatasets")
      assert is_list(configs)
      assert Enum.count(configs) > 0
      assert Enum.all?(configs, &is_binary/1)
      assert Enum.member?(configs, "csv")
    end
  end
end


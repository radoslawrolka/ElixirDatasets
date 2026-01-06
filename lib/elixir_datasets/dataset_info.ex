defmodule ElixirDatasets.DatasetInfo do
  @moduledoc """
  Represents dataset information from the HuggingFace API.

  This struct encapsulates the metadata about a dataset configuration,
  including features and split information.
  """

  @typedoc """
  Feature information in a dataset.
  Contains the feature name and its data type.
  """
  @type feature :: map()

  @typedoc """
  Split information for a dataset configuration.
  Contains the split name and the number of examples.
  """
  @type split :: map()

  @typedoc """
  DatasetInfo struct represents metadata about a dataset configuration.

  Fields:
    * `config_name` - The configuration name for the dataset (e.g., "csv", "default")
    * `features` - List of feature definitions in the dataset
    * `splits` - List of data splits (train, test, validation, etc.) with example counts
    * `description` - Optional description of the dataset configuration
    * `homepage` - Optional homepage URL for the dataset
    * `license` - Optional license information
    * `citation` - Optional citation information
  """
  defstruct [
    :config_name,
    :features,
    :splits,
    :description,
    :homepage,
    :license,
    :citation
  ]

  @type t :: %__MODULE__{
          config_name: String.t(),
          features: [map()] | nil,
          splits: [map()] | nil,
          description: String.t() | nil,
          homepage: String.t() | nil,
          license: String.t() | nil,
          citation: String.t() | nil
        }

  @doc """
  Creates a DatasetInfo struct from a map (typically from JSON response).

  ## Examples

      iex> map = %{
      ...>   "config_name" => "csv",
      ...>   "features" => [%{"name" => "id", "dtype" => "int64"}],
      ...>   "splits" => [%{"name" => "train", "num_examples" => 10}]
      ...> }
      iex> ElixirDatasets.DatasetInfo.from_map(map)
      %ElixirDatasets.DatasetInfo{
        config_name: "csv",
        features: [%{"name" => "id", "dtype" => "int64"}],
        splits: [%{"name" => "train", "num_examples" => 10}],
        description: nil,
        homepage: nil,
        license: nil,
        citation: nil
      }
  """
  @spec from_map(map()) :: t()
  def from_map(data) when is_map(data) do
    %__MODULE__{
      config_name: Map.get(data, "config_name"),
      features: Map.get(data, "features"),
      splits: Map.get(data, "splits"),
      description: Map.get(data, "description"),
      homepage: Map.get(data, "homepage"),
      license: Map.get(data, "license"),
      citation: Map.get(data, "citation")
    }
  end

  @doc """
  Converts a DatasetInfo struct to a map.
  """
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = dataset_info) do
    %{
      "config_name" => dataset_info.config_name,
      "features" => dataset_info.features,
      "splits" => dataset_info.splits,
      "description" => dataset_info.description,
      "homepage" => dataset_info.homepage,
      "license" => dataset_info.license,
      "citation" => dataset_info.citation
    }
  end
end

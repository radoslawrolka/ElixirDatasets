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

  @doc """
  Writes DatasetInfo to a directory as a JSON file.

  Creates a directory if it doesn't exist and saves the dataset information
  as 'dataset_info.json' in that directory.

  ## Parameters

    * `dataset_info` - The DatasetInfo struct to write
    * `directory` - The directory path where the file will be saved

  ## Returns

    * `{:ok, filepath}` - Success with the path to the saved file
    * `{:error, reason}` - If directory creation or file writing fails

  ## Examples

      iex> dataset_info = %ElixirDatasets.DatasetInfo{
      ...>   config_name: "csv",
      ...>   features: [%{"name" => "id", "dtype" => "int64"}],
      ...>   splits: [%{"name" => "train", "num_examples" => 10}]
      ...> }
      iex> ElixirDatasets.DatasetInfo.write_to_directory(dataset_info, "/tmp/my_dataset")
      {:ok, "/tmp/my_dataset/dataset_info.json"}
  """
  @spec write_to_directory([t()], String.t()) :: {:ok, String.t()} | {:error, any()}
  def write_to_directory(dataset_info, directory) when is_binary(directory) do
    with :ok <- File.mkdir_p(directory) do
      filepath = Path.join(directory, "dataset_info.json")

      json_data =
        dataset_info
        |> case do
          list when is_list(list) -> Enum.map(list, &to_map/1)
          single -> to_map(single)
        end
        |> Jason.encode!()

      case File.write(filepath, json_data) do
        :ok -> {:ok, filepath}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @doc """
  Reads DatasetInfo from a directory JSON file.

  Reads the 'dataset_info.json' file from the specified directory and
  returns a DatasetInfo struct.

  ## Parameters

    * `directory` - The directory path containing the 'dataset_info.json' file

  ## Returns

    * `{:ok, dataset_info}` - Success with the parsed DatasetInfo struct
    * `{:error, reason}` - If the file doesn't exist or parsing fails

  ## Examples

      iex> dataset_info = %ElixirDatasets.DatasetInfo{
      ...>   config_name: "csv",
      ...>   features: [%{"name" => "id", "dtype" => "int64"}],
      ...>   splits: [%{"name" => "train", "num_examples" => 10}]
      ...> }
      iex> ElixirDatasets.DatasetInfo.write_to_directory(dataset_info, "/tmp/my_dataset")
      {:ok, "/tmp/my_dataset/dataset_info.json"}
      iex> ElixirDatasets.DatasetInfo.from_directory("/tmp/my_dataset")
      {:ok, %ElixirDatasets.DatasetInfo{
        config_name: "csv",
        features: [%{"name" => "id", "dtype" => "int64"}],
        splits: [%{"name" => "train", "num_examples" => 10}],
        description: nil,
        homepage: nil,
        license: nil,
        citation: nil
      }}
  """
  @spec from_directory(String.t()) :: {:ok, t()} | {:error, any()}
  def from_directory(directory, filename \\ "dataset_info.json") when is_binary(directory) do
    filepath = Path.join(directory, filename)

    with {:ok, content} <- File.read(filepath),
         {:ok, data} <- Jason.decode(content) do
      dataset_info =
        case data do
          list when is_list(list) -> Enum.map(list, &from_map/1)
          single -> from_map(single)
        end

      {:ok, dataset_info}
    else
      {:error, reason} -> {:error, reason}
    end
  end
end

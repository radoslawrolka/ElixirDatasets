defmodule ElixirDatasets.Info do
  @moduledoc """
  Functions for fetching and parsing dataset metadata from Hugging Face Hub.
  """

  alias ElixirDatasets.HuggingFace
  alias ElixirDatasets.DatasetInfo

  @doc """
  Fetches dataset information from the Hugging Face API.

  ## Parameters

    * `repository_id` - the Hugging Face dataset repository ID (e.g., "aaaaa32r/elixirDatasets")
    * `opts` - optional keyword list with the following options:
      * `:auth_token` - the token to use as HTTP bearer authorization

  ## Returns

  Returns `{:ok, dataset_info}` where `dataset_info` is a map containing the dataset metadata,
  or `{:error, reason}` if the request fails.
  """
  @spec get_dataset_info(String.t(), keyword()) :: {:ok, map()} | {:error, String.t()}
  def get_dataset_info(repository_id, opts \\ []) when is_binary(repository_id) do
    url = HuggingFace.Hub.dataset_info_url(repository_id)

    headers =
      case HuggingFace.Hub.get_auth_token(opts) do
        {:ok, auth_token} -> [{"Authorization", "Bearer #{auth_token}"}]
        {:error, _} -> []
      end

    with {:ok, response} <- ElixirDatasets.Utils.HTTP.request(:get, url, headers: headers),
         {:ok, data} <- Jason.decode(response.body) do
      {:ok, data}
    end
  end

  @doc """
  Fetches dataset information from the Hugging Face API and returns a list of DatasetInfo structs.

  This function retrieves all available dataset configurations for a given repository.

  ## Parameters

    * `repository_id` - the Hugging Face dataset repository ID (e.g., "aaaaa32r/elixirDatasets")
    * `opts` - optional keyword list with the following options:
      * `:auth_token` - the token to use as HTTP bearer authorization

  ## Returns

  Returns `{:ok, dataset_infos}` where `dataset_infos` is a list of DatasetInfo structs,
  or `{:error, reason}` if the request fails.

  ## Examples

      iex> {:ok, infos} = ElixirDatasets.Info.get_dataset_infos("aaaaa32r/elixirDatasets")
      iex> Enum.map(infos, & &1.config_name)
      ["csv", "default"]
  """
  @spec get_dataset_infos(String.t(), keyword()) ::
          {:ok, [DatasetInfo.t()]} | {:error, String.t()}
  def get_dataset_infos(repository_id, opts \\ []) when is_binary(repository_id) do
    case get_dataset_info(repository_id, opts) do
      {:ok, info} ->
        dataset_infos = parse_dataset_infos(info)
        {:ok, dataset_infos}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Parses raw dataset info map into a list of DatasetInfo structs.

  Extracts the dataset_info array from the HuggingFace API response's cardData field
  and converts each entry into a DatasetInfo struct.

  ## Parameters

    * `data` - the raw response map from the HuggingFace API

  ## Returns

  A list of DatasetInfo structs.
  """
  @spec parse_dataset_infos(map()) :: [DatasetInfo.t()]
  def parse_dataset_infos(data) when is_map(data) do
    data
    |> Map.get("cardData", %{})
    |> Map.get("dataset_info", [])
    |> case do
      list when is_list(list) -> Enum.map(list, &DatasetInfo.from_map/1)
      single -> [DatasetInfo.from_map(single)]
    end
  end

  @doc """
  Gets the split names (e.g., 'train', 'test', 'validation') for a dataset.

  ## Parameters

    * `repository_id` - the Hugging Face dataset repository ID (e.g., "cornell-movie-review-data/rotten_tomatoes")
    * `opts` - optional keyword list with the following options:
      * `:auth_token` - the token to use as HTTP bearer authorization

  ## Returns

  Returns `{:ok, split_names}` where `split_names` is a list of strings representing
  the available splits, or `{:error, reason}` if the request fails.

  ## Examples

      iex> {:ok, splits} = ElixirDatasets.Info.get_dataset_split_names("cornell-movie-review-data/rotten_tomatoes")
      iex> splits
      ["train", "validation", "test"]
  """
  @spec get_dataset_split_names(String.t(), keyword()) ::
          {:ok, [String.t()]} | {:error, String.t()}
  def get_dataset_split_names(repository_id, opts \\ []) when is_binary(repository_id) do
    case get_dataset_infos(repository_id, opts) do
      {:ok, infos} ->
        split_names =
          infos
          |> Enum.flat_map(fn info ->
            case info.splits do
              nil -> []
              splits -> Enum.map(splits, fn split -> split["name"] end)
            end
          end)
          |> Enum.uniq()

        {:ok, split_names}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Gets the configuration names available for a dataset.

  ## Parameters

    * `repository_id` - the Hugging Face dataset repository ID (e.g., "glue")
    * `opts` - optional keyword list with the following options:
      * `:auth_token` - the token to use as HTTP bearer authorization

  ## Returns

  Returns `{:ok, config_names}` where `config_names` is a list of configuration names,
  or `{:error, reason}` if the request fails.

  ## Examples

      iex> {:ok, configs} = ElixirDatasets.Info.get_dataset_config_names("glue")
      iex> Enum.member?(configs, "cola")
      true
  """
  @spec get_dataset_config_names(String.t(), keyword()) ::
          {:ok, [String.t()]} | {:error, String.t()}
  def get_dataset_config_names(repository_id, opts \\ []) when is_binary(repository_id) do
    case get_dataset_infos(repository_id, opts) do
      {:ok, infos} ->
        config_names = Enum.map(infos, fn info -> info.config_name end)
        {:ok, config_names}

      {:error, reason} ->
        {:error, reason}
    end
  end
end


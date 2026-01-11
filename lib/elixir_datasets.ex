# This file, part of the ElixirDatasets project, has been adapted from code originally under Apache License 2.0.
# The original code can be found at:
# https://github.com/elixir-nx/bumblebee/blob/710a645222948f80208c348d3a2589cbd3ab8e7d/lib/bumblebee.ex

defmodule ElixirDatasets do
  @moduledoc """
  ElixirDatasets is a comprehensive library for accessing and managing datasets from Hugging Face Hub in Elixir.

  This module provides the main public API for loading datasets, fetching metadata,
  and uploading datasets to Hugging Face Hub.

  ## Main Functions

    * `load_dataset/2` - Load datasets from Hugging Face or local files
    * `load_dataset!/2` - Same as `load_dataset/2` but raises on error
    * `get_dataset_info/2` - Fetch dataset metadata
    * `get_dataset_infos/2` - Fetch all dataset configurations
    * `get_dataset_split_names/2` - Get available splits (train/test/validation)
    * `get_dataset_config_names/2` - Get available configurations
    * `upload_dataset/3` - Upload a dataset to Hugging Face Hub
    * `cache_dir/0` - Get the cache directory path

  ## Examples

      # Load a dataset from Hugging Face
      iex> {:ok, datasets} = ElixirDatasets.load_dataset({:hf, "imdb"})

      # Load with specific split
      iex> {:ok, train_data} = ElixirDatasets.load_dataset({:hf, "imdb"}, split: "train")

      # Stream large datasets
      iex> {:ok, stream} = ElixirDatasets.load_dataset({:hf, "c4"}, streaming: true)
      iex> stream |> Enum.take(100)

      # Get dataset information
      iex> {:ok, info} = ElixirDatasets.get_dataset_info("imdb")

  """
  @compile if Mix.env() == :test, do: :export_all

  alias ElixirDatasets.{Info, Loader, Repository}

  @typedoc """
  A location to fetch dataset files from.
  Can be either a Hugging Face repository or a local resources:

    * `{:hf, repository_id}` - the Hugging Face repository ID

    * `{:hf, repository_id, options}` - the Hugging Face repository ID
      with additional options

    * `{:local, path}` - a local directory or file path containing the datasets
  """
  @type t_repository :: Repository.t_repository()

  # Delegated to Loader module for backward compatibility with tests
  def do_load_spec(repository, repo_files, num_proc) do
    Loader.load_spec(repository, repo_files, num_proc)
  end

  # Delegated to Repository module for backward compatibility with tests
  def decode_config(path) do
    path
    |> File.read!()
    |> Jason.decode()
    |> case do
      {:ok, data} ->
        {:ok, data}

      {:error, reason} ->
        {:error,
         "failed to parse the config file, it is not a valid JSON. Reason: #{inspect(reason)}"}
    end
  end

  @doc """
  Fetches dataset information from the Hugging Face API.

  Delegates to `ElixirDatasets.Info.get_dataset_info/2`.

  ## Parameters

    * `repository_id` - the Hugging Face dataset repository ID (e.g., "aaaaa32r/elixirDatasets")
    * `opts` - optional keyword list with the following options:
      * `:auth_token` - the token to use as HTTP bearer authorization

  ## Returns

  Returns `{:ok, dataset_info}` where `dataset_info` is a map containing the dataset metadata,
  or `{:error, reason}` if the request fails.
  """
  @spec get_dataset_info(String.t(), keyword()) :: {:ok, map()} | {:error, String.t()}
  defdelegate get_dataset_info(repository_id, opts \\ []), to: Info

  @doc """
  Fetches dataset information from the Hugging Face API and returns a list of DatasetInfo structs.

  Delegates to `ElixirDatasets.Info.get_dataset_infos/2`.

  This function retrieves all available dataset configurations for a given repository.

  ## Parameters

    * `repository_id` - the Hugging Face dataset repository ID (e.g., "aaaaa32r/elixirDatasets")
    * `opts` - optional keyword list with the following options:
      * `:auth_token` - the token to use as HTTP bearer authorization

  ## Returns

  Returns `{:ok, dataset_infos}` where `dataset_infos` is a list of DatasetInfo structs,
  or `{:error, reason}` if the request fails.

  ## Examples

      iex> {:ok, infos} = ElixirDatasets.get_dataset_infos("aaaaa32r/elixirDatasets")
      iex> Enum.map(infos, & &1.config_name)
      ["csv", "default"]
  """
  @spec get_dataset_infos(String.t(), keyword()) ::
          {:ok, [ElixirDatasets.DatasetInfo.t()]} | {:error, String.t()}
  defdelegate get_dataset_infos(repository_id, opts \\ []), to: Info

  @doc """
  Parses raw dataset info map into a list of DatasetInfo structs.

  Delegates to `ElixirDatasets.Info.parse_dataset_infos/1`.

  Extracts the dataset_info array from the HuggingFace API response's cardData field
  and converts each entry into a DatasetInfo struct.

  ## Parameters

    * `data` - the raw response map from the HuggingFace API

  ## Returns

  A list of DatasetInfo structs.
  """
  @spec parse_dataset_infos(map()) :: [ElixirDatasets.DatasetInfo.t()]
  defdelegate parse_dataset_infos(data), to: Info

  @doc """
  Gets the split names (e.g., 'train', 'test', 'validation') for a dataset.

  Delegates to `ElixirDatasets.Info.get_dataset_split_names/2`.

  ## Parameters

    * `repository_id` - the Hugging Face dataset repository ID (e.g., "cornell-movie-review-data/rotten_tomatoes")
    * `opts` - optional keyword list with the following options:
      * `:auth_token` - the token to use as HTTP bearer authorization

  ## Returns

  Returns `{:ok, split_names}` where `split_names` is a list of strings representing
  the available splits, or `{:error, reason}` if the request fails.

  ## Examples

      iex> {:ok, splits} = ElixirDatasets.get_dataset_split_names("cornell-movie-review-data/rotten_tomatoes")
      iex> splits
      ["train", "validation", "test"]
  """
  @spec get_dataset_split_names(String.t(), keyword()) ::
          {:ok, [String.t()]} | {:error, String.t()}
  defdelegate get_dataset_split_names(repository_id, opts \\ []), to: Info

  @doc """
  Gets the configuration names available for a dataset.

  Delegates to `ElixirDatasets.Info.get_dataset_config_names/2`.

  ## Parameters

    * `repository_id` - the Hugging Face dataset repository ID (e.g., "glue")
    * `opts` - optional keyword list with the following options:
      * `:auth_token` - the token to use as HTTP bearer authorization

  ## Returns

  Returns `{:ok, config_names}` where `config_names` is a list of configuration names,
  or `{:error, reason}` if the request fails.

  ## Examples

      iex> {:ok, configs} = ElixirDatasets.get_dataset_config_names("glue")
      iex> Enum.member?(configs, "cola")
      true
  """
  @spec get_dataset_config_names(String.t(), keyword()) ::
          {:ok, [String.t()]} | {:error, String.t()}
  defdelegate get_dataset_config_names(repository_id, opts \\ []), to: Info

  @doc """
  Loads a dataset from the given repository.

  Delegates to `ElixirDatasets.Loader.load_dataset/2`.

  The repository can be either a local directory or a Hugging Face repository.

  ## Options

  ### Data Loading Options

    * `:split` - which split of the data to load (e.g., "train", "test", "validation").
      If not specified, all splits are loaded. Files are matched by name patterns
      (e.g., "train.csv", "test-00000.parquet", "validation.jsonl").

    * `:name` - the name of the dataset configuration to load. For datasets with
      multiple configurations, this specifies which one to use. Files are matched
      by looking for the config name in the file path (e.g., "sst2/train.parquet").

    * `:streaming` - if `true`, returns an enumerable that progressively yields
      data rows (maps) without loading the entire dataset into memory. Data is
      fetched on-demand as you iterate. Useful for large datasets. Default is `false`.

  ### HuggingFace Hub Options

    * `:auth_token` - the token to use as HTTP bearer authorization
      for remote files. If not provided, the token from the
      `ELIXIR_DATASETS_HF_TOKEN` environment variable is used.

    * `:cache_dir` - the directory to store downloaded files in.
      Defaults to the standard cache location for the operating system.

    * `:offline` - if `true`, only cached files are used and no network
      requests are made. Returns an error if the file is not cached.

    * `:etag` - if provided, skips the HEAD request to fetch the latest
      ETag value and uses this value instead.

    * `:download_mode` - controls download/cache behavior. Can be:
      - `:reuse_dataset_if_exists` (default) - reuse cached data if available
      - `:force_redownload` - always download, even if cached

    * `:verification_mode` - controls verification checks. Can be:
      - `:basic_checks` (default) - basic validation
      - `:no_checks` - skip all validation

    * `:num_proc` - number of processes to use for parallel dataset processing.
      Default is `1` (no parallelization). Set to a higher number to speed up
      dataset downloading and loading. For example, `num_proc: 4` will use 4
      parallel processes.

  ## Returns

  - When `streaming: false` (default): `{:ok, datasets}` where `datasets` is a list of Explorer.DataFrame.t()
  - When `streaming: true`: `{:ok, stream}` where `stream` is an Enumerable that yields rows progressively
  - On error: `{:error, reason}`

  ## Examples

      iex> ElixirDatasets.load_dataset({:hf, "cornell-movie-review-data/rotten_tomatoes"}, split: "train")

      iex> ElixirDatasets.load_dataset({:hf, "glue"}, name: "sst2")

      iex> ElixirDatasets.load_dataset({:hf, "glue"}, name: "sst2", split: "train")
      iex> {:ok, stream} = ElixirDatasets.load_dataset(
      ...>  {:hf, "cornell-movie-review-data/rotten_tomatoes"},
      ...>  split: "train",
      ...>  streaming: true
      ...> )

      ...> stream |> Stream.take(3) |> IO.inspect()

  """
  @spec load_dataset(t_repository(), keyword()) ::
          {:ok, [Explorer.DataFrame.t()] | Enumerable.t()} | {:error, Exception.t()}
  defdelegate load_dataset(repository, opts \\ []), to: Loader

  @doc """
  Similar to `load_dataset/2` but raises an error if loading fails.

  Delegates to `ElixirDatasets.Loader.load_dataset!/2`.

  Accepts the same options as `load_dataset/2`:
    * `:split` - which split to load (e.g., "train", "test", "validation")
    * `:name` - dataset configuration name
    * `:streaming` - if `true`, returns a Stream instead of loaded data

  ## Returns

    * a list of loaded datasets (or a Stream if streaming is enabled)
    * raises an error if loading fails

  ## Examples

      iex> datasets = ElixirDatasets.load_dataset!({:hf, "cornell-movie-review-data/rotten_tomatoes"}, split: "train")

      iex> stream = ElixirDatasets.load_dataset!({:hf, "cornell-movie-review-data/rotten_tomatoes"}, streaming: true)
      iex> stream |> Enum.take(10)

  """
  @spec load_dataset!(t_repository(), keyword()) ::
          [Explorer.DataFrame.t()] | Enumerable.t()
  defdelegate load_dataset!(repository, opts \\ []), to: Loader

  @doc """
  Uploads a dataset to Hugging Face Hub.

  ## Parameters

    * `df` - Explorer.DataFrame to upload
    * `repository` - repository ID (e.g., "username/dataset-name")
    * `file_extension` - keyword list with file extension option

  ## Returns

  `{:ok, response}` on success, or `{:error, reason}` on failure.
  """
  @spec upload_dataset(Explorer.DataFrame.t(), String.t(), keyword()) ::
          {:error, String.t()} | {:ok, binary()}
  def upload_dataset(df, repository, file_extension) do
    ElixirDatasets.Utils.Uploader.upload_dataset(df, repository, file_extension)
  end

  @doc """
  Returns the directory where downloaded files are stored.

  Defaults to the standard cache location for the given operating system.
  Can be configured with the `ELIXIR_DATASETS_CACHE_DIR` environment variable.

  ## Examples

      iex> is_binary(ElixirDatasets.cache_dir())
      true

      iex> String.ends_with?(ElixirDatasets.cache_dir(), "elixir_datasets")
      true

  """
  @spec cache_dir() :: String.t()
  def cache_dir() do
    if dir = System.get_env("ELIXIR_DATASETS_CACHE_DIR") do
      Path.expand(dir)
    else
      :filename.basedir(:user_cache, "elixir_datasets")
    end
  end
end

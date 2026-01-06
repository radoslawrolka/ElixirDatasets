# This file, part of the ElixirDatasets project, has been adapted from code originally under Apache License 2.0.
# The original code can be found at:
# https://github.com/elixir-nx/bumblebee/blob/710a645222948f80208c348d3a2589cbd3ab8e7d/lib/bumblebee.ex

defmodule ElixirDatasets do
  @moduledoc """
  Todo: Add documentation for ElixirDatasets.
  """
  @compile if Mix.env() == :test, do: :export_all
  alias ElixirDatasets.HuggingFace
  alias ElixirDatasets.DatasetInfo
  @valid_extensions_list ["jsonl", "csv", "parquet"]

  @typedoc """
  A location to fetch dataset files from.
  Can be either a Hugging Face repository or a local resources:

    * `{:hf, repository_id}` - the Hugging Face repository ID

    * `{:hf, repository_id, options}` - the Hugging Face repository ID
      with additional options

    * `{:local, path}` - a local directory or file path containing the datasets
  """
  @type t_repository :: {:hf, String.t()} | {:hf, String.t(), keyword()} | {:local, Path.t()}

  defp do_load_spec(repository, repo_files) do
    paths =
      Enum.reduce_while(repo_files, [], fn {file_name, etag}, acc ->
        extension = file_name |> Path.extname() |> String.trim_leading(".")

        if extension in @valid_extensions_list do
          case download(repository, file_name, etag) do
            {:ok, path} ->
              {:cont, [{path, extension} | acc]}

            {:error, reason} ->
              {:halt,
               {:error, "failed to download #{file_name} from #{inspect(repository)}: #{reason}"}}
          end
        else
          {:cont, acc}
        end
      end)

    case paths do
      {:error, _} = error -> error
      paths -> {:ok, Enum.reverse(paths)}
    end
  end

  defp decode_config(path) do
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

      iex> {:ok, infos} = ElixirDatasets.get_dataset_infos("aaaaa32r/elixirDatasets")
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

      iex> {:ok, splits} = ElixirDatasets.get_dataset_split_names("cornell-movie-review-data/rotten_tomatoes")
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

      iex> {:ok, configs} = ElixirDatasets.get_dataset_config_names("glue")
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

  @doc """
  Loads a dataset from the given repository.

  The repository can be either a local directory or a Hugging Face repository.

  ## Options

  ### Data Loading Options

    * `:split` - which split of the data to load (e.g., "train", "test", "validation").
      If not specified, all splits are loaded. Files are matched by name patterns
      (e.g., "train.csv", "test-00000.parquet", "validation.jsonl").

    * `:name` - the name of the dataset configuration to load. For datasets with
      multiple configurations, this specifies which one to use. Files are matched
      by looking for the config name in the file path (e.g., "sst2/train.parquet").

    * `:streaming` - if `true`, returns file paths for streaming instead of loading
      all data into memory. Useful for large datasets. Default is `false`.

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
      - `:force_redownload_and_prepare` - redownload and reprocess

    * `:verification_mode` - controls verification checks. Can be:
      - `:basic_checks` (default) - basic validation
      - `:all_checks` - comprehensive validation
      - `:no_checks` - skip all validation

    * `:storage_options` - key/value pairs for cloud storage backends
      (e.g., AWS S3, Google Cloud Storage).

  ## Returns

  An `{:ok, datasets}` tuple, where `datasets` is a list of Explorer.DataFrame.t().
  If the dataset cannot be loaded, an `{:error, reason}` tuple is returned.

  ## Examples

      # Load only the training split
      ElixirDatasets.load_dataset({:hf, "dataset_name"}, split: "train")

      # Load a specific configuration
      ElixirDatasets.load_dataset({:hf, "glue"}, name: "sst2")

      # Load a specific split of a specific configuration
      ElixirDatasets.load_dataset({:hf, "glue"}, name: "sst2", split: "train")

  """
  @spec load_dataset(t_repository(), keyword()) ::
          {:ok, [Explorer.DataFrame.t()]} | {:error, Exception.t()}
  def load_dataset(repository, opts \\ []) do
    repository = normalize_repository!(repository)
    split = opts[:split]
    name = opts[:name]
    streaming = opts[:streaming] || false

    with {:ok, repo_files} <- get_repo_files(repository),
         {:ok, filtered_files} <- filter_files_by_config_and_split(repo_files, name, split),
         {:ok, paths_with_extensions} <- maybe_load_model_spec(opts, repository, filtered_files) do
      if streaming do
        {:ok, paths_with_extensions}
      else
        ElixirDatasets.Utils.Loader.load_datasets_from_paths(paths_with_extensions)
      end
    end
  end

  @doc """
  Similar to `load_dataset/2` but raises an error if loading fails.

  Accepts the same options as `load_dataset/2`:
    * `:split` - which split to load (e.g., "train", "test", "validation")
    * `:name` - dataset configuration name
    * `:streaming` - if `true`, returns file paths instead of loaded data

  ## Returns

    * a list of loaded datasets (or file paths if streaming is enabled)
    * raises an error if loading fails

  ## Examples

      # Load only training data
      datasets = ElixirDatasets.load_dataset!({:hf, "dataset_name"}, split: "train")

  """
  @spec load_dataset!(t_repository(), keyword()) ::
          [Explorer.DataFrame.t()] | [{Path.t(), String.t()}]
  def load_dataset!(repository, opts \\ []) do
    case load_dataset(repository, opts) do
      {:ok, datasets} -> datasets
      {:error, reason} -> raise reason
    end
  end

  @spec upload_dataset(Explorer.DataFrame.t(), String.t(), keyword()) ::
          {:error, String.t()} | {:ok, binary()}
  def upload_dataset(df, repository, file_extension) do
    ElixirDatasets.Utils.Uploader.upload_dataset(df, repository, file_extension)
  end

  defp filter_files_by_config_and_split(repo_files, name, split) do
    filtered =
      repo_files
      |> filter_by_config_name(name)
      |> filter_by_split(split)

    {:ok, filtered}
  end

  defp filter_by_config_name(repo_files, nil), do: repo_files

  defp filter_by_config_name(repo_files, config_name) do
    Enum.filter(repo_files, fn {file_name, _etag} ->
      String.contains?(file_name, config_name)
    end)
    |> Map.new()
  end

  defp filter_by_split(repo_files, nil), do: repo_files

  defp filter_by_split(repo_files, split) when is_binary(split) do
    Enum.filter(repo_files, fn {file_name, _etag} ->
      base_name = Path.basename(file_name, Path.extname(file_name))
      String.contains?(base_name, split)
    end)
    |> Map.new()
  end

  defp maybe_load_model_spec(_opts, repository, repo_files) do
    with {:ok, spec} <- do_load_spec(repository, repo_files) do
      {:ok, spec}
    end
  end

  defp get_repo_files({:local, dir}) do
    case File.ls(dir) do
      {:ok, filenames} ->
        repo_files =
          for filename <- filenames,
              path = Path.join(dir, filename),
              File.regular?(path),
              into: %{},
              do: {filename, nil}

        {:ok, repo_files}

      {:error, reason} ->
        {:error, "could not read #{dir}, reason: #{:file.format_error(reason)}"}
    end
  end

  defp get_repo_files({:hf, repository_id, opts}) do
    subdir = opts[:subdir]
    url = HuggingFace.Hub.file_listing_url(repository_id, subdir, opts[:revision])
    cache_scope = repository_id_to_cache_scope(repository_id)

    passthrough_opts = [
      :cache_dir,
      :offline,
      :auth_token,
      :etag,
      :download_mode,
      :verification_mode,
      :storage_options
    ]

    result =
      HuggingFace.Hub.cached_download(
        url,
        [cache_scope: cache_scope] ++ Keyword.take(opts, passthrough_opts)
      )

    with {:ok, path} <- result,
         {:ok, data} <- decode_config(path) do
      repo_files =
        for entry <- data, entry["type"] == "file", into: %{} do
          path = entry["path"]

          name =
            if subdir do
              String.replace_leading(path, subdir <> "/", "")
            else
              path
            end

          etag_content = entry["lfs"]["oid"] || entry["oid"]
          etag = <<?", etag_content::binary, ?">>
          {name, etag}
        end

      {:ok, repo_files}
    end
  end

  defp download({:local, dir}, filename, _etag) do
    path = Path.join(dir, filename)

    if File.exists?(path) do
      {:ok, path}
    else
      {:error, "local file #{inspect(path)} does not exist"}
    end
  end

  defp download({:hf, repository_id, opts}, filename, etag) do
    filename =
      if subdir = opts[:subdir] do
        subdir <> "/" <> filename
      else
        filename
      end

    url = HuggingFace.Hub.file_url(repository_id, filename, opts[:revision])
    cache_scope = repository_id_to_cache_scope(repository_id)

    passthrough_opts = [
      :cache_dir,
      :offline,
      :auth_token,
      :download_mode,
      :verification_mode,
      :storage_options
    ]

    HuggingFace.Hub.cached_download(
      url,
      [etag: etag, cache_scope: cache_scope] ++
        Keyword.take(opts, passthrough_opts)
    )
  end

  defp repository_id_to_cache_scope(repository_id) do
    repository_id
    |> String.replace("/", "--")
    |> String.replace(~r/[^\w-]/, "")
  end

  defp normalize_repository!({:hf, repository_id}) when is_binary(repository_id) do
    {:hf, repository_id, []}
  end

  defp normalize_repository!({:hf, repository_id, opts}) when is_binary(repository_id) do
    opts = Keyword.validate!(opts, [:revision, :cache_dir, :offline, :auth_token, :subdir])
    {:hf, repository_id, opts}
  end

  defp normalize_repository!({:local, dir}) when is_binary(dir) do
    {:local, dir}
  end

  defp normalize_repository!(other) do
    raise ArgumentError,
          "expected repository to be either {:hf, repository_id}, {:hf, repository_id, options}" <>
            " or {:local, directory}, got: #{inspect(other)}"
  end

  @doc """
  Returns the directory where downloaded files are stored.

  Defaults to the standard cache location for the given operating system.
  Can be configured with the `ELIXIR_DATASETS_CACHE_DIR` environment variable.
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

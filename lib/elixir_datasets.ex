# This file, part of the ElixirDatasets project, has been adapted from code originally under Apache License 2.0.
# The original code can be found at:
# https://github.com/elixir-nx/bumblebee/blob/710a645222948f80208c348d3a2589cbd3ab8e7d/lib/bumblebee.ex

defmodule ElixirDatasets do
  @moduledoc """
  Todo: Add documentation for ElixirDatasets.
  """
  @compile if Mix.env() == :test, do: :export_all
  alias ElixirDatasets.HuggingFace
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
      case opts[:auth_token] || System.get_env("HF_TOKEN") do
        nil -> []
        auth_token -> [{"Authorization", "Bearer #{auth_token}"}]
      end

    with {:ok, response} <- ElixirDatasets.Utils.HTTP.request(:get, url, headers: headers),
         {:ok, data} <- Jason.decode(response.body) do
      {:ok, data}
    end
  end

  @doc """
  Loads a dataset from the given repository.

  The repository can be either a local directory or a Hugging Face repository.

  ## Options

    * `:auth_token` - the token to use as HTTP bearer authorization
      for remote files. If not provided, the token from the
      `ELIXIR_DATASETS_HF_TOKEN` environment variable is used.

  ## Returns

  An `{:ok, %{dataset: paths}}` tuple, where `paths` is a list of
  paths to the downloaded dataset files. If the dataset cannot be
  loaded, an `{:error, reason}` tuple is returned.
  If the dataset is not found, an error is raised.

  ## Examples

      todo
  """
  @spec load_dataset(t_repository(), keyword()) ::
          {:ok, [Explorer.DataFrame.t()]} | {:error, Exception.t()}
  def load_dataset(repository, opts \\ []) do
    repository = normalize_repository!(repository)

    with {:ok, repo_files} <- get_repo_files(repository),
         {:ok, paths_with_extensions} <- maybe_load_model_spec(opts, repository, repo_files) do
      ElixirDatasets.Utils.Loader.load_datasets_from_paths(paths_with_extensions)
    end
  end

  @doc """
  Similar to `load_dataset/2` but raises an error if loading fails.

  ## Returns

    * a list of loaded datasets
    * raises an error if loading fails
  """
  @spec load_dataset!(t_repository(), keyword()) :: [Explorer.DataFrame.t()]
  def load_dataset!(repository, opts \\ []) do
    case load_dataset(repository, opts) do
      {:ok, datasets} -> datasets
      {:error, reason} -> raise reason
    end
  end

  def upload_dataset(df, repository, file_extension) do
    ElixirDatasets.Utils.Uploader.upload_dataset(df, repository, file_extension)
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

    result =
      HuggingFace.Hub.cached_download(
        url,
        [cache_scope: cache_scope] ++ Keyword.take(opts, [:cache_dir, :offline, :auth_token])
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

    HuggingFace.Hub.cached_download(
      url,
      [etag: etag, cache_scope: cache_scope] ++
        Keyword.take(opts, [:cache_dir, :offline, :auth_token])
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

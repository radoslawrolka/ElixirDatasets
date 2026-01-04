# This file, part of the ElixirDatasets project, has been adapted from code originally under Apache License 2.0.
# The original code can be found at:
# https://github.com/elixir-nx/bumblebee/blob/710a645222948f80208c348d3a2589cbd3ab8e7d/lib/bumblebee.ex

defmodule ElixirDatasets do
  @moduledoc """
  Todo: Add documentation for ElixirDatasets.
  """
  @compile if Mix.env() == :test, do: :export_all
  alias ElixirDatasets.HuggingFace
  @valid_extensions ["json", "csv", "txt", "parquet"]

  @typedoc """
  A location to fetch model files from.

  Can be either:

    * `{:hf, repository_id}` - the repository on Hugging Face. Options
      may be passed as the third element:

        * `:revision` - the specific model version to use, it can be
          any valid git identifier, such as branch name, tag name, or
          a commit hash

        * `:cache_dir` - the directory to store the downloaded files
          in. Defaults to the standard cache location for the given
          operating system. You can also configure it globally by
          setting the `ELIXIR_DATASETS_CACHE_DIR` environment variable

        * `:offline` - if `true`, only cached files are accessed and
          missing files result in an error. You can also configure it
          globally by setting the `ELIXIR_DATASETS_OFFLINE` environment
          variable to `true`

        * `:auth_token` - the token to use as HTTP bearer authorization
          for remote files

        * `:subdir` - the directory within the repository where the
          files are located

    * `{:local, directory}` - the directory containing model files

  """
  @type repository :: {:hf, String.t()} | {:hf, String.t(), keyword()} | {:local, Path.t()}

  defp do_load_spec(repository, repo_files) do
    case repo_files do
      %{} ->
        paths =
          Enum.reduce(repo_files, [], fn {file_name, etag}, acc ->
            extension = file_name |> String.split(".") |> List.last()

            if extension in @valid_extensions do
              case download(repository, file_name, etag) do
                {:ok, path} ->
                  path =
                    if String.downcase(Path.extname(path)) ==
                         ".eizwkyjyhe2gkmzzgrrwimrugqytgyrxhaytoojqgy4dgojsgrqtenjzha2tan3ege2dmzdbhfrdiylegbqteyjqge4dgmddg43wembqei" do
                      convert_parquet_to_csv(path)
                    else
                      path
                    end

                  [path | acc]

                {:error, reason} ->
                  raise ArgumentError, """
                  failed to download #{file_name} from #{inspect(repository)}: #{reason}
                  """
              end
            else
              acc
            end
          end)

        {:ok, paths}
    end
  end

  defp convert_parquet_to_csv(parquet_path) do
    try do
      df = Explorer.DataFrame.from_parquet!(parquet_path)

      csv_path =
        parquet_path
        |> Path.rootname(".parquet")
        |> Kernel.<>(".csv")

      :ok = Explorer.DataFrame.to_csv(df, csv_path, header: true)
      csv_path
    rescue
      e ->
        IO.warn("Failed to convert #{parquet_path} to CSV: #{Exception.message(e)}")
        parquet_path
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

  ## Examples

      # Example: ElixirDatasets.get_dataset_info("aaaaa32r/elixirDatasets")
      # {:ok, %{"id" => "aaaaa32r/elixirDatasets", "author" => "aaaaa32r", ...}}

      # Example: ElixirDatasets.get_dataset_info("aaaaa32r/elixirDatasets", auth_token: "hf_...")
      # {:ok, %{"id" => "aaaaa32r/elixirDatasets", "author" => "aaaaa32r", ...}}

  """
  @spec get_dataset_info(String.t(), keyword()) :: {:ok, map()} | {:error, String.t()}
  def get_dataset_info(repository_id, opts \\ []) when is_binary(repository_id) do
    url = HuggingFace.Hub.dataset_info_url(repository_id)

    headers =
      if auth_token = System.get_env("HF_TOKEN") do
        [{"Authorization", "Bearer #{auth_token}"}]
      else
        []
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
  @spec load_dataset(repository(), keyword()) ::
          {:ok, %{dataset: [Path.t()]}} | {:error, String.t()}
  def load_dataset(repository, opts \\ []) do
    repository = normalize_repository!(repository)

    with {:ok, repo_files} <- get_repo_files(repository),
         {:ok, paths} <- maybe_load_model_spec(opts, repository, repo_files) do
      {:ok, %{dataset: paths}}
    end
  end

  defp maybe_load_model_spec(opts, repository, repo_files) do
    spec_result =
      if spec = opts[:spec] do
        {:ok, spec}
      else
        do_load_spec(repository, repo_files)
      end

    with {:ok, spec} <- spec_result do
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

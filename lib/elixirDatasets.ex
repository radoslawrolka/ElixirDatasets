# This file, part of the ElixirDatasets project, has been adapted from code originally under Apache License 2.0.
# The original code can be found at:
# https://github.com/elixir-nx/bumblebee/blob/710a645222948f80208c348d3a2589cbd3ab8e7d/lib/bumblebee.ex

defmodule ElixirDatasets do
  @moduledoc """
  Todo: Add documentation for ElixirDatasets.
  """

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
          setting the `ELIXIRDATASETS_CACHE_DIR` environment variable

        * `:offline` - if `true`, only cached files are accessed and
          missing files result in an error. You can also configure it
          globally by setting the `ELIXIRDATASETS_OFFLINE` environment
          variable to `true`

        * `:auth_token` - the token to use as HTTP bearer authorization
          for remote files

        * `:subdir` - the directory within the repository where the
          files are located

    * `{:local, directory}` - the directory containing model files

  """
  @type repository :: {:hf, String.t()} | {:hf, String.t(), keyword()} | {:local, Path.t()}

  defp do_load_spec(repository, repo_files, _moduleREMOVE, _architectureREMOVE) do
    case repo_files do
      %{} ->
        paths =
          Enum.reduce(repo_files, [], fn {file_name, etag}, acc ->
            extension = file_name |> String.split(".") |> List.last()

            if extension in @valid_extensions do
              case download(repository, file_name, etag) do
                {:ok, path} ->
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

  defp decode_config(path) do
    path
    |> File.read!()
    |> Jason.decode()
    |> case do
      {:ok, data} -> {:ok, data}
      _ -> {:error, "failed to parse the config file, it is not a valid JSON"}
    end
  end

  def load_dataset(repository, opt \\ []) do
    repository = normalize_repository!(repository)
    {:hf, ri, opts} = repository
    new_opts = Keyword.put(opts, :auth_token, opt[:auth_token])

    with {:ok, repo_files} <- get_repo_files({:hf, ri, new_opts}),
         {:ok, paths} <- maybe_load_model_spec(new_opts, repository, repo_files) do
      {:ok, %{dataset: paths}}
    end
  end

  defp maybe_load_model_spec(opts, repository, repo_files) do
    spec_result =
      if spec = opts[:spec] do
        {:ok, spec}
      else
        do_load_spec(repository, repo_files, opts[:module], opts[:architecture])
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
  Can be configured with the `ELIXIRDATASETS_CACHE_DIR` environment variable.
  """
  @spec cache_dir() :: String.t()
  def cache_dir() do
    if dir = System.get_env("ELIXIRDATASETS_CACHE_DIR") do
      Path.expand(dir)
    else
      :filename.basedir(:user_cache, "elixirDatasets")
    end
  end
end

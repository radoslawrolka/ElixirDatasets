defmodule ElixirDatasets.Repository do
  @moduledoc """
  Functions for managing dataset repositories (local and Hugging Face).
  """

  alias ElixirDatasets.HuggingFace

  @typedoc """
  A location to fetch dataset files from.
  Can be either a Hugging Face repository or a local resources:

    * `{:hf, repository_id}` - the Hugging Face repository ID

    * `{:hf, repository_id, options}` - the Hugging Face repository ID
      with additional options

    * `{:local, path}` - a local directory or file path containing the datasets
  """
  @type t_repository :: {:hf, String.t()} | {:hf, String.t(), keyword()} | {:local, Path.t()}

  @doc """
  Normalizes repository specification to a consistent format.

  ## Examples

      iex> ElixirDatasets.Repository.normalize!({:hf, "repo/name"})
      {:hf, "repo/name", []}

      iex> ElixirDatasets.Repository.normalize!({:local, "/path/to/data"})
      {:local, "/path/to/data"}
  """
  @spec normalize!(t_repository()) :: t_repository()
  def normalize!({:hf, repository_id}) when is_binary(repository_id) do
    {:hf, repository_id, []}
  end

  def normalize!({:hf, repository_id, opts}) when is_binary(repository_id) do
    opts = Keyword.validate!(opts, [:revision, :cache_dir, :offline, :auth_token, :subdir])
    {:hf, repository_id, opts}
  end

  def normalize!({:local, dir}) when is_binary(dir) do
    {:local, dir}
  end

  def normalize!(other) do
    raise ArgumentError,
          "expected repository to be either {:hf, repository_id}, {:hf, repository_id, options}" <>
            " or {:local, directory}, got: #{inspect(other)}"
  end

  @doc """
  Gets the list of files in a repository.

  For local repositories, lists files in the directory.
  For Hugging Face repositories, fetches the file listing from the API.

  ## Returns

  `{:ok, repo_files}` where `repo_files` is a map of `%{filename => etag}`,
  or `{:error, reason}` if the operation fails.
  """
  @spec get_files(t_repository()) :: {:ok, map()} | {:error, String.t()}
  def get_files({:local, dir}) do
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

  def get_files({:hf, repository_id, opts}) do
    subdir = opts[:subdir]
    url = HuggingFace.Hub.file_listing_url(repository_id, subdir, opts[:revision])
    cache_scope = repository_id_to_cache_scope(repository_id)

    passthrough_opts = [
      :cache_dir,
      :offline,
      :auth_token,
      :etag,
      :download_mode,
      :verification_mode
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

  @doc """
  Downloads a file from a repository.

  For local repositories, verifies the file exists.
  For Hugging Face repositories, downloads the file using the Hub API.

  ## Returns

  `{:ok, path}` where `path` is the local file path,
  or `{:error, reason}` if the download fails.
  """
  @spec download(t_repository(), String.t(), String.t() | nil) ::
          {:ok, String.t()} | {:error, String.t()}
  def download({:local, dir}, filename, _etag) do
    path = Path.join(dir, filename)

    if File.exists?(path) do
      {:ok, path}
    else
      {:error, "local file #{inspect(path)} does not exist"}
    end
  end

  def download({:hf, repository_id, opts}, filename, etag) do
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
      :verification_mode
    ]

    HuggingFace.Hub.cached_download(
      url,
      [etag: etag, cache_scope: cache_scope] ++
        Keyword.take(opts, passthrough_opts)
    )
  end

  @doc """
  Converts a repository ID to a cache scope string.

  Replaces slashes with double dashes and removes non-word characters.

  ## Examples

      iex> ElixirDatasets.Repository.repository_id_to_cache_scope("user/repo-name")
      "user--repo-name"
  """
  @spec repository_id_to_cache_scope(String.t()) :: String.t()
  def repository_id_to_cache_scope(repository_id) do
    repository_id
    |> String.replace("/", "--")
    |> String.replace(~r/[^\w-]/, "")
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
end

# This file, part of the ElixirDatasets project, has been adapted from code originally under Apache License 2.0.
# The original code can be found at:
# https://github.com/elixir-nx/bumblebee/blob/710a645222948f80208c348d3a2589cbd3ab8e7d/lib/bumblebee/huggingface/hub.ex

defmodule ElixirDatasets.HuggingFace.Hub do
  @moduledoc false
  @compile if Mix.env() == :test, do: :export_all
  alias ElixirDatasets.Utils.HTTP

  @huggingface_endpoint "https://huggingface.co"

  @doc """
  Returns a URL pointing to the given file in a Hugging Face repository.
  """
  @spec file_url(String.t(), String.t(), String.t() | nil) :: String.t()
  def file_url(repository_id, filename, revision) do
    revision = revision || "main"
    @huggingface_endpoint <> "/datasets/#{repository_id}/resolve/#{revision}/#{filename}"
  end

  @doc """
  Returns a URL to list the contents of a Hugging Face repository.
  """
  @spec file_listing_url(String.t(), String.t() | nil, String.t() | nil) :: String.t()
  def file_listing_url(repository_id, subdir, revision) do
    revision = revision || "main"
    path = if(subdir, do: "/" <> subdir)
    @huggingface_endpoint <> "/api/datasets/#{repository_id}/tree/#{revision}#{path}"
  end

  @doc """
  Returns a URL to fetch dataset information from the Hugging Face API.
  """
  @spec dataset_info_url(String.t()) :: String.t()
  def dataset_info_url(repository_id) do
    @huggingface_endpoint <> "/api/datasets/#{repository_id}"
  end

  @doc """
  Downloads file from the given URL and returns a path to the file.

  The file is cached based on the received ETag. Subsequent requests
  for the same URL validate the ETag and return a file from the cache
  if there is a match.

  ## Options

    * `:cache_dir` - the directory to store the downloaded files in.
      Defaults to the standard cache location for the given operating
      system

    * `:offline` - if `true`, cached path is returned if exists and
      and error otherwise

    * `:auth_token` - the token to use as HTTP bearer authorization
      for remote files

    * `:etag` - by default a HEAD request is made to fetch the latest
      ETag value, however if the value is already known, it can be
      passed as an option instead (to skip the extra request)

    * `:cache_scope` - a namespace to put the cached files under in
      the cache directory

    * `:download_mode` - controls download/cache behavior. Can be:
      - `:reuse_dataset_if_exists` (default) - reuse cached data if available
      - `:force_redownload` - always download, even if cached

    * `:verification_mode` - controls verification checks. Can be:
      - `:basic_checks` (default) - basic validation
      - `:all_checks` - comprehensive validation
      - `:no_checks` - skip all validation

    * `:storage_options` - key/value pairs for cloud storage backends.
      Currently not implemented but reserved for future use.

  """
  @spec cached_download(String.t(), keyword()) :: {:ok, String.t()} | {:error, String.t()}
  def cached_download(url, opts \\ []) do
    cache_dir = opts[:cache_dir] || ElixirDatasets.cache_dir()
    offline = Keyword.get(opts, :offline, elixir_datasets_offline?())
    auth_token = opts[:auth_token]
    download_mode = opts[:download_mode] || :reuse_dataset_if_exists
    verification_mode = opts[:verification_mode] || :basic_checks

    dir = Path.join(cache_dir, "huggingface")

    dir =
      if cache_scope = opts[:cache_scope] do
        Path.join(dir, cache_scope)
      else
        dir
      end

    File.mkdir_p!(dir)

    headers =
      if auth_token do
        [{"Authorization", "Bearer " <> auth_token}]
      else
        []
      end

    metadata_path = Path.join(dir, metadata_filename(url))

    # Handle force_redownload mode - delete cached files
    if download_mode == :force_redownload do
      File.rm(metadata_path)
    end

    cond do
      offline ->
        case load_json(metadata_path) do
          {:ok, %{"etag" => etag}} ->
            entry_path = Path.join(dir, entry_filename(url, etag))

            # Verify file exists unless verification is disabled
            if verification_mode == :no_checks or File.exists?(entry_path) do
              {:ok, entry_path}
            else
              {:error, "cached file not found: #{entry_path}"}
            end

          _ ->
            {:error,
             "could not find file in local cache and outgoing traffic is disabled, url: #{url}"}
        end

      entry_path = opts[:etag] && cached_path_for_etag(dir, url, opts[:etag]) ->
        {:ok, entry_path}

      true ->
        with {:ok, etag, download_url, redirect?} <- head_download(url, headers) do
          # Check if we should reuse cached file (unless force_redownload)
          cached_entry =
            if download_mode != :force_redownload, do: cached_path_for_etag(dir, url, etag)

          if cached_entry do
            {:ok, cached_entry}
          else
            entry_path = Path.join(dir, entry_filename(url, etag))

            headers =
              if redirect? do
                List.keydelete(headers, "Authorization", 0)
              else
                headers
              end

            download_url
            |> HTTP.download(entry_path, headers: headers)
            |> finish_request(download_url)
            |> case do
              :ok ->
                :ok = store_json(metadata_path, %{"etag" => etag, "url" => url})
                {:ok, entry_path}

              error ->
                File.rm_rf!(metadata_path)
                File.rm_rf!(entry_path)
                error
            end
          end
        end
    end
  end

  defp cached_path_for_etag(dir, url, etag) do
    metadata_path = Path.join(dir, metadata_filename(url))

    case load_json(metadata_path) do
      {:ok, %{"etag" => ^etag}} ->
        path = Path.join(dir, entry_filename(url, etag))

        # Make sure the file exists, in case someone manually removed it
        if File.exists?(path) do
          path
        end

      _ ->
        nil
    end
  end

  defp head_download(url, headers) do
    with {:ok, response} <-
           HTTP.request(:head, url, follow_redirects: false, headers: headers)
           |> finish_request(url) do
      if response.status in 300..399 do
        location = HTTP.get_header(response, "location")

        # Follow relative redirects
        if URI.parse(location).host == nil do
          url =
            url
            |> URI.parse()
            |> Map.replace!(:path, location)
            |> URI.to_string()

          head_download(url, headers)
        else
          with {:ok, etag} <- fetch_etag(response), do: {:ok, etag, location, true}
        end
      else
        with {:ok, etag} <- fetch_etag(response), do: {:ok, etag, url, false}
      end
    end
  end

  defp finish_request(:ok, _url), do: :ok

  defp finish_request({:ok, response}, _url) when response.status in 100..399, do: {:ok, response}

  defp finish_request({:ok, response}, url) do
    case HTTP.get_header(response, "x-error-code") do
      code when code == "RepoNotFound" or response.status == 401 ->
        {:error,
         "repository not found, url: #{url}. Please make sure you specified" <>
           " the correct repository id. If you are trying to access a private" <>
           " or gated repository, use an authentication token"}

      "EntryNotFound" ->
        {:error, "file not found, url: #{url}"}

      "RevisionNotFound" ->
        {:error, "revision not found, url: #{url}"}

      "GatedRepo" ->
        {:error,
         "cannot access gated repository, url: #{url}. Make sure to request access" <>
           " for the repository and use an authentication token"}

      _ ->
        {:error, "HTTP request failed with status #{response.status}, url: #{url}"}
    end
  end

  defp finish_request({:error, reason}, _url) do
    {:error, "failed to make an HTTP request, reason: #{inspect(reason)}"}
  end

  defp fetch_etag(response) do
    if etag = HTTP.get_header(response, "x-linked-etag") || HTTP.get_header(response, "etag") do
      {:ok, etag}
    else
      {:error, "no ETag found on the resource"}
    end
  end

  @doc """
  Gets the HuggingFace authentication token. Requires that it starts with "hf_".

  Looks for the token in the following order:
  1. From options (`:auth_token` key)
  2. From system environment variable (`HF_TOKEN`)
  3. Returns error if not found

  ## Parameters

    * `opts` - keyword list with optional `:auth_token` key

  ## Returns

    * `{:ok, String.t()}` - the authentication token
    * `{:error, String.t()}` - if no token is found or invalid

  ## Examples

      iex> ElixirDatasets.HuggingFace.Hub.get_auth_token(auth_token: "hf_my_token")
      {:ok, "hf_my_token"}

      iex> ElixirDatasets.HuggingFace.Hub.get_auth_token(auth_token: "my_invalid_token")
      {:error, "The provided Hugging Face authentication token does not start with 'hf_'."}

      # iex> ElixirDatasets.HuggingFace.Hub.get_auth_token([])
      # the value of HF_TOKEN environment variable if valid else error
  """
  @spec get_auth_token(keyword()) :: {:ok, String.t()} | {:error, String.t()}
  def get_auth_token(opts \\ []) do
    token = opts[:auth_token] || System.get_env("HF_TOKEN")
    validate_auth_token(token)
  end

  @spec validate_auth_token(String.t() | nil) :: {:ok, String.t()} | {:error, String.t()}
  defp validate_auth_token(token) when is_binary(token) do
    cond do
      String.starts_with?(token, "hf_") ->
        {:ok, token}

      true ->
        {:error, "The provided Hugging Face authentication token does not start with 'hf_'."}
    end
  end

  defp validate_auth_token(_), do: {:error, "No Hugging Face authentication token provided."}

  defp metadata_filename(url) do
    encode_url(url) <> ".json"
  end

  defp entry_filename(url, etag) do
    encode_url(url) <> "." <> encode_etag(etag)
  end

  defp encode_url(url) do
    url |> :erlang.md5() |> Base.encode32(case: :lower, padding: false)
  end

  defp encode_etag(etag) do
    Base.encode32(etag, case: :lower, padding: false)
  end

  defp load_json(path) do
    case File.read(path) do
      {:ok, content} -> {:ok, Jason.decode!(content)}
      _error -> :error
    end
  end

  defp store_json(path, data) do
    json = Jason.encode!(data)
    File.write(path, json)
  end

  defp elixir_datasets_offline?() do
    System.get_env("ELIXIR_DATASETS_OFFLINE") in ~w(1 true)
  end
end

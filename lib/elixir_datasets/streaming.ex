defmodule ElixirDatasets.Streaming do
  @moduledoc """
  Functions for streaming datasets progressively without loading everything into memory.
  """

  alias ElixirDatasets.HuggingFace

  @doc """
  Builds a streaming dataset that yields rows progressively.

  ## Parameters

    * `repository` - normalized repository tuple
    * `filtered_files` - map of files to stream from
    * `opts` - options including:
      * `:batch_size` - number of rows to fetch per batch (default: 1000)
      * `:auth_token` - authentication token for Hugging Face

  ## Returns

  A Stream that yields rows as maps.
  """
  @spec build(tuple(), map(), keyword()) :: Enumerable.t()
  def build(repository, filtered_files, opts) do
    batch_size = opts[:batch_size] || 1000

    urls = build_urls(repository, filtered_files, opts)

    Stream.resource(
      fn -> init_state(urls, batch_size) end,
      &fetch_next_batch/1,
      &cleanup/1
    )
  end

  @doc """
  Builds URLs for streaming from repository files.

  For Hugging Face repositories, creates HTTP URLs.
  For local repositories, uses file paths.
  """
  @spec build_urls(tuple(), map(), keyword()) :: list()
  def build_urls({:hf, repository_id, repo_opts}, filtered_files, load_opts) do
    auth_token = load_opts[:auth_token]

    Enum.map(filtered_files, fn {file_name, _etag} ->
      filename =
        if subdir = repo_opts[:subdir] do
          subdir <> "/" <> file_name
        else
          file_name
        end

      extension = file_name |> Path.extname() |> String.trim_leading(".")
      url = HuggingFace.Hub.file_url(repository_id, filename, repo_opts[:revision])

      {url, extension, auth_token}
    end)
  end

  def build_urls({:local, dir}, filtered_files, _opts) do
    Enum.map(filtered_files, fn {file_name, _etag} ->
      path = Path.join(dir, file_name)
      extension = file_name |> Path.extname() |> String.trim_leading(".")
      {path, extension, nil}
    end)
  end

  defp init_state(urls, batch_size) do
    %{
      urls: urls,
      current_url_index: 0,
      current_lazy_df: nil,
      current_offset: 0,
      batch_size: batch_size,
      total_urls: length(urls)
    }
  end

  defp fetch_next_batch(%{current_url_index: idx, total_urls: total} = state)
       when idx >= total do
    {:halt, state}
  end

  defp fetch_next_batch(state) do
    case ensure_lazy_df_loaded(state) do
      {:ok, state_with_df} ->
        fetch_batch_from_lazy_df(state_with_df)

      {:error, _reason} ->
        new_state = %{state | current_url_index: state.current_url_index + 1, current_offset: 0}
        fetch_next_batch(new_state)
    end
  end

  defp ensure_lazy_df_loaded(%{current_lazy_df: nil} = state) do
    {url, extension, auth_token} = Enum.at(state.urls, state.current_url_index)

    case load_lazy_dataframe(url, extension, auth_token) do
      {:ok, lazy_df} ->
        {:ok, %{state | current_lazy_df: lazy_df}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp ensure_lazy_df_loaded(state), do: {:ok, state}

  defp load_lazy_dataframe(url_or_path, extension, _auth_token) do
    is_url =
      String.starts_with?(url_or_path, "http://") or String.starts_with?(url_or_path, "https://")

    case {extension, is_url} do
      {"parquet", true} ->
        Explorer.DataFrame.from_parquet(url_or_path, lazy: true)

      {"parquet", false} ->
        Explorer.DataFrame.from_parquet(url_or_path, lazy: true)

      {"csv", false} ->
        Explorer.DataFrame.from_csv(url_or_path, lazy: true)

      {"jsonl", false} ->
        Explorer.DataFrame.from_ndjson(url_or_path, lazy: true)

      {"csv", true} ->
        case Explorer.DataFrame.from_csv(url_or_path) do
          {:ok, df} -> {:ok, df}
          error -> error
        end

      {"jsonl", true} ->
        case Explorer.DataFrame.from_ndjson(url_or_path) do
          {:ok, df} -> {:ok, df}
          error -> error
        end

      _ ->
        {:error, "Unsupported format for streaming: #{extension}"}
    end
  end

  defp fetch_batch_from_lazy_df(state) do
    %{current_lazy_df: df, current_offset: offset, batch_size: batch_size} = state

    batch_df =
      df
      |> Explorer.DataFrame.slice(offset, batch_size)
      |> then(fn sliced ->
        if Explorer.DataFrame.lazy?(sliced) do
          Explorer.DataFrame.collect(sliced)
        else
          sliced
        end
      end)

    batch_rows = Explorer.DataFrame.to_rows(batch_df)
    num_rows = length(batch_rows)

    cond do
      num_rows == 0 ->
        new_state = %{
          state
          | current_url_index: state.current_url_index + 1,
            current_lazy_df: nil,
            current_offset: 0
        }

        fetch_next_batch(new_state)

      num_rows < batch_size ->
        new_state = %{
          state
          | current_url_index: state.current_url_index + 1,
            current_lazy_df: nil,
            current_offset: 0
        }

        {batch_rows, new_state}

      true ->
        new_state = %{state | current_offset: offset + batch_size}
        {batch_rows, new_state}
    end
  end

  defp cleanup(_state), do: :ok
end


defmodule ElixirDatasets.Loader do
  @moduledoc """
  Functions for loading datasets from repositories.
  """

  alias ElixirDatasets.Repository
  alias ElixirDatasets.Filter
  alias ElixirDatasets.Streaming

  @valid_extensions_list ["jsonl", "csv", "parquet"]

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

      ElixirDatasets.Loader.load_dataset({:hf, "dataset_name"}, split: "train")

      ElixirDatasets.Loader.load_dataset({:hf, "glue"}, name: "sst2")

      {:ok, stream} = ElixirDatasets.Loader.load_dataset(
        {:hf, "large_dataset"},
        split: "train",
        streaming: true
      )

      stream |> Stream.take(100) |> Enum.each(&process_row/1)

  """
  @spec load_dataset(Repository.t_repository(), keyword()) ::
          {:ok, [Explorer.DataFrame.t()] | Enumerable.t()} | {:error, Exception.t()}
  def load_dataset(repository, opts \\ []) do
    repository = Repository.normalize!(repository)
    split = opts[:split]
    name = opts[:name]
    streaming = opts[:streaming] || false
    num_proc = opts[:num_proc] || 1

    with {:ok, repo_files} <- Repository.get_files(repository),
         {:ok, filtered_files} <- Filter.by_config_and_split(repo_files, name, split) do
      if streaming do
        {:ok, Streaming.build(repository, filtered_files, opts)}
      else
        with {:ok, paths_with_extensions} <- load_spec(repository, filtered_files, num_proc) do
          ElixirDatasets.Utils.Loader.load_datasets_from_paths(paths_with_extensions, num_proc)
        end
      end
    end
  end

  @doc """
  Similar to `load_dataset/2` but raises an error if loading fails.

  Accepts the same options as `load_dataset/2`.

  ## Returns

    * a list of loaded datasets (or a Stream if streaming is enabled)
    * raises an error if loading fails

  ## Examples

      datasets = ElixirDatasets.Loader.load_dataset!({:hf, "dataset_name"}, split: "train")

      stream = ElixirDatasets.Loader.load_dataset!({:hf, "dataset"}, streaming: true)
      stream |> Enum.take(10)

  """
  @spec load_dataset!(Repository.t_repository(), keyword()) ::
          [Explorer.DataFrame.t()] | Enumerable.t()
  def load_dataset!(repository, opts \\ []) do
    case load_dataset(repository, opts) do
      {:ok, datasets} -> datasets
      {:error, reason} -> raise reason
    end
  end

  @doc """
  Loads the specification of files to download from a repository.

  Filters files by valid extensions and downloads them in parallel if num_proc > 1.

  ## Parameters

    * `repository` - normalized repository tuple
    * `repo_files` - map of files from repository
    * `num_proc` - number of parallel processes to use

  ## Returns

  `{:ok, paths_with_extensions}` where each element is `{path, extension}`,
  or `{:error, reason}` if download fails.
  """
  @spec load_spec(tuple(), map(), pos_integer()) ::
          {:ok, list({String.t(), String.t()})} | {:error, String.t()}
  def load_spec(repository, repo_files, num_proc) do
    files_to_download =
      Enum.filter(repo_files, fn {file_name, _etag} ->
        extension = file_name |> Path.extname() |> String.trim_leading(".")
        extension in @valid_extensions_list
      end)

    if num_proc > 1 do
      download_parallel(repository, files_to_download, num_proc)
    else
      download_sequential(repository, files_to_download)
    end
  end

  defp download_parallel(repository, files_to_download, num_proc) do
    files_to_download
    |> Task.async_stream(
      fn {file_name, etag} ->
        extension = file_name |> Path.extname() |> String.trim_leading(".")

        case Repository.download(repository, file_name, etag) do
          {:ok, path} -> {:ok, {path, extension}}
          {:error, reason} -> {:error, "failed to download #{file_name}: #{reason}"}
        end
      end,
      max_concurrency: num_proc,
      ordered: true
    )
    |> Enum.reduce_while({:ok, []}, fn
      {:ok, {:ok, path_ext}}, {:ok, acc} ->
        {:cont, {:ok, [path_ext | acc]}}

      {:ok, {:error, reason}}, _acc ->
        {:halt, {:error, reason}}

      {:exit, reason}, _acc ->
        {:halt, {:error, "task failed: #{inspect(reason)}"}}
    end)
    |> case do
      {:ok, paths} -> {:ok, Enum.reverse(paths)}
      error -> error
    end
  end

  defp download_sequential(repository, files_to_download) do
    Enum.reduce_while(files_to_download, [], fn {file_name, etag}, acc ->
      extension = file_name |> Path.extname() |> String.trim_leading(".")

      case Repository.download(repository, file_name, etag) do
        {:ok, path} ->
          {:cont, [{path, extension} | acc]}

        {:error, reason} ->
          {:halt,
           {:error, "failed to download #{file_name} from #{inspect(repository)}: #{reason}"}}
      end
    end)
    |> case do
      {:error, _} = error -> error
      paths -> {:ok, Enum.reverse(paths)}
    end
  end
end


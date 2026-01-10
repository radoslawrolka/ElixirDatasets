defmodule ElixirDatasets.Utils.Loader do
  @moduledoc """
  Utility functions for loading datasets from various file formats.

  Supports loading from CSV, Parquet, and JSONL files. Each format is handled
  by its corresponding loader function that returns a dataframe or decoded data.
  """

  @doc """
  Loads datasets from multiple file paths with optional parallel processing.

  Automatically detects the file format based on the extension and loads each file accordingly.
  When `num_proc` is greater than 1, files are loaded in parallel using multiple processes,
  which can significantly speed up loading when dealing with multiple files.

  ## Parameters

    * `paths_with_extensions` - list of {path, extension} tuples to load
    * `num_proc` - number of processes to use for parallel loading (default: 1).
      When set to 1, files are loaded sequentially. When greater than 1, files
      are loaded in parallel using `Task.async_stream` with the specified concurrency.

  ## Returns

    * `{:ok, [datasets]}` - a list of loaded datasets in the same order as input
    * `{:error, reason}` - if any file fails to load

  ## Examples

      # Sequential loading
      paths = [{"data1.csv", "csv"}, {"data2.parquet", "parquet"}]
      {:ok, datasets} = load_datasets_from_paths(paths)

      # Parallel loading with 4 processes
      {:ok, datasets} = load_datasets_from_paths(paths, 4)

  """
  @spec load_datasets_from_paths([{Path.t(), String.t()}], pos_integer()) ::
          {:ok, [Explorer.DataFrame.t()]} | {:error, Exception.t()}
  def load_datasets_from_paths(paths_with_extensions, num_proc \\ 1) do
    if num_proc > 1 do
      paths_with_extensions
      |> Task.async_stream(
        fn {path, extension} ->
          load_dataset_from_file(path, extension)
        end,
        max_concurrency: num_proc,
        ordered: true
      )
      |> Enum.reduce_while({:ok, []}, fn
        {:ok, {:ok, df}}, {:ok, acc} ->
          {:cont, {:ok, [df | acc]}}

        {:ok, {:error, _} = error}, _acc ->
          {:halt, error}

        {:exit, reason}, _acc ->
          {:halt, {:error, "task failed: #{inspect(reason)}"}}
      end)
      |> case do
        {:ok, datasets} -> {:ok, Enum.reverse(datasets)}
        error -> error
      end
    else
      Enum.reduce_while(paths_with_extensions, {:ok, []}, fn {path, extension}, {:ok, acc} ->
        case load_dataset_from_file(path, extension) do
          {:ok, df} -> {:cont, {:ok, [df | acc]}}
          error -> {:halt, error}
        end
      end)
      |> then(fn
        {:ok, datasets} -> {:ok, Enum.reverse(datasets)}
        error -> error
      end)
    end
  end

  @doc """
  Similar to `load_datasets_from_paths/2` but raises an error if loading fails.

  Loads datasets from multiple file paths with optional parallel processing.
  Raises an exception if any file fails to load.

  ## Parameters

    * `paths_with_extensions` - list of {path, extension} tuples to load
    * `num_proc` - number of processes to use for parallel loading (default: 1).
      When set to 1, files are loaded sequentially. When greater than 1, files
      are loaded in parallel.

  ## Returns

    * a list of loaded datasets in the same order as input
    * raises an error if any file fails to load

  ## Examples

      # Sequential loading
      paths = [{"data1.csv", "csv"}, {"data2.parquet", "parquet"}]
      datasets = load_datasets_from_paths!(paths)

      # Parallel loading with 4 processes
      datasets = load_datasets_from_paths!(paths, 4)

  """
  @spec load_datasets_from_paths!([{Path.t(), String.t()}], pos_integer()) :: [
          Explorer.DataFrame.t()
        ]
  def load_datasets_from_paths!(paths_with_extensions, num_proc \\ 1) do
    case load_datasets_from_paths(paths_with_extensions, num_proc) do
      {:ok, datasets} -> datasets
      {:error, reason} -> raise reason
    end
  end

  @spec load_dataset_from_file(Path.t(), String.t()) ::
          {:ok, Explorer.DataFrame.t()} | {:error, Exception.t()}
  defp load_dataset_from_file(path, "jsonl") do
    Explorer.DataFrame.from_ndjson(path)
  end

  defp load_dataset_from_file(path, "csv") do
    Explorer.DataFrame.from_csv(path)
  end

  defp load_dataset_from_file(path, "parquet") do
    Explorer.DataFrame.from_parquet(path)
  end

  defp load_dataset_from_file(path, _unsupported_format) do
    {:error, "Unsupported file format for file: #{path}"}
  end
end

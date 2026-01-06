defmodule ElixirDatasets.Utils.Loader do
  @moduledoc """
  Utility functions for loading datasets from various file formats.

  Supports loading from CSV, Parquet, and JSONL files. Each format is handled
  by its corresponding loader function that returns a dataframe or decoded data.
  """

  @doc """
  Loads datasets from multiple file paths.

  Automatically detects the file format based on the extension and loads each file accordingly.

  ## Parameters

    * `paths_with_extensions` - list of {path, extension} tuples
    * `num_proc` - number of processes for parallel loading (default: 1)

  ## Returns

    * `{:ok, [datasets]}` - a list of loaded datasets
    * `{:error, reason}` - if any file fails to load
  """
  @spec load_datasets_from_paths([{Path.t(), String.t()}], pos_integer()) ::
          {:ok, [Explorer.DataFrame.t()]} | {:error, Exception.t()}
  def load_datasets_from_paths(paths_with_extensions, num_proc \\ 1) do
    if num_proc > 1 do
      # Parallel processing
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
      # Sequential processing (original behavior)
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

  ## Parameters

    * `paths_with_extensions` - list of {path, extension} tuples
    * `num_proc` - number of processes for parallel loading (default: 1)

  ## Returns

    * a list of loaded datasets
    * raises an error if any file fails to load
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

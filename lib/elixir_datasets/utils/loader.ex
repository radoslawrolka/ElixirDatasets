defmodule ElixirDatasets.Utils.Loader do
  @moduledoc """
  Utility functions for loading datasets from various file formats.

  Supports loading from CSV, Parquet, and JSONL files. Each format is handled
  by its corresponding loader function that returns a dataframe or decoded data.
  """

  @doc """
  Loads datasets from multiple file paths.

  Automatically detects the file format based on the extension and loads each file accordingly.

  ## Returns

    * `{:ok, [datasets]}` - a list of loaded datasets
    * `{:error, reason}` - if any file fails to load
  """
  @spec load_datasets_from_paths([{Path.t(), String.t()}]) ::
          {:ok, [Explorer.DataFrame.t()]} | {:error, Exception.t()}
  def load_datasets_from_paths(paths_with_extensions) do
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

  @doc """
  Similar to `load_datasets_from_paths/1` but raises an error if loading fails.

  ## Returns

    * a list of loaded datasets
    * raises an error if any file fails to load
  """
  @spec load_datasets_from_paths!([{Path.t(), String.t()}]) :: [Explorer.DataFrame.t()]
  def load_datasets_from_paths!(paths_with_extensions) do
    case load_datasets_from_paths(paths_with_extensions) do
      {:ok, datasets} -> datasets
      {:error, reason} -> raise reason
    end
  end

  @spec load_dataset_from_file(Path.t(), String.t()) ::
          Explorer.DataFrame.t() | {:error, Exception.t()}
  defp load_dataset_from_file(path, "jsonl") do
    Explorer.DataFrame.from_ndjson(path)
  end

  @spec load_dataset_from_file(Path.t(), String.t()) ::
          Explorer.DataFrame.t() | {:error, Exception.t()}
  defp load_dataset_from_file(path, "csv") do
    Explorer.DataFrame.from_csv(path)
  end

  @spec load_dataset_from_file(Path.t(), String.t()) ::
          Explorer.DataFrame.t() | {:error, Exception.t()}
  defp load_dataset_from_file(path, "parquet") do
    Explorer.DataFrame.from_parquet(path)
  end

  @spec load_dataset_from_file(Path.t(), String.t()) ::
          Explorer.DataFrame.t() | {:error, Exception.t()}
  defp load_dataset_from_file(path, _unsupported_format) do
    {:error, "Unsupported file format for file: #{path}"}
  end
end

defmodule ElixirDatasets.Utils.Saver do
  @moduledoc """
  Utility functions for saving datasets to various file formats.

  Supports saving to CSV, Parquet, and JSONL files.
  """

  @valid_extensions ["jsonl", "csv", "parquet"]

  @doc """
  Saves the given dataframe to a file specified in the options.
  Options can include:
    - :filepath - the path to save the file
    - :file_extension - the file extension/type (csv, parquet, jsonl)
  """
  @spec save_dataset_to_file(Explorer.DataFrame.t(), keyword()) :: Path.t() | {:error, String.t()}
  def save_dataset_to_file(df, options) do
    verify_options!(options)
    filepath = options[:filepath] || Briefly.create!()
    file_extension = options[:file_extension] || Path.extname(filepath) |> String.trim_leading(".")
    case file_extension do
      "jsonl" -> Explorer.DataFrame.to_ndjson(df, filepath)
      "csv" -> Explorer.DataFrame.to_csv(df, filepath)
      "parquet" -> Explorer.DataFrame.to_parquet(df, filepath)
      _ -> {:error, "Unsupported file format: #{file_extension}"}
    end
    filepath
  end

  @doc """
  Verifies that the provided options for saving are valid.
  """
  @spec verify_options!(keyword()) :: :ok | no_return()
  defp verify_options!(options) do
    verify_file_extension!(options)
    verify_filepath!(options)
  end

  @doc """
  Verifies that the provided filepath is valid if given.
  """
  @spec verify_filepath!(keyword()) :: :ok | no_return()
  defp verify_filepath!(options) do
    case Keyword.get(options, :filepath) do
      nil -> :ok
      path when is_binary(path) -> :ok
      _ -> raise ArgumentError, "Invalid filepath provided. It must be a string."
    end
  end

  @doc """
  Verifies that the provided file extension is valid if given.
  """
  @spec verify_file_extension!(keyword()) :: :ok | no_return()
  defp verify_file_extension!(options) do
    case Keyword.get(options, :file_extension) do
      nil -> :ok
      ext when ext in @valid_extensions -> :ok
      ext -> raise ArgumentError, "Invalid file extension: #{ext}. Supported extensions are: #{@valid_extensions |> Enum.join(", ")}"
    end
  end
end

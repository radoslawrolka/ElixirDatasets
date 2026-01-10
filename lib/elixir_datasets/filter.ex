defmodule ElixirDatasets.Filter do
  @moduledoc """
  Functions for filtering dataset files by configuration and split.
  """

  @doc """
  Filters repository files by configuration name and split.

  ## Parameters

    * `repo_files` - map of files from repository (%{filename => etag})
    * `name` - optional configuration name to filter by
    * `split` - optional split name to filter by (e.g., "train", "test")

  ## Returns

  `{:ok, filtered_files}` where `filtered_files` is a map of matching files.

  ## Examples

      iex> files = %{"train.csv" => nil, "test.csv" => nil}
      iex> ElixirDatasets.Filter.by_config_and_split(files, nil, "train")
      {:ok, %{"train.csv" => nil}}
  """
  @spec by_config_and_split(map(), String.t() | nil, String.t() | nil) :: {:ok, map()}
  def by_config_and_split(repo_files, name, split) do
    filtered =
      repo_files
      |> by_config_name(name)
      |> by_split(split)

    {:ok, filtered}
  end

  @doc """
  Filters files by configuration name.

  If `config_name` is nil, returns all files unchanged.
  Otherwise, returns only files whose path contains the config name.

  ## Parameters

    * `repo_files` - map or list of files
    * `config_name` - optional configuration name to filter by

  ## Returns

  Filtered files in the same format as input (map or list).
  """
  @spec by_config_name(map() | list(), String.t() | nil) :: map() | list()
  def by_config_name(repo_files, nil), do: repo_files

  def by_config_name(repo_files, config_name) do
    filtered =
      Enum.filter(repo_files, fn {file_name, _etag} ->
        String.contains?(file_name, config_name)
      end)

    if is_map(repo_files) do
      Map.new(filtered)
    else
      filtered
    end
  end

  @doc """
  Filters files by split name.

  If `split` is nil, returns all files unchanged.
  Otherwise, returns only files whose basename (without extension) contains the split name.

  ## Parameters

    * `repo_files` - map or list of files
    * `split` - optional split name to filter by (e.g., "train", "test", "validation")

  ## Returns

  Filtered files in the same format as input (map or list).

  ## Examples

      iex> files = %{"train.csv" => nil, "test.csv" => nil, "validation.csv" => nil}
      iex> ElixirDatasets.Filter.by_split(files, "train")
      %{"train.csv" => nil}
  """
  @spec by_split(map() | list(), String.t() | nil) :: map() | list()
  def by_split(repo_files, nil), do: repo_files

  def by_split(repo_files, split) when is_binary(split) do
    filtered =
      Enum.filter(repo_files, fn {file_name, _etag} ->
        base_name = Path.basename(file_name, Path.extname(file_name))
        String.contains?(base_name, split)
      end)

    if is_map(repo_files) do
      Map.new(filtered)
    else
      filtered
    end
  end
end


defmodule ElixirDatasets.Utils.Uploader do
  @moduledoc """
  Utility functions for uploading datasets to huggingface.co .
  """

  @huggingface_endpoint "https://huggingface.co"
  @valid_extensions ["jsonl", "csv", "parquet"]

  @doc """
  Uploads a dataset to a specified Hugging Face repository.
  """
  @spec upload_dataset(Explorer.DataFrame.t(), String.t(), keyword()) ::
          {:ok, String.t()} | {:error, Exception.t()}
  def upload_dataset(df, repository, options) do
    verify_options!(options)
    temp_file = ElixirDatasets.Utils.Saver.save_dataset_to_file(df, options)
    # file_extension = Keyword.get(options, :file_extension) || Path.extname(temp_file) |> String.trim_leading(".")

    with {:ok, token} <- get_hf_token(),
         {:ok, file_content} <- File.read(temp_file),
         encoded_content <- Base.encode64(file_content),
         {:ok, filename} <- get_filename(temp_file, options),
         commit_msg <- Keyword.get(options, :commit_message, "Commit from ElixirDatasets"),
         description <- Keyword.get(options, :description, ""),
         {:ok, response} <- commit_to_huggingface(repository, token, filename, encoded_content, commit_msg, description) do
      File.rm(temp_file)
      {:ok, response}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Deletes a file from a specified Hugging Face dataset repository.

  ## Parameters
    - `repository`: The Hugging Face repository path (e.g., "username/dataset-name")
    - `filename`: The path of the file to delete in the repository
    - `options`: A keyword list with optional parameters:
      - `:commit_message`: Custom commit message (default: "Delete file from ElixirDatasets")
      - `:description`: Optional description for the commit

  ## Examples
      iex> delete_file_from_dataset("username/dataset", "old_file.csv", [])
      {:ok, response_body}

      iex> delete_file_from_dataset("username/dataset", "data.csv",
      ...>   commit_message: "Removing outdated data",
      ...>   description: "Data no longer needed"
      ...> )
      {:ok, response_body}
  """
  @spec delete_file_from_dataset(String.t(), String.t(), keyword()) ::
          {:ok, String.t()} | {:error, String.t() | Exception.t()}
  def delete_file_from_dataset(repository, filename, options \\ []) when is_binary(repository) and is_binary(filename) do
    commit_msg = Keyword.get(options, :commit_message, "Delete file from ElixirDatasets")
    description = Keyword.get(options, :description, "")

    with {:ok, token} <- get_hf_token(),
         {:ok, response} <- delete_from_huggingface(repository, token, filename, commit_msg, description) do
      {:ok, response}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Uploads a large file to Hugging Face using Git LFS.

  This function handles the complete LFS upload workflow:
  1. Calculates SHA256 hash and file size
  2. Initiates LFS batch request
  3. Uploads file to S3
  4. Verifies upload
  5. Creates commit with LFS reference

  ## Parameters
    - `file_path`: The local path to the file to upload
    - `repository`: The Hugging Face repository path (e.g., "username/dataset-name")
    - `options`: A keyword list with optional parameters:
      - `:commit_message`: Custom commit message (default: "Upload file via LFS from ElixirDatasets")
      - `:description`: Optional description for the commit
      - `:repo_filename`: The path in the repository (default: basename of file_path)

  ## Examples
      iex> upload_file_via_lfs("/path/to/large_file.csv", "username/dataset", [])
      {:ok, response_body}

      iex> upload_file_via_lfs("/path/to/data.parquet", "username/dataset",
      ...>   commit_message: "Upload large dataset",
      ...>   repo_filename: "datasets/v1/data.parquet"
      ...> )
      {:ok, response_body}
  """
  @spec upload_file_via_lfs(String.t(), String.t(), keyword()) ::
          {:ok, String.t()} | {:error, String.t()}
  def upload_file_via_lfs(file_path, repository, options \\ []) when is_binary(file_path) and is_binary(repository) do
    commit_msg = Keyword.get(options, :commit_message, "Upload file via LFS from ElixirDatasets")
    description = Keyword.get(options, :description, "")
    repo_filename = Keyword.get(options, :repo_filename, Path.basename(file_path))

    with {:ok, token} <- get_hf_token(),
         {:ok, file_content} <- File.read(file_path),
         oid <- calculate_sha256(file_content),
         size <- byte_size(file_content),
         {:ok, upload_url, verify_url, verify_token} <- initiate_lfs_batch(repository, token, oid, size),
         IO.inspect({:upload_url, upload_url, :verify_url, verify_url}, label: "LFS URLs"),
         :ok <- upload_to_s3(upload_url, file_content),
         :ok <- verify_lfs_upload(verify_url, verify_token, oid, size),
         {:ok, response} <- commit_lfs_file(repository, token, repo_filename, oid, size, commit_msg, description) do
      {:ok, response}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc false
  defp calculate_sha256(content) do
    :crypto.hash(:sha256, content)
    |> Base.encode16(case: :lower)
  end

  @doc false
  defp initiate_lfs_batch(repository, token, oid, size) do
    url = "#{@huggingface_endpoint}/datasets/#{repository}.git/info/lfs/objects/batch"

    payload = Jason.encode!(%{
      "operation" => "upload",
      "transfers" => ["basic"],
      "objects" => [
        %{
          "oid" => oid,
          "size" => size
        }
      ],
      "hash_algo" => "sha256"
    })

    headers = [
      {~c"Content-Type", ~c"application/vnd.git-lfs+json"},
      {~c"Accept", ~c"application/vnd.git-lfs+json"},
      {~c"Authorization", ~c"Bearer #{token}"}
    ]

    case :httpc.request(:post, {String.to_charlist(url), headers, ~c"application/vnd.git-lfs+json", String.to_charlist(payload)}, [], []) do
      {:ok, {{_protocol, 200, _message}, _headers, response_body}} ->
        case Jason.decode(response_body) do
          {:ok, %{"objects" => [%{"actions" => %{"upload" => %{"href" => upload_url}, "verify" => %{"href" => verify_url, "header" => %{"Authorization" => verify_token}}}}]}} ->
            {:ok, upload_url, verify_url, verify_token}
          {:ok, _} ->
            {:error, "Unexpected LFS batch response format: #{response_body}"}
          {:error, _} ->
            {:error, "Failed to parse LFS batch response"}
        end

      {:ok, {{_protocol, status_code, _message}, _headers, response_body}} ->
        {:error, "LFS batch request failed with status #{status_code}: #{response_body}"}

      {:error, reason} ->
        {:error, "LFS batch request error: #{inspect(reason)}"}
    end
  end

  @doc false
  defp upload_to_s3(upload_url, file_content) do
    headers = [
      {~c"Content-Type", ~c"application/octet-stream"}
    ]

    case :httpc.request(:put, {String.to_charlist(upload_url), headers, ~c"application/octet-stream", file_content}, [], []) do
      {:ok, {{_protocol, status_code, _message}, _headers, _response_body}} when status_code in 200..299 ->
        :ok

      {:ok, {{_protocol, status_code, _message}, _headers, response_body}} ->
        {:error, "S3 upload failed with status #{status_code}: #{response_body}"}

      {:error, reason} ->
        {:error, "S3 upload error: #{inspect(reason)}"}
    end
  end

  @doc false
  defp verify_lfs_upload(verify_url, _verify_token, oid, size) do
    payload = Jason.encode!(%{
      "oid" => oid,
      "size" => size
    })

    headers = [
      {~c"Authorization", ~c"Bearer #{get_hf_token() |> elem(1)}"},
      {~c"Content-Type", ~c"application/json"}
    ]

    case :httpc.request(:post, {String.to_charlist(verify_url), headers, ~c"application/json", String.to_charlist(payload)}, [], []) do
      {:ok, {{_protocol, 200, _message}, _headers, _response_body}} ->
        :ok

      {:ok, {{_protocol, status_code, _message}, _headers, response_body}} ->
        {:error, "LFS verification failed with status #{status_code}: #{response_body}"}

      {:error, reason} ->
        {:error, "LFS verification error: #{inspect(reason)}"}
    end
  end

  @doc false
  defp commit_lfs_file(repository, token, filename, oid, size, commit_msg, description) do
    url = "#{@huggingface_endpoint}/api/datasets/#{repository}/commit/main"

    # Prepare NDJSON payload
    header_line = Jason.encode!(%{
      "key" => "header",
      "value" => %{
        "summary" => commit_msg,
        "description" => description
      }
    })

    lfs_file_line = Jason.encode!(%{
      "key" => "lfsFile",
      "value" => %{
        "path" => filename,
        "algo" => "sha256",
        "oid" => oid,
        "size" => size
      }
    })

    body = "#{header_line}\n#{lfs_file_line}"

    headers = [
      {~c"Authorization", ~c"Bearer #{token}"},
      {~c"Content-Type", ~c"application/x-ndjson"}
    ]

    case :httpc.request(:post, {String.to_charlist(url), headers, ~c"application/x-ndjson", String.to_charlist(body)}, [], []) do
      {:ok, {{_protocol, status_code, _message}, _headers, response_body}} when status_code in 200..299 ->
        {:ok, response_body}

      {:ok, {{_protocol, status_code, _message}, _headers, response_body}} ->
        {:error, "HTTP Error #{status_code}: #{response_body}"}

      {:error, reason} ->
        {:error, inspect(reason)}
    end
  end

  @doc """
  Verifies that the provided options for uploading are valid.
  """
  @spec verify_options!(keyword()) :: :ok | no_return()
  defp verify_options!(options) do
    verify_file_extension!(options)
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

  @doc false
  defp get_hf_token do
    case System.get_env("HF_TOKEN") do
      nil -> {:error, "HF_TOKEN environment variable not set"}
      token -> {:ok, token}
    end
  end

  @doc false
  defp get_filename(temp_file, options) do
    case Keyword.get(options, :filename) do
      nil -> {:ok, Path.basename(temp_file)}
      filename -> {:ok, filename}
    end
  end

  @doc false
  defp commit_to_huggingface(repository, token, filename, encoded_content, commit_msg, description) do
    url = "#{@huggingface_endpoint}/api/datasets/#{repository}/commit/main"

    # Prepare NDJSON payload
    header_line = Jason.encode!(%{
      "key" => "header",
      "value" => %{
        "summary" => commit_msg,
        "description" => description
      }
    })

    file_line = Jason.encode!(%{
      "key" => "file",
      "value" => %{
        "content" => encoded_content,
        "path" => filename,
        "encoding" => "base64"
      }
    })

    body = "#{header_line}\n#{file_line}"

    headers = [
      {~c"Authorization", ~c"Bearer #{token}"},
      {~c"Content-Type", ~c"application/x-ndjson"}
    ]

    case :httpc.request(:post, {String.to_charlist(url), headers, ~c"application/x-ndjson", String.to_charlist(body)}, [], []) do
      {:ok, {{_protocol, status_code, _message}, _headers, response_body}} when status_code in 200..299 ->
        {:ok, response_body}
      {:ok, {{_protocol, status_code, _message}, _headers, response_body}} ->
        {:error, "HTTP Error #{status_code}: #{response_body}"}
      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc false
  defp delete_from_huggingface(repository, token, filename, commit_msg, description) do
    url = "#{@huggingface_endpoint}/api/datasets/#{repository}/commit/main"

    # Prepare NDJSON payload
    header_line = Jason.encode!(%{
      "key" => "header",
      "value" => %{
        "summary" => commit_msg,
        "description" => description
      }
    })

    deleted_file_line = Jason.encode!(%{
      "key" => "deletedFile",
      "value" => %{
        "path" => filename
      }
    })

    body = "#{header_line}\n#{deleted_file_line}"

    headers = [
      {~c"Authorization", ~c"Bearer #{token}"},
      {~c"Content-Type", ~c"application/x-ndjson"}
    ]

    case :httpc.request(:post, {String.to_charlist(url), headers, ~c"application/x-ndjson", String.to_charlist(body)}, [], []) do
      {:ok, {{_protocol, status_code, _message}, _headers, response_body}} when status_code in 200..299 ->
        {:ok, response_body}
      {:ok, {{_protocol, status_code, _message}, _headers, response_body}} ->
        {:error, "HTTP Error #{status_code}: #{response_body}"}
      {:error, reason} ->
        {:error, reason}
    end
  end
end

defmodule ElixirDatasets.HuggingFace.HubTest do
  use ExUnit.Case, async: true

  doctest ElixirDatasets.HuggingFace.Hub

  describe "file_url/3" do
    @repository_id "test-user/test-repo"
    @filename "test-file.txt"

    test "returns correct URL with no revision" do
      revision = nil

      expected_url =
        "https://huggingface.co/datasets/test-user/test-repo/resolve/main/test-file.txt"

      assert ElixirDatasets.HuggingFace.Hub.file_url(@repository_id, @filename, revision) ==
               expected_url
    end

    test "returns correct URL with revision" do
      revision = "test-revision"

      expected_url =
        "https://huggingface.co/datasets/test-user/test-repo/resolve/test-revision/test-file.txt"

      assert ElixirDatasets.HuggingFace.Hub.file_url(@repository_id, @filename, revision) ==
               expected_url
    end
  end

  describe "file_listing_url/3" do
    @repository_id "test-user/test-repo"
    test "returns correct URL with no subdir and no revision" do
      subdir = nil
      revision = nil

      expected_url =
        "https://huggingface.co/api/datasets/test-user/test-repo/tree/main"

      assert ElixirDatasets.HuggingFace.Hub.file_listing_url(@repository_id, subdir, revision) ==
               expected_url
    end

    test "returns correct URL with subdir and revision" do
      subdir = "test-subdir/test-subdir2"
      revision = "test-revision"

      expected_url =
        "https://huggingface.co/api/datasets/test-user/test-repo/tree/test-revision/test-subdir/test-subdir2"

      assert ElixirDatasets.HuggingFace.Hub.file_listing_url(@repository_id, subdir, revision) ==
               expected_url
    end
  end

  describe "cached_download/2" do
    @url "https://huggingface.co/datasets/aaaaa32r/elixirDatasets"
    @url_redirect "https://huggingface.co/datasets/FreedomIntelligence/medical-o1-reasoning-SFT/resolve/main/medical_o1_sft_Chinese.json"
    @cache_dir "test_cache_dir_cached_download"
    @cache_scope "test_cache_scope"
    @opts [cache_dir: @cache_dir, cache_scope: @cache_scope]

    test "No cache_scope" do
      assert {:ok, _path} =
               ElixirDatasets.HuggingFace.Hub.cached_download(@url, cache_dir: @cache_dir)

      # Clean up
      File.rm_rf!(@cache_dir)
    end

    test "With cache_scope" do
      File.mkdir_p!(@cache_dir)

      assert {:ok, _path} = ElixirDatasets.HuggingFace.Hub.cached_download(@url, @opts)

      # Clean up
      File.rm_rf!(@cache_dir)
    end

    test "with cache_scope, redirect" do
      File.mkdir_p!(@cache_dir)

      assert {:ok, _path} = ElixirDatasets.HuggingFace.Hub.cached_download(@url_redirect, @opts)

      # Clean up
      File.rm_rf!(@cache_dir)
    end
  end

  describe "cached_path_for_etag/3" do
    @dir "test_cache_dir_cached_path_for_etag"
    @url "https://example.com/test-file.txt"
    @etag "1234567890abcdef"
    @fileContent "jrdifprgyy26hfylusnlbth2ie.gezdgnbvgy3tqojqmfrggzdfmy"
    @fileJson "jrdifprgyy26hfylusnlbth2ie.json"

    test "returns cached path for known etag" do
      File.mkdir_p!(@dir)
      File.write!(Path.join(@dir, @fileContent), "test content")
      File.write!(Path.join(@dir, @fileJson), Jason.encode!(%{"etag" => @etag}))
      expected_path = @dir <> "/" <> @fileContent

      assert ElixirDatasets.HuggingFace.Hub.cached_path_for_etag(@dir, @url, @etag) ==
               expected_path

      # Clean up
      File.rm!(Path.join(@dir, @fileContent))
      File.rm!(Path.join(@dir, @fileJson))
      File.rmdir!(@dir)
    end

    test "returns nil for invalid etag" do
      File.mkdir_p!(@dir)
      File.write!(Path.join(@dir, @fileJson), Jason.encode!(%{"etag" => "invalid-etag"}))

      assert ElixirDatasets.HuggingFace.Hub.cached_path_for_etag(@dir, @url, @etag) == nil

      # Clean up
      File.rm!(Path.join(@dir, @fileJson))
      File.rmdir!(@dir)
    end
  end

  describe "head_download/2" do
    @url "https://huggingface.co/datasets/aaaaa32r/elixirDatasets"
    @url_redirect "https://huggingface.co/datasets/FreedomIntelligence/medical-o1-reasoning-SFT/resolve/main/medical_o1_sft_Chinese.json"
    # @urlNilHost "http://localhost:32123/sessions/7xre6dqd37a6olsi4dmdddndzz6te5cdimmshjblbbsot2cg" # This URL is not valid for testing, as it does not exist outside of my local environment
    @headers [{"Content-Type", "application/json"}]

    test "returns :ok with valid response, without redirection" do
      assert {:ok, _etag, @url, false} =
               ElixirDatasets.HuggingFace.Hub.head_download(@url, @headers)
    end

    test "returns :ok with valid response, with redirection" do
      assert {:ok, _etag, _url_redirect, true} =
               ElixirDatasets.HuggingFace.Hub.head_download(@url_redirect, @headers)
    end

    # test "returns :error, when host location is nil" do # todo
    #   assert {:error, _reason} =
    #            ElixirDatasets.HuggingFace.Hub.head_download(@urlNilHost, @headers)
    # end
  end

  describe "finish_request" do
    test "response is :ok" do
      assert ElixirDatasets.HuggingFace.Hub.finish_request(:ok, @url) == :ok
    end

    test "response is :ok, status in 100..399" do
      response = {:ok, %{status: 200}}
      assert ElixirDatasets.HuggingFace.Hub.finish_request(response, @url) == response
    end

    test "response is :ok, status is out 100..399" do
      responses = [
        {:ok, %{status: 404, headers: [{"x-error-code", "RepoNotFound"}]}},
        {:ok, %{status: 404, headers: [{"x-error-code", "GatedRepo"}]}},
        {:ok, %{status: 404, headers: [{"x-error-code", "OtherError"}]}},
        {:ok, %{status: 500, headers: [{"x-error-code", "EntryNotFound"}]}},
        {:ok, %{status: 500, headers: [{"x-error-code", "RevisionNotFound"}]}}
      ]

      Enum.each(responses, fn response ->
        assert {:error, _} = ElixirDatasets.HuggingFace.Hub.finish_request(response, @url)
      end)
    end

    test "response is error" do
      response = {:error, "test-error"}

      assert ElixirDatasets.HuggingFace.Hub.finish_request(response, @url) ==
               {:error, "failed to make an HTTP request, reason: \"test-error\""}
    end
  end

  describe "fetch_etag/1" do
    test "when etag is present" do
      response = %{
        status: 200,
        headers: [{"Content-Type", "application/json"}, {"etag", "1234567890abcdef"}],
        body: "{}"
      }

      assert ElixirDatasets.HuggingFace.Hub.fetch_etag(response) ==
               {:ok, "1234567890abcdef"}
    end

    test "when etag is not present" do
      response = %{
        status: 200,
        headers: [{"Content-Type", "application/json"}],
        body: "{}"
      }

      assert ElixirDatasets.HuggingFace.Hub.fetch_etag(response) ==
               {:error, "no ETag found on the resource"}
    end
  end

  describe "metadata_filename/1" do
    @url "https://example.com/test-file.txt"

    test "generates correct metadata filename from URL" do
      expected_filename = "jrdifprgyy26hfylusnlbth2ie.json"

      assert ElixirDatasets.HuggingFace.Hub.metadata_filename(@url) == expected_filename
    end
  end

  describe "entry_filename/2, encode_url/1, encode_etag/1" do
    test "generates correct filenames based on URL and ETag" do
      etag = "1234567890abcdef"

      expected_entry_filename = "jrdifprgyy26hfylusnlbth2ie.gezdgnbvgy3tqojqmfrggzdfmy"

      assert ElixirDatasets.HuggingFace.Hub.entry_filename(@url, etag) ==
               expected_entry_filename
    end
  end

  describe "store_json/2, load_json/1" do
    @data %{"key" => "value"}
    test "stores JSON data to a file and loads it back" do
      path = "test_data.json"

      assert ElixirDatasets.HuggingFace.Hub.store_json(path, @data) == :ok
      assert File.exists?(path)

      assert ElixirDatasets.HuggingFace.Hub.load_json(path) == {:ok, @data}

      # Clean up
      File.rm!(path)
    end

    test "returns error when unable to write to file and returns error when trying to load" do
      path = "/invalid/path/test_data.json"

      assert ElixirDatasets.HuggingFace.Hub.store_json(path, @data) ==
               {:error, :enoent}

      assert ElixirDatasets.HuggingFace.Hub.load_json(path) == :error
    end
  end

  describe "elixirDatasets_offline?/0" do
    test "returns true when ELIXIR_DATASETS_OFFLINE is set to '1'" do
      System.put_env("ELIXIR_DATASETS_OFFLINE", "1")
      assert ElixirDatasets.HuggingFace.Hub.elixir_datasets_offline?() == true
      System.delete_env("ELIXIR_DATASETS_OFFLINE")
    end

    test "returns true when ELIXIR_DATASETS_OFFLINE is set to 'true'" do
      System.put_env("ELIXIR_DATASETS_OFFLINE", "true")
      assert ElixirDatasets.HuggingFace.Hub.elixir_datasets_offline?() == true
      System.delete_env("ELIXIR_DATASETS_OFFLINE")
    end

    test "returns false when ELIXIR_DATASETS_OFFLINE is not set" do
      assert ElixirDatasets.HuggingFace.Hub.elixir_datasets_offline?() == false
    end

    test "returns false when ELIXIR_DATASETS_OFFLINE is set to '0'" do
      System.put_env("ELIXIR_DATASETS_OFFLINE", "0")
      assert ElixirDatasets.HuggingFace.Hub.elixir_datasets_offline?() == false
      System.delete_env("ELIXIR_DATASETS_OFFLINE")
    end

    test "returns false when ELIXIR_DATASETS_OFFLINE is set to 'false'" do
      System.put_env("ELIXIR_DATASETS_OFFLINE", "false")
      assert ElixirDatasets.HuggingFace.Hub.elixir_datasets_offline?() == false
      System.delete_env("ELIXIR_DATASETS_OFFLINE")
    end
  end
end

defmodule ElixirDatasets.RepositoryTest do
  use ExUnit.Case, async: true
  doctest ElixirDatasets.Repository

  alias ElixirDatasets.Repository

  describe "normalize!/1" do
    test "normalizes {:hf, repository_id} format" do
      assert {:hf, "user/repo", []} = Repository.normalize!({:hf, "user/repo"})
    end

    test "normalizes {:hf, repository_id, opts} format" do
      opts = [revision: "main", cache_dir: "/tmp"]
      {:hf, "user/repo", normalized_opts} = Repository.normalize!({:hf, "user/repo", opts})
      assert Keyword.get(normalized_opts, :revision) == "main"
      assert Keyword.get(normalized_opts, :cache_dir) == "/tmp"
    end

    test "normalizes {:local, dir} format" do
      assert {:local, "/path/to/dir"} = Repository.normalize!({:local, "/path/to/dir"})
    end

    test "raises error for invalid format" do
      assert_raise ArgumentError, fn ->
        Repository.normalize!({:invalid, "repo"})
      end
    end

    test "raises error for invalid options" do
      assert_raise ArgumentError, fn ->
        Repository.normalize!({:hf, "user/repo", [invalid_opt: true]})
      end
    end
  end

  describe "get_files/1" do
    test "gets files from local directory" do
      repository = {:local, "resources"}
      assert {:ok, files} = Repository.get_files(repository)
      assert is_map(files)
      assert map_size(files) > 0
    end

    test "returns error for non-existent local directory" do
      repository = {:local, "non_existent_dir"}
      assert {:error, _reason} = Repository.get_files(repository)
    end
  end

  describe "repository_id_to_cache_scope/1" do
    test "converts repository ID to cache scope" do
      assert "user--repo" = Repository.repository_id_to_cache_scope("user/repo")
    end

    test "removes special characters" do
      assert "user--repo-name" = Repository.repository_id_to_cache_scope("user/repo-name")
    end

    test "handles underscores" do
      assert "user--repo_name" = Repository.repository_id_to_cache_scope("user/repo_name")
    end
  end
end


defmodule ElixirDatasets.Utils.HTTPTest do
  use ExUnit.Case, async: false

  doctest ElixirDatasets.Utils.HTTP

  describe "request/3" do
    test "with body" do
      method = :post

      url =
        "https://huggingface.co/datasets/FreedomIntelligence/medical-o1-reasoning-SFT/resolve/main/medical_o1_sft_Chinese.json"

      opts = [
        follow_redirects: false,
        headers: [{"Content-Type", "application/json"}],
        body: {"application/json", ~s({"key": "value"})}
      ]

      assert {:ok, _response} = ElixirDatasets.Utils.HTTP.request(method, url, opts)
    end
  end

  describe "http_ssl_opts/0" do
    test "returns SSL options with CA certificate file" do
      path = "/test/path"
      System.put_env("ELIXIR_DATASETS_CACERTS_PATH", path)

      assert [{:cacertfile, ^path}, {_, _}, {_, _}] =
               ElixirDatasets.Utils.HTTP.http_ssl_opts()

      System.delete_env("ELIXIR_DATASETS_CACERTS_PATH")
    end
  end

  # describe "set_proxy_options/0" do
  #   test "sets proxy options" do
  #     assert nil == ElixirDatasets.Utils.HTTP.set_proxy_options()

  #     System.put_env("HTTP_PROXY", "http://proxy.example.com:8080")
  #     System.put_env("HTTPS_PROXY", "https://proxy.example.com:8080")

  #     assert :ok == ElixirDatasets.Utils.HTTP.set_proxy_options()

  #     System.delete_env("HTTP_PROXY")
  #     System.delete_env("HTTPS_PROXY")

  #     assert nil == ElixirDatasets.Utils.HTTP.set_proxy_options()
  #   end
  # end
end

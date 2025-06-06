defmodule ElixirDatasets.MixProject do
  use Mix.Project

  def project do
    [
      app: :elixir_datasets,
      name: "ElixirDatasets",
      description: "A library for loading datasets from the Hugging Face Hub and local paths.",
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_coverage: [tool: ExCoveralls]
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      # Dev
      {:ex_doc, "~> 0.21"},

      # Test
      {:excoveralls, "~> 0.13", only: :test}
    ]
  end
end

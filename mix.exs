defmodule ElixirDatasets.MixProject do
  use Mix.Project

  @description "A library for loading datasets from the Hugging Face Hub and local paths."
  @github_url "https://github.com/radoslawrolka/elixirDatasets"

  def project do
    [
      app: :elixir_datasets,
      name: "ElixirDatasets",
      description: @description,
      package: package(),
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_coverage: [tool: ExCoveralls],
      source_url: @github_url,
      homepage_url: @github_url
    ]
  end

  def application do
    [
      mod: {ElixirDatasets.Application, []},
      extra_applications: [:logger, :inets, :ssl]
    ]
  end

  defp deps do
    [
      {:explorer, "~> 0.10.0"},
      {:jason, "~> 1.4.0"},
      {:progress_bar, "~> 3.0"},
      {:httpoison, "~> 2.1"},
      {:briefly, "~> 0.3"},

      # Dev
      {:ex_doc, "~> 0.21", only: :dev, runtime: false},

      # Test
      {:excoveralls, "~> 0.13", only: :test}
    ]
  end

  defp package do
    [
      maintainers: ["Radoslaw Rolka", "Weronika Wojtas"],
      description: @description,
      licenses: ["MIT"],
      links: %{
        "GitHub" => @github_url,
        "Documentation" => "https://radoslawrolka.github.io/ElixirDatasets"
      }
    ]
  end
end

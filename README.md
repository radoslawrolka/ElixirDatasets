# ElixirDatasets

[![Hex.pm](https://img.shields.io/hexpm/v/elixir_datasets.svg)](https://hex.pm/packages/elixir_datasets)
[![Documentation](https://img.shields.io/badge/docs-hexdocs-blue.svg)](https://hexdocs.pm/elixir_datasets)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**ElixirDatasets** is a comprehensive library for accessing and managing datasets from Hugging Face Hub in Elixir. Inspired by the [Python `datasets` library](https://github.com/huggingface/datasets), it brings powerful dataset management capabilities to the Elixir ecosystem with seamless integration with Explorer DataFrames.

## ✨ Features

- 🚀 **Easy Access to Hugging Face Hub** - Load thousands of datasets with a single function call
- 📊 **Explorer Integration** - Automatic conversion to Explorer DataFrames for data manipulation
- 💾 **Smart Caching** - Intelligent local caching to avoid redundant downloads
- 🌊 **Streaming Support** - Process large datasets without loading everything into memory
- 📤 **Upload Datasets** - Publish your own datasets to Hugging Face Hub
- 🔒 **Private Repositories** - Full support for authentication and private datasets
- 🎯 **Multiple Formats** - Support for CSV, Parquet, and JSONL files

## 📦 Installation

Add `elixir_datasets` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:elixir_datasets, "~> 0.1.0"}
  ]
end
```

## 🚀 Quick Start

```elixir
{:ok, [train_df]} = ElixirDatasets.load_dataset(
  {:hf, "cornell-movie-review-data/rotten_tomatoes"},
  split: "train"
)

{:ok, datasets} = ElixirDatasets.load_dataset({:local, "./data"})

{:ok, stream} = ElixirDatasets.load_dataset(
  {:hf, "stanfordnlp/imdb", subdir: "plain_text"},
  split: "train",
  streaming: true
)

stream |> Enum.take(100) |> IO.inspect()
```

## 📚 Examples

All examples can be found in the [examples](examples) directory.
- `examples/usage_examples.livemd` - Comprehensive usage examples of the elixir_datasets api
- `examples/integration_examples.livemd` - Examples demonstrating integration with other Elixir libraries like [Nx](https://github.com/elixir-nx/nx), [Axon](https://github.com/elixir-nx/axon), and [Bumblebee](https://github.com/elixir-nx/bumblebee)

## 🔧 Configuration

### Environment Variables

- `ELIXIR_DATASETS_CACHE_DIR` - Custom cache directory
- `ELIXIR_DATASETS_OFFLINE` - Enable offline mode (`"1"` or `"true"`)
- `HF_TOKEN` - Authentication token for private datasets
- [🚧 In-progress] `HF_DEBUG` - Enable debug logging (`"1"` or `"true"`)

## 📖 Documentation

Full documentation is available at [HexDocs](https://hexdocs.pm/elixir_datasets) and hosted on [GitHub Pages](https://radoslawrolka.github.io/ElixirDatasets/api-reference.html) for current status of under-development features. Documentation can be generated locally using:

```bash
mix docs
```

## 🧪 Testing

```bash
MIX_ENV=test mix test
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

Copyright (c) 2025 Radosław Rolka, Weronika Wojtas

---

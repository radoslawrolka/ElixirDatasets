# ElixirDatasets

[![Hex.pm](https://img.shields.io/hexpm/v/elixir_datasets.svg)](https://hex.pm/packages/elixir_datasets)
[![Documentation](https://img.shields.io/badge/docs-hexdocs-blue.svg)](https://hexdocs.pm/elixir_datasets)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**ElixirDatasets** is a comprehensive library for accessing and managing datasets from Hugging Face Hub in Elixir. Inspired by the Python `datasets` library, it brings powerful dataset management capabilities to the Elixir ecosystem with seamless integration with Explorer DataFrames.

## ✨ Features

- 🚀 **Easy Access to Hugging Face Hub** - Load thousands of datasets with a single function call
- 📊 **Explorer Integration** - Automatic conversion to Explorer DataFrames for data manipulation
- ⚡ **High Performance** - Parallel processing support for loading multiple files
- 💾 **Smart Caching** - Intelligent local caching to avoid redundant downloads
- 🌊 **Streaming Support** - Process large datasets without loading everything into memory
- 📤 **Upload Datasets** - Publish your own datasets to Hugging Face Hub
- 🔒 **Private Repositories** - Full support for authentication and private datasets
- 🔌 **Offline Mode** - Work with cached datasets without internet connection
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
# Load a dataset from Hugging Face
{:ok, [train_df]} = ElixirDatasets.load_dataset(
  {:hf, "cornell-movie-review-data/rotten_tomatoes"},
  split: "train"
)

# Load from local directory
{:ok, datasets} = ElixirDatasets.load_dataset({:local, "./data"})

# Stream large datasets without loading into memory
{:ok, stream} = ElixirDatasets.load_dataset(
  {:hf, "stanfordnlp/imdb", subdir: "plain_text"},
  split: "train",
  streaming: true
)

stream |> Enum.take(100) |> Enum.each(&process_row/1)
```

## 📚 Examples

### Text Classification with Sentiment Analysis

```elixir
# Load training data
{:ok, [train_df]} = ElixirDatasets.load_dataset(
  {:hf, "cornell-movie-review-data/rotten_tomatoes"},
  split: "train"
)

# Explore the data with Explorer
require Explorer.DataFrame, as: DF

train_df
|> DF.head(5)
|> IO.inspect()

# Get dataset metadata
{:ok, splits} = ElixirDatasets.get_dataset_split_names(
  "cornell-movie-review-data/rotten_tomatoes"
)
IO.inspect(splits)  # ["train", "validation", "test"]
```

### Streaming Large Datasets

```elixir
# Stream data without loading everything into memory
{:ok, stream} = ElixirDatasets.load_dataset(
  {:hf, "stanfordnlp/imdb", subdir: "plain_text"},
  split: "train",
  streaming: true
)

# Process data progressively
stream
|> Stream.filter(fn row -> String.length(row["text"]) > 100 end)
|> Stream.take(1000)
|> Enum.each(&process_review/1)
```

### Working Offline

```elixir
# Download once
{:ok, _} = ElixirDatasets.load_dataset(
  {:hf, "cornell-movie-review-data/rotten_tomatoes"},
  split: "train"
)

# Use cached version offline
{:ok, [data]} = ElixirDatasets.load_dataset(
  {:hf, "cornell-movie-review-data/rotten_tomatoes"},
  split: "train",
  offline: true
)
```

## 🔧 Configuration

### Environment Variables

- `ELIXIR_DATASETS_CACHE_DIR` - Custom cache directory
- `ELIXIR_DATASETS_OFFLINE` - Enable offline mode (`"1"` or `"true"`)
- `HF_TOKEN` - Authentication token for private datasets

### Common Options

```elixir
# Load specific split
ElixirDatasets.load_dataset({:hf, "dataset"}, split: "train")

# Stream large datasets
ElixirDatasets.load_dataset({:hf, "dataset"}, streaming: true)

ElixirDatasets.load_dataset({:hf, "dataset"}, num_proc: 4)

ElixirDatasets.load_dataset({:hf, "dataset"}, offline: true)

ElixirDatasets.load_dataset({:hf, "dataset"}, download_mode: :force_redownload)
```

See the [full documentation](https://hexdocs.pm/elixir_datasets) for all available options.

## 🔗 Integration with Elixir ML Ecosystem

Works seamlessly with Explorer, Nx, Axon, and Bumblebee:

```elixir
{:ok, [train_df]} = ElixirDatasets.load_dataset(
  {:hf, "cornell-movie-review-data/rotten_tomatoes"},
  split: "train"
)

require Explorer.DataFrame, as: DF
train_df |> DF.filter(label == 1) |> DF.head(10)

texts = DF.pull(train_df, "text")
labels = DF.pull(train_df, "label") |> Nx.tensor()

{:ok, tokenizer} = Bumblebee.load_tokenizer({:hf, "bert-base-uncased"})
inputs = Bumblebee.apply_tokenizer(tokenizer, texts)
```

## 📖 Documentation

Full documentation is available at [HexDocs](https://hexdocs.pm/elixir_datasets).

## 📓 Interactive Examples

Explore interactive examples in Livebook: `examples/usage_examples.livemd`

```bash
mix escript.install hex livebook

livebook server examples/usage_examples.livemd
```

The notebook includes examples for loading, streaming, parallel processing, and uploading datasets.

## 🧪 Testing

```bash
mix test

mix coveralls

mix test test/elixir_datasets_test.exs
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

Copyright (c) 2025 Radosław Rolka, Weronika Wojtas

## 🙏 Acknowledgments

- Inspired by [Hugging Face Datasets](https://github.com/huggingface/datasets)
- Built with [Explorer](https://github.com/elixir-nx/explorer) for DataFrame operations
- Uses [Req](https://github.com/wojtekmach/req) for HTTP requests

## 📞 Support

- 📚 [Documentation](https://hexdocs.pm/elixir_datasets)
- 🐛 [Issue Tracker](https://github.com/yourusername/elixir_datasets/issues)
- 💬 [Discussions](https://github.com/yourusername/elixir_datasets/discussions)

---

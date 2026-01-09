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

### Load a Dataset from Hugging Face

```elixir
# Load the IMDB dataset
{:ok, dataset} = ElixirDatasets.load_dataset({:hf, "imdb"})

# Load a specific split
{:ok, train_data} = ElixirDatasets.load_dataset(
  {:hf, "imdb"},
  split: "train"
)

# Load a specific configuration
{:ok, dataset} = ElixirDatasets.load_dataset(
  {:hf, "glue"},
  name: "sst2",
  split: "train"
)
```

### Stream Large Datasets

```elixir
# Stream data without loading everything into memory
{:ok, stream} = ElixirDatasets.load_dataset(
  {:hf, "c4"},
  split: "train",
  streaming: true
)

# Process first 1000 rows
stream
|> Enum.take(1000)
|> Enum.each(&process_row/1)
```

### Parallel Loading for Performance

```elixir
# Use all CPU cores for faster loading
{:ok, dataset} = ElixirDatasets.load_dataset(
  {:hf, "multi-file-dataset"},
  num_proc: System.schedulers_online()
)
```

### Upload Your Own Dataset

```elixir
# Create a DataFrame
df = Explorer.DataFrame.new(%{
  id: [1, 2, 3],
  text: ["Hello", "World", "!"],
  label: [0, 1, 0]
})

# Upload to Hugging Face
{:ok, _response} = ElixirDatasets.upload_dataset(
  df,
  "username/my-dataset",
  file_extension: "parquet",
  commit_message: "Initial upload",
  auth_token: System.get_env("HF_TOKEN")
)
```

### Work with Local Files

```elixir
# Load from local directory
{:ok, dataset} = ElixirDatasets.load_dataset(
  {:local, "./data"},
  split: "train"
)
```

## 📚 Examples

### Example 1: Text Classification with GLUE

```elixir
# Load SST-2 sentiment classification dataset
{:ok, train} = ElixirDatasets.load_dataset(
  {:hf, "glue"},
  name: "sst2",
  split: "train"
)

# Explore the data
IO.inspect(Explorer.DataFrame.head(train, 5))

# Filter positive examples
positive = Explorer.DataFrame.filter(train, label == 1)

# Get statistics
stats = Explorer.DataFrame.summarise(train,
  total: count(label),
  positive: sum(label)
)
```

### Example 2: Streaming Large Dataset

```elixir
# Stream Wikipedia dataset
{:ok, stream} = ElixirDatasets.load_dataset(
  {:hf, "wikipedia"},
  name: "20220301.en",
  split: "train",
  streaming: true
)

# Process in batches
stream
|> Stream.chunk_every(100)
|> Stream.each(fn batch ->
  # Process batch
  batch |> Enum.each(&analyze_text/1)
end)
|> Stream.run()
```

### Example 3: Offline Mode

```elixir
# First, download the dataset
{:ok, _} = ElixirDatasets.load_dataset({:hf, "imdb"})

# Later, work offline
System.put_env("ELIXIR_DATASETS_OFFLINE", "1")

{:ok, dataset} = ElixirDatasets.load_dataset(
  {:hf, "imdb"},
  download_mode: :reuse_dataset_if_exists
)
```

## 🔧 Configuration

### Environment Variables

- `ELIXIR_DATASETS_CACHE_DIR` - Custom cache directory (default: system cache)
- `ELIXIR_DATASETS_OFFLINE` - Enable offline mode (`"1"` or `"true"`)
- `HUGGING_FACE_HUB_TOKEN` - Authentication token for private datasets

### Cache Management

```elixir
# Get cache directory
cache_dir = ElixirDatasets.cache_dir()

# Force redownload
{:ok, dataset} = ElixirDatasets.load_dataset(
  {:hf, "dataset_name"},
  download_mode: :force_redownload
)

# Skip verification for faster loading
{:ok, dataset} = ElixirDatasets.load_dataset(
  {:hf, "dataset_name"},
  verification_mode: :no_checks
)
```

## 🆚 Comparison with Python `datasets`

| Feature | ElixirDatasets | Python `datasets` |
|---------|----------------|-------------------|
| Load from Hugging Face Hub | ✅ | ✅ |
| Streaming | ✅ | ✅ |
| Caching | ✅ | ✅ |
| Parallel Processing | ✅ | ✅ |
| Upload to Hub | ✅ | ✅ |
| Multiple Formats (CSV, Parquet, JSONL) | ✅ | ✅ |
| Offline Mode | ✅ | ✅ |
| Private Datasets | ✅ | ✅ |
| DataFrame Integration | ✅ (Explorer) | ✅ (Pandas/Polars) |
| Map/Filter Operations | ⚠️ (via Explorer) | ✅ |
| Custom Dataset Scripts | ❌ | ✅ |
| Audio/Image Processing | ❌ | ✅ |
| Metrics | ❌ | ✅ |

**Legend:** ✅ Fully Supported | ⚠️ Partial Support | ❌ Not Supported

### What's Supported

ElixirDatasets focuses on core dataset loading and management features:
- ✅ Loading datasets from Hugging Face Hub
- ✅ Streaming for large datasets
- ✅ Parallel processing with `num_proc`
- ✅ Smart caching and offline mode
- ✅ Upload and manage datasets
- ✅ CSV, Parquet, and JSONL formats
- ✅ Integration with Explorer DataFrames

### What's Different

- **DataFrame Library**: Uses Explorer instead of Pandas
- **Data Processing**: Leverage Explorer's powerful API for transformations
- **Concurrency**: Built on Elixir's process model for true parallelism
- **Simplicity**: Focused API without custom dataset scripts

## 🔗 Integration with Elixir ML Ecosystem

### Axon (Neural Networks)

```elixir
# Load dataset
{:ok, train} = ElixirDatasets.load_dataset({:hf, "mnist"})

# Convert to Nx tensors for Axon
train_tensors = train
|> Explorer.DataFrame.to_rows()
|> Enum.map(fn row ->
  {Nx.tensor(row["image"]), Nx.tensor(row["label"])}
end)

# Train with Axon
model = Axon.input("input", shape: {nil, 784})
|> Axon.dense(128, activation: :relu)
|> Axon.dense(10, activation: :softmax)
```

### Bumblebee (Transformers)

```elixir
# Load text dataset
{:ok, dataset} = ElixirDatasets.load_dataset({:hf, "imdb"}, split: "train")

# Load Bumblebee model
{:ok, model_info} = Bumblebee.load_model({:hf, "bert-base-uncased"})
{:ok, tokenizer} = Bumblebee.load_tokenizer({:hf, "bert-base-uncased"})

# Process dataset
texts = Explorer.DataFrame.pull(dataset, "text")
inputs = Bumblebee.apply_tokenizer(tokenizer, texts)
```

### Nx (Numerical Computing)

```elixir
# Load numerical dataset
{:ok, dataset} = ElixirDatasets.load_dataset({:hf, "california_housing"})

# Convert to Nx tensors
features = dataset
|> Explorer.DataFrame.select(["feature1", "feature2", "feature3"])
|> Explorer.DataFrame.to_columns()
|> Map.values()
|> Enum.map(&Nx.tensor/1)
|> Nx.stack()
```

## 📖 Documentation

Full documentation is available at [HexDocs](https://hexdocs.pm/elixir_datasets).

### Key Modules

- `ElixirDatasets` - Main API for loading and managing datasets
- `ElixirDatasets.DatasetInfo` - Dataset metadata management
- `ElixirDatasets.Utils.Loader` - File loading utilities
- `ElixirDatasets.Utils.Uploader` - Upload functionality
- `ElixirDatasets.HuggingFace.Hub` - Hugging Face Hub integration

## 🧪 Testing

```bash
# Run all tests
mix test

# Run with coverage
mix coveralls

# Run specific test file
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

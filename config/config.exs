import Config

config :elixir_datasets,
  hf_token: System.get_env("HF_TOKEN"),
  progress_bar_enabled: true

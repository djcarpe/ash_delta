defmodule AshDelta.MixProject do
  use Mix.Project

  def project do
    [
      app: :ash_delta,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description:
        "Delta Lake-style table format for Ash: Parquet on S3, transaction log and data-skipping stats in Postgres."
    ]
  end

  def application do
    [extra_applications: [:logger], mod: {AshDelta.Application, []}]
  end

  defp deps do
    [
      {:ash, "~> 3.0"},
      {:spark, "~> 2.0"},
      {:ecto_sql, "~> 3.11"},
      {:postgrex, "~> 0.19"},
      # Parquet encode/decode + DataFrame ops (Polars)
      {:explorer, "~> 0.10"},
      # Columnar scans over S3 Parquet with predicate pushdown
      {:duckdbex, "~> 0.3"},
      # S3 object deletion for VACUUM
      {:ex_aws, "~> 2.5"},
      {:ex_aws_s3, "~> 2.5"},
      {:jason, "~> 1.4"},
      # UUIDv7 for time-ordered Parquet object names
      {:uniq, "~> 0.6"}
    ]
  end
end

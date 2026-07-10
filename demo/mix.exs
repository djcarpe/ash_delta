defmodule DeltaDemo.MixProject do
  use Mix.Project

  def project do
    [
      app: :delta_demo,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      mod: {DeltaDemo.Application, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:ash_delta, path: ".."},
      # Pinned so the precompiled Polars NIF matches the tarball vendored in
      # nif-cache/ (Docker builds pre-seed it to avoid a GitHub CDN download).
      {:explorer, "0.11.1"},
      {:ash, "~> 3.0"},
      {:ash_graphql, "~> 1.4"},
      {:absinthe_plug, "~> 1.5"},
      {:bandit, "~> 1.5"},
      # 3.14 requires decimal ~> 3.0, which conflicts with explorer 0.11.x.
      {:ecto_sql, "~> 3.13.0"},
      {:postgrex, "~> 0.19"},
      {:jason, "~> 1.4"}
    ]
  end
end

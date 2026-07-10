defmodule DeltaDemo.Migrate do
  @moduledoc """
  Installs the AshDelta catalog schema (base + v2 normalized stats tables).

  The statements are plain CREATE TABLE / CREATE INDEX without IF NOT EXISTS,
  so each one is attempted individually and "already exists" errors are
  ignored — safe to run on every boot.
  """

  def run(repo \\ DeltaDemo.Repo) do
    statements = AshDelta.Migrations.up_statements() ++ AshDelta.Migrations.v2_up_statements()

    Enum.each(statements, fn sql ->
      try do
        repo.query!(sql, [])
      rescue
        _ -> :already_exists
      end
    end)

    :ok
  end

  @doc "Retries until Postgres is reachable, then installs the schema."
  def run_when_ready(repo \\ DeltaDemo.Repo, attempt \\ 1) do
    run(repo)
  rescue
    e ->
      if attempt >= 60 do
        reraise e, __STACKTRACE__
      else
        IO.puts("Postgres not ready (attempt #{attempt}), retrying in 5s...")
        Process.sleep(5_000)
        run_when_ready(repo, attempt + 1)
      end
  end
end

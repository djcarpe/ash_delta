defmodule AshDelta.ConnectionPool do
  @moduledoc """
  One DuckDB database per resource, shared across all queries.

  `Duckdbex.open/0` creates a fresh in-memory database and triggers
  extension installation on first use (~200-400 ms cold). Caching one
  database per resource module eliminates that cost from every query after
  the first. Individual connections are cheap (~1 ms) and created per-call
  from the cached database; each connection gets its own session settings
  (thread cap) and the S3 secret is set idempotently via CREATE OR REPLACE.

  ETS provides lock-free reads for the common hot path. The GenServer
  serialises database creation so that concurrent first-callers don't race.
  """

  use GenServer

  alias AshDelta.Info

  @table __MODULE__

  def start_link(_opts), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  @doc """
  Returns a DuckDB connection ready for queries against `resource`'s S3 files.
  Callers own the connection and should not share it across processes.
  """
  def checkout(resource) do
    db = get_db(resource)
    {:ok, s3_config} = Info.s3_config(resource)

    with {:ok, conn} <- Duckdbex.connection(db) do
      setup_connection(conn, s3_config)
    end
  end

  @doc """
  Removes the cached DuckDB database for `resource` so the next `checkout/1`
  opens a fresh database. Used by benchmarks to force a cold-start condition.
  """
  def evict(resource) do
    :ets.delete(@table, resource)
    :ok
  end

  # ── GenServer ─────────────────────────────────────────────────────────────

  @impl true
  def init(:ok) do
    :ets.new(@table, [:named_table, :public, read_concurrency: true])
    {:ok, :ok}
  end

  @impl true
  def handle_call({:get_or_create, resource}, _from, state) do
    db =
      case :ets.lookup(@table, resource) do
        [{_, db}] ->
          db

        [] ->
          {:ok, db} = Duckdbex.open()
          :ets.insert(@table, {resource, db})
          db
      end

    {:reply, db, state}
  end

  # ── Private ───────────────────────────────────────────────────────────────

  defp get_db(resource) do
    case :ets.lookup(@table, resource) do
      [{_, db}] -> db
      [] -> GenServer.call(__MODULE__, {:get_or_create, resource}, 30_000)
    end
  end

  @doc false
  def setup_connection(conn, s3_config) do
    Duckdbex.query(
      conn,
      "SET autoinstall_known_extensions=1; SET autoload_known_extensions=1; SET threads=4;"
    )

    {endpoint_val, extra_fields} =
      case s3_config[:endpoint] do
        "http://" <> rest -> {rest, [{"USE_SSL", "false"}, {"URL_STYLE", "path"}]}
        "https://" <> rest -> {rest, []}
        other -> {other, []}
      end

    secret_fields =
      [
        {"KEY_ID", s3_config[:access_key_id]},
        {"SECRET", s3_config[:secret_access_key]},
        {"REGION", s3_config[:region]},
        {"ENDPOINT", endpoint_val}
      ]
      |> Enum.reject(fn {_, v} -> is_nil(v) end)
      |> Kernel.++(extra_fields)

    case secret_fields do
      [] ->
        Duckdbex.query(conn, "CREATE OR REPLACE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN);")

      fields ->
        assignments = Enum.map_join(fields, ", ", fn {k, v} -> "#{k} '#{v}'" end)
        Duckdbex.query(conn, "CREATE OR REPLACE SECRET (TYPE S3, #{assignments});")
    end

    {:ok, conn}
  end
end

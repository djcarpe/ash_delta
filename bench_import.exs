################################################################################
# bench_import.exs
#
# Demonstrates wrapping an EXISTING S3 bucket of Parquet files with an Ash
# resource via AshDelta.import_existing/1.
#
# Data is generated entirely inside DuckDB using its synthetic-data SQL
# (range() + hash() + random()) and written directly to SeaweedFS as Parquet
# via DuckDB's COPY command — bypassing AshDelta's writer entirely.  This
# simulates a data lake that was created by Spark, Flink, or another writer.
#
# After generation, AshDelta.import_existing/1 runs a single DuckDB glob scan
# to discover all files, collect row counts and column stats, and commit them
# into the catalog in one pass.  From that point on the data is a first-class
# Ash resource: filterable, time-travelable, compactable, query-able.
#
# Dataset: 10 sites × 10 days = 100 partitions × 100 000 rows = 10 000 000 rows
#
# Usage:
#   mix run bench_import.exs
################################################################################

Application.put_env(:ash, :validate_domain_resource_inclusion?, false)
Application.put_env(:ash, :validate_domain_config_inclusion?, false)
Logger.configure(level: :warning)

Application.put_env(:import_bench, ImportBench.Repo,
  url: "postgres://postgres:postgres@localhost:5432/ash_delta_demo",
  pool_size: 5
)

Application.put_env(:ex_aws, :access_key_id, "any")
Application.put_env(:ex_aws, :secret_access_key, "any")
Application.put_env(:ex_aws, :region, "us-east-1")
Application.put_env(:ex_aws, :s3, scheme: "http://", host: "localhost", port: 8333)

defmodule ImportBench.Repo do
  use Ecto.Repo, otp_app: :import_bench, adapter: Ecto.Adapters.Postgres
end

{:ok, _} = ImportBench.Repo.start_link()

# ── The wrapped resource ───────────────────────────────────────────────────────
#
# This resource points at "raw" Parquet files that were NOT written by AshDelta.
# The schema must match the column names in those files.

defmodule ImportBench.SensorReading do
  use Ash.Resource, domain: ImportBench.Domain, data_layer: AshDelta.DataLayer

  delta do
    repo    ImportBench.Repo
    bucket  "demo-lake"
    name    "import_bench_sensor"
    prefix  "external/raw"
    partition_by    [:site, :day]
    stats_columns   [:recorded_at, :value]
    sort_within_files [:value]
    s3_config access_key_id:     "any",
              secret_access_key: "any",
              region:            "us-east-1",
              endpoint:          "http://localhost:8333"
  end

  attributes do
    attribute :id, :string, primary_key?: true, writable?: false, allow_nil?: false, public?: true
    attribute :site,        :string,            allow_nil?: false, public?: true
    attribute :day,         :date,              allow_nil?: false, public?: true
    attribute :sensor_id,   :string,            public?: true
    attribute :recorded_at, :utc_datetime_usec, public?: true
    attribute :value,       :float,             public?: true
  end

  actions do
    defaults [:read]
  end
end

defmodule ImportBench.Domain do
  use Ash.Domain, validate_config_inclusion?: false
  resources do
    resource ImportBench.SensorReading
  end
end

require Ash.Query

try do
  AshDelta.Migrations.run(ImportBench.Repo)
rescue
  _ -> :ok
end

# ── Dataset plan ───────────────────────────────────────────────────────────────
#
# 10 sites × 10 days = 100 Parquet files × 100 000 rows = 10 000 000 rows
# Each file is written by a single DuckDB COPY statement using range() + hash()
# and random(), then uploaded directly to SeaweedFS.

sites    = Enum.map(1..10, &"SITE#{String.pad_leading(to_string(&1), 2, "0")}")
days     = Enum.map(0..9,  &Date.add(~D[2026-01-01], &1))
rows_per = 100_000

expected_files = length(sites) * length(days)  # 100

# Set to true to wipe catalog + overwrite S3 files even when expected_files
# matches; useful after changing the generation SQL.
# NOTE: set to true once to regenerate files with the fixed value formula
# (10.0 + random() * 40.0 instead of the broken hash() version).
# After regeneration set back to false for fast warm re-runs.
force_regen = true

# ── Catalog reset helper ───────────────────────────────────────────────────────

defmodule ImportBench.Catalog do
  def wipe(repo) do
    repo.transaction(fn ->
      %{rows: [[id]]} =
        repo.query!(
          "INSERT INTO delta_tables (resource, bucket, prefix)
           VALUES ($1, $2, $3)
           ON CONFLICT (resource) DO UPDATE SET bucket = EXCLUDED.bucket
           RETURNING id",
          ["import_bench_sensor", "demo-lake", "external/raw"]
        )
      repo.query!("DELETE FROM delta_files   WHERE table_id = $1", [id])
      repo.query!("DELETE FROM delta_commits WHERE table_id = $1", [id])
      repo.query!("UPDATE delta_tables SET current_version = 0 WHERE id = $1", [id])
    end)
  end
end

# ── Phase 1: generate raw Parquet files via DuckDB ─────────────────────────────
#
# Uses DuckDB's synthetic data primitives (range, hash, random) to generate
# rows entirely inside DuckDB and write them to S3 as Parquet — no Elixir
# row-by-row generation, no AshDelta writer.
#
# COPY (...) TO 's3://bucket/key.parquet' (FORMAT PARQUET) uses the httpfs
# extension and S3 secret already configured on the connection.

existing = AshDelta.snapshot(ImportBench.SensorReading)

need_generate =
  if !force_regen and length(existing) == expected_files do
    total = Enum.sum(Enum.map(existing, & &1.row_count))
    IO.puts("── existing data found ───────────────────────────────────────────────")
    IO.puts("   #{length(existing)} files / #{total} rows — skipping generation\n")
    false
  else
    true
  end

if need_generate do
  IO.puts("── generating synthetic Parquet via DuckDB ───────────────────────────")
  IO.puts("   #{expected_files} files × #{rows_per} rows = #{expected_files * rows_per} rows")
  IO.puts("   writing directly to s3://demo-lake/external/raw/ (no AshDelta writer)")

  ImportBench.Catalog.wipe(ImportBench.Repo)
  {:ok, conn} = AshDelta.ConnectionPool.checkout(ImportBench.SensorReading)

  t0 = System.monotonic_time(:millisecond)

  for {site, s_idx} <- Enum.with_index(sites, 1),
      {day, d_idx}  <- Enum.with_index(days, 0) do
    # Deterministic seed per partition so re-runs produce the same data.
    seed = s_idx * 100 + d_idx
    path = "s3://demo-lake/external/raw/site=#{site}/day=#{Date.to_iso8601(day)}/data.parquet"

    sql = """
    COPY (
      SELECT
        hash((range + #{seed * rows_per})::BIGINT)::VARCHAR AS id,
        '#{site}'::VARCHAR                                   AS site,
        DATE '#{Date.to_iso8601(day)}'                       AS day,
        's' || ((range % 20) + 1)::VARCHAR                  AS sensor_id,
        (TIMESTAMP '#{Date.to_iso8601(day)}' +
          to_seconds((range % 86400) + random() * 60))      AS recorded_at,
        10.0 + random() * 40.0                              AS value
      FROM range(#{rows_per}) t(range)
      ORDER BY value
    ) TO '#{path}' (FORMAT PARQUET)
    """

    case Duckdbex.query(conn, sql, []) do
      {:ok, _}         -> IO.write(".")
      {:error, reason} -> IO.puts("\n   ERROR on #{path}: #{inspect(reason)}")
    end
  end

  elapsed = System.monotonic_time(:millisecond) - t0
  rate    = div(expected_files * rows_per, max(div(elapsed, 1000), 1))
  IO.puts("\n   done in #{div(elapsed, 1000)}s  (~#{rate} rows/s)")

  # ── Phase 2: import ────────────────────────────────────────────────────────
  #
  # AshDelta.import_existing runs one DuckDB glob scan across all 100 files,
  # computes per-file row_count + min/max stats, parses hive partition values
  # from directory names, and commits everything as a single "import" version.

  IO.puts("\n── importing into AshDelta catalog ──────────────────────────────────")
  ti = System.monotonic_time(:millisecond)

  case AshDelta.import_existing(ImportBench.SensorReading) do
    {:ok, version} ->
      elapsed_i = System.monotonic_time(:millisecond) - ti
      snap       = AshDelta.snapshot(ImportBench.SensorReading)
      total      = Enum.sum(Enum.map(snap, & &1.row_count))
      IO.puts("   imported #{length(snap)} files / #{total} rows → version #{version} (#{elapsed_i}ms)")

    {:error, reason} ->
      IO.puts("   IMPORT FAILED: #{inspect(reason)}")
      System.halt(1)
  end
end

snap       = AshDelta.snapshot(ImportBench.SensorReading)
total_rows = Enum.sum(Enum.map(snap, & &1.row_count))
IO.puts("\n   catalog: #{length(snap)} files, #{total_rows} rows\n")

# ── Pre-built queries ──────────────────────────────────────────────────────────

first_site = hd(sites)
first_day  = hd(days)

# 1 file / 100k rows
q_1 = Ash.Query.filter(
  ImportBench.SensorReading,
  site == ^first_site and day == ^first_day
)

# 10 files / 1M rows  (one site, all days)
q_10 = Ash.Query.filter(ImportBench.SensorReading, site == ^first_site)

# 1 file, top-50 only
q_top = q_1 |> Ash.Query.sort(recorded_at: :asc) |> Ash.Query.limit(50)

# 10 files, stats-prune on value (roughly half the files skipped: value range 10-50)
q_prune = Ash.Query.filter(ImportBench.SensorReading, site == ^first_site and value > 45.0)

# 10 files, projection: only 2 of 6 columns (site + value)
q_proj = Ash.Query.filter(ImportBench.SensorReading, site == ^first_site)
         |> Ash.Query.select([:site, :value])

# ── Cold-start benchmarks ──────────────────────────────────────────────────────
#
# Each iteration evicts the DuckDB database from the pool, forcing a full cold
# start: Duckdbex.open() (~200ms) + httpfs extension load + S3 auth setup +
# actual query. Measures the worst-case latency (process restart / k8s pod cold
# start / first query after a long idle period).
#
# Note: full-table scans (all 100 files / 10M rows) are slow because Ash.read!
# materialises every row into an Elixir struct. For large result sets, use
# Ash.stream!/2 which drives offset-based pagination and keeps one page in
# memory at a time. Demonstrated below with the 10M-row query.

IO.puts("── cold-start reads ──────────────────────────────────────────────────")

Benchee.run(
  %{
    "cold  1 file  / 100 000 rows (1 partition)" => {
      fn _ -> Ash.read!(q_1) end,
      before_each: fn _ -> AshDelta.ConnectionPool.evict(ImportBench.SensorReading) end
    },
    "cold 10 files / 1 000 000 rows (1 site)" => {
      fn _ -> Ash.read!(q_10) end,
      before_each: fn _ -> AshDelta.ConnectionPool.evict(ImportBench.SensorReading) end
    },
    "cold  1 file  / top-50 via LIMIT (cold)" => {
      fn _ -> Ash.read!(q_top) end,
      before_each: fn _ -> AshDelta.ConnectionPool.evict(ImportBench.SensorReading) end
    },
  },
  warmup: 0,
  time: 30,
  memory_time: 0,
  print: [fast_warning: false]
)

# ── Sustained (warm) reads ─────────────────────────────────────────────────────
#
# DuckDB database already cached. Each call opens a cheap new connection (~1ms),
# applies session settings, and runs the scan. Demonstrates steady-state
# throughput after the first query warms the connection pool.
#
# For the 10M-row full scan, Ash.stream!/2 keeps memory flat by paginating:
#   ImportBench.SensorReading |> Ash.stream!(page_size: 10_000) |> Stream.run()
# This is the recommended pattern for ETL / export use cases.

IO.puts("\n── sustained (warm) reads ────────────────────────────────────────────")

q_stream_page =
  ImportBench.SensorReading
  |> Ash.Query.filter(site in ^sites)
  |> Ash.Query.sort([:site, :day])
  |> Ash.Query.limit(10_000)

Benchee.run(
  %{
    "warm  1 file  / 100 000 rows (1 partition)" =>
      fn -> Ash.read!(q_1) end,

    "warm 10 files / 1 000 000 rows (1 site, all cols)" =>
      fn -> Ash.read!(q_10) end,

    "warm 10 files / 1 000 000 rows (2 of 6 cols, projection)" =>
      fn -> Ash.read!(q_proj) end,

    "warm  1 file  / top-50 via LIMIT" =>
      fn -> Ash.read!(q_top) end,

    "warm 10 files / stats prune value > 45" =>
      fn -> Ash.read!(q_prune) end,

    "warm all files / 10 000 rows via page" =>
      fn -> Ash.read!(q_stream_page) end,

    "warm 10 files / count(*) aggregate (no row materialize)" =>
      fn ->
        Ash.count!(q_10)
      end,

    "warm 10 files / Explorer.DataFrame (no struct alloc)" =>
      fn -> AshDelta.to_dataframe!(q_10) end,
  },
  warmup: 3,
  time: 15,
  memory_time: 2,
  print: [fast_warning: false]
)

IO.puts("\n── done ✓ ────────────────────────────────────────────────────────────")

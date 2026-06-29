Application.put_env(:ash, :validate_domain_resource_inclusion?, false)
Application.put_env(:ash, :validate_domain_config_inclusion?, false)
Logger.configure(level: :warning)

Application.put_env(:bench, Bench.Repo,
  url: "postgres://postgres:postgres@localhost:5432/ash_delta_demo",
  pool_size: 5
)

Application.put_env(:ex_aws, :access_key_id, "any")
Application.put_env(:ex_aws, :secret_access_key, "any")
Application.put_env(:ex_aws, :region, "us-east-1")
Application.put_env(:ex_aws, :s3, scheme: "http://", host: "localhost", port: 8333)

defmodule Bench.Repo do
  use Ecto.Repo, otp_app: :bench, adapter: Ecto.Adapters.Postgres
end

{:ok, _} = Bench.Repo.start_link()

defmodule Bench.Event do
  use Ash.Resource, domain: Bench.Domain, data_layer: AshDelta.DataLayer

  delta do
    repo    Bench.Repo
    bucket  "demo-lake"
    name    "bench_event"
    prefix  "bench/perf"
    partition_by   [:site, :day]
    sort_within_files [:sensor_id, :recorded_at]
    stats_columns [:recorded_at, :value]
    s3_config access_key_id:     "any",
              secret_access_key: "any",
              region:            "us-east-1",
              endpoint:          "http://localhost:8333"
  end

  attributes do
    uuid_primary_key :id
    attribute :site,        :string,            allow_nil?: false, public?: true
    attribute :day,         :date,              allow_nil?: false, public?: true
    attribute :sensor_id,   :string,            public?: true
    attribute :recorded_at, :utc_datetime_usec, public?: true
    attribute :value,       :float,             public?: true
  end

  actions do
    defaults [:read, :create]
    default_accept [:site, :day, :sensor_id, :recorded_at, :value]
  end
end

defmodule Bench.Domain do
  use Ash.Domain, validate_config_inclusion?: false
  resources do
    resource Bench.Event
  end
end

require Ash.Query

try do
  AshDelta.Migrations.run(Bench.Repo)
rescue
  _ -> :ok
end

# ── Row generator ──────────────────────────────────────────────────────────────

sensors = Enum.map(1..20, &"s#{&1}")

defmodule Bench.Gen do
  def rows(site, day, count, sensors) do
    for _ <- 1..count do
      h = :rand.uniform(24) - 1
      m = :rand.uniform(60) - 1
      s = :rand.uniform(60) - 1
      %{
        site:        site,
        day:         day,
        sensor_id:   Enum.random(sensors),
        recorded_at: DateTime.new!(day, Time.new!(h, m, s, 0), "Etc/UTC"),
        value:       :rand.uniform() * 40 + 10
      }
    end
  end
end

# ── Fixture plan ───────────────────────────────────────────────────────────────
#
# Scale    Sites × Days     Files   Rows/file   Total rows
# -----    ------------     -----   ---------   ----------
# small    1 × 1            1       2 000        2 000
# medium   4 × 5            20      2 000       40 000
# large    10 × 10          100     5 000      500 000
#
# IMPORTANT: Ash.bulk_create chunks streams into batches before calling the
# data layer. We pass `batch_size:` equal to the partition size so each call
# produces exactly one Parquet file regardless of Ash's internal page size.
# Fixture loading iterates per partition (site × day) to keep each batch
# homogeneous and predictably sized.

small_site = "SMALL"
med_sites  = Enum.map(1..4,  &"MED#{&1}")
lrg_sites  = Enum.map(1..10, &"LRG#{&1}")
days_med   = Enum.map(0..4,  &Date.add(~D[2026-01-01], &1))   # 5 days
days_lrg   = Enum.map(0..9,  &Date.add(~D[2026-01-01], &1))   # 10 days
rows_small = 2_000
rows_med   = 2_000
rows_lrg   = 5_000

expected_files =
  1 +
  length(med_sites) * length(days_med) +
  length(lrg_sites) * length(days_lrg)
# = 1 + 20 + 100 = 121

# ── Catalog setup ──────────────────────────────────────────────────────────────

defmodule Bench.Catalog do
  def reset(repo) do
    repo.transaction(fn ->
      %{rows: [[id]]} =
        repo.query!(
          "INSERT INTO delta_tables (resource, bucket, prefix)
           VALUES ($1, $2, $3)
           ON CONFLICT (resource) DO UPDATE SET bucket = EXCLUDED.bucket
           RETURNING id",
          ["bench_event", "demo-lake", "bench/perf"]
        )
      repo.query!("DELETE FROM delta_files   WHERE table_id = $1", [id])
      repo.query!("DELETE FROM delta_commits WHERE table_id = $1", [id])
      repo.query!("UPDATE delta_tables SET current_version = 0 WHERE id = $1", [id])
    end)
  end
end

existing = AshDelta.snapshot(Bench.Event)

if length(existing) == expected_files do
  total = Enum.sum(Enum.map(existing, & &1.row_count))
  IO.puts("── fixtures already loaded ───────────────────────────────────────────")
  IO.puts("   #{length(existing)} files / #{total} rows — skipping load\n")
else
  IO.puts("── resetting + loading fixtures ──────────────────────────────────────")
  IO.puts("   small  :   1 file  ×  2 000 rows =   2 000 rows")
  IO.puts("   medium :  20 files ×  2 000 rows =  40 000 rows")
  IO.puts("   large  : 100 files ×  5 000 rows = 500 000 rows")
  Bench.Catalog.reset(Bench.Repo)

  t0 = System.monotonic_time(:millisecond)

  # Each bulk_create is one partition: batch_size = rows so Ash doesn't split.
  Ash.bulk_create(
    Bench.Gen.rows(small_site, ~D[2026-01-01], rows_small, sensors),
    Bench.Event, :create,
    return_records?: false, return_errors?: false, batch_size: rows_small
  )
  IO.write("   small done | medium: ")

  for site <- med_sites, day <- days_med do
    Ash.bulk_create(
      Bench.Gen.rows(site, day, rows_med, sensors),
      Bench.Event, :create,
      return_records?: false, return_errors?: false, batch_size: rows_med
    )
    IO.write(".")
  end
  IO.write(" done | large: ")

  for site <- lrg_sites, day <- days_lrg do
    Ash.bulk_create(
      Bench.Gen.rows(site, day, rows_lrg, sensors),
      Bench.Event, :create,
      return_records?: false, return_errors?: false, batch_size: rows_lrg
    )
    IO.write(".")
  end

  elapsed = System.monotonic_time(:millisecond) - t0
  IO.puts(" done (#{div(elapsed, 1000)}s)")
end

snap       = AshDelta.snapshot(Bench.Event)
total_rows = Enum.sum(Enum.map(snap, & &1.row_count))
IO.puts("   catalog: #{length(snap)} files, #{total_rows} rows\n")

# ── Pre-built queries ──────────────────────────────────────────────────────────

# 1 file / 2 000 rows
q_1  = Ash.Query.filter(Bench.Event, site == "SMALL" and day == ^~D[2026-01-01])

# 10 files / 50 000 rows  (one LRG site × 10 days)
q_10 = Ash.Query.filter(Bench.Event, site == "LRG1")

# 50 files / 250 000 rows  (5 LRG sites × 10 days)
q_50 = Ash.Query.filter(Bench.Event, site in ^Enum.take(lrg_sites, 5))

# 100 files / 500 000 rows  (all LRG)
q_100 = Ash.Query.filter(Bench.Event, site in ^lrg_sites)

# 100 files, return only top-50 rows — DuckDB short-circuits after limit
q_top50 = q_100 |> Ash.Query.sort(recorded_at: :asc) |> Ash.Query.limit(50)

# 10 files, stats prune on value — roughly half the files should be skipped
q_prune = Ash.Query.filter(Bench.Event, site == "LRG1" and value > 45.0)

# 10 files, narrow column projection — only 2 of 5 columns fetched from Parquet
q_proj = q_10 |> Ash.Query.select([:site, :value])

# ── Cold-start benchmarks ──────────────────────────────────────────────────────
# before_each evicts the DuckDB database from the pool so every iteration
# measures: Duckdbex.open + extension load + S3 auth setup + actual query.

IO.puts("── cold-start reads ──────────────────────────────────────────────────")

Benchee.run(
  %{
    "cold   1 file  /   2 000 rows" => {
      fn _ -> Ash.read!(q_1) end,
      before_each: fn _ -> AshDelta.ConnectionPool.evict(Bench.Event) end
    },
    "cold  10 files /  50 000 rows" => {
      fn _ -> Ash.read!(q_10) end,
      before_each: fn _ -> AshDelta.ConnectionPool.evict(Bench.Event) end
    },
    "cold  50 files / 250 000 rows" => {
      fn _ -> Ash.read!(q_50) end,
      before_each: fn _ -> AshDelta.ConnectionPool.evict(Bench.Event) end
    },
    "cold 100 files / 500 000 rows" => {
      fn _ -> Ash.read!(q_100) end,
      before_each: fn _ -> AshDelta.ConnectionPool.evict(Bench.Event) end
    },
  },
  warmup: 0,
  time: 30,
  memory_time: 0,
  print: [fast_warning: false]
)

# ── Sustained (warm) read benchmarks ──────────────────────────────────────────
# DuckDB database already cached. Each iteration: cheap connection create
# (~1 ms) + session settings + actual S3 scan.

IO.puts("\n── sustained (warm) reads ────────────────────────────────────────────")

Benchee.run(
  %{
    "warm   1 file  /   2 000 rows" =>
      fn -> Ash.read!(q_1) end,

    "warm  10 files /  50 000 rows" =>
      fn -> Ash.read!(q_10) end,

    "warm  50 files / 250 000 rows" =>
      fn -> Ash.read!(q_50) end,

    "warm 100 files / 500 000 rows" =>
      fn -> Ash.read!(q_100) end,

    "warm 100 files / top-50 (LIMIT)" =>
      fn -> Ash.read!(q_top50) end,

    "warm  10 files / stats prune (value > 45)" =>
      fn -> Ash.read!(q_prune) end,

    "warm  10 files / 2-col projection" =>
      fn -> Ash.read!(q_proj) end,
  },
  warmup: 3,
  time: 15,
  memory_time: 2,
  print: [fast_warning: false]
)

# ── Write benchmarks ───────────────────────────────────────────────────────────

IO.puts("\n── writes ────────────────────────────────────────────────────────────")

rows_1k  = Bench.Gen.rows("WR", ~D[2026-06-01], 1_000,  sensors)
rows_10k = Bench.Gen.rows("WR", ~D[2026-06-01], 10_000, sensors)

Benchee.run(
  %{
    "bulk_create  1 000 rows" => fn ->
      Ash.bulk_create(rows_1k, Bench.Event, :create,
        return_records?: false, return_errors?: false, batch_size: 1_000)
    end,
    "bulk_create 10 000 rows" => fn ->
      Ash.bulk_create(rows_10k, Bench.Event, :create,
        return_records?: false, return_errors?: false, batch_size: 10_000)
    end,
  },
  warmup: 1,
  time: 20,
  print: [fast_warning: false]
)

IO.puts("\n── done ✓ ────────────────────────────────────────────────────────────")

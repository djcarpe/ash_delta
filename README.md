# AshDelta

A Delta Lake-style table format as an Ash data layer: **Parquet files in S3**
hold the data (hive-partitioned, sorted within files), **Postgres** holds the
transaction log, file manifest, and column statistics that make queries skip
most of those files.

```
                 ┌───────────────────────────────────────────────┐
   Ash.Query ───▶│ 1. Resolve version (time travel?)   Postgres  │
                 │ 2. Prune files: partition values +  delta_*   │
                 │    min/max stats (GIN/B-tree)       tables    │
                 └──────────────────┬────────────────────────────┘
                                    │ surviving file list
                 ┌──────────────────▼────────────────────────────┐
                 │ 3. DuckDB read_parquet([...]) with compiled   │
                 │    WHERE/ORDER BY/LIMIT — row-group pushdown  │
                 │    inside each file                  S3       │
                 └───────────────────────────────────────────────┘
```

## Why Postgres as the log instead of `_delta_log/`

Delta Lake's log is JSON files made atomic by object-store rename semantics,
replayed (with checkpoints) to reconstruct a snapshot. Putting the log in
Postgres buys:

- **Snapshot = one indexed query.** No log replay, no checkpoint files.
- **Real transactions for commits.** A `FOR UPDATE` lock on the table's
  version row serializes commits; Parquet uploads happen *before* the lock,
  so the critical section is microseconds of metadata writes.
- **Conflict detection for free.** Tombstoning a file requires
  `removed_version IS NULL`; if a concurrent OPTIMIZE got there first, the
  commit rolls back and the rewrite retries from a fresh snapshot.
- **Stats you can index.** Min/max live in JSONB with GIN coverage, and you
  can add expression B-tree indexes for hot columns — Delta has nothing
  comparable; it scans checkpoint Parquet for stats.
- **It joins.** File-level metadata sits next to your relational data in the
  same SQL plan, which is exactly why you'd pick this over Iceberg/Delta for
  analytical scans joined against operational tables.

## What maps to what

| Delta Lake                  | AshDelta                                      |
|-----------------------------|-----------------------------------------------|
| `_delta_log/N.json`         | `delta_commits` row per version               |
| Add/Remove file actions     | `delta_files.added_version/removed_version`   |
| Optimistic concurrency      | Per-table row lock + tombstone conflict check |
| Partition pruning           | `partition_values @> '{...}'::jsonb` (GIN)    |
| Data skipping (file stats)  | `column_stats` min/max/null_count (JSONB)     |
| Z-ordering (approximately)  | `sort_within_files` clustering                |
| `VERSION AS OF / TIMESTAMP` | `Ash.Query.set_context(%{delta: %{...}})`     |
| `OPTIMIZE`                  | `AshDelta.optimize/2`                         |
| `VACUUM`                    | `AshDelta.vacuum/2`                           |
| Checkpoints                 | Unnecessary — Postgres is materialized state  |

## Getting Started

### Requirements

- Elixir ~> 1.17
- PostgreSQL (stores the transaction log, file manifest, and column stats)
- An S3-compatible object store (AWS S3, MinIO, etc.)
- A Docker Compose file is included for running Postgres and a local S3 (MinIO) during development

### Install dependencies

```bash
docker-compose up          # starts Postgres + MinIO
mix deps.get
```

Add to your `mix.exs`:

```elixir
def deps do
  [
    {:ash_delta, "~> 0.1"},
    {:ash, "~> 3.0"},
    {:ecto_sql, "~> 3.11"},
    {:postgrex, "~> 0.19"},
    {:explorer, "~> 0.10"},   # Parquet encode/decode
    {:duckdbex, "~> 0.3"},    # columnar scans
    {:ex_aws, "~> 2.5"},
    {:ex_aws_s3, "~> 2.5"},
    {:jason, "~> 1.4"},
    {:uniq, "~> 0.6"}
  ]
end
```

### Run migrations

Generate and run the AshDelta metadata tables in your Postgres repo:

```elixir
defmodule MyApp.Repo.Migrations.InstallAshDelta do
  use Ecto.Migration
  use AshDelta.Migrations
end
```

```bash
mix ecto.migrate
```

### Configure S3 credentials

```elixir
# config/config.exs
config :ex_aws,
  access_key_id: System.get_env("AWS_ACCESS_KEY_ID"),
  secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY"),
  region: "us-east-1"
```

## Install

```elixir
def deps do
  [{:ash_delta, path: "path/to/ash_delta"}]
end
```

```elixir
defmodule MyApp.Repo.Migrations.InstallAshDelta do
  use Ecto.Migration
  use AshDelta.Migrations
end
```

## Define a resource

```elixir
defmodule MyApp.Telemetry.Event do
  use Ash.Resource,
    domain: MyApp.Telemetry,
    data_layer: AshDelta.DataLayer

  delta do
    repo MyApp.Repo
    bucket "mes-lake"
    prefix "telemetry/events"
    partition_by [:site, :day]
    sort_within_files [:station_id, :recorded_at]
    stats_columns [:recorded_at, :station_id, :unit_id]
    target_file_size_mb 128
    vacuum_retention_hours 168
  end

  attributes do
    uuid_primary_key :id
    attribute :site, :string, allow_nil?: false
    attribute :day, :date, allow_nil?: false
    attribute :station_id, :string
    attribute :unit_id, :string
    attribute :recorded_at, :utc_datetime_usec
    attribute :payload, :map
  end

  actions do
    defaults [:read, :create, :destroy]
  end
end
```

## Use it

```elixir
# Bulk ingest → one sorted Parquet file per touched partition, one commit
MyApp.Telemetry.Event
|> Ash.bulk_create!(events, :create)

# Reads prune on partitions + stats before DuckDB touches a byte
MyApp.Telemetry.Event
|> Ash.Query.filter(site == "CHI" and recorded_at > ^cutoff)
|> Ash.Query.sort(recorded_at: :desc)
|> Ash.Query.limit(500)
|> Ash.read!()

# Time travel
Event |> Ash.Query.set_context(%{delta: %{version: 42}}) |> Ash.read!()
Event |> Ash.Query.set_context(%{delta: %{as_of: ~U[2026-06-01 00:00:00Z]}}) |> Ash.read!()

# Bulk delete (copy-on-write rewrite of only the affected files)
AshDelta.delete_where(MyApp.Telemetry.Event, day: ~D[2025-01-01])

# Maintenance — schedule from Oban / Quantum / a Horde singleton
AshDelta.optimize(MyApp.Telemetry.Event)
AshDelta.vacuum(MyApp.Telemetry.Event)

# Audit
AshDelta.history(MyApp.Telemetry.Event)
AshDelta.snapshot(MyApp.Telemetry.Event, 42)
```

## Operational notes

- **Single-row `create` writes a single-row Parquet file.** That's correct
  but wasteful at rate. For ingestion paths, batch upstream (a buffering
  GenServer flushing on size/interval works well) and rely on `OPTIMIZE` to
  fix what slips through.
- **Stats column count is a real cost.** Every column listed in
  `stats_columns` is computed per file and stored per file. Same tradeoff as
  Delta's `dataSkippingNumIndexedCols` — list the columns you actually
  filter on.
- **Hot stats columns deserve expression indexes:**
  ```sql
  CREATE INDEX ON delta_files (
    ((column_stats->'recorded_at'->>'min')),
    ((column_stats->'recorded_at'->>'max'))
  );
  ```
- **Retention vs. time travel:** `vacuum` deletes data files referenced only
  by versions older than retention. Don't vacuum shorter than your longest
  `as_of` horizon.
- **Timestamps/dates compare as ISO-8601 text** in both stats pruning and
  partition keys — lexicographic order equals temporal order, so this is
  sound. Mixed-type columns are not.

## Limitations (honest list)

- **Filters:** comparison/equality/IN/is_nil over direct attributes, AND/OR/
  NOT. No relationship traversal, calculations, expressions, or aggregates
  pushed down. Unsupported filters raise rather than over-return.
- **Updates** are static set-value only (no atomic/expression updates) —
  same copy-on-write rewrite as delete.
- **No transactions across actions** (`can?(:transact) == false`); each
  commit is individually atomic, which is the Delta model.
- **Schema evolution** is append-friendly only: `read_parquet(...,
  union_by_name)` tolerates added columns (old files yield NULL), but
  renames/type-changes need a rewrite. No enforced schema-on-write check
  against historical files.
- **No streaming/CDF.** If you need change feeds, you already have the
  pieces: the commit log is a Postgres table — LISTEN/NOTIFY on it, or run
  it through your `Postgrex.ReplicationConnection` CDC pipeline.
- **Rewrite retry is unbounded** on concurrent conflicts; under heavy
  delete/optimize contention you'd want backoff + a retry cap.
- Orphaned Parquet from aborted commits (upload succeeded, commit failed) is
  never referenced and therefore harmless, but a GC pass comparing S3
  listings against the manifest would reclaim the space.

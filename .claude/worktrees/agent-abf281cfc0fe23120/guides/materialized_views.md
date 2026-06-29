# Materialized views over AshDelta

A view is a delta table whose contents are a deterministic function of one or
more source tables. Because it stores rows the same way (Parquet in S3,
manifest + log in Postgres), **reads use the identical path** — pruning, stats
skipping, DuckDB scan, time travel — and the only new machinery is *refresh*.

## Define a view

```elixir
defmodule Mes.Telemetry.StationDailyStats do
  use Ash.Resource, domain: Mes.Telemetry, data_layer: AshDelta.ViewLayer

  delta_view do
    repo Mes.Repo
    bucket "mes-lake"
    prefix "views/station_daily"
    sources source: Mes.Telemetry.ProcessEvent   # alias => source resource
    refresh :partition_incremental
    partition_by [:site, :day]                    # ⊆ source partition_by
    sort_within_files [:station_id]
    stats_columns [:site, :day, :station_id]
    freshness max_staleness: {15, :minute}
    query """
      SELECT site, day, station_id,
             count(*)                                          AS event_count,
             count(*) FILTER (WHERE event_type = 'fault')      AS fault_count,
             avg((payload->>'cycle_time_ms')::DOUBLE)          AS avg_cycle_ms
      FROM source
      GROUP BY site, day, station_id
    """
  end

  attributes do
    uuid_primary_key :id, writable?: false
    attribute :site, :string
    attribute :day, :date
    attribute :station_id, :string
    attribute :event_count, :integer
    attribute :fault_count, :integer
    attribute :avg_cycle_ms, :float
  end

  actions do
    defaults [:read]
  end
end
```

Query it like any Ash resource — filters become pruning, and time travel
resolves against the *view's* own version history:

```elixir
StationDailyStats
|> Ash.Query.filter(site == "CHI" and day == ^Date.utc_today())
|> Ash.read!()

# the view as of one of its own earlier versions
StationDailyStats
|> Ash.Query.set_context(%{delta: %{version: 8}})
|> Ash.read!()
```

## Refresh

```elixir
AshDelta.refresh(StationDailyStats)                      # declared strategy
AshDelta.refresh(StationDailyStats, strategy: :recompute) # force full rebuild
```

Returns `{:ok, :up_to_date}` when no source has advanced, otherwise a summary
(`view_version`, `strategy`, `partitions`, `rows_written`, `files_added`,
`watermarks`).

### How the two strategies work

Both are one mechanism: recompute the `query` over a chosen set of source
**partitions**, then replace exactly the view files for those partitions.

* **`:recompute`** — all partitions. Feeds every live source file into
  `read_parquet([...])`, replaces every live view file. Correct for any query
  shape (arbitrary joins, window functions, non-partition-aligned group bys).

* **`:partition_incremental`** — only partitions touched by source commits
  since the watermark. The diff scans `delta_files` for rows whose
  `added_version` **or** `removed_version` falls in `(watermark, current]`, so
  a source DELETE/OPTIMIZE forces re-aggregation of the affected partitions
  rather than leaving stale view rows. Each refresh recomputes only those
  partitions and replaces only the matching view files.

### The alignment invariant

`:partition_incremental` requires the view's `partition_by` to be a **subset**
of every source's `partition_by`. Otherwise a single view partition's rows
could depend on source rows in *other* partitions, and recomputing one
partition in isolation would be wrong. This is checked at refresh time (when
source modules are loaded) and raises with an explanation if violated. When
in doubt, use `:recompute`.

## Watermarks and atomicity

Per `(view, source)`, `delta_view_state.consumed_version` records the source
version last folded in. The watermark advance and the view's new commit happen
in **one transaction** (inside the view's commit lock), so refresh is
crash-safe: a failure leaves the view at its prior version with its prior
watermark, and the next run reprocesses the same delta. Concurrent refreshes
are safe (the loser hits the tombstone conflict check and retries) but
wasteful — run one refresher per view.

## Automatic refresh

`AshDelta.View.RefreshWorker` drives refresh on the `freshness` policy. Run it
as a **Horde singleton** so exactly one node refreshes each view:

```elixir
# interval-driven from freshness: max_staleness
{AshDelta.View.RefreshWorker, view: Mes.Telemetry.StationDailyStats}
```

For event-driven refresh, `LISTEN` on `delta_commits` inserts (or call it from
your ingestion code after a source commit) and forward to the worker, which
debounces bursts:

```elixir
AshDelta.View.RefreshWorker.notify(Mes.Telemetry.StationDailyStats)
```

## Views on views

A view may list another view as a source — it's just another watermarked
table. Refresh the base view before the dependent one (resolve order from the
`sources` declarations); a scheduled job that refreshes in dependency order is
the simplest approach.

## OPTIMIZE / VACUUM

Both work on a view unchanged, since a view's files are ordinary delta files:

```elixir
AshDelta.optimize(StationDailyStats)
AshDelta.vacuum(StationDailyStats)
```

## Limitations

* Incremental is partition-granular, not row-granular — there is no algebraic
  IVM (no `SUM += delta`); a touched partition is fully recomputed. That keeps
  correctness simple and is the right tradeoff when partitions are the natural
  query/refresh unit (time-windowed MES rollups). Row-level IVM for specific
  invertible aggregates could layer on later.
* The `query` runs in DuckDB, so it's DuckDB SQL, not Ash expressions. Output
  column names must match the view's attribute names.
* Joins across sources are supported in `:recompute`. Under
  `:partition_incremental` a join is only sound if every joined source is
  partition-aligned with the view on the join/group keys; otherwise use
  `:recompute`.

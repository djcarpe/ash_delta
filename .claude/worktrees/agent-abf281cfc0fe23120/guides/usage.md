# AshDelta usage — actions in practice

A worked example in MES terms: per-station process telemetry flowing into a
delta table, with the action layer doing what it normally does in Ash —
validation, defaults, named queries, code interfaces — while AshDelta handles
storage.

## The resource

```elixir
defmodule Mes.Telemetry.ProcessEvent do
  use Ash.Resource,
    domain: Mes.Telemetry,
    data_layer: AshDelta.DataLayer

  delta do
    repo Mes.Repo
    bucket "mes-lake"
    prefix "telemetry/process_events"
    partition_by [:site, :day]
    sort_within_files [:station_id, :recorded_at]
    stats_columns [:recorded_at, :station_id, :unit_id, :work_order]
    target_file_size_mb 128
    vacuum_retention_hours 720   # 30 days of time travel
  end

  attributes do
    uuid_primary_key :id
    attribute :site, :string, allow_nil?: false
    attribute :day, :date, allow_nil?: false
    attribute :station_id, :string, allow_nil?: false
    attribute :unit_id, :string
    attribute :work_order, :string
    attribute :event_type, :atom,
      constraints: [one_of: [:cycle_start, :cycle_complete, :fault, :andon]]
    attribute :recorded_at, :utc_datetime_usec, allow_nil?: false
    attribute :payload, :map, default: %{}
  end

  actions do
    defaults [:read]

    # ── Ingestion ────────────────────────────────────────────────────────
    # Used exclusively through Ash.bulk_create — one commit per batch, one
    # sorted Parquet file per (site, day) partition the batch touches.
    create :ingest do
      accept [:site, :station_id, :unit_id, :work_order,
              :event_type, :recorded_at, :payload]

      # Derive the partition column so callers can't desync it from the
      # event timestamp.
      change fn changeset, _ctx ->
        case Ash.Changeset.get_attribute(changeset, :recorded_at) do
          %DateTime{} = ts ->
            Ash.Changeset.force_change_attribute(changeset, :day, DateTime.to_date(ts))

          _ ->
            changeset
        end
      end

      validate present([:site, :station_id, :recorded_at])
    end

    # ── Named reads: filters become pruning ──────────────────────────────
    # site + day hit partition pruning; station_id + recorded_at hit
    # min/max stats. With sort_within_files leading on station_id, a
    # station-scoped query typically touches a handful of row groups.
    read :for_station do
      argument :site, :string, allow_nil?: false
      argument :station_id, :string, allow_nil?: false
      argument :since, :utc_datetime_usec, allow_nil?: false

      filter expr(
        site == ^arg(:site) and
        station_id == ^arg(:station_id) and
        recorded_at >= ^arg(:since)
      )

      prepare build(sort: [recorded_at: :desc], limit: 1_000)
    end

    read :faults_for_work_order do
      argument :work_order, :string, allow_nil?: false

      filter expr(work_order == ^arg(:work_order) and event_type == :fault)
    end

    # ── Time travel as an action ─────────────────────────────────────────
    # "What did this table look like before last night's backfill?"
    read :as_of do
      argument :version, :integer

      prepare fn query, _ctx ->
        case Ash.Query.get_argument(query, :version) do
          nil -> query
          v -> Ash.Query.set_context(query, %{delta: %{version: v}})
        end
      end
    end

    # ── Bulk delete as a generic action ──────────────────────────────────
    # Per-record destroy works (Ash.destroy! → pk-pruned copy-on-write
    # rewrite) but is the wrong tool for retention. This routes a
    # filter-shaped delete through one rewrite commit.
    action :purge_day, :map do
      argument :site, :string, allow_nil?: false
      argument :day, :date, allow_nil?: false

      run fn input, _ctx ->
        AshDelta.delete_where(__MODULE__,
          site: input.arguments.site,
          day: input.arguments.day
        )
      end
    end
  end

  code_interface do
    define :ingest
    define :for_station, args: [:site, :station_id, :since]
    define :faults_for_work_order, args: [:work_order]
    define :as_of, args: [:version]
    define :purge_day, args: [:site, :day]
  end
end

defmodule Mes.Telemetry do
  use Ash.Domain

  resources do
    resource Mes.Telemetry.ProcessEvent
  end
end
```

## Call sites

```elixir
alias Mes.Telemetry.ProcessEvent

# ── Ingest a NATS batch ──────────────────────────────────────────────────
# In your JetStream consumer (or the flush callback of a buffering
# GenServer/Broadway batcher), turn the batch into one delta commit:
events = [
  %{site: "CHI", station_id: "OP-110", unit_id: "U7K3...",
    work_order: "WO-88412", event_type: :cycle_complete,
    recorded_at: ~U[2026-06-11 14:03:11.182000Z],
    payload: %{cycle_time_ms: 41_230, torque_ok: true}},
  # ... a few thousand more
]

%Ash.BulkResult{status: :success} =
  Ash.bulk_create(events, ProcessEvent, :ingest,
    return_errors?: true,
    stop_on_error?: true
  )

# ── Dashboard / LiveView queries ─────────────────────────────────────────
recent =
  ProcessEvent.for_station!("CHI", "OP-110",
    DateTime.add(DateTime.utc_now(), -3600, :second))

faults = ProcessEvent.faults_for_work_order!("WO-88412")

# Ad hoc — same pruning, no named action needed:
ProcessEvent
|> Ash.Query.filter(site == "CHI" and day == ^Date.utc_today())
|> Ash.Query.filter(event_type in [:fault, :andon])
|> Ash.Query.sort(recorded_at: :desc)
|> Ash.Query.limit(50)
|> Ash.read!()

# ── Time travel ──────────────────────────────────────────────────────────
[%{version: current} | _] = AshDelta.history(ProcessEvent, limit: 1)
before_backfill = ProcessEvent.as_of!(current - 3)

# Or by timestamp, inline:
ProcessEvent
|> Ash.Query.set_context(%{delta: %{as_of: ~U[2026-06-10 00:00:00Z]}})
|> Ash.Query.filter(work_order == "WO-88412")
|> Ash.read!()

# ── Retention / corrections ──────────────────────────────────────────────
{:ok, %{rows_affected: n}} = ProcessEvent.purge_day("CHI", ~D[2025-06-01])

# ── Maintenance (Oban worker / Horde singleton) ──────────────────────────
defmodule Mes.Telemetry.CompactionWorker do
  use Oban.Worker, queue: :maintenance

  @impl true
  def perform(_job) do
    {:ok, _} = AshDelta.optimize(Mes.Telemetry.ProcessEvent, min_files: 8)
    {:ok, _} = AshDelta.vacuum(Mes.Telemetry.ProcessEvent)
    :ok
  end
end
```

## What's happening underneath each action

| Action call | Storage behavior |
|---|---|
| `Ash.bulk_create(..., :ingest)` | One commit; one sorted Parquet file per touched `(site, day)` partition; stats recorded per file |
| `for_station!/3` | Postgres prunes by partition + `station_id`/`recorded_at` min-max; DuckDB scans survivors with `WHERE`/`ORDER BY`/`LIMIT` pushed down |
| `as_of!/1` | Snapshot reconstructed at that version — reads tombstoned files that VACUUM hasn't reaped |
| `Ash.destroy!(event)` | PK-pruned copy-on-write rewrite of the one file holding that row |
| `purge_day/2` | Partition-pruned rewrite; whole-partition deletes remove files without writing replacements |

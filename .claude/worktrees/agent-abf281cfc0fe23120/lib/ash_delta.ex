defmodule AshDelta do
  @moduledoc """
  An Ash data layer that replicates Delta Lake semantics using S3 for data
  (sorted, partitioned Parquet files) and Postgres for the transaction log,
  file manifest, and column statistics ("indexes").

  ## How this maps to Delta Lake

  | Delta Lake concept        | AshDelta implementation                          |
  |---------------------------|--------------------------------------------------|
  | `_delta_log/*.json`       | `delta_commits` table (one row per version)      |
  | Add/Remove file actions   | `delta_files` rows with added/removed versions   |
  | Optimistic concurrency    | Row lock on `delta_tables.current_version`       |
  | Data skipping (stats)     | JSONB min/max/null_count + GIN indexes           |
  | Partition pruning         | `partition_values` JSONB containment queries     |
  | Time travel               | Snapshot reconstruction at any version/timestamp |
  | OPTIMIZE (compaction)     | `AshDelta.optimize/2`                            |
  | VACUUM                    | `AshDelta.vacuum/2`                              |
  | Checkpoints               | Not needed — Postgres *is* the materialized state|

  Because the log lives in Postgres rather than as JSON files that must be
  replayed, snapshot reconstruction is a single indexed query and checkpoints
  are unnecessary. Commits are serialized per-table via `SELECT ... FOR UPDATE`
  on the table's version row; since a commit is a tiny metadata transaction
  (the heavy Parquet upload happens *before* the commit), this is cheap and
  gives strict serializability rather than Delta's retry-based OCC.

  ## Usage

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

  ## Time travel

      MyApp.Telemetry.Event
      |> Ash.Query.set_context(%{delta: %{version: 42}})
      |> Ash.read!()

      MyApp.Telemetry.Event
      |> Ash.Query.set_context(%{delta: %{as_of: ~U[2026-06-01 00:00:00Z]}})
      |> Ash.read!()
  """

  @doc "Compact small files into target-sized files. Options: `:partition` to scope."
  defdelegate optimize(resource, opts \\ []), to: AshDelta.Maintenance

  @doc "Physically delete tombstoned S3 objects past the retention window."
  defdelegate vacuum(resource, opts \\ []), to: AshDelta.Maintenance

  @doc "Bulk delete by Ash filter expression via copy-on-write file rewrites."
  defdelegate delete_where(resource, filter), to: AshDelta.Rewrite

  @doc "Commit history (most recent first). Options: `:limit`."
  defdelegate history(resource, opts \\ []), to: AshDelta.Log

  @doc "The list of active files at a given version (or current)."
  defdelegate snapshot(resource, version \\ nil), to: AshDelta.Log

  @doc """
  Refresh a materialized view (a resource using `AshDelta.ViewLayer`) to its
  sources' current versions, using its declared strategy. Pass
  `strategy: :recompute` to force a full rebuild. Returns `{:ok, summary}` or
  `{:ok, :up_to_date}`.
  """
  defdelegate refresh(view, opts \\ []), to: AshDelta.View
end

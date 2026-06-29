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

  @doc """
  Import existing Parquet files at the resource's S3 prefix into the catalog.

  For wrapping a data lake that was written outside AshDelta (Spark, Flink,
  DuckDB COPY, etc.).  A single DuckDB scan discovers all files, collects row
  counts and min/max stats, parses hive partition values from directory names,
  and commits them as one "import" version.  Options: `:prefix` to override
  the resource's configured prefix.

      {:ok, version} = AshDelta.import_existing(MyApp.Telemetry.Event)
  """
  defdelegate import_existing(resource, opts \\ []), to: AshDelta.Importer

  @doc """
  Run an Ash query and return an `Explorer.DataFrame` instead of a list of structs.

  Bypasses Elixir struct allocation: DuckDB rows are transposed directly into
  column vectors and handed to Explorer, so memory scales with data size rather
  than Elixir term overhead. Ideal for analytics, exports, and Livebook charting.

      df = MyResource
           |> Ash.Query.filter(site == "CHI" and day >= ~D[2026-01-01])
           |> AshDelta.to_dataframe!()

      Explorer.DataFrame.describe(df)
  """
  def to_dataframe!(query, opts \\ []) do
    case to_dataframe(query, opts) do
      {:ok, df} -> df
      {:error, reason} -> raise inspect(reason)
    end
  end

  def to_dataframe(query, _opts \\ []) do
    query = if is_atom(query), do: Ash.Query.new(query), else: query
    resource = query.resource

    with {:ok, version} <- AshDelta.Log.resolve_version(resource, query.context[:delta] || %{}),
         {:ok, files}   <- AshDelta.Pruner.candidate_files(resource, version, query.filter) do
      case files do
        [] -> {:ok, empty_dataframe(resource, query)}
        _  -> AshDelta.Reader.scan_dataframe(resource, files, query)
      end
    end
  end

  defp empty_dataframe(resource, query) do
    attrs = Ash.Resource.Info.attributes(resource)

    selected =
      case query.select do
        nil  -> attrs
        cols -> Enum.filter(attrs, &(&1.name in cols))
      end

    series_map =
      Map.new(selected, fn attr ->
        dtype = AshDelta.Reader.ash_to_explorer_dtype_pub(attr.type)
        {to_string(attr.name), Explorer.Series.from_list([], dtype: dtype)}
      end)

    Explorer.DataFrame.new(series_map)
  end

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

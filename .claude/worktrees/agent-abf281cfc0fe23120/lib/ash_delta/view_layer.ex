defmodule AshDelta.ViewLayer do
  @moduledoc """
  Data layer for **materialized views** over AshDelta tables.

  A view is itself a delta table: its rows live in S3 Parquet and its
  manifest/log live in Postgres exactly like a base table, so *reads go
  through the same path* (`AshDelta.Read`) — pruning, stats skipping, DuckDB
  scan, and time travel all work unchanged. The only differences from
  `AshDelta.DataLayer` are:

    * storage is configured via the `delta_view` section, which additionally
      declares `sources`, a `query`, a `refresh` strategy, and `freshness`;
    * the contents are not written by Ash create/update/destroy actions —
      they are produced by `AshDelta.refresh/2` (or an automatic refresh
      worker). Direct mutations are rejected with a clear error.

  ## Refresh strategies

    * `:recompute` — rebuild the entire view from the source snapshot. Always
      correct for any query shape.
    * `:partition_incremental` — recompute only the partitions touched by
      source commits since the last refresh watermark. Requires the view's
      `partition_by` to be a subset of every source's `partition_by` (checked
      at compile time); otherwise a partition's rows could depend on source
      rows outside that partition and the incremental result would be wrong.

  ## Example

      defmodule Mes.Telemetry.StationDailyStats do
        use Ash.Resource, domain: Mes.Telemetry, data_layer: AshDelta.ViewLayer

        delta_view do
          repo Mes.Repo
          bucket "mes-lake"
          prefix "views/station_daily"
          sources source: Mes.Telemetry.ProcessEvent
          refresh :partition_incremental
          partition_by [:site, :day]
          stats_columns [:site, :day, :station_id]
          query \"""
            SELECT site, day, station_id,
                   count(*) AS event_count,
                   count(*) FILTER (WHERE event_type = 'fault') AS fault_count,
                   avg((payload->>'cycle_time_ms')::DOUBLE) AS avg_cycle_ms
            FROM source
            GROUP BY site, day, station_id
          \"""
          freshness max_staleness: {15, :minute}
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
  """

  @delta_view %Spark.Dsl.Section{
    name: :delta_view,
    describe: "A materialized view over one or more AshDelta tables.",
    schema: [
      repo: [type: :atom, required: true, doc: "Ecto repo holding the log/catalog."],
      bucket: [type: :string, required: true, doc: "S3 bucket for the view's Parquet files."],
      prefix: [type: :string, default: "", doc: "Key prefix within the bucket."],
      name: [type: :string, doc: "Catalog name. Defaults to the resource short name."],
      sources: [
        type: :keyword_list,
        required: true,
        doc: """
        Named source resources referenced by `query`, e.g.
        `sources source: MyApp.Events`. Each key becomes a table alias in the
        SQL (`FROM source`, `JOIN other ...`). All sources must use
        `AshDelta.DataLayer` (or `AshDelta.ViewLayer` for views-on-views).
        """
      ],
      query: [
        type: :string,
        required: true,
        doc: """
        DuckDB SQL producing the view rows. Reference sources by their alias;
        each alias is substituted with `read_parquet([...])` over that source's
        live (optionally partition-restricted) files. Output columns must match
        this resource's attribute names.
        """
      ],
      refresh: [
        type: {:one_of, [:recompute, :partition_incremental]},
        default: :recompute,
        doc: "Refresh strategy. See module docs."
      ],
      partition_by: [type: {:list, :atom}, default: [], doc: "Hive partitioning for the view."],
      sort_within_files: [type: {:list, :atom}, default: [], doc: "In-file sort order."],
      stats_columns: [type: {:list, :atom}, default: [], doc: "Columns to collect min/max for."],
      target_file_size_mb: [type: :pos_integer, default: 128, doc: "OPTIMIZE target size."],
      vacuum_retention_hours: [type: :pos_integer, default: 168, doc: "Tombstone retention."],
      s3_config: [type: :keyword_list, default: [], doc: "S3 client config."],
      freshness: [
        type: :keyword_list,
        default: [],
        doc: """
        Auto-refresh policy consumed by `AshDelta.View.RefreshWorker`, e.g.
        `max_staleness: {15, :minute}`. Empty means refresh only on demand.
        """
      ]
    ]
  }

  use Spark.Dsl.Extension,
    sections: [@delta_view],
    transformers: [AshDelta.Transformers.ValidateView]

  @behaviour Ash.DataLayer

  alias AshDelta.DataLayer
  alias AshDelta.DataLayer.Query

  # ── Reads: identical to the base layer ─────────────────────────────────────

  @impl true
  def can?(_, :read), do: true
  def can?(resource, capability), do: DataLayer.can?(resource, capability)

  @impl true
  def resource_to_query(resource, domain), do: %Query{resource: resource, domain: domain}

  @impl true
  def filter(query, filter, resource), do: DataLayer.filter(query, filter, resource)

  @impl true
  def sort(query, sort, resource), do: DataLayer.sort(query, sort, resource)

  @impl true
  def limit(query, limit, resource), do: DataLayer.limit(query, limit, resource)

  @impl true
  def offset(query, offset, resource), do: DataLayer.offset(query, offset, resource)

  @impl true
  def set_context(resource, query, context),
    do: DataLayer.set_context(resource, query, context)

  @impl true
  def run_query(%Query{} = query, resource), do: AshDelta.Read.run(query, resource)

  # ── Writes: not allowed; views are refreshed, not mutated ──────────────────

  @impl true
  def create(resource, _changeset), do: {:error, write_error(resource)}

  @impl true
  def update(resource, _changeset), do: {:error, write_error(resource)}

  @impl true
  def destroy(resource, _changeset), do: {:error, write_error(resource)}

  defp write_error(resource) do
    Ash.Error.Invalid.exception(
      errors: [
        Ash.Error.Unknown.UnknownError.exception(
          error: """
          #{inspect(resource)} is an AshDelta materialized view and cannot be \
          written directly. Update its sources and call \
          AshDelta.refresh(#{inspect(resource)}).
          """
        )
      ]
    )
  end
end

defmodule AshDelta.Transformers.ValidateView do
  @moduledoc false
  use Spark.Dsl.Transformer

  # Runs after compilation of all resources isn't guaranteed, so we validate
  # what we can statically here (attribute existence, query presence) and
  # defer cross-resource partition-alignment checks to refresh time, where the
  # source modules are guaranteed loaded.
  def transform(dsl_state) do
    module = Spark.Dsl.Transformer.get_persisted(dsl_state, :module)

    attrs =
      dsl_state
      |> Ash.Resource.Info.attributes()
      |> MapSet.new(& &1.name)

    for opt <- [:partition_by, :sort_within_files, :stats_columns],
        col <- Spark.Dsl.Transformer.get_option(dsl_state, [:delta_view], opt) || [],
        not MapSet.member?(attrs, col) do
      raise Spark.Error.DslError,
        module: module,
        path: [:delta_view, opt],
        message: "#{inspect(col)} is not an attribute on this view"
    end

    refresh = Spark.Dsl.Transformer.get_option(dsl_state, [:delta_view], :refresh)
    parts = Spark.Dsl.Transformer.get_option(dsl_state, [:delta_view], :partition_by) || []

    if refresh == :partition_incremental and parts == [] do
      raise Spark.Error.DslError,
        module: module,
        path: [:delta_view, :refresh],
        message: ":partition_incremental requires a non-empty partition_by"
    end

    {:ok, dsl_state}
  end
end

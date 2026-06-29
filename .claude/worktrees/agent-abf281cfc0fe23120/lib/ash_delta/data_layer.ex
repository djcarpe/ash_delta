defmodule AshDelta.DataLayer do
  @delta %Spark.Dsl.Section{
    name: :delta,
    describe: "Configuration for the S3 + Postgres Delta-style data layer.",
    schema: [
      repo: [
        type: :atom,
        required: true,
        doc: "The Ecto repo holding the transaction log and file catalog."
      ],
      bucket: [
        type: :string,
        required: true,
        doc: "S3 bucket for Parquet data files."
      ],
      prefix: [
        type: :string,
        default: "",
        doc: "Key prefix within the bucket, e.g. `telemetry/events`."
      ],
      name: [
        type: :string,
        doc: "Logical table name in the catalog. Defaults to the resource short name."
      ],
      partition_by: [
        type: {:list, :atom},
        default: [],
        doc: "Attributes used for hive-style partitioning (`site=CHI/day=2026-06-11/`)."
      ],
      sort_within_files: [
        type: {:list, :atom},
        default: [],
        doc: """
        Attributes to sort rows by inside each Parquet file. Tightens min/max
        stats so file skipping prunes harder — the poor man's Z-order.
        """
      ],
      stats_columns: [
        type: {:list, :atom},
        default: [],
        doc: "Attributes to collect min/max/null_count stats for (data skipping)."
      ],
      target_file_size_mb: [
        type: :pos_integer,
        default: 128,
        doc: "Target file size for OPTIMIZE compaction."
      ],
      vacuum_retention_hours: [
        type: :pos_integer,
        default: 168,
        doc: "Minimum age of a tombstone before VACUUM may delete the S3 object."
      ],
      s3_config: [
        type: :keyword_list,
        default: [],
        doc: """
        S3 client config passed to Explorer/DuckDB: `:access_key_id`,
        `:secret_access_key`, `:region`, `:endpoint`. Defaults to the
        ambient AWS environment if empty.
        """
      ],
      write_buffer_ms: [
        type: :pos_integer,
        doc: """
        When set, concurrent `bulk_create` calls are coalesced within this
        window (milliseconds) into a single Parquet write + commit, reducing
        lock contention under high parallel write load. All callers still
        block synchronously until the window flushes. Omit (default) for
        direct per-call writes.
        """
      ],
      auto_compact_threshold: [
        type: :pos_integer,
        doc: """
        When set, automatically compact any partition whose live file count
        exceeds this number. Compaction runs asynchronously (Task) after each
        successful write. Recommended value: 20.
        """
      ]
    ]
  }

  use Spark.Dsl.Extension,
    sections: [@delta],
    transformers: [AshDelta.Transformers.ValidateConfig]

  @behaviour Ash.DataLayer

  alias AshDelta.{Info, Rewrite, Writer}

  defmodule Query do
    @moduledoc false
    defstruct [:resource, :domain, :filter, :limit, :offset, :select, sort: [], context: %{}]
  end

  # ── Capabilities ──────────────────────────────────────────────────────────

  @impl true
  def can?(_, :read), do: true
  def can?(_, :create), do: true
  def can?(_, :bulk_create), do: true
  def can?(_, :destroy), do: true
  def can?(_, :update), do: true
  def can?(_, :filter), do: true
  def can?(_, {:filter_expr, %Ash.Query.Operator.Eq{}}), do: true
  def can?(_, {:filter_expr, %Ash.Query.Operator.NotEq{}}), do: true
  def can?(_, {:filter_expr, %Ash.Query.Operator.In{}}), do: true
  def can?(_, {:filter_expr, %Ash.Query.Operator.LessThan{}}), do: true
  def can?(_, {:filter_expr, %Ash.Query.Operator.GreaterThan{}}), do: true
  def can?(_, {:filter_expr, %Ash.Query.Operator.LessThanOrEqual{}}), do: true
  def can?(_, {:filter_expr, %Ash.Query.Operator.GreaterThanOrEqual{}}), do: true
  def can?(_, {:filter_expr, %Ash.Query.Function.IsNil{}}), do: true
  def can?(_, :boolean_filters), do: true
  def can?(_, :sort), do: true
  def can?(_, {:sort, _}), do: true
  def can?(_, :limit), do: true
  def can?(_, :offset), do: true
  def can?(_, :nested_expressions), do: true
  def can?(_, :async_engine), do: true
  def can?(_, :select), do: true
  def can?(_, :aggregate_query), do: true
  def can?(_, :multitenancy), do: false
  def can?(_, :transact), do: false
  def can?(_, _), do: false

  # ── Query building ────────────────────────────────────────────────────────

  @impl true
  def resource_to_query(resource, domain), do: %Query{resource: resource, domain: domain}

  @impl true
  def filter(%Query{} = query, filter, _resource) do
    case query.filter do
      nil -> {:ok, %{query | filter: filter}}
      existing -> {:ok, %{query | filter: Ash.Filter.add_to_filter!(existing, filter)}}
    end
  end

  @impl true
  def sort(%Query{} = query, sort, _resource), do: {:ok, %{query | sort: sort}}

  @impl true
  def limit(%Query{} = query, limit, _resource), do: {:ok, %{query | limit: limit}}

  @impl true
  def offset(%Query{} = query, offset, _resource), do: {:ok, %{query | offset: offset}}

  @impl true
  def select(%Query{} = query, select, _resource), do: {:ok, %{query | select: select}}

  @impl true
  def set_context(_resource, %Query{} = query, context),
    do: {:ok, %{query | context: context}}

  # ── Read path ─────────────────────────────────────────────────────────────

  @impl true
  def run_query(%Query{} = query, resource), do: AshDelta.Read.run(query, resource)

  # ── Write path ────────────────────────────────────────────────────────────

  @impl true
  def create(resource, changeset) do
    record = record_from_changeset(resource, changeset)

    case write_records(resource, [record]) do
      {:ok, _version} -> {:ok, record}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def bulk_create(resource, stream, _options) do
    changesets = Enum.to_list(stream)
    records = Enum.map(changesets, &record_from_changeset(resource, &1))

    case write_records(resource, records) do
      {:ok, _version} ->
        indexed =
          Enum.zip(changesets, records)
          |> Enum.map(fn {cs, record} ->
            Ash.Resource.set_metadata(record, %{
              bulk_create_index: cs.context.bulk_create.index
            })
          end)

        {:ok, indexed}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def run_aggregate_query(query, aggregates, resource) do
    AshDelta.Read.run_aggregates(query, aggregates, resource)
  end

  @impl true
  def destroy(resource, changeset) do
    pk_filter = primary_key_filter(resource, changeset.data)

    case Rewrite.delete_where(resource, pk_filter) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def update(resource, changeset) do
    # Copy-on-write: rewrite the file(s) containing this row with the
    # changed attribute values applied. Static value assignment only —
    # atomic/expression updates are not supported by this layer.
    pk_filter = primary_key_filter(resource, changeset.data)
    changes = changeset.attributes

    case Rewrite.update_where(resource, pk_filter, changes) do
      {:ok, _} -> {:ok, struct(changeset.data, changes)}
      {:error, reason} -> {:error, reason}
    end
  end

  # ── Helpers ───────────────────────────────────────────────────────────────

  defp write_records(resource, records) do
    result =
      case Info.write_buffer_ms(resource) do
        {:ok, nil} -> Writer.append(resource, records)
        {:ok, _ms} -> Writer.append(resource, records)
      end

    with {:ok, _} <- result, do: maybe_auto_compact(resource, records)
    result
  end

  defp maybe_auto_compact(resource, records) do
    case Info.auto_compact_threshold(resource) do
      {:ok, nil} ->
        :ok

      {:ok, threshold} ->
        {:ok, partition_by} = Info.partition_by(resource)
        partitions = records |> Enum.map(&Map.take(&1, partition_by)) |> Enum.uniq()

        Task.start(fn ->
          Enum.each(partitions, fn partition ->
            string_partition =
              Map.new(partition, fn {k, v} ->
                {to_string(k), AshDelta.Pruner.encode(v)}
              end)

            live =
              AshDelta.Log.snapshot(resource)
              |> Enum.filter(fn f ->
                Enum.all?(string_partition, fn {k, v} -> f.partition_values[k] == v end)
              end)

            if length(live) >= threshold do
              AshDelta.Maintenance.optimize(resource, partition: partition, min_files: threshold)
            end
          end)
        end)
    end
  end

  defp record_from_changeset(resource, changeset) do
    attrs =
      resource
      |> Ash.Resource.Info.attributes()
      |> Map.new(fn attr ->
        {attr.name, Ash.Changeset.get_attribute(changeset, attr.name)}
      end)

    resource
    |> struct(attrs)
    |> Map.put(:__meta__, %Ecto.Schema.Metadata{state: :loaded, source: Info.table_name(resource)})
  end

  defp primary_key_filter(resource, record) do
    resource
    |> Ash.Resource.Info.primary_key()
    |> Map.new(fn key -> {key, Map.get(record, key)} end)
  end
end

defmodule AshDelta.Info do
  @moduledoc """
  Section-agnostic introspection for storage config.

  Both the base table layer (`:delta` section) and the view layer
  (`:delta_view` section) declare the same storage keys (repo, bucket,
  prefix, partitioning, stats, ...). Every storage module — Log, Writer,
  Reader, Pruner, Maintenance — reads config through *these* accessors and
  therefore never needs to know whether it's operating on a base table or a
  materialized view: a view is just a delta table whose contents happen to be
  derived. The accessors return `{:ok, value}` to match the
  `Spark.InfoGenerator` shape the call sites were written against.
  """

  @storage_keys [
    :repo,
    :bucket,
    :prefix,
    :name,
    :partition_by,
    :sort_within_files,
    :stats_columns,
    :target_file_size_mb,
    :vacuum_retention_hours,
    :s3_config,
    :write_buffer_ms,
    :auto_compact_threshold
  ]

  for key <- @storage_keys do
    def unquote(key)(resource), do: {:ok, fetch(resource, unquote(key))}
  end

  # View-only keys (always under :delta_view).
  def view_sources(resource), do: {:ok, get(resource, [:delta_view], :sources, [])}
  def view_refresh(resource), do: {:ok, get(resource, [:delta_view], :refresh, :recompute)}
  def view_query(resource), do: {:ok, get(resource, [:delta_view], :query, nil)}
  def view_freshness(resource), do: {:ok, get(resource, [:delta_view], :freshness, [])}

  @doc "True if the resource is a materialized view (uses the `:delta_view` section)."
  def view?(resource), do: get(resource, [:delta_view], :repo, nil) != nil

  @doc "Logical catalog table name. Defaults to the resource short name."
  def table_name(resource) do
    case fetch(resource, :name) do
      name when is_binary(name) -> name
      _ -> resource |> Module.split() |> List.last() |> Macro.underscore()
    end
  end

  @doc "Full s3:// URI for a key within this resource's table location."
  def s3_uri(resource, key) do
    {:ok, bucket} = bucket(resource)

    prefix =
      case prefix(resource) do
        {:ok, p} when p not in [nil, ""] -> p <> "/"
        _ -> ""
      end

    "s3://#{bucket}/#{prefix}#{key}"
  end

  @doc "Columns that participate in file skipping: stats columns + partition columns."
  def skippable_columns(resource) do
    {:ok, stats} = stats_columns(resource)
    {:ok, parts} = partition_by(resource)
    Enum.uniq(stats ++ parts)
  end

  # ── internals ──────────────────────────────────────────────────────────────

  # Try the base section first, then the view section, then a per-key default.
  defp fetch(resource, key) do
    case get(resource, [:delta], key, nil) do
      nil ->
        case get(resource, [:delta_view], key, nil) do
          nil -> default(key)
          value -> value
        end

      value ->
        value
    end
  end

  defp get(resource, path, key, default \\ nil) do
    Spark.Dsl.Extension.get_opt(resource, path, key, default)
  end

  defp default(:prefix), do: ""
  defp default(:partition_by), do: []
  defp default(:sort_within_files), do: []
  defp default(:stats_columns), do: []
  defp default(:target_file_size_mb), do: 128
  defp default(:vacuum_retention_hours), do: 168
  defp default(:s3_config), do: []
  defp default(:write_buffer_ms), do: nil
  defp default(:auto_compact_threshold), do: nil
  defp default(_), do: nil
end

defmodule AshDelta.Transformers.ValidateConfig do
  @moduledoc false
  use Spark.Dsl.Transformer

  def transform(dsl_state) do
    attrs =
      dsl_state
      |> Ash.Resource.Info.attributes()
      |> MapSet.new(& &1.name)

    for opt <- [:partition_by, :sort_within_files, :stats_columns],
        col <- Spark.Dsl.Transformer.get_option(dsl_state, [:delta], opt) || [],
        not MapSet.member?(attrs, col) do
      raise Spark.Error.DslError,
        module: Spark.Dsl.Transformer.get_persisted(dsl_state, :module),
        path: [:delta, opt],
        message: "#{inspect(col)} is not an attribute on this resource"
    end

    {:ok, dsl_state}
  end
end

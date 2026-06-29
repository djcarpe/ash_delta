defmodule AshDelta.Writer do
  @moduledoc """
  Turns Ash records into sorted, hive-partitioned Parquet files in S3 and
  commits the corresponding add-file actions to the log.

  Write order per append:

    1. Group records by `partition_by` values
    2. Sort each group by `sort_within_files` (tightens min/max stats)
    3. Write one Parquet object per partition group (upload happens *outside*
       the commit lock)
    4. Collect min/max/null_count for `stats_columns`
    5. `Log.commit/4` the add-file actions atomically

  Uses Explorer (Polars) for the DataFrame work and Parquet encoding.
  """

  alias AshDelta.{Info, Log, Pruner}
  alias Explorer.{DataFrame, Series}

  @doc "Append records as a new commit. Returns `{:ok, version}`."
  def append(_resource, []), do: {:ok, :noop}

  def append(resource, records) do
    file_specs = write_files(resource, records)

    Log.commit(resource, :append, %{"row_count" => length(records)}, fn repo, table_id, version ->
      Log.add_files(repo, table_id, version, file_specs)
    end)
  end

  @doc """
  Write records to S3 *without* committing — returns file specs for the
  caller to commit alongside removes (used by rewrites and OPTIMIZE).
  """
  def write_files(resource, records) do
    {:ok, partition_by} = Info.partition_by(resource)
    {:ok, sort_by} = Info.sort_within_files(resource)

    records
    |> Enum.group_by(fn r -> Map.new(partition_by, &{&1, Map.get(r, &1)}) end)
    |> Enum.map(fn {partition_values, group} ->
      df = to_dataframe(resource, group)

      df =
        if sort_by == [] do
          df
        else
          DataFrame.sort_with(df, fn ldf -> Enum.map(sort_by, &{:asc, ldf[to_string(&1)]}) end)
        end

      write_partition(resource, partition_values, df)
    end)
  end

  defp write_partition(resource, partition_values, df) do
    key = partition_key(partition_values) <> "part-#{Uniq.UUID.uuid7()}.parquet"
    uri = Info.s3_uri(resource, key)
    {:ok, s3_config} = Info.s3_config(resource)
    fss_config = Keyword.take(s3_config, [:access_key_id, :secret_access_key, :region, :endpoint, :token, :bucket])

    :ok = DataFrame.to_parquet!(df, uri, config: fss_config, compression: {:zstd, 3})

    %{
      path: uri,
      size_bytes: estimate_size(df),
      row_count: DataFrame.n_rows(df),
      partition_values: encode_map(partition_values),
      column_stats: collect_stats(resource, df)
    }
  end

  defp partition_key(partition_values) when partition_values == %{}, do: ""

  defp partition_key(partition_values) do
    partition_values
    |> Enum.sort()
    |> Enum.map_join("", fn {k, v} -> "#{k}=#{Pruner.encode(v)}/" end)
  end

  # ── Stats ────────────────────────────────────────────────────────────────

  @doc false
  def collect_stats(resource, df) do
    {:ok, stats_columns} = Info.stats_columns(resource)
    names = DataFrame.names(df)

    stats_columns
    |> Enum.filter(&(to_string(&1) in names))
    |> Map.new(fn col ->
      series = df[to_string(col)]
      {min, max} = min_max(series)

      {col,
       %{
         "min" => encode_stat(min),
         "max" => encode_stat(max),
         "null_count" => Series.nil_count(series)
       }}
    end)
  end

  defp min_max(series) do
    case Series.dtype(series) do
      dtype when dtype in [:string, :binary, :boolean, :category] -> {nil, nil}
      {:list, _} -> {nil, nil}
      {:struct, _} -> {nil, nil}
      _ -> {Series.min(series), Series.max(series)}
    end
  end

  defp encode_stat(nil), do: nil
  defp encode_stat(v), do: Pruner.encode(v)

  defp encode_map(map), do: Map.new(map, fn {k, v} -> {to_string(k), Pruner.encode(v)} end)

  # ── Records → DataFrame ──────────────────────────────────────────────────

  @doc false
  def to_dataframe(resource, records) do
    attrs = Ash.Resource.Info.attributes(resource)

    columns =
      Map.new(attrs, fn attr ->
        {to_string(attr.name), Enum.map(records, &dump(Map.get(&1, attr.name)))}
      end)

    DataFrame.new(columns)
  end

  # Parquet-friendly scalar coercion. Maps/embeds serialize as JSON text —
  # they round-trip through Ash.Type casting on read.
  defp dump(nil), do: nil
  defp dump(%DateTime{} = v), do: DateTime.to_naive(v)
  defp dump(%NaiveDateTime{} = v), do: v
  defp dump(%Date{} = v), do: v
  defp dump(%Decimal{} = v), do: Decimal.to_float(v)
  defp dump(v) when is_map(v) or is_list(v), do: Jason.encode!(v)
  defp dump(v) when is_atom(v) and not is_boolean(v), do: to_string(v)
  defp dump(v), do: v

  # Polars doesn't expose serialized size pre-write cheaply; approximate from
  # in-memory size. Only used to pick OPTIMIZE candidates, so coarse is fine.
  defp estimate_size(df) do
    df
    |> DataFrame.names()
    |> Enum.map(&Series.size(df[&1]))
    |> Enum.sum()
    |> Kernel.*(8)
  end
end

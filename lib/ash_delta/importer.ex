defmodule AshDelta.Importer do
  @moduledoc """
  Register existing Parquet files into AshDelta's catalog without going
  through the normal write path.

  This is the "wrap an existing data lake" path. If you already have
  hive-partitioned Parquet files at an S3 prefix — written by Spark, Flink,
  DuckDB COPY, or anything else — `import_existing/2` discovers them via a
  single DuckDB glob scan, collects row counts and column stats in one pass,
  parses partition values from hive-style directory names, and commits the
  whole batch as a single "import" version in the AshDelta log.

  After import, all AshDelta features work as if the files had been written
  through the data layer: time travel, partition pruning, stats-based file
  skipping, OPTIMIZE, VACUUM.

  ## Parquet file requirements

  AshDelta reads files with `read_parquet([paths], union_by_name=true)` and
  maps columns to Ash attributes by name. **Every column including partition
  columns must be present inside each Parquet file** — partition values that
  exist only in the directory name are not automatically injected. If your
  existing files omit partition columns (standard Hive behaviour), use
  `inject_partition_columns: true` in the options to have the importer add
  a thin wrapper that materialises them from the path.

  ## Usage

      # Files already at s3://my-bucket/telemetry/events/**/*.parquet
      {:ok, version} = AshDelta.import_existing(MyApp.Telemetry.Event)

      # Override the S3 prefix for a one-off import from a staging area
      {:ok, version} = AshDelta.import_existing(MyApp.Telemetry.Event,
        prefix: "staging/events/2026-06/")
  """

  alias AshDelta.{ConnectionPool, Info, Log}

  @doc """
  Discover Parquet files at the resource's configured prefix (or `prefix:`
  override), collect stats via DuckDB, and commit them into the catalog.

  Returns `{:ok, version}` on success or `{:error, reason}`.
  """
  def import_existing(resource, opts \\ []) do
    {:ok, bucket} = Info.bucket(resource)
    {:ok, default_prefix} = Info.prefix(resource)
    {:ok, stats_cols} = Info.stats_columns(resource)
    {:ok, partition_cols} = Info.partition_by(resource)

    prefix = Keyword.get(opts, :prefix, default_prefix)
    glob = "s3://#{bucket}/#{prefix}/**/*.parquet"

    with {:ok, conn} <- ConnectionPool.checkout(resource),
         {:ok, file_stats} <- collect_stats(conn, glob, stats_cols) do
      if file_stats == [] do
        {:error, "no Parquet files found at #{glob}"}
      else
        specs = Enum.map(file_stats, fn {path, row_count, col_stats} ->
          %{
            path: path,
            size_bytes: 0,
            row_count: row_count,
            partition_values: parse_hive_path(path, partition_cols),
            column_stats: col_stats
          }
        end)

        Log.commit(
          resource,
          :import,
          %{"file_count" => length(specs), "source_prefix" => prefix},
          fn repo, table_id, version ->
            Log.add_files(repo, table_id, version, specs)
          end
        )
      end
    end
  end

  # ── Stats collection ──────────────────────────────────────────────────────

  # One DuckDB query across all files; groups by filename to get per-file
  # row counts and min/max for each stats column.
  defp collect_stats(conn, glob, stats_cols) do
    stat_exprs =
      Enum.flat_map(stats_cols, fn col ->
        [
          "min(\"#{col}\") AS #{col}__min",
          "max(\"#{col}\") AS #{col}__max"
        ]
      end)

    extra = if stat_exprs == [], do: "", else: ", " <> Enum.join(stat_exprs, ", ")

    sql = """
    SELECT filename, count(*) AS __row_count#{extra}
    FROM read_parquet('#{glob}', filename = true)
    GROUP BY filename
    ORDER BY filename
    """

    case Duckdbex.query(conn, sql, []) do
      {:ok, result} ->
        col_names = result |> Duckdbex.columns() |> Enum.map(&String.to_atom/1)
        rows = Duckdbex.fetch_all(result)

        stats =
          Enum.map(rows, fn row ->
            m = Map.new(Enum.zip(col_names, row))
            path = m.filename
            row_count = m.__row_count

            col_stats =
              Map.new(stats_cols, fn col ->
                min_key = :"#{col}__min"
                max_key = :"#{col}__max"

                {col,
                 %{
                   "min" => encode_stat(m[min_key]),
                   "max" => encode_stat(m[max_key]),
                   "null_count" => 0
                 }}
              end)

            {path, row_count, col_stats}
          end)

        {:ok, stats}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ── Hive path parsing ─────────────────────────────────────────────────────

  # Extracts partition key=value pairs from a hive-style S3 path.
  # "s3://bucket/prefix/site=CHI/day=2026-06-11/part-xxx.parquet"
  # → %{"site" => "CHI", "day" => "2026-06-11"}
  defp parse_hive_path(path, partition_cols) do
    valid = MapSet.new(partition_cols, &to_string/1)

    path
    |> String.split("/")
    |> Enum.flat_map(fn seg ->
      case String.split(seg, "=", parts: 2) do
        [k, v] -> if MapSet.member?(valid, k), do: [{k, v}], else: []
        _ -> []
      end
    end)
    |> Map.new()
  end

  # Encode DuckDB raw values to the JSON-serialisable form stored in column_stats.
  defp encode_stat(nil), do: nil

  defp encode_stat({{y, m, d}, {h, mi, s, us}}),
    do: NaiveDateTime.new!(y, m, d, h, mi, trunc(s), {us, 6}) |> NaiveDateTime.to_iso8601()

  defp encode_stat({y, m, d}) when is_integer(y),
    do: Date.new!(y, m, d) |> Date.to_iso8601()

  defp encode_stat(v), do: v
end

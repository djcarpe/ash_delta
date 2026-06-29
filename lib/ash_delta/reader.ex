defmodule AshDelta.Reader do
  @moduledoc """
  Executes the residual query over the pruned file set with DuckDB:

      SELECT cols FROM read_parquet(['s3://...', ...])
      WHERE <compiled Ash filter> ORDER BY ... LIMIT ... OFFSET ...

  Postgres decided *which files*; DuckDB does the columnar scan with its own
  row-group-level predicate pushdown inside each file (where the
  `sort_within_files` ordering pays off a second time).

  ## Connection reuse

  `AshDelta.ConnectionPool` keeps one DuckDB database alive per resource
  module. Each query obtains a new connection from that cached database
  (~1 ms) rather than opening a fresh database (~200-400 ms cold). The S3
  secret and extension settings are applied idempotently per connection.

  ## Column projection

  When the Ash query carries a `select` list (from `Ash.Query.select/2`),
  only those columns are fetched from Parquet. DuckDB pushes the projection
  down to the row-group reader, so unselected columns are never read from S3.

  ## Streaming large result sets

  `run_query` returns a full list because the Ash data layer contract
  requires it. For result sets too large to fit in memory, use
  `Ash.stream!/2` — it drives offset-based pagination through the data layer
  automatically, keeping at most one page in memory at a time.
  """

  alias AshDelta.{ConnectionPool, Sql}

  @doc "Run an `AshDelta.DataLayer.Query` over candidate files, returning records."
  def scan(resource, files, query) do
    paths = Enum.map(files, & &1.path)
    all_attrs = Ash.Resource.Info.attributes(resource)

    # Narrow to selected columns when Ash passes a select list; push the
    # projection into DuckDB so unselected Parquet columns are never read.
    selected_attrs =
      case query.select do
        nil -> all_attrs
        cols -> Enum.filter(all_attrs, &(&1.name in cols))
      end

    select_sql = Enum.map_join(selected_attrs, ", ", &~s("#{&1.name}"))

    {where_sql, params} =
      case query.filter do
        nil -> {"TRUE", []}
        filter -> Sql.compile(filter)
      end

    sql =
      """
      SELECT #{select_sql}
      FROM read_parquet([#{Enum.map_join(paths, ", ", &"'#{&1}'")}], union_by_name = true)
      WHERE #{where_sql}
      """
      |> append_sort(query.sort)
      |> append_limit_offset(query.limit, query.offset)

    with {:ok, conn} <- ConnectionPool.checkout(resource),
         {:ok, result} <- Duckdbex.query(conn, sql, params) do
      columns = result |> Duckdbex.columns() |> Enum.map(&String.to_atom/1)
      {:ok, stream_records(all_attrs, columns, result, resource)}
    end
  end

  defp append_sort(sql, []), do: sql
  defp append_sort(sql, nil), do: sql

  defp append_sort(sql, sort) do
    clauses =
      Enum.map_join(sort, ", ", fn
        {field, :asc} -> ~s("#{field}" ASC)
        {field, :desc} -> ~s("#{field}" DESC)
        {field, :asc_nils_first} -> ~s("#{field}" ASC NULLS FIRST)
        {field, :desc_nils_last} -> ~s("#{field}" DESC NULLS LAST)
        {field, _} -> ~s("#{field}" ASC)
      end)

    sql <> " ORDER BY #{clauses}"
  end

  defp append_limit_offset(sql, nil, nil), do: sql
  defp append_limit_offset(sql, limit, nil), do: sql <> " LIMIT #{limit}"
  defp append_limit_offset(sql, nil, offset), do: sql <> " OFFSET #{offset}"
  defp append_limit_offset(sql, limit, offset), do: sql <> " LIMIT #{limit} OFFSET #{offset}"

  # ── Row materialisation ────────────────────────────────────────────────────

  @doc """
  Run a query and return an `Explorer.DataFrame` instead of a list of structs.

  This avoids Elixir struct allocation entirely: DuckDB rows are fetched as raw
  tuples, transposed into column vectors, and handed directly to Explorer. For
  analytics workloads (aggregations, exports, charting) this is dramatically
  faster than `scan/3` — no `Ash.Type.cast_stored/3` per cell, no struct
  construction. Memory scales with actual data size, not Elixir term overhead.

  The returned DataFrame has column names matching the resource's attribute
  names. Use `AshDelta.to_dataframe!/1` as the public entry point.
  """
  def scan_dataframe(resource, files, query) do
    paths = Enum.map(files, & &1.path)
    all_attrs = Ash.Resource.Info.attributes(resource)

    selected_attrs =
      case query.select do
        nil -> all_attrs
        cols -> Enum.filter(all_attrs, &(&1.name in cols))
      end

    select_sql = Enum.map_join(selected_attrs, ", ", &~s("#{&1.name}"))

    {where_sql, params} =
      case query.filter do
        nil -> {"TRUE", []}
        filter -> Sql.compile(filter)
      end

    sql =
      """
      SELECT #{select_sql}
      FROM read_parquet([#{Enum.map_join(paths, ", ", &"'#{&1}'")}], union_by_name = true)
      WHERE #{where_sql}
      """
      |> append_sort(query.sort)
      |> append_limit_offset(query.limit, query.offset)

    with {:ok, conn} <- ConnectionPool.checkout(resource),
         {:ok, result} <- Duckdbex.query(conn, sql, params) do
      col_names = Duckdbex.columns(result)
      n_cols = length(col_names)
      attr_by_name = Map.new(selected_attrs, &{to_string(&1.name), &1})

      # Collect chunks and transpose rows→columns in one pass (O(1) per cell).
      col_vectors =
        Stream.resource(
          fn -> result end,
          fn r ->
            case Duckdbex.fetch_chunk(r) do
              [] -> {:halt, r}
              rows -> {rows, r}
            end
          end,
          fn _ -> :ok end
        )
        |> Enum.reduce(List.duplicate([], n_cols), fn row, acc ->
          values = if is_list(row), do: row, else: Tuple.to_list(row)
          Enum.zip_with(acc, values, fn col_acc, val -> [normalize(val) | col_acc] end)
        end)
        |> Enum.map(&Enum.reverse/1)

      series_map =
        Map.new(Enum.zip(col_names, col_vectors), fn {col_name, values} ->
          attr = Map.get(attr_by_name, col_name)
          dtype = if attr, do: ash_to_explorer_dtype(attr.type), else: :string
          {col_name, Explorer.Series.from_list(values, dtype: dtype)}
        end)

      {:ok, Explorer.DataFrame.new(series_map)}
    end
  end

  @doc """
  Execute an arbitrary DuckDB query (used by view refresh) and materialise the
  result rows into structs of `resource`, casting each column through its Ash
  type. The query's output columns must match `resource`'s attribute names.
  """
  def query_records(resource, sql, params \\ []) do
    attrs = Ash.Resource.Info.attributes(resource)

    with {:ok, conn} <- ConnectionPool.checkout(resource),
         {:ok, result} <- Duckdbex.query(conn, sql, params) do
      columns = result |> Duckdbex.columns() |> Enum.map(&String.to_atom/1)
      {:ok, stream_records(attrs, columns, result, resource)}
    end
  end

  # Fetch DuckDB result in columnar chunks and materialise each row into a
  # resource struct. Chunked iteration avoids a single large allocation when
  # the result set is big; the final Enum.to_list/1 is unavoidable because
  # the data layer contract requires a list.
  defp stream_records(attrs, columns, result, resource) do
    attr_by_name = Map.new(attrs, &{&1.name, &1})

    Stream.resource(
      fn -> result end,
      fn r ->
        case Duckdbex.fetch_chunk(r) do
          [] -> {:halt, r}
          rows -> {Enum.map(rows, &to_record(resource, attr_by_name, columns, &1)), r}
        end
      end,
      fn _ -> :ok end
    )
    |> Enum.to_list()
  end

  defp to_record(resource, attr_by_name, columns, row) do
    fields =
      columns
      |> Enum.zip(row)
      |> Map.new(fn {col, raw} ->
        attr = Map.fetch!(attr_by_name, col)
        {:ok, value} = Ash.Type.cast_stored(attr.type, normalize(raw), attr.constraints)
        {col, value}
      end)

    struct(resource, fields)
  end

  @doc false
  def ash_to_explorer_dtype_pub(type), do: ash_to_explorer_dtype(type)

  defp ash_to_explorer_dtype(Ash.Type.Integer),          do: {:s, 64}
  defp ash_to_explorer_dtype(Ash.Type.Float),            do: {:f, 64}
  defp ash_to_explorer_dtype(Ash.Type.Decimal),          do: {:f, 64}
  defp ash_to_explorer_dtype(Ash.Type.Boolean),          do: :boolean
  defp ash_to_explorer_dtype(Ash.Type.Date),             do: :date
  defp ash_to_explorer_dtype(Ash.Type.NaiveDateTime),    do: {:naive_datetime, :microsecond}
  defp ash_to_explorer_dtype(Ash.Type.NaiveDatetimeUsec), do: {:naive_datetime, :microsecond}
  defp ash_to_explorer_dtype(Ash.Type.UtcDatetime),      do: {:naive_datetime, :microsecond}
  defp ash_to_explorer_dtype(Ash.Type.UtcDatetimeUsec),  do: {:naive_datetime, :microsecond}
  defp ash_to_explorer_dtype(:integer),                  do: {:s, 64}
  defp ash_to_explorer_dtype(t) when t in [:float, :decimal], do: {:f, 64}
  defp ash_to_explorer_dtype(:boolean),                  do: :boolean
  defp ash_to_explorer_dtype(:date),                     do: :date
  defp ash_to_explorer_dtype(t) when t in [:naive_datetime, :naive_datetime_usec,
                                            :utc_datetime, :utc_datetime_usec],
    do: {:naive_datetime, :microsecond}
  defp ash_to_explorer_dtype(_),                         do: :string

  # Duckdbex returns timestamps/dates as tuples; normalise before Ash casting.
  defp normalize({{y, m, d}, {h, mi, s, us}}),
    do: NaiveDateTime.new!(y, m, d, h, mi, trunc(s), {us, 6})

  defp normalize({y, m, d}) when is_integer(y), do: Date.new!(y, m, d)
  defp normalize(v), do: v
end

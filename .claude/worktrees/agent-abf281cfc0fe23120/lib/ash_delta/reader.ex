defmodule AshDelta.Reader do
  @moduledoc """
  Executes the residual query over the pruned file set with DuckDB:

      SELECT cols FROM read_parquet(['s3://...', ...])
      WHERE <compiled Ash filter> ORDER BY ... LIMIT ... OFFSET ...

  Postgres decided *which files*; DuckDB does the columnar scan with its own
  row-group-level predicate pushdown inside each file (where the
  `sort_within_files` ordering pays off a second time).
  """

  alias AshDelta.{Info, Sql}

  @doc "Run an `AshDelta.DataLayer.Query` over candidate files, returning records."
  def scan(resource, files, query) do
    paths = Enum.map(files, & &1.path)
    all_attrs = Ash.Resource.Info.attributes(resource)

    selected_attrs =
      case Map.get(query, :select) do
        nil -> all_attrs
        cols -> Enum.filter(all_attrs, &(&1.name in cols))
      end

    select_sql = Enum.map_join(selected_attrs, ", ", &~s("#{&1.name}"))

    {where_sql, params} =
      case query.filter do
        nil -> {"TRUE", []}
        filter -> Sql.compile(filter)
      end

    columns_param =
      case Map.get(query, :select) do
        nil -> ""
        _ ->
          cols = Enum.map_join(selected_attrs, ", ", fn a ->
            "'#{a.name}': '#{ash_to_duckdb_type(a.type)}'"
          end)
          ", columns = {#{cols}}"
      end

    sql =
      """
      SELECT #{select_sql}
      FROM read_parquet([#{Enum.map_join(paths, ", ", &"'#{&1}'")}], union_by_name = true#{columns_param})
      WHERE #{where_sql}
      """
      |> append_sort(query.sort)
      |> append_limit_offset(query.limit, query.offset)

    with {:ok, conn} <- open_connection(resource),
         {:ok, result} <- Duckdbex.query(conn, sql, params) do
      columns = result |> Duckdbex.columns() |> Enum.map(&String.to_atom/1)
      rows = Duckdbex.fetch_all(result)
      {:ok, Enum.map(rows, &to_record(resource, all_attrs, columns, &1))}
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

  # ── DuckDB session with S3 access ────────────────────────────────────────

  @doc false
  def open_connection(resource) do
    {:ok, s3_config} = Info.s3_config(resource)

    with {:ok, db} <- Duckdbex.open(),
         {:ok, conn} <- Duckdbex.connection(db) do
      Duckdbex.query(conn, "INSTALL httpfs; LOAD httpfs;")

      secret_fields =
        [
          {"KEY_ID", s3_config[:access_key_id]},
          {"SECRET", s3_config[:secret_access_key]},
          {"REGION", s3_config[:region]},
          {"ENDPOINT", s3_config[:endpoint]}
        ]
        |> Enum.reject(fn {_, v} -> is_nil(v) end)

      case secret_fields do
        [] ->
          # Fall back to the ambient credential chain (env/instance profile).
          Duckdbex.query(conn, "CREATE OR REPLACE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN);")

        fields ->
          assignments = Enum.map_join(fields, ", ", fn {k, v} -> "#{k} '#{v}'" end)
          Duckdbex.query(conn, "CREATE OR REPLACE SECRET (TYPE S3, #{assignments});")
      end

      {:ok, conn}
    end
  end

  # ── Row → record ─────────────────────────────────────────────────────────

  @doc """
  Execute an arbitrary DuckDB query (used by view refresh) and materialize the
  result rows into structs of `resource`, casting each column through its Ash
  type. The query's output columns must match `resource`'s attribute names.
  """
  def query_records(resource, sql, params \\ []) do
    attrs = Ash.Resource.Info.attributes(resource)

    with {:ok, conn} <- open_connection(resource),
         {:ok, result} <- Duckdbex.query(conn, sql, params) do
      columns = result |> Duckdbex.columns() |> Enum.map(&String.to_atom/1)
      rows = Duckdbex.fetch_all(result)
      {:ok, Enum.map(rows, &to_record(resource, attrs, columns, &1))}
    end
  end

  defp to_record(resource, attrs, columns, row) do
    attr_by_name = Map.new(attrs, &{&1.name, &1})

    fields =
      columns
      |> Enum.zip(row)
      |> Map.new(fn {col, raw} ->
        attr = Map.fetch!(attr_by_name, col)
        {:ok, value} = Ash.Type.cast_stored(attr.type, normalize(raw), attr.constraints)
        {col, value}
      end)

    resource
    |> struct(fields)
    |> Map.put(:__meta__, %Ecto.Schema.Metadata{
      state: :loaded,
      source: Info.table_name(resource)
    })
  end

  defp ash_to_duckdb_type(t) when t in [:string, :uuid, :ci_string, :atom], do: "VARCHAR"
  defp ash_to_duckdb_type(:integer), do: "BIGINT"
  defp ash_to_duckdb_type(t) when t in [:float, :decimal], do: "DOUBLE"
  defp ash_to_duckdb_type(:boolean), do: "BOOLEAN"
  defp ash_to_duckdb_type(:date), do: "DATE"
  defp ash_to_duckdb_type(t) when t in [:utc_datetime, :utc_datetime_usec], do: "TIMESTAMPTZ"
  defp ash_to_duckdb_type(t) when t in [:naive_datetime, :naive_datetime_usec], do: "TIMESTAMP"
  defp ash_to_duckdb_type(_), do: "VARCHAR"

  # Duckdbex returns timestamps/dates as tuples; normalize before Ash casting.
  defp normalize({{y, m, d}, {h, mi, s, us}}),
    do: NaiveDateTime.new!(y, m, d, h, mi, trunc(s), {us, 6})

  defp normalize({y, m, d}) when is_integer(y), do: Date.new!(y, m, d)
  defp normalize(v), do: v
end

defmodule AshDelta.Read do
  @moduledoc """
  The read path, shared verbatim by `AshDelta.DataLayer` (base tables) and
  `AshDelta.ViewLayer` (materialized views). Because a view's contents live in
  the same `delta_files` manifest as a base table, reads — version
  resolution, partition/stats pruning, the DuckDB scan — are identical. Time
  travel on a view (`set_context(%{delta: %{version: v}})`) therefore resolves
  against the *view's* own commit history.
  """

  alias AshDelta.{ConnectionPool, Log, Pruner, Reader, Sql}

  @doc "Run a built query (the shared `DataLayer.Query` struct) against `resource`."
  def run(query, resource) do
    with {:ok, version} <- Log.resolve_version(resource, query.context[:delta] || %{}),
         {:ok, files} <- Pruner.candidate_files(resource, version, query.filter) do
      case files do
        [] -> {:ok, []}
        files -> Reader.scan(resource, files, query)
      end
    end
  end

  @doc "Run aggregate expressions in DuckDB without materializing rows."
  def run_aggregates(query, aggregates, resource) do
    delta_context = Map.get(query.context, :delta, %{})

    with {:ok, version} <- Log.resolve_version(resource, delta_context),
         {:ok, files} <- Pruner.candidate_files(resource, version, query.filter) do
      if files == [] do
        {:ok, Map.new(aggregates, fn agg -> {agg.name, zero_for(agg.kind)} end)}
      else
        paths = Enum.map(files, & &1.path)

        {where_sql, params} =
          case query.filter do
            nil -> {"TRUE", []}
            filter -> Sql.compile(filter)
          end

        agg_exprs =
          Enum.map_join(aggregates, ", ", fn agg ->
            case agg.kind do
              :count -> "count(*) AS \"#{agg.name}\""
              :sum -> "sum(\"#{agg.field}\") AS \"#{agg.name}\""
              :avg -> "avg(\"#{agg.field}\") AS \"#{agg.name}\""
              :min -> "min(\"#{agg.field}\") AS \"#{agg.name}\""
              :max -> "max(\"#{agg.field}\") AS \"#{agg.name}\""
              _ -> "count(*) AS \"#{agg.name}\""
            end
          end)

        file_list = Enum.map_join(paths, ", ", &"'#{&1}'")

        sql = """
        SELECT #{agg_exprs}
        FROM read_parquet([#{file_list}], union_by_name = true)
        WHERE #{where_sql}
        """

        with {:ok, conn} <- ConnectionPool.checkout(resource),
             {:ok, result} <- Duckdbex.query(conn, sql, params) do
          col_names = result |> Duckdbex.columns() |> Enum.map(&String.to_atom/1)
          rows = Duckdbex.fetch_all(result)

          case rows do
            [row] ->
              {:ok, Map.new(Enum.zip(col_names, row))}

            [] ->
              {:ok, Map.new(aggregates, fn agg -> {agg.name, zero_for(agg.kind)} end)}
          end
        end
      end
    end
  end

  defp zero_for(:count), do: 0
  defp zero_for(_), do: nil
end

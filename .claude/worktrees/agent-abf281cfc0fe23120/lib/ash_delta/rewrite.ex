defmodule AshDelta.Rewrite do
  @moduledoc """
  Copy-on-write mutations — the Delta DELETE/UPDATE mechanism:

    1. Prune to files that may contain matching rows
    2. For each, scan with DuckDB and split rows into kept vs. affected
    3. Write replacement files containing the surviving (or updated) rows
    4. Commit `remove(old) + add(new)` atomically

  Files with zero matching rows are left untouched (the pruner overshoots by
  design; the scan confirms). A file whose every row matches a delete is
  simply removed with no replacement.
  """

  alias AshDelta.{DataLayer.Query, Log, Pruner, Reader, Sql, Writer}

  @doc "Delete all rows matching `filter` (an Ash filter, expression, or keyword)."
  def delete_where(resource, filter) do
    rewrite(resource, filter, :delete, fn _affected -> [] end)
  end

  @doc "Set `changes` (map of attr => value) on all rows matching `filter`."
  def update_where(resource, filter, changes) do
    rewrite(resource, filter, :update, fn affected ->
      Enum.map(affected, &struct(&1, changes))
    end)
  end

  defp rewrite(resource, filter, operation, transform) do
    filter = normalize_filter(resource, filter)

    with {:ok, version} <- Log.resolve_version(resource, %{}),
         {:ok, candidates} <- Pruner.candidate_files(resource, version, filter) do
      {where_sql, params} = Sql.compile(filter)

      # Scan + rewrite outside the commit lock; only metadata inside it.
      plans =
        candidates
        |> Enum.map(&plan_file(resource, &1, where_sql, params, transform))
        |> Enum.reject(&is_nil/1)

      case plans do
        [] ->
          {:ok, %{version: version, files_rewritten: 0, rows_affected: 0}}

        plans ->
          removed_ids = Enum.map(plans, & &1.remove_id)
          added = Enum.flat_map(plans, & &1.add_specs)
          rows = Enum.sum_by(plans, & &1.rows_affected)

          result =
            Log.commit(
              resource,
              operation,
              %{"rows_affected" => rows, "files_rewritten" => length(plans)},
              fn repo, table_id, new_version ->
                with :ok <- Log.remove_files(repo, table_id, new_version, removed_ids) do
                  Log.add_files(repo, table_id, new_version, added)
                end
              end
            )

          case result do
            {:ok, new_version} ->
              {:ok, %{version: new_version, files_rewritten: length(plans), rows_affected: rows}}

            {:error, :concurrent_modification} ->
              # A concurrent commit tombstoned one of our files between the
              # scan and the commit. The orphaned replacement Parquet objects
              # are unreferenced and harmless; VACUUM-style GC could reap
              # them. Retry from a fresh snapshot.
              rewrite(resource, filter, operation, transform)

            {:error, reason} ->
              {:error, reason}
          end
      end
    end
  end

  # Returns nil when the file contains no matching rows (skip), otherwise a
  # plan with the file id to remove and replacement file specs to add.
  defp plan_file(resource, file, where_sql, params, transform) do
    query = %Query{resource: resource}

    {:ok, conn} = Reader.open_connection(resource)

    {:ok, count_result} =
      Duckdbex.query(
        conn,
        "SELECT count(*) FROM read_parquet('#{file.path}') WHERE #{where_sql}",
        params
      )

    [[match_count]] = Duckdbex.fetch_all(count_result)

    if match_count == 0 do
      nil
    else
      {:ok, all_rows} = Reader.scan(resource, [file], query)

      {:ok, affected} =
        Reader.scan(resource, [file], %{query | filter: {:raw, where_sql, params}})

      affected_keys = MapSet.new(affected, &primary_key_of(resource, &1))
      kept = Enum.reject(all_rows, &MapSet.member?(affected_keys, primary_key_of(resource, &1)))
      survivors = kept ++ transform.(affected)

      add_specs =
        case survivors do
          [] -> []
          survivors -> Writer.write_files(resource, survivors)
        end

      %{remove_id: file.id, add_specs: add_specs, rows_affected: match_count}
    end
  end

  defp primary_key_of(resource, record) do
    resource |> Ash.Resource.Info.primary_key() |> Enum.map(&Map.get(record, &1))
  end

  defp normalize_filter(_resource, %Ash.Filter{} = filter), do: filter

  defp normalize_filter(resource, filter) when is_map(filter) or is_list(filter) do
    Ash.Filter.parse!(resource, Enum.to_list(filter))
  end

  defp normalize_filter(_resource, expr), do: expr
end

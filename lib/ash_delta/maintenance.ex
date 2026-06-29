defmodule AshDelta.Maintenance do
  @moduledoc """
  `OPTIMIZE` and `VACUUM`, the two background jobs that keep a Delta-style
  table healthy. Run them from Oban, a Quantum schedule, a Horde singleton —
  whatever your cluster already uses for periodic work.
  """

  alias AshDelta.{Info, Log, Reader, Writer}
  alias AshDelta.DataLayer.Query

  # ── OPTIMIZE ─────────────────────────────────────────────────────────────

  @doc """
  Bin-packing compaction: within each partition, gather files smaller than
  half the target size and rewrite them into target-sized files. Sorting is
  re-applied during the rewrite, so OPTIMIZE also restores clustering that
  interleaved small appends destroyed.

  Options:

    * `:partition` — map of partition values to scope compaction
    * `:min_files` — only compact partitions with at least this many small
      files (default `4`)
  """
  def optimize(resource, opts \\ []) do
    {:ok, target_mb} = Info.target_file_size_mb(resource)
    target_bytes = target_mb * 1024 * 1024
    min_files = Keyword.get(opts, :min_files, 4)

    {:ok, version} = Log.resolve_version(resource, %{})

    groups =
      resource
      |> Log.snapshot(version)
      |> Enum.filter(&(&1.size_bytes < div(target_bytes, 2)))
      |> filter_partition(opts[:partition])
      |> Enum.group_by(& &1.partition_values)
      |> Enum.filter(fn {_, files} -> length(files) >= min_files end)

    results = Enum.map(groups, fn {_partition, files} -> compact_group(resource, files) end)

    {:ok,
     %{
       partitions_compacted: length(results),
       files_removed: Enum.sum_by(results, &elem(&1, 0)),
       files_added: Enum.sum_by(results, &elem(&1, 1))
     }}
  end

  defp compact_group(resource, files) do
    # Read every row from the small files and rewrite. Writer re-applies
    # partitioning (a no-op here, same partition) and sort_within_files.
    {:ok, records} = Reader.scan(resource, files, %Query{resource: resource})

    add_specs = Writer.write_files(resource, records)
    remove_ids = Enum.map(files, & &1.id)

    {:ok, _version} =
      Log.commit(
        resource,
        :optimize,
        %{"removed" => length(remove_ids), "added" => length(add_specs)},
        fn repo, table_id, version ->
          with :ok <- Log.remove_files(repo, table_id, version, remove_ids) do
            Log.add_files(repo, table_id, version, add_specs)
          end
        end
      )

    {length(remove_ids), length(add_specs)}
  end

  defp filter_partition(files, nil), do: files

  defp filter_partition(files, partition) do
    target = Map.new(partition, fn {k, v} -> {to_string(k), AshDelta.Pruner.encode(v)} end)

    Enum.filter(files, fn f ->
      Enum.all?(target, fn {k, v} -> f.partition_values[k] == v end)
    end)
  end

  # ── VACUUM ───────────────────────────────────────────────────────────────

  @doc """
  Physically delete S3 objects for files tombstoned longer than the
  retention window, then purge their manifest rows.

  Retention matters for the same reason it does in Delta: a long-running
  time-travel read of an old version may still reference tombstoned files.
  Don't set retention below your longest expected query/as-of horizon.

  Options:

    * `:retention_hours` — override the DSL `vacuum_retention_hours`
    * `:dry_run` — return the candidate paths without deleting
  """
  def vacuum(resource, opts \\ []) do
    repo = Log.repo!(resource)
    table_id = Log.ensure_table!(resource)
    {:ok, default_retention} = Info.vacuum_retention_hours(resource)
    retention = Keyword.get(opts, :retention_hours, default_retention)
    cutoff = DateTime.add(DateTime.utc_now(), -retention * 3600, :second)

    %{rows: rows} =
      repo.query!(
        """
        SELECT f.id, f.path
        FROM delta_files f
        JOIN delta_commits c
          ON c.table_id = f.table_id AND c.version = f.removed_version
        WHERE f.table_id = $1
          AND f.removed_version IS NOT NULL
          AND c.committed_at < $2
        """,
        [table_id, cutoff]
      )

    if opts[:dry_run] do
      {:ok, %{candidates: Enum.map(rows, fn [_, path] -> path end)}}
    else
      deleted =
        Enum.reduce(rows, 0, fn [id, path], acc ->
          case delete_object(resource, path) do
            :ok ->
              repo.query!("DELETE FROM delta_files WHERE id = $1", [id])
              acc + 1

            {:error, _} ->
              # Leave the manifest row; the next vacuum retries.
              acc
          end
        end)

      {:ok, %{deleted: deleted, candidates: length(rows)}}
    end
  end

  defp delete_object(resource, "s3://" <> rest) do
    {:ok, s3_config} = Info.s3_config(resource)
    [bucket, key] = String.split(rest, "/", parts: 2)

    case bucket |> ExAws.S3.delete_object(key) |> ExAws.request(s3_config) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end

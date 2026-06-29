defmodule AshDelta.Log do
  @moduledoc """
  The Postgres-backed transaction log — the equivalent of `_delta_log/`.

  Three tables (see `AshDelta.Migrations`):

    * `delta_tables`  — one row per logical table, holds `current_version`
    * `delta_commits` — one row per version: operation + params + timestamp
    * `delta_files`   — file manifest: every Parquet file ever added, with
      `added_version` / `removed_version` interval, partition values, and
      column stats. A file is *live at version V* when
      `added_version <= V AND (removed_version IS NULL OR removed_version > V)`.

  ## Commit protocol

  Delta Lake gets atomicity from an atomic rename of `N.json`; concurrent
  writers race and the loser retries. Here we instead take a `FOR UPDATE`
  row lock on the table's `delta_tables` row, which serializes commits
  per table. The Parquet uploads happen *before* `commit/4` is called, so
  the critical section is a handful of metadata inserts — microseconds of
  lock hold time. Conflict checking (e.g. concurrent OPTIMIZE removed a
  file your DELETE also wants to remove) is enforced by the
  `delta_files.removed_version IS NULL` predicate in `remove_files/4`:
  if another commit already tombstoned the file, the affected-rows count
  mismatches and the transaction rolls back.
  """

  alias AshDelta.Info

  # ── Snapshot + table-id caches ──────────────────────────────────────────

  def init_cache do
    if :ets.whereis(:ash_delta_snapshot_cache) == :undefined do
      :ets.new(:ash_delta_snapshot_cache, [:named_table, :public, read_concurrency: true])
    end

    if :ets.whereis(:ash_delta_table_ids) == :undefined do
      :ets.new(:ash_delta_table_ids, [:named_table, :public, read_concurrency: true])
    end

    if :ets.whereis(:ash_delta_compacting) == :undefined do
      :ets.new(:ash_delta_compacting, [:named_table, :public])
    end
  end

  def cache_snapshot(table_id, version, files) do
    :ets.insert(:ash_delta_snapshot_cache, {table_id, {version, files}})
  end

  def get_cached_snapshot(table_id, version) do
    case :ets.lookup(:ash_delta_snapshot_cache, table_id) do
      [{_, {^version, files}}] -> {:hit, files}
      _ -> :miss
    end
  end

  def invalidate_snapshot(table_id) do
    :ets.delete(:ash_delta_snapshot_cache, table_id)
  end

  @type file_entry :: %{
          id: integer(),
          path: String.t(),
          size_bytes: integer(),
          row_count: integer(),
          partition_values: map(),
          column_stats: map(),
          added_version: integer()
        }

  # ── Table registration ───────────────────────────────────────────────────

  @doc "Idempotently registers the resource in `delta_tables`, returns its id."
  def ensure_table!(resource) do
    case :ets.lookup(:ash_delta_table_ids, resource) do
      [{_, id}] ->
        id

      [] ->
        repo = repo!(resource)
        name = Info.table_name(resource)
        {:ok, bucket} = Info.bucket(resource)
        {:ok, prefix} = Info.prefix(resource)

        %{rows: [[id]]} =
          repo.query!(
            """
            INSERT INTO delta_tables (resource, bucket, prefix)
            VALUES ($1, $2, $3)
            ON CONFLICT (resource) DO UPDATE SET bucket = EXCLUDED.bucket
            RETURNING id
            """,
            [name, bucket, prefix || ""]
          )

        :ets.insert(:ash_delta_table_ids, {resource, id})
        id
    end
  end

  # ── Committing ───────────────────────────────────────────────────────────

  @doc """
  Run `fun.(repo, table_id, new_version)` inside a serialized commit.

  `fun` should insert file adds/removes for `new_version` and may raise or
  return `{:error, reason}` to abort. On success the commit row is written
  and `current_version` advanced.
  """
  def commit(resource, operation, params \\ %{}, fun) do
    repo = repo!(resource)
    table_id = ensure_table!(resource)

    repo.transaction(fn ->
      %{rows: [[current]]} =
        repo.query!(
          "SELECT current_version FROM delta_tables WHERE id = $1 FOR UPDATE",
          [table_id]
        )

      version = current + 1

      case fun.(repo, table_id, version) do
        {:error, reason} ->
          repo.rollback(reason)

        _ok ->
          repo.query!(
            """
            INSERT INTO delta_commits (table_id, version, operation, operation_params)
            VALUES ($1, $2, $3, $4)
            """,
            [table_id, version, to_string(operation), params]
          )

          repo.query!(
            "UPDATE delta_tables SET current_version = $1 WHERE id = $2",
            [version, table_id]
          )

          invalidate_snapshot(table_id)
          repo.query!("SELECT pg_notify('ash_delta_invalidations', $1::text)", [to_string(table_id)])
          version
      end
    end)
  end

  @doc "Insert add-file actions for `version`. Call inside `commit/4`'s fun."
  def add_files(repo, table_id, version, file_specs) do
    Enum.each(file_specs, fn spec ->
      %{rows: [[file_id]]} =
        repo.query!(
          """
          INSERT INTO delta_files
            (table_id, path, size_bytes, row_count, partition_values, column_stats, added_version)
          VALUES ($1, $2, $3, $4, $5, $6, $7)
          RETURNING id
          """,
          [table_id, spec.path, spec.size_bytes, spec.row_count,
           spec.partition_values, spec.column_stats, version]
        )

      Enum.each(spec.partition_values, fn {key, value} ->
        repo.query!(
          "INSERT INTO delta_file_partitions (file_id, col_name, col_value) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
          [file_id, to_string(key), to_string(value)]
        )
      end)

      Enum.each(spec.column_stats, fn {col, stats} ->
        min_raw = stats["min"]
        max_raw = stats["max"]
        {min_num, max_num} = {parse_num(min_raw), parse_num(max_raw)}

        repo.query!(
          """
          INSERT INTO delta_file_stats
            (file_id, col_name, min_val, max_val, min_num, max_num, null_count)
          VALUES ($1, $2, $3, $4, $5, $6, $7)
          ON CONFLICT DO NOTHING
          """,
          [file_id, to_string(col), to_string_or_nil(min_raw), to_string_or_nil(max_raw),
           min_num, max_num, stats["null_count"] || 0]
        )
      end)
    end)
  end

  defp parse_num(nil), do: nil
  defp parse_num(v) when is_number(v), do: v / 1
  defp parse_num(s) when is_binary(s) do
    case Float.parse(s) do
      {f, ""} -> f
      _ -> nil
    end
  end

  defp to_string_or_nil(nil), do: nil
  defp to_string_or_nil(v), do: to_string(v)

  @doc """
  Tombstone files at `version`. Fails the commit if any file was already
  removed by a concurrent commit (the Delta-style conflict check).
  """
  def remove_files(repo, table_id, version, file_ids) do
    %{num_rows: n} =
      repo.query!(
        """
        UPDATE delta_files SET removed_version = $1
        WHERE table_id = $2 AND id = ANY($3) AND removed_version IS NULL
        """,
        [version, table_id, file_ids]
      )

    if n == length(file_ids) do
      :ok
    else
      {:error, :concurrent_modification}
    end
  end

  # ── Snapshots & time travel ──────────────────────────────────────────────

  @doc "Resolve `%{version: v}` / `%{as_of: datetime}` / `%{}` to a concrete version."
  def resolve_version(resource, delta_context) do
    repo = repo!(resource)
    table_id = ensure_table!(resource)

    case delta_context do
      %{version: v} when is_integer(v) ->
        {:ok, v}

      %{as_of: %DateTime{} = ts} ->
        %{rows: rows} =
          repo.query!(
            """
            SELECT max(version) FROM delta_commits
            WHERE table_id = $1 AND committed_at <= $2
            """,
            [table_id, ts]
          )

        case rows do
          [[nil]] -> {:ok, 0}
          [[v]] -> {:ok, v}
        end

      _ ->
        %{rows: [[v]]} =
          repo.query!("SELECT current_version FROM delta_tables WHERE id = $1", [table_id])

        {:ok, v}
    end
  end

  @doc "All live files at `version` (current version when nil)."
  def snapshot(resource, version \\ nil) do
    {:ok, version} =
      resolve_version(resource, if(version, do: %{version: version}, else: %{}))

    table_id = ensure_table!(resource)

    case get_cached_snapshot(table_id, version) do
      {:hit, cached_files} ->
        cached_files

      :miss ->
        result = files(resource, version, "TRUE", [])
        cache_snapshot(table_id, version, result)
        result
    end
  end

  @doc """
  Live files at `version` matching extra SQL conditions (used by the pruner).
  `conditions` is a SQL fragment over the `delta_files` columns with
  placeholders starting at `$3`; `params` supplies their values.
  """
  def files(resource, version, conditions, params) do
    repo = repo!(resource)
    table_id = ensure_table!(resource)

    %{rows: rows, columns: cols} =
      repo.query!(
        """
        SELECT f.id, f.path, f.size_bytes, f.row_count, f.partition_values, f.column_stats, f.added_version
        FROM delta_files f
        WHERE f.table_id = $1
          AND f.added_version <= $2
          AND (f.removed_version IS NULL OR f.removed_version > $2)
          AND (#{conditions})
        """,
        [table_id, version | params]
      )

    cols = Enum.map(cols, &String.to_atom/1)
    Enum.map(rows, fn row -> cols |> Enum.zip(row) |> Map.new() end)
  end

  @doc "Commit history, most recent first."
  def history(resource, opts \\ []) do
    repo = repo!(resource)
    table_id = ensure_table!(resource)
    limit = Keyword.get(opts, :limit, 50)

    %{rows: rows} =
      repo.query!(
        """
        SELECT version, operation, operation_params, committed_at
        FROM delta_commits
        WHERE table_id = $1
        ORDER BY version DESC
        LIMIT $2
        """,
        [table_id, limit]
      )

    Enum.map(rows, fn [v, op, params, ts] ->
      %{version: v, operation: op, params: params, committed_at: ts}
    end)
  end

  @doc false
  def repo!(resource) do
    {:ok, repo} = Info.repo(resource)
    repo
  end
end

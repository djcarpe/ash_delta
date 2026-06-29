defmodule AshDelta.Migrations do
  @moduledoc """
  Catalog schema. Either `use AshDelta.Migrations` inside one of your own
  Ecto migrations, or copy the SQL into a migration by hand.

      defmodule MyApp.Repo.Migrations.InstallAshDelta do
        use Ecto.Migration
        use AshDelta.Migrations
      end
  """

  defmacro __using__(_opts) do
    quote do
      def up, do: AshDelta.Migrations.up(__MODULE__)
      def down, do: AshDelta.Migrations.down(__MODULE__)
    end
  end

  def up(migration) do
    Enum.each(up_statements(), &migration.execute/1)
  end

  def down(migration) do
    Enum.each(down_statements(), &migration.execute/1)
  end

  def up_statements do
    [
      """
      CREATE TABLE delta_tables (
        id              bigserial PRIMARY KEY,
        resource        text NOT NULL UNIQUE,
        bucket          text NOT NULL,
        prefix          text NOT NULL DEFAULT '',
        current_version bigint NOT NULL DEFAULT 0,
        inserted_at     timestamptz NOT NULL DEFAULT now()
      )
      """,
      """
      CREATE TABLE delta_commits (
        table_id         bigint NOT NULL REFERENCES delta_tables(id),
        version          bigint NOT NULL,
        operation        text NOT NULL,
        operation_params jsonb NOT NULL DEFAULT '{}',
        committed_at     timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (table_id, version)
      )
      """,
      """
      CREATE TABLE delta_files (
        id               bigserial PRIMARY KEY,
        table_id         bigint NOT NULL REFERENCES delta_tables(id),
        path             text NOT NULL,
        size_bytes       bigint NOT NULL,
        row_count        bigint NOT NULL,
        partition_values jsonb NOT NULL DEFAULT '{}',
        column_stats     jsonb NOT NULL DEFAULT '{}',
        added_version    bigint NOT NULL,
        removed_version  bigint,
        UNIQUE (table_id, path)
      )
      """,
      # Snapshot reconstruction: live files for a table.
      """
      CREATE INDEX delta_files_live_idx
        ON delta_files (table_id, added_version)
        WHERE removed_version IS NULL
      """,
      # Partition pruning via @> containment.
      """
      CREATE INDEX delta_files_partition_idx
        ON delta_files USING gin (partition_values jsonb_path_ops)
      """,
      # Stats pruning expressions. jsonb_path_ops doesn't help range
      # extraction; a plain GIN covers existence, and for genuinely hot
      # stats columns add expression B-tree indexes, e.g.:
      #   CREATE INDEX ON delta_files
      #     (((column_stats->'recorded_at'->>'max')), ((column_stats->'recorded_at'->>'min')));
      """
      CREATE INDEX delta_files_stats_idx
        ON delta_files USING gin (column_stats)
      """,
      # Vacuum candidate lookup.
      """
      CREATE INDEX delta_files_tombstone_idx
        ON delta_files (table_id, removed_version)
        WHERE removed_version IS NOT NULL
      """,
      # Materialized-view refresh watermarks: per (view, source), the source
      # version the view has consumed. Advanced atomically with the view's
      # own commit during refresh.
      """
      CREATE TABLE delta_view_state (
        view_table_id    bigint NOT NULL REFERENCES delta_tables(id),
        source_table_id  bigint NOT NULL REFERENCES delta_tables(id),
        consumed_version bigint NOT NULL DEFAULT 0,
        refreshed_at     timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (view_table_id, source_table_id)
      )
      """
    ]
  end

  def down_statements do
    [
      "DROP TABLE delta_view_state",
      "DROP TABLE delta_files",
      "DROP TABLE delta_commits",
      "DROP TABLE delta_tables"
    ]
  end

  def v2_up_statements do
    [
      """
      CREATE TABLE delta_file_partitions (
        file_id   bigint NOT NULL REFERENCES delta_files(id) ON DELETE CASCADE,
        col_name  text   NOT NULL,
        col_value text   NOT NULL,
        PRIMARY KEY (file_id, col_name)
      )
      """,
      """
      CREATE INDEX delta_file_partitions_lookup
        ON delta_file_partitions (col_name, col_value)
      """,
      """
      CREATE TABLE delta_file_stats (
        file_id    bigint           NOT NULL REFERENCES delta_files(id) ON DELETE CASCADE,
        col_name   text             NOT NULL,
        min_val    text,
        max_val    text,
        min_num    double precision,
        max_num    double precision,
        null_count bigint           NOT NULL DEFAULT 0,
        PRIMARY KEY (file_id, col_name)
      )
      """,
      """
      CREATE INDEX delta_file_stats_num  ON delta_file_stats (col_name, min_num, max_num)
      """,
      """
      CREATE INDEX delta_file_stats_text ON delta_file_stats (col_name, min_val, max_val)
      """
    ]
  end

  @doc "Run v2 migrations against an Ecto repo, skipping statements that already ran."
  def run_v2(repo) do
    Enum.each(v2_up_statements(), fn sql ->
      try do
        repo.query!(sql, [])
      rescue
        _ -> :already_exists
      end
    end)
  end

  @doc "Backfill normalized partition and stats tables from existing JSONB data in delta_files."
  def backfill_normalized(repo) do
    %{rows: rows} =
      repo.query!("SELECT id, partition_values, column_stats FROM delta_files", [])

    Enum.each(rows, fn [file_id, partition_values, column_stats] ->
      if is_map(partition_values) do
        Enum.each(partition_values, fn {key, value} ->
          repo.query!(
            "INSERT INTO delta_file_partitions (file_id, col_name, col_value) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
            [file_id, to_string(key), to_string(value)]
          )
        end)
      end

      if is_map(column_stats) do
        Enum.each(column_stats, fn {col, stats} ->
          min_raw = stats["min"]
          max_raw = stats["max"]
          min_num = parse_num(min_raw)
          max_num = parse_num(max_raw)

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
      end
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
end

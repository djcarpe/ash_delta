# ──────────────────────────────────────────────────────────────────────────────
# seed.exs — bulk-load synthetic events as hive-partitioned Parquet on S3,
# then register everything in the AshDelta catalog with import_existing/1.
#
# One DuckDB COPY per day writes s3://<bucket>/events/day=YYYY-MM-DD/part-N.parquet
# directly (range() + hash() + random() — Elixir never materialises rows).
# Rows are occurred_at-ordered within each file, so min/max stats are tight.
#
# The synthetic data is IDENTICAL to the ash_iceberg demo seeder (same id,
# user_id, event_type, occurred_at derivations) so query results and times
# are comparable across the two stacks.
#
# Environment:
#   SEED_ROWS        total rows                          (default 1_000_000_000)
#   SEED_DAYS        time window in days                 (default 365)
#   SEED_START       ISO8601 window start                (default 2025-07-09T00:00:00Z)
#   SEED_WINDOW_ROWS denominator for the id→timestamp step (default SEED_ROWS).
#                    Set to another stack's total row count when capping this
#                    one short, so row i gets the identical occurred_at.
#   SEED_PAUSE_MS    pause between day-files             (default 500)
#
# Restart-safe AND target-growable: existing day-files are verified by
# footer row-count (a file cut short by an earlier lower SEED_ROWS cap is
# rewritten); if the catalog registration doesn't match SEED_ROWS exactly it
# is cleared and rebuilt by a fresh import at the end.
# ──────────────────────────────────────────────────────────────────────────────

defmodule Seed do
  @event_types ~w[view click purchase share bookmark login logout search]

  def run do
    rows = env_int("SEED_ROWS", 1_000_000_000)
    days = env_int("SEED_DAYS", 365)
    window_rows = env_int("SEED_WINDOW_ROWS", rows)
    pause_ms = env_int("SEED_PAUSE_MS", 500)
    start_iso = System.get_env("SEED_START", "2025-07-09T00:00:00Z")

    {:ok, start_dt, 0} = DateTime.from_iso8601(start_iso)
    start_date = DateTime.to_date(start_dt)
    epoch_start = DateTime.to_unix(start_dt)

    {:ok, bucket} = AshDelta.Info.bucket(DeltaDemo.Event)
    {:ok, prefix} = AshDelta.Info.prefix(DeltaDemo.Event)

    # Row i belongs to day floor(i * days / window_rows); the exact integer
    # boundary for day d is ceil(d * window_rows / days). This keeps the hive
    # `day=` label identical to occurred_at::date for every row.
    day_start = fn d -> div(d * window_rows + days - 1, days) end
    num_days = Enum.count(0..(days - 1), fn d -> day_start.(d) < rows end)

    IO.puts(
      "Seeding #{fmt(rows)} rows over #{num_days} day-files " <>
        "(timestamp window sized for #{fmt(window_rows)} rows) into s3://#{bucket}/#{prefix}/"
    )

    DeltaDemo.Migrate.run_when_ready()
    {:ok, conn} = checkout_when_ready()

    case registered_rows() do
      ^rows ->
        IO.puts("Catalog already registers exactly #{fmt(rows)} rows — nothing to do.")

      registered ->
        if registered > 0 do
          IO.puts("Registration covers #{fmt(registered)} rows ≠ target — clearing catalog entry.")
          wipe_registration()
        end

        existing = existing_files(conn, bucket, prefix)
        IO.puts("Found #{map_size(existing)} existing day-files (complete ones are skipped).")

        t0 = System.monotonic_time(:millisecond)

        for d <- 0..(num_days - 1) do
          date = Date.add(start_date, d)
          from = day_start.(d)
          to = min(day_start.(d + 1), rows)
          path = "#{prefix}/day=#{date}/part-#{String.pad_leading(to_string(d), 5, "0")}.parquet"

          if Map.get(existing, path) == to - from do
            :skip
          else
            copy_day(conn, bucket, path, date, from, to, epoch_start, window_rows, days)
            progress(d + 1, num_days, to, t0)
            Process.sleep(pause_ms)
          end
        end

        IO.puts("All files written. Importing into the AshDelta catalog...")

        case AshDelta.import_existing(DeltaDemo.Event) do
          {:ok, version} -> IO.puts("Import committed as version #{version}.")
          {:error, reason} -> raise "import_existing failed: #{inspect(reason)}"
        end
    end

    IO.puts("Seed complete.")
  end

  defp copy_day(conn, bucket, path, date, from, to, epoch_start, window_rows, days) do
    types_sql = "['" <> Enum.join(@event_types, "','") <> "']"
    window_seconds = days * 86_400
    step = window_seconds / window_rows

    sql = """
    COPY (
      SELECT
        i                                                          AS id,
        CAST((i * 2654435761) % 10000000 AS INTEGER)               AS user_id,
        (#{types_sql})[1 + CAST(hash(i) % #{length(@event_types)} AS INTEGER)] AS event_type,
        round(random() * 500, 2)                                   AS value,
        to_timestamp(#{epoch_start} + i * #{step})                 AS occurred_at,
        DATE '#{date}'                                             AS day
      FROM range(#{from}, #{to}) t(i)
    ) TO 's3://#{bucket}/#{path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """

    case Duckdbex.query(conn, sql) do
      {:ok, _} -> :ok
      {:error, reason} -> raise "COPY failed for #{path}: #{inspect(reason)}"
    end
  end

  # Map of relative path => row count (count(*) is answered from parquet
  # footers, so this is one cheap metadata pass over the prefix).
  defp existing_files(conn, bucket, prefix) do
    sql = """
    SELECT filename, count(*) AS n
    FROM read_parquet('s3://#{bucket}/#{prefix}/**/*.parquet', filename = true)
    GROUP BY filename
    """

    case Duckdbex.query(conn, sql) do
      {:ok, res} ->
        res
        |> Duckdbex.fetch_all()
        |> Map.new(fn [f, n] -> {String.replace_prefix(f, "s3://#{bucket}/", ""), n} end)

      {:error, _} ->
        %{}
    end
  end

  defp registered_rows do
    # ::bigint — sum(bigint) is numeric in Postgres, which Postgrex decodes
    # as a Decimal struct; that breaks integer comparison and formatting.
    %{rows: [[n]]} =
      DeltaDemo.Repo.query!(
        """
        SELECT COALESCE(sum(f.row_count), 0)::bigint FROM delta_tables t
        JOIN delta_files f ON f.table_id = t.id AND f.removed_version IS NULL
        WHERE t.resource = $1
        """,
        [AshDelta.Info.table_name(DeltaDemo.Event)]
      )

    case n do
      n when is_integer(n) -> n
      %Decimal{} = d -> Decimal.to_integer(d)
    end
  rescue
    _ -> 0
  end

  # Remove every catalog row for this resource so import_existing can
  # re-register the (grown) file set from scratch. Data files are untouched.
  # NOTE: delta_tables.resource stores Info.table_name/1, not the module name.
  defp wipe_registration do
    resource = AshDelta.Info.table_name(DeltaDemo.Event)

    statements = [
      """
      DELETE FROM delta_file_stats WHERE file_id IN (
        SELECT f.id FROM delta_files f
        JOIN delta_tables t ON f.table_id = t.id WHERE t.resource = $1)
      """,
      """
      DELETE FROM delta_file_partitions WHERE file_id IN (
        SELECT f.id FROM delta_files f
        JOIN delta_tables t ON f.table_id = t.id WHERE t.resource = $1)
      """,
      "DELETE FROM delta_files WHERE table_id IN (SELECT id FROM delta_tables WHERE resource = $1)",
      "DELETE FROM delta_commits WHERE table_id IN (SELECT id FROM delta_tables WHERE resource = $1)",
      "DELETE FROM delta_tables WHERE resource = $1"
    ]

    Enum.each(statements, &DeltaDemo.Repo.query!(&1, [resource]))
  end

  defp checkout_when_ready(attempt \\ 1) do
    AshDelta.ConnectionPool.checkout(DeltaDemo.Event)
  rescue
    e ->
      if attempt >= 60 do
        reraise e, __STACKTRACE__
      else
        IO.puts("DuckDB pool not ready (attempt #{attempt}), retrying in 5s...")
        Process.sleep(5_000)
        checkout_when_ready(attempt + 1)
      end
  end

  defp progress(day, days, rows_done, t0) do
    elapsed = System.monotonic_time(:millisecond) - t0
    rate = rows_done * 1000 / max(elapsed, 1)
    eta_min = (days - day) * (elapsed / day) / 60_000

    IO.puts(
      "day #{day}/#{days}  rows #{fmt(rows_done)}  avg #{fmt(round(rate))} rows/s  " <>
        "ETA #{Float.round(eta_min, 1)} min"
    )
  end

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      v -> String.to_integer(v)
    end
  end

  defp fmt(n) when is_integer(n) do
    n
    |> Integer.to_string()
    |> String.replace(~r/(\d)(?=(\d{3})+$)/, "\\1_")
  end
end

Seed.run()

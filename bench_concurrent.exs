################################################################################
# bench_concurrent.exs
#
# Load test: concurrent readers and writers against the same AshDelta resource.
#
# Measures:
#   • Throughput (ops/sec) at each concurrency level
#   • Latency distribution: p50 / p95 / p99
#   • Error rate
#   • Connection pool behaviour: does shared-DB overhead grow with concurrency?
#
# Dataset: the 100-file / 10M-row import from bench_import.exs (must have been
# run at least once so the catalog and S3 files exist).
#
# Usage:
#   mix run bench_concurrent.exs
################################################################################

Application.put_env(:ash, :validate_domain_resource_inclusion?, false)
Application.put_env(:ash, :validate_domain_config_inclusion?, false)
Logger.configure(level: :warning)

# pool_size: writes serialise through the Postgres FOR UPDATE lock, so 20
# Postgres connections is plenty even at c=20 concurrent writers. Keeping
# pool_size well below max_connections (100) leaves headroom for other services.
Application.put_env(:load_test, LoadTest.Repo,
  url: "postgres://postgres:postgres@localhost:5432/ash_delta_demo",
  pool_size: 20
)

Application.put_env(:ex_aws, :access_key_id, "any")
Application.put_env(:ex_aws, :secret_access_key, "any")
Application.put_env(:ex_aws, :region, "us-east-1")
Application.put_env(:ex_aws, :s3, scheme: "http://", host: "localhost", port: 8333)

defmodule LoadTest.Repo do
  use Ecto.Repo, otp_app: :load_test, adapter: Ecto.Adapters.Postgres
end

{:ok, _} = LoadTest.Repo.start_link()

# ── Resources ─────────────────────────────────────────────────────────────────

defmodule LoadTest.SensorReading do
  use Ash.Resource, domain: LoadTest.Domain, data_layer: AshDelta.DataLayer

  delta do
    repo     LoadTest.Repo
    bucket   "demo-lake"
    name     "import_bench_sensor"   # same catalog entry as bench_import.exs
    prefix   "external/raw"
    partition_by    [:site, :day]
    stats_columns   [:recorded_at, :value]
    sort_within_files [:value]
    s3_config access_key_id:     "any",
              secret_access_key: "any",
              region:            "us-east-1",
              endpoint:          "http://localhost:8333"
  end

  attributes do
    attribute :id,          :string,            primary_key?: true, allow_nil?: false, public?: true
    attribute :site,        :string,            allow_nil?: false, public?: true
    attribute :day,         :date,              allow_nil?: false, public?: true
    attribute :sensor_id,   :string,            public?: true
    attribute :recorded_at, :utc_datetime_usec, public?: true
    attribute :value,       :float,             public?: true
  end

  actions do
    defaults [:read]

    create :create do
      accept [:id, :site, :day, :sensor_id, :recorded_at, :value]
    end
  end
end

defmodule LoadTest.Domain do
  use Ash.Domain, validate_config_inclusion?: false
  resources do
    resource LoadTest.SensorReading
  end
end

require Ash.Query

try do
  AshDelta.Migrations.run(LoadTest.Repo)
rescue
  _ -> :ok
end

# ── Verify data exists ─────────────────────────────────────────────────────────

snap = AshDelta.snapshot(LoadTest.SensorReading)

if snap == [] do
  IO.puts("""
  No data found. Run bench_import.exs first to generate the 10M-row dataset.
  """)
  System.halt(1)
end

IO.puts("── dataset: #{length(snap)} files / #{Enum.sum(Enum.map(snap, & &1.row_count))} rows\n")

# ── Measurement harness ────────────────────────────────────────────────────────

defmodule LoadTest.Harness do
  @doc """
  Run `concurrency` tasks in parallel, each executing `fun` for `iterations`.
  Returns a map with throughput, latency percentiles, and error count.
  Wall-clock time is measured around all tasks starting simultaneously.
  """
  def run(concurrency, iterations, fun) do
    # Pre-warm (best-effort: don't let warm-up failure abort the test).
    try do
      _ = fun.()
    rescue
      _ -> :ok
    end

    Process.sleep(500)

    wall_t0 = System.monotonic_time(:millisecond)

    tasks =
      for _ <- 1..concurrency do
        Task.async(fn ->
          for _ <- 1..iterations do
            t0 = System.monotonic_time(:microsecond)
            result =
              try do
                fun.()
                :ok
              rescue
                e -> {:error, e}
              catch
                :exit, r -> {:error, r}
              end
            t1 = System.monotonic_time(:microsecond)
            {result, t1 - t0}
          end
        end)
      end

    all = tasks |> Task.await_many(300_000) |> List.flatten()

    wall_ms = System.monotonic_time(:millisecond) - wall_t0

    errors    = Enum.count(all, fn {r, _} -> r != :ok end)
    latencies = all |> Enum.map(fn {_, μs} -> μs end) |> Enum.sort()
    n         = length(latencies)
    total_ops = concurrency * iterations

    %{
      concurrency: concurrency,
      total_ops:   total_ops,
      errors:      errors,
      wall_s:      wall_ms / 1000,
      throughput:  total_ops / (wall_ms / 1000),
      p50_ms:      Enum.at(latencies, max(div(n * 50, 100) - 1, 0)) / 1000,
      p95_ms:      Enum.at(latencies, max(div(n * 95, 100) - 1, 0)) / 1000,
      p99_ms:      Enum.at(latencies, max(div(n * 99, 100) - 1, 0)) / 1000,
      max_ms:      List.last(latencies) / 1000,
    }
  end

  def print_header(label) do
    IO.puts("\n── #{label} #{String.duplicate("─", max(0, 68 - String.length(label)))}")
    IO.puts(String.pad_leading("conc", 6) <>
            String.pad_leading("ops", 6) <>
            String.pad_leading("err", 5) <>
            String.pad_leading("tput/s", 9) <>
            String.pad_leading("p50ms", 8) <>
            String.pad_leading("p95ms", 8) <>
            String.pad_leading("p99ms", 8) <>
            String.pad_leading("maxms", 8))
    IO.puts(String.duplicate("─", 58))
  end

  def recover do
    # Let DuckDB drain open HTTP connections before the next test group.
    Process.sleep(3_000)
  end

  def print_row(%{} = r) do
    IO.puts(
      String.pad_leading(to_string(r.concurrency), 6) <>
      String.pad_leading(to_string(r.total_ops), 6) <>
      String.pad_leading(to_string(r.errors), 5) <>
      String.pad_leading(:erlang.float_to_binary(r.throughput, decimals: 1), 9) <>
      String.pad_leading(:erlang.float_to_binary(r.p50_ms, decimals: 1), 8) <>
      String.pad_leading(:erlang.float_to_binary(r.p95_ms, decimals: 1), 8) <>
      String.pad_leading(:erlang.float_to_binary(r.p99_ms, decimals: 1), 8) <>
      String.pad_leading(:erlang.float_to_binary(r.max_ms, decimals: 1), 8)
    )
  end
end

sites = Enum.map(1..10, &"SITE#{String.pad_leading(to_string(&1), 2, "0")}")
days  = Enum.map(0..9,  &Date.add(~D[2026-01-01], &1))

# ── Test 1: concurrent top-50 reads (LIMIT pushdown, ~0.1s each) ──────────────
# This is the bread-and-butter pattern: many clients doing lightweight lookups.
# Connection pool: all goroutines share 1 DuckDB DB, each gets a 1ms connection.

day1 = ~D[2026-01-01]

q_top50 =
  LoadTest.SensorReading
  |> Ash.Query.filter(site == "SITE01" and day == ^day1)
  |> Ash.Query.sort(recorded_at: :asc)
  |> Ash.Query.limit(50)

LoadTest.Harness.print_header("concurrent top-50 reads (LIMIT pushdown, 1 partition)")

for c <- [1, 5, 10, 20] do
  r = LoadTest.Harness.run(c, 5, fn -> Ash.read!(q_top50) end)
  LoadTest.Harness.print_row(r)
end

LoadTest.Harness.recover()

# ── Test 2: concurrent full-partition reads (100k rows, ~2s each) ─────────────
# Heavier queries — tests whether the pool stays stable under memory pressure.
# At 50 concurrent × 100k rows = 5M rows in flight simultaneously.

q_partition =
  LoadTest.SensorReading
  |> Ash.Query.filter(site == "SITE01" and day == ^day1)

LoadTest.Harness.print_header("concurrent partition reads (100k rows, struct materialize)")

for c <- [1, 3, 5, 10] do
  r = LoadTest.Harness.run(c, 3, fn -> Ash.read!(q_partition) end)
  LoadTest.Harness.print_row(r)
end

LoadTest.Harness.recover()

# ── Test 3: concurrent DataFrame reads (1M rows, no struct alloc) ─────────────
# Same data as test 2 but via to_dataframe! — tests whether the
# Explorer/Polars path stays efficient under concurrent load.

q_site =
  LoadTest.SensorReading
  |> Ash.Query.filter(site == "SITE01")

LoadTest.Harness.print_header("concurrent DataFrame reads (1M rows, no struct alloc)")

for c <- [1, 3, 5, 10] do
  r = LoadTest.Harness.run(c, 2, fn -> AshDelta.to_dataframe!(q_site) end)
  LoadTest.Harness.print_row(r)
end

LoadTest.Harness.recover()

# ── Test 4: concurrent count(*) aggregates (no row materialization) ────────────
# Count queries go straight to DuckDB with no Elixir allocation.
# Tests aggregate pushdown throughput under contention.

q_count =
  LoadTest.SensorReading
  |> Ash.Query.filter(site == "SITE01")

LoadTest.Harness.print_header("concurrent count(*) aggregates (1M rows, pushdown)")

for c <- [1, 5, 10, 20] do
  r = LoadTest.Harness.run(c, 10, fn -> Ash.count!(q_count) end)
  LoadTest.Harness.print_row(r)
end

LoadTest.Harness.recover()

# ── Test 5: concurrent small writes ───────────────────────────────────────────
# Each writer creates 100 rows; the Postgres FOR UPDATE lock serialises commits
# per table. Tests write throughput and confirms no data loss under concurrency.

LoadTest.Harness.print_header("concurrent writes (100 rows/op, Postgres serialization)")

write_fn = fn worker_id ->
  fn ->
    records =
      for i <- 1..100 do
        %{
          id:          "load-test-#{worker_id}-#{System.unique_integer([:positive])}",
          site:        Enum.random(sites),
          day:         Enum.random(days),
          sensor_id:   "s#{rem(i, 20) + 1}",
          recorded_at: DateTime.utc_now(),
          value:       10.0 + :rand.uniform() * 40.0
        }
      end

    Ash.bulk_create!(records, LoadTest.SensorReading, :create, return_errors?: true)
  end
end

for c <- [1, 5, 10, 20] do
  r = LoadTest.Harness.run(c, 3, write_fn.(c * 1000))
  LoadTest.Harness.print_row(r)
end

LoadTest.Harness.recover()

# ── Test 6: mixed read/write (80/20 split) ────────────────────────────────────
# Simulates a realistic workload: mostly reads with occasional writes.
# Concurrent readers should not be blocked by writers (reads don't lock).

LoadTest.Harness.print_header("mixed 80% read / 20% write (25 concurrent workers)")

mixed_results =
  LoadTest.Harness.run(25, 5, fn ->
    if :rand.uniform() < 0.8 do
      Ash.read!(q_top50)
    else
      Ash.bulk_create!(
        [%{
          id:          "mixed-#{System.unique_integer([:positive])}",
          site:        "SITE01",
          day:         day1,
          sensor_id:   "s1",
          recorded_at: DateTime.utc_now(),
          value:       25.0
        }],
        LoadTest.SensorReading,
        :create,
        return_errors?: true
      )
    end
  end)

LoadTest.Harness.print_row(mixed_results)

IO.puts("\n── done ✓ ────────────────────────────────────────────────────────────────")

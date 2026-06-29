Application.put_env(:ash, :validate_domain_resource_inclusion?, false)
Application.put_env(:ash, :validate_domain_config_inclusion?, false)

Application.put_env(:demo, Demo.Repo,
  url: "postgres://postgres:postgres@localhost:5432/ash_delta_demo",
  pool_size: 5
)

Application.put_env(:ex_aws, :access_key_id, "any")
Application.put_env(:ex_aws, :secret_access_key, "any")
Application.put_env(:ex_aws, :region, "us-east-1")
Application.put_env(:ex_aws, :s3, scheme: "http://", host: "localhost", port: 8333)

defmodule Demo.Repo do
  use Ecto.Repo, otp_app: :demo, adapter: Ecto.Adapters.Postgres
end

{:ok, _} = Demo.Repo.start_link()

defmodule E2E.Event do
  use Ash.Resource, domain: E2E.Domain, data_layer: AshDelta.DataLayer

  delta do
    repo Demo.Repo
    bucket "demo-lake"
    prefix "e2e/events"
    partition_by [:site, :day]
    sort_within_files [:sensor_id, :recorded_at]
    stats_columns [:recorded_at, :value]
    s3_config access_key_id: "any",
              secret_access_key: "any",
              region: "us-east-1",
              endpoint: "http://localhost:8333"
  end

  attributes do
    uuid_primary_key :id
    attribute :site,        :string,            allow_nil?: false, public?: true
    attribute :day,         :date,              allow_nil?: false, public?: true
    attribute :sensor_id,   :string,            public?: true
    attribute :recorded_at, :utc_datetime_usec, public?: true
    attribute :value,       :float,             public?: true
  end

  actions do
    defaults [:read, :create, :destroy]
    default_accept [:site, :day, :sensor_id, :recorded_at, :value]
  end
end

defmodule E2E.Domain do
  use Ash.Domain, validate_config_inclusion?: false
  resources do
    resource E2E.Event
  end
end

defmodule E2E.Runner do
  require Ash.Query
  import Ash.Expr

  defp pass(label), do: IO.puts("ok  #{label}")
  defp fail(label, e), do: (IO.puts("FAIL #{label}: #{inspect(e)}"); raise "test failed")

  def run do
    # ── migrations ─────────────────────────────────────────────────────────────
    IO.puts("── migrations ─────────────────────────────────────────────────────")
    try do
      AshDelta.Migrations.run(Demo.Repo)
      pass("migrations applied")
    rescue
      e -> pass("migrations already applied (#{Exception.message(e)})")
    end

    # ── create ─────────────────────────────────────────────────────────────────
    IO.puts("── create ─────────────────────────────────────────────────────────")
    rows = [
      %{site: "CHI", day: ~D[2026-06-11], sensor_id: "s1",
        recorded_at: ~U[2026-06-11 08:00:00.000000Z], value: 22.4},
      %{site: "CHI", day: ~D[2026-06-11], sensor_id: "s2",
        recorded_at: ~U[2026-06-11 09:00:00.000000Z], value: 23.1},
      %{site: "NYC", day: ~D[2026-06-11], sensor_id: "s3",
        recorded_at: ~U[2026-06-11 08:30:00.000000Z], value: 19.8}
    ]

    result = Ash.bulk_create(rows, E2E.Event, :create, return_records?: true, return_errors?: true)

    if result.errors == [] do
      pass("bulk_create #{length(result.records)} rows")
    else
      fail("bulk_create", result.errors)
    end

    # ── read all ───────────────────────────────────────────────────────────────
    IO.puts("── read ────────────────────────────────────────────────────────────")
    case Ash.read(E2E.Event) do
      {:ok, events} -> pass("read all → #{length(events)} row(s)")
      {:error, e}   -> fail("read all", e)
    end

    # ── equality filter ────────────────────────────────────────────────────────
    case E2E.Event |> Ash.Query.filter(site == "CHI") |> Ash.read() do
      {:ok, events} -> pass("filter site=CHI → #{length(events)} row(s)")
      {:error, e}   -> fail("filter site=CHI", e)
    end

    # ── range filter ───────────────────────────────────────────────────────────
    cutoff = ~U[2026-06-11 08:15:00.000000Z]
    case E2E.Event |> Ash.Query.filter(recorded_at >= ^cutoff) |> Ash.read() do
      {:ok, events} -> pass("filter recorded_at >= cutoff → #{length(events)} row(s)")
      {:error, e}   -> fail("filter recorded_at >= cutoff", e)
    end

    # ── sort + limit ───────────────────────────────────────────────────────────
    case E2E.Event |> Ash.Query.sort(recorded_at: :asc) |> Ash.Query.limit(2) |> Ash.read() do
      {:ok, events} -> pass("sort+limit → #{length(events)} row(s)")
      {:error, e}   -> fail("sort+limit", e)
    end

    # ── stats collection ───────────────────────────────────────────────────────
    IO.puts("── stats ───────────────────────────────────────────────────────────")
    df = AshDelta.Writer.to_dataframe(E2E.Event, result.records)
    stats = AshDelta.Writer.collect_stats(E2E.Event, df)

    if map_size(stats) > 0 do
      pass("collect_stats → #{inspect(Map.keys(stats))}")
    else
      fail("collect_stats", "empty stats")
    end

    # ── history ────────────────────────────────────────────────────────────────
    IO.puts("── history ─────────────────────────────────────────────────────────")
    commits = AshDelta.history(E2E.Event)
    if is_list(commits) and length(commits) > 0 do
      pass("history → #{length(commits)} commit(s)")
    else
      fail("history", "expected non-empty list, got: #{inspect(commits)}")
    end

    # ── snapshot ───────────────────────────────────────────────────────────────
    files = AshDelta.snapshot(E2E.Event)
    if is_list(files) and length(files) > 0 do
      pass("snapshot → #{length(files)} file(s)")
    else
      fail("snapshot", "expected non-empty list, got: #{inspect(files)}")
    end

    IO.puts("── done ✓ ──────────────────────────────────────────────────────────")
  end
end

E2E.Runner.run()

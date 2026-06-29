defmodule AshDelta.CacheInvalidator do
  @moduledoc """
  Listens for Postgres NOTIFY messages and invalidates the local ETS snapshot
  cache, keeping all nodes in a cluster coherent without polling.

  On every successful commit, `AshDelta.Log` issues:

      SELECT pg_notify('ash_delta_invalidations', '<table_id>')

  Any node running a `CacheInvalidator` for the same database will receive
  that notification and evict the stale snapshot from its local ETS cache. The
  next read on that node re-fetches from Postgres instead of serving a stale
  file list.

  ## Setup

  Add to your application's supervision tree after your Ecto repo:

      children = [
        MyApp.Repo,
        {AshDelta.CacheInvalidator, repo: MyApp.Repo}
      ]

  If you have multiple repos backed by different databases, start one
  CacheInvalidator per repo.
  """

  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: name_for(Keyword.fetch!(opts, :repo)))
  end

  @impl true
  def init(opts) do
    repo = Keyword.fetch!(opts, :repo)
    pg_opts = pg_config(repo) ++ [auto_reconnect: true]

    case Postgrex.Notifications.start_link(pg_opts) do
      {:ok, notif_pid} ->
        Postgrex.Notifications.listen!(notif_pid, "ash_delta_invalidations")
        {:ok, %{notif_pid: notif_pid, repo: repo}}

      {:error, reason} ->
        Logger.warning("AshDelta.CacheInvalidator could not connect: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  @impl true
  def handle_info({:notification, _pid, _ref, "ash_delta_invalidations", payload}, state) do
    case Integer.parse(payload) do
      {table_id, ""} -> AshDelta.Log.invalidate_snapshot(table_id)
      _ -> :ok
    end

    {:noreply, state}
  end

  def handle_info(_, state), do: {:noreply, state}

  # ── Helpers ──────────────────────────────────────────────────────────────

  defp name_for(repo), do: :"AshDelta.CacheInvalidator.#{repo}"

  defp pg_config(repo) do
    config = repo.config()

    if url = config[:url] do
      [url: url]
    else
      Keyword.take(config, [:hostname, :port, :database, :username, :password, :ssl, :ssl_opts, :parameters])
    end
  end
end

defmodule AshDelta.View.RefreshWorker do
  @moduledoc """
  Drives automatic refresh for one view on its `freshness` policy.

  Run **one instance per view, cluster-wide** — register it under
  `Horde.Registry` and start it via `Horde.DynamicSupervisor` so exactly one
  node refreshes a given view at a time. (Concurrent refreshes are
  *safe* — the view's commit lock serializes them and the loser retries — but
  a singleton avoids wasted recompute.)

  Two trigger modes, not mutually exclusive:

    * **Interval** — `freshness: [max_staleness: {15, :minute}]` polls each
      source's current version on that interval and refreshes when any has
      advanced past the watermark.
    * **Notify** — call `AshDelta.View.RefreshWorker.notify(view)` from a
      Postgres `LISTEN` handler on `delta_commits` inserts (or from your
      ingestion code after a source commit) to make refresh event-driven.
      Notifications are debounced by `min_interval_ms` so a burst of source
      commits collapses into one refresh.

  ## Example child spec (Horde)

      {Horde.DynamicSupervisor, :start_child,
       [MyApp.ViewSupervisor,
        {AshDelta.View.RefreshWorker,
         view: Mes.Telemetry.StationDailyStats, name: via(StationDailyStats)}]}
  """
  use GenServer
  require Logger

  alias AshDelta.{Info, View}

  def start_link(opts) do
    view = Keyword.fetch!(opts, :view)
    name = Keyword.get(opts, :name, {:global, {__MODULE__, view}})
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Nudge a running worker to refresh soon (debounced)."
  def notify(view, name \\ nil) do
    target = name || {:global, {__MODULE__, view}}
    GenServer.cast(target, :notify)
  end

  @impl true
  def init(opts) do
    view = Keyword.fetch!(opts, :view)
    {:ok, freshness} = Info.view_freshness(view)

    interval_ms =
      case Keyword.get(opts, :interval_ms) || staleness_ms(freshness[:max_staleness]) do
        nil -> nil
        ms -> ms
      end

    state = %{
      view: view,
      interval_ms: interval_ms,
      min_interval_ms: Keyword.get(opts, :min_interval_ms, 1_000),
      last_refresh: nil,
      pending_timer: nil
    }

    if interval_ms, do: schedule(interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    state = do_refresh(state)
    if state.interval_ms, do: schedule(state.interval_ms)
    {:noreply, state}
  end

  def handle_info(:debounced_refresh, state) do
    {:noreply, do_refresh(%{state | pending_timer: nil})}
  end

  @impl true
  def handle_cast(:notify, %{pending_timer: nil} = state) do
    timer = Process.send_after(self(), :debounced_refresh, state.min_interval_ms)
    {:noreply, %{state | pending_timer: timer}}
  end

  def handle_cast(:notify, state), do: {:noreply, state}

  defp do_refresh(state) do
    case View.refresh(state.view) do
      {:ok, :up_to_date} ->
        state

      {:ok, result} ->
        Logger.debug("AshDelta refreshed #{inspect(state.view)}: #{inspect(result)}")
        %{state | last_refresh: System.monotonic_time(:millisecond)}

      {:error, reason} ->
        Logger.warning("AshDelta refresh of #{inspect(state.view)} failed: #{inspect(reason)}")
        state
    end
  end

  defp schedule(ms), do: Process.send_after(self(), :tick, ms)

  defp staleness_ms(nil), do: nil
  defp staleness_ms({n, :second}), do: n * 1_000
  defp staleness_ms({n, :minute}), do: n * 60_000
  defp staleness_ms({n, :hour}), do: n * 3_600_000
end

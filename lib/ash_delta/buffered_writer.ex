defmodule AshDelta.BufferedWriter do
  @moduledoc """
  Optional batching layer for high-concurrency write workloads.

  When `write_buffer_ms` is set on a resource's `delta` section, all
  `bulk_create` calls route through this GenServer instead of writing
  directly. Records arriving within the same window are merged into a single
  `Writer.append` call, producing one Parquet file per partition per window
  rather than one per concurrent caller.

  Every caller blocks until its window flushes so the return path stays
  synchronous: callers receive `{:ok, version}` or `{:error, reason}` just as
  with direct writes. The `FOR UPDATE` commit lock is held for one flush
  instead of N, which dramatically reduces contention under concurrent load.

  ## Example

      delta do
        # ...
        write_buffer_ms 100   # merge writes within 100 ms windows
      end

  One GenServer is started per resource (lazily, on first write) under
  `AshDelta.BufferedWriter.Supervisor`.
  """

  use GenServer

  alias AshDelta.Writer

  defstruct resource: nil,
            buffer_ms: 100,
            buffer: [],
            waiters: [],
            timer_ref: nil

  # ── Public API ────────────────────────────────────────────────────────────

  @doc "Append records, coalescing with concurrent callers within the buffer window."
  def append(resource, records, buffer_ms) do
    pid = get_or_start(resource, buffer_ms)
    GenServer.call(pid, {:append, records}, :infinity)
  end

  # ── GenServer ─────────────────────────────────────────────────────────────

  @doc false
  def start_link({resource, buffer_ms}) do
    GenServer.start_link(__MODULE__, {resource, buffer_ms},
      name: {:via, Registry, {AshDelta.BufferedWriter.Registry, resource}}
    )
  end

  @impl true
  def init({resource, buffer_ms}) do
    {:ok, %__MODULE__{resource: resource, buffer_ms: buffer_ms}}
  end

  @impl true
  def handle_call({:append, records}, from, state) do
    state = %{state | buffer: state.buffer ++ records, waiters: [from | state.waiters]}
    state = maybe_start_timer(state)
    {:noreply, state}
  end

  @impl true
  def handle_info(:flush, state) do
    {:noreply, flush(%{state | timer_ref: nil})}
  end

  # ── Private ───────────────────────────────────────────────────────────────

  defp get_or_start(resource, buffer_ms) do
    via = {:via, Registry, {AshDelta.BufferedWriter.Registry, resource}}

    case GenServer.whereis(via) do
      nil ->
        case DynamicSupervisor.start_child(
               AshDelta.BufferedWriter.Supervisor,
               {__MODULE__, {resource, buffer_ms}}
             ) do
          {:ok, pid} -> pid
          {:error, {:already_started, pid}} -> pid
        end

      pid ->
        pid
    end
  end

  defp maybe_start_timer(%{timer_ref: nil} = state) do
    ref = Process.send_after(self(), :flush, state.buffer_ms)
    %{state | timer_ref: ref}
  end

  defp maybe_start_timer(state), do: state

  defp flush(%{buffer: [], waiters: []} = state), do: state

  defp flush(state) do
    result = Writer.append(state.resource, state.buffer)
    Enum.each(state.waiters, &GenServer.reply(&1, result))
    %{state | buffer: [], waiters: []}
  end
end

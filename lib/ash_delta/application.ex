defmodule AshDelta.Application do
  use Application

  @impl true
  def start(_type, _args) do
    AshDelta.Log.init_cache()

    children = [
      AshDelta.ConnectionPool,
      {Registry, keys: :unique, name: AshDelta.BufferedWriter.Registry},
      {DynamicSupervisor, name: AshDelta.BufferedWriter.Supervisor, strategy: :one_for_one}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: AshDelta.Supervisor)
  end
end

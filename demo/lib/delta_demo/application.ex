defmodule DeltaDemo.Application do
  use Application

  @impl Application
  def start(_type, _args) do
    port = String.to_integer(System.get_env("PORT", "4000"))

    children = [
      DeltaDemo.Repo,
      {Task, &DeltaDemo.Migrate.run_when_ready/0},
      {Bandit, plug: DeltaDemo.Router, port: port}
    ]

    opts = [strategy: :one_for_one, name: DeltaDemo.Supervisor]
    Supervisor.start_link(children, opts)
  end
end

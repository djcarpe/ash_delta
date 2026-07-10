defmodule DeltaDemo.Repo do
  use Ecto.Repo, otp_app: :delta_demo, adapter: Ecto.Adapters.Postgres

  @impl true
  def init(_type, config) do
    {:ok,
     Keyword.merge(config,
       url:
         System.get_env(
           "DATABASE_URL",
           "postgres://postgres:postgres@localhost:5432/delta_demo"
         ),
       pool_size: String.to_integer(System.get_env("POOL_SIZE", "10"))
     )}
  end
end

import Config

config :logger, level: :info

config :delta_demo, ash_domains: [DeltaDemo.Domain]
config :delta_demo, ecto_repos: [DeltaDemo.Repo]

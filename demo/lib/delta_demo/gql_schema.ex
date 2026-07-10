defmodule DeltaDemo.GqlSchema do
  @moduledoc "Absinthe schema exposing DeltaDemo.Domain via AshGraphql."

  use Absinthe.Schema
  use AshGraphql, domains: [DeltaDemo.Domain]

  query do
    # Ash queries are injected by AshGraphql
  end
end

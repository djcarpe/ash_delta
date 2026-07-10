defmodule DeltaDemo.Router do
  @moduledoc """
  HTTP entry point.

    /gql            → GraphQL API
    /gql/playground → GraphQL Playground UI
    /healthz        → liveness/readiness probe
  """

  use Plug.Router

  plug Plug.Logger
  plug :match

  plug Plug.Parsers,
    parsers: [:urlencoded, :multipart, :json, Absinthe.Plug.Parser],
    pass: ["*/*"],
    json_decoder: Jason

  plug :dispatch

  forward "/gql/playground",
    to: Absinthe.Plug.GraphiQL,
    init_opts: [
      schema: DeltaDemo.GqlSchema,
      interface: :playground,
      default_url: "/gql"
    ]

  forward "/gql",
    to: Absinthe.Plug,
    init_opts: [schema: DeltaDemo.GqlSchema]

  get "/healthz" do
    send_resp(conn, 200, "ok")
  end

  get "/" do
    conn
    |> put_resp_header("location", "/gql/playground")
    |> send_resp(302, "")
  end

  match _ do
    send_resp(conn, 404, "not found")
  end
end

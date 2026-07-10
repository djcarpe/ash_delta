# otp-27 pinned: the vendored duckdbex NIF tarball targets NIF 2.17.
FROM elixir:1.19-otp-27

# ── system deps ──────────────────────────────────────────────────────────────
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential git curl && \
    rm -rf /var/lib/apt/lists/*

RUN mix local.hex --force && mix local.rebar --force

ENV MIX_ENV=prod

# ── copy ash_delta library into /ash_delta ────────────────────────────────────
# The demo's mix.exs points to ".." (i.e. /ash_delta) via a path dependency.
WORKDIR /ash_delta
COPY lib        ./lib
COPY priv       ./priv
COPY mix.exs    ./mix.exs
COPY mix.lock   ./mix.lock

# ── copy the demo application into /ash_delta/demo ────────────────────────────
WORKDIR /ash_delta/demo
COPY demo/mix.exs   ./mix.exs
COPY demo/mix.lock  ./mix.lock
COPY demo/config    ./config
COPY demo/lib       ./lib

# ── fetch + compile (includes duckdbex NIF + explorer precompiled NIF) ───────
# Pre-seed NIF caches so nothing is downloaded from GitHub's CDN at build
# time (both are checksum-verified against their hex packages).
COPY demo/nif-cache/rustler_precompiled/ /root/.cache/rustler_precompiled/precompiled_nifs/
COPY demo/nif-cache/elixir_make/ /root/.cache/elixir_make/
RUN mix deps.get
RUN mix compile

# ── pre-install DuckDB httpfs extension ──────────────────────────────────────
# This step runs during docker build so the container starts without internet.
RUN mix run -e '{:ok, db} = Duckdbex.open(); {:ok, conn} = Duckdbex.connection(db); {:ok, _} = Duckdbex.query(conn, "INSTALL httpfs"); IO.puts("DuckDB extensions pre-installed.")'

# Scripts are interpreted at runtime — copied last so editing them never
# invalidates the compile layers above.
COPY demo/scripts   ./scripts

EXPOSE 4000

# Serves /gql and /gql/playground. Seed jobs override this with:
#   mix run scripts/seed.exs
CMD ["mix", "run", "--no-halt"]

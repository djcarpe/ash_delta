defmodule DeltaDemo.Event do
  @moduledoc """
  An analytics event stored as hive-partitioned Parquet on S3 with the
  transaction log and data-skipping stats in Postgres.

  Files are partitioned by `day` (one directory per day) and carry min/max
  stats on `occurred_at`, `user_id`, and `value`, so time-range and
  numeric-range queries skip most files before DuckDB ever opens them.

  The schema mirrors the ash_iceberg demo's `Demo.Event` (plus the `day`
  partition column) so query times are comparable across the two stacks.
  """

  use Ash.Resource,
    domain: DeltaDemo.Domain,
    data_layer: AshDelta.DataLayer,
    extensions: [AshGraphql.Resource]

  delta do
    repo DeltaDemo.Repo
    bucket System.get_env("S3_BUCKET", "lake")
    name "events"
    prefix "delta/events"
    partition_by [:day]
    stats_columns [:occurred_at, :user_id, :value]
    sort_within_files [:occurred_at]

    # NOTE: evaluated at compile time (image build). s3.storage is a
    # selectorless k8s Service pointing at the SeaweedFS S3 endpoint, so the
    # actual backend can be swapped without rebuilding this image.
    s3_config access_key_id: System.get_env("S3_ACCESS_KEY", "any"),
              secret_access_key: System.get_env("S3_SECRET_KEY", "any"),
              region: System.get_env("S3_REGION", "us-east-1"),
              endpoint: System.get_env("S3_ENDPOINT", "http://s3.storage:9000")
  end

  graphql do
    type :event

    queries do
      get :event, :read
      list :events, :sample
      list :events_by_user, :by_user
      list :events_by_type, :by_type
      list :events_in_range, :in_time_range
      list :top_events, :top_values
      list :events_by_type_prefix, :by_type_prefix
    end
  end

  attributes do
    attribute :id, :integer do
      primary_key? true
      allow_nil? false
      writable? true
      public? true
    end

    attribute :user_id, :integer do
      allow_nil? false
      public? true
    end

    attribute :event_type, :string do
      allow_nil? false
      constraints max_length: 64
      public? true
    end

    attribute :value, :float, default: 0.0, public?: true
    attribute :occurred_at, :utc_datetime_usec, public?: true

    # Hive partition column — one directory per day on S3.
    attribute :day, :date, allow_nil?: false, public?: true
  end

  actions do
    defaults [:read]

    # All list actions use keyset (cursor) pagination with an on-demand
    # count(*) — GraphQL exposes first/after/before/last args plus a `count`
    # field on the page. Mirrors the ash_iceberg demo exactly.
    read :sample do
      pagination keyset?: true, countable: true, default_limit: 50, max_page_size: 1000
    end

    read :by_user do
      argument :user_id, :integer, allow_nil?: false
      filter expr(user_id == ^arg(:user_id))
      pagination keyset?: true, countable: true, default_limit: 50, max_page_size: 1000
    end

    read :by_type do
      argument :event_type, :string, allow_nil?: false
      filter expr(event_type == ^arg(:event_type))
      pagination keyset?: true, countable: true, default_limit: 50, max_page_size: 1000
    end

    read :top_values do
      argument :limit, :integer, default: 10
      prepare build(sort: [value: :desc], limit: 10)
    end

    # Time-range filter — stats-based file skipping kicks in here.
    read :in_time_range do
      argument :from, :utc_datetime, allow_nil?: false
      argument :to, :utc_datetime, allow_nil?: false
      filter expr(occurred_at >= ^arg(:from) and occurred_at <= ^arg(:to))
      pagination keyset?: true, countable: true, default_limit: 50, max_page_size: 1000
    end

    read :by_type_prefix do
      argument :prefix, :string, allow_nil?: false
      filter expr(string_starts_with(event_type, ^arg(:prefix)))
      pagination keyset?: true, countable: true, default_limit: 50, max_page_size: 1000
    end
  end
end

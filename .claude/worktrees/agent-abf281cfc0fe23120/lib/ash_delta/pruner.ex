defmodule AshDelta.Pruner do
  @moduledoc """
  Data skipping: translates an Ash filter into Postgres predicates over the
  `delta_files` manifest so only Parquet files that *might* contain matching
  rows get scanned.

  Two pruning mechanisms:

    * **Partition pruning** — equality/IN predicates on `partition_by` columns
      become subqueries against `delta_file_partitions` (B-tree indexed).
    * **Stats pruning** — range/equality predicates on `stats_columns` become
      NOT EXISTS subqueries against `delta_file_stats` numeric or text columns.

  The translation is *conservative*: any predicate we can't map keeps the
  file in the candidate set (it still gets filtered correctly during the
  scan). `OR` branches union their candidate conditions; an untranslatable
  branch makes the whole `OR` a no-op, as it must.
  """

  alias Ash.Query.{BooleanExpression, Not, Ref}
  alias Ash.Query.Operator.{Eq, In, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual}
  alias AshDelta.{Info, Log}

  @doc "Returns the live files at `version` that may contain rows matching `filter`."
  def candidate_files(resource, version, filter) do
    {sql, params} =
      case filter do
        nil -> {"TRUE", []}
        %Ash.Filter{expression: expr} -> compile(resource, expr, 3)
        expr -> compile(resource, expr, 3)
      end

    {:ok, Log.files(resource, version, sql, params)}
  rescue
    e -> {:error, e}
  end

  # ── Compilation ───────────────────────────────────────────────────────────
  # Returns {sql_fragment, params}. Placeholders start at index `n` because
  # $1/$2 are reserved for table_id/version in Log.files/4.

  defp compile(resource, %BooleanExpression{op: :and, left: l, right: r}, n) do
    {ls, lp} = compile(resource, l, n)
    {rs, rp} = compile(resource, r, n + length(lp))
    {"(#{ls}) AND (#{rs})", lp ++ rp}
  end

  defp compile(resource, %BooleanExpression{op: :or, left: l, right: r}, n) do
    {ls, lp} = compile(resource, l, n)
    {rs, rp} = compile(resource, r, n + length(lp))
    {"(#{ls}) OR (#{rs})", lp ++ rp}
  end

  # NOT over file-level stats is not sound to push down (a file whose range
  # contains v may still hold rows != v), so we keep all files.
  defp compile(_resource, %Not{}, _n), do: {"TRUE", []}

  defp compile(resource, %Eq{left: %Ref{} = ref, right: value}, n) do
    with {:ok, col} <- skippable(resource, ref) do
      if partition_column?(resource, col) do
        partition_eq(col, value, n)
      else
        stats_overlap(col, value, value, n)
      end
    else
      _ -> {"TRUE", []}
    end
  end

  defp compile(resource, %In{left: %Ref{} = ref, right: values}, n) do
    values = Enum.to_list(values)

    with {:ok, col} <- skippable(resource, ref) do
      if partition_column?(resource, col) do
        partition_in(col, values, n)
      else
        {min_v, max_v} = Enum.min_max(values)
        stats_overlap(col, min_v, max_v, n)
      end
    else
      _ -> {"TRUE", []}
    end
  end

  defp compile(resource, %LessThan{left: %Ref{} = ref, right: v}, n),
    do: stats_bound(resource, ref, :min, v, n)

  defp compile(resource, %LessThanOrEqual{left: %Ref{} = ref, right: v}, n),
    do: stats_bound(resource, ref, :min, v, n)

  defp compile(resource, %GreaterThan{left: %Ref{} = ref, right: v}, n),
    do: stats_bound(resource, ref, :max, v, n)

  defp compile(resource, %GreaterThanOrEqual{left: %Ref{} = ref, right: v}, n),
    do: stats_bound(resource, ref, :max, v, n)

  defp compile(_resource, _expr, _n), do: {"TRUE", []}

  # ── Partition predicates ──────────────────────────────────────────────────

  defp partition_eq(col, value, n) do
    sql = "f.id IN (SELECT file_id FROM delta_file_partitions WHERE col_name = $#{n} AND col_value = $#{n + 1})"
    {sql, [to_string(col), to_string(encode(value))]}
  end

  defp partition_in(col, values, n) do
    encoded = Enum.map(values, &to_string(encode(&1)))
    sql = "f.id IN (SELECT file_id FROM delta_file_partitions WHERE col_name = $#{n} AND col_value = ANY($#{n + 1}))"
    {sql, [to_string(col), encoded]}
  end

  # ── Stats predicates ─────────────────────────────────────────────────────

  # Exclude files where the relevant bound makes the predicate impossible.
  # For col < v or col <= v: exclude files where min_num/min_val > v.
  # For col > v or col >= v: exclude files where max_num/max_val < v.
  defp stats_bound(resource, ref, side, value, n) do
    case skippable(resource, ref) do
      {:ok, col} ->
        {stat_col, cmp} =
          case side do
            :min -> if is_number(value), do: {"min_num", ">"}, else: {"min_val", ">"}
            :max -> if is_number(value), do: {"max_num", "<"}, else: {"max_val", "<"}
          end

        sql = """
        NOT EXISTS (
          SELECT 1 FROM delta_file_stats s
          WHERE s.file_id = f.id AND s.col_name = $#{n}
            AND s.#{stat_col} IS NOT NULL AND s.#{stat_col} #{cmp} $#{n + 1}
        )
        """

        {sql, [to_string(col), encode(value)]}

      _ ->
        {"TRUE", []}
    end
  end

  defp stats_overlap(col, lo, hi, n) do
    {lo_col, hi_col} =
      if is_number(lo) or is_number(hi), do: {"min_num", "max_num"}, else: {"min_val", "max_val"}

    sql = """
    NOT EXISTS (
      SELECT 1 FROM delta_file_stats s
      WHERE s.file_id = f.id AND s.col_name = $#{n}
        AND s.#{hi_col} IS NOT NULL AND s.#{hi_col} < $#{n + 1}
    )
    AND NOT EXISTS (
      SELECT 1 FROM delta_file_stats s
      WHERE s.file_id = f.id AND s.col_name = $#{n}
        AND s.#{lo_col} IS NOT NULL AND s.#{lo_col} > $#{n + 2}
    )
    """

    {sql, [to_string(col), encode(lo), encode(hi)]}
  end

  # ── Helpers ──────────────────────────────────────────────────────────────

  defp skippable(resource, %Ref{attribute: %{name: name}, relationship_path: []}) do
    if name in Info.skippable_columns(resource), do: {:ok, name}, else: :error
  end

  defp skippable(_, _), do: :error

  defp partition_column?(resource, col) do
    {:ok, parts} = AshDelta.Info.partition_by(resource)
    col in parts
  end

  @doc false
  def encode(%DateTime{} = dt), do: DateTime.to_iso8601(dt)
  def encode(%NaiveDateTime{} = dt), do: NaiveDateTime.to_iso8601(dt)
  def encode(%Date{} = d), do: Date.to_iso8601(d)
  def encode(v) when is_number(v), do: v
  def encode(v), do: to_string(v)
end

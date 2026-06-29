defmodule AshDelta.Sql do
  @moduledoc """
  Compiles an Ash filter expression to a parameterized DuckDB `WHERE` clause.

  Unlike the pruner (which may safely under-translate), this compiler is the
  source of row-level correctness, so an unsupported expression raises rather
  than silently widening results. The data layer's `can?({:filter_expr, _})`
  declarations keep Ash from sending anything we don't list, but a raise here
  is the backstop.
  """

  alias Ash.Query.{BooleanExpression, Not, Ref}

  alias Ash.Query.Operator.{
    Eq,
    NotEq,
    In,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual
  }

  defmodule UnsupportedExpression do
    defexception [:message]
  end

  @doc "Returns `{where_sql, params}` using `?` placeholders."
  def compile(%Ash.Filter{expression: expr}), do: compile(expr)

  def compile(%Ash.Filter.Simple{predicates: []}), do: {"TRUE", []}

  def compile(%Ash.Filter.Simple{predicates: predicates}) do
    predicates
    |> Enum.map(&walk/1)
    |> Enum.reduce(fn {sql, ps}, {acc_sql, acc_ps} ->
      {"(#{acc_sql}) AND (#{sql})", acc_ps ++ ps}
    end)
  end

  def compile(nil), do: {"TRUE", []}
  def compile({:raw, sql, params}), do: {sql, params}

  def compile(expr) do
    {sql, params} = walk(expr)
    {sql, params}
  end

  defp walk(%BooleanExpression{op: op, left: l, right: r}) when op in [:and, :or] do
    {ls, lp} = walk(l)
    {rs, rp} = walk(r)
    {"(#{ls}) #{String.upcase(to_string(op))} (#{rs})", lp ++ rp}
  end

  defp walk(%Not{expression: expr}) do
    {sql, params} = walk(expr)
    {"NOT (#{sql})", params}
  end

  defp walk(%Eq{left: l, right: nil}), do: {"#{operand!(l)} IS NULL", []}
  defp walk(%Eq{left: l, right: r}), do: binary(l, "=", r)
  defp walk(%NotEq{left: l, right: nil}), do: {"#{operand!(l)} IS NOT NULL", []}
  defp walk(%NotEq{left: l, right: r}), do: binary(l, "!=", r)
  defp walk(%LessThan{left: l, right: r}), do: binary(l, "<", r)
  defp walk(%LessThanOrEqual{left: l, right: r}), do: binary(l, "<=", r)
  defp walk(%GreaterThan{left: l, right: r}), do: binary(l, ">", r)
  defp walk(%GreaterThanOrEqual{left: l, right: r}), do: binary(l, ">=", r)

  defp walk(%In{left: l, right: values}) do
    values = Enum.to_list(values)
    placeholders = Enum.map_join(values, ", ", fn _ -> "?" end)
    {"#{operand!(l)} IN (#{placeholders})", Enum.map(values, &dump/1)}
  end

  defp walk(%Ash.Query.Function.IsNil{arguments: [arg]}),
    do: {"#{operand!(arg)} IS NULL", []}

  defp walk(%Ash.Filter.Simple.Not{predicate: predicate}) do
    {sql, params} = walk(predicate)
    {"NOT (#{sql})", params}
  end

  defp walk(true), do: {"TRUE", []}
  defp walk(false), do: {"FALSE", []}

  defp walk(other) do
    raise UnsupportedExpression,
      message: "AshDelta cannot compile filter expression: #{inspect(other)}"
  end

  defp binary(left, op, right) do
    {"#{operand!(left)} #{op} ?", [dump(right)]}
  end

  defp operand!(%Ref{attribute: %{name: name}, relationship_path: []}), do: ~s("#{name}")

  defp operand!(other) do
    raise UnsupportedExpression,
      message: "AshDelta only supports direct attribute references, got: #{inspect(other)}"
  end

  # Duckdbex expects Erlang calendar tuples: {{y,m,d},{h,min,s,us}} for timestamps.
  defp dump(%DateTime{} = v) do
    ndt = DateTime.to_naive(v)
    {date, {h, m, s}} = NaiveDateTime.to_erl(ndt)
    {us, _} = ndt.microsecond
    {date, {h, m, s, us}}
  end

  defp dump(%NaiveDateTime{} = v) do
    {date, {h, m, s}} = NaiveDateTime.to_erl(v)
    {us, _} = v.microsecond
    {date, {h, m, s, us}}
  end

  defp dump(%Date{} = v), do: Date.to_erl(v)
  defp dump(%Decimal{} = v), do: Decimal.to_float(v)
  defp dump(v) when is_atom(v) and not is_boolean(v) and not is_nil(v), do: to_string(v)
  defp dump(v), do: v
end

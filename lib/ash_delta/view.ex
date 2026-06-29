defmodule AshDelta.View do
  @moduledoc """
  Refresh engine for `AshDelta.ViewLayer` materialized views.

  ## Watermarks

  A view records, per source, the source version it last consumed in
  `delta_view_state`. Refresh runs inside the *view's* commit transaction
  (`AshDelta.Log.commit/4`), so advancing the watermark and writing the new
  view version are one atomic step — a crash mid-refresh leaves the view at
  its previous version with its previous watermark, and the next run simply
  reprocesses the same source delta. This is the same durable-acknowledger
  shape as gating an LSN on a downstream publish, pointed at the commit log.

  ## Strategies

  Both strategies are expressed through one mechanism: recompute the view
  `query` over a chosen set of *source partitions*, then replace exactly the
  view files belonging to those partitions.

    * `:recompute` chooses *all* partitions (feeds every live source file,
      replaces every live view file).
    * `:partition_incremental` chooses only partitions touched by source
      commits since the watermark — where "touched" includes partitions of
      both added *and removed* source files, so source DELETE/OPTIMIZE
      correctly forces re-aggregation rather than leaving stale view rows.

  Correctness of the incremental path depends on the alignment invariant
  (view `partition_by` ⊆ each source `partition_by`), validated here at
  refresh time when source modules are guaranteed loaded.
  """

  alias AshDelta.{Info, Log, Reader, Writer}

  @doc """
  Refresh a view to its sources' current versions.

  Returns `{:ok, %{view_version:, strategy:, partitions:, rows_written:,
  files_removed:, files_added:, watermarks:}}` or `{:ok, :up_to_date}`.
  """
  def refresh(view, opts \\ []) do
    {:ok, strategy} = Info.view_refresh(view)
    strategy = Keyword.get(opts, :strategy, strategy)
    sources = source_modules(view)

    validate_alignment!(view, sources, strategy)

    targets = source_targets(sources)
    watermarks = read_watermarks(view, targets)

    cond do
      strategy == :recompute ->
        do_refresh(view, sources, targets, watermarks, :all)

      up_to_date?(targets, watermarks) ->
        {:ok, :up_to_date}

      true ->
        case affected_partitions(view, sources, watermarks) do
          [] ->
            # Source advanced but no partition-bearing files changed (e.g. a
            # metadata-only commit); just advance the watermark.
            advance_only(view, targets)

          partitions ->
            do_refresh(view, sources, targets, watermarks, {:partitions, partitions})
        end
    end
  end

  # ── Core refresh ───────────────────────────────────────────────────────────

  defp do_refresh(view, sources, targets, _watermarks, scope) do
    partition_cols = partition_by(view)

    # Recompute view rows for the chosen scope by feeding the SQL only the
    # source files in those partitions. With the alignment invariant this
    # yields exactly the view rows for those partitions.
    sql = build_sql(view, sources, scope)
    {:ok, records} = Reader.query_records(view, sql)

    add_specs = if records == [], do: [], else: Writer.write_files(view, records)

    Log.commit(view, refresh_op(scope), refresh_params(scope, records), fn repo,
                                                                           table_id,
                                                                           version ->
      remove_ids = view_files_to_remove(view, version, scope, partition_cols)

      with :ok <- Log.remove_files(repo, table_id, version, remove_ids),
           :ok <- maybe_add(repo, table_id, version, add_specs),
           :ok <- write_watermarks(repo, table_id, targets) do
        :ok
      end
    end)
    |> case do
      {:ok, version} ->
        {:ok,
         %{
           view_version: version,
           strategy: scope_strategy(scope),
           partitions: scope_partition_count(scope),
           rows_written: length(records),
           files_added: length(add_specs),
           watermarks: Map.new(targets, fn {_m, id, v, _name} -> {id, v} end)
         }}

      {:error, :concurrent_modification} ->
        # A concurrent refresh or OPTIMIZE moved the view; retry from a fresh
        # snapshot/watermark.
        refresh(view)

      other ->
        other
    end
  end

  defp maybe_add(_repo, _table_id, _version, []), do: :ok

  defp maybe_add(repo, table_id, version, specs),
    do: Log.add_files(repo, table_id, version, specs)

  # Which existing live view files must be replaced.
  defp view_files_to_remove(view, version, :all, _cols) do
    # version-1 is the snapshot the commit supersedes.
    Log.files(view, version - 1, "TRUE", []) |> Enum.map(& &1.id)
  end

  defp view_files_to_remove(view, version, {:partitions, partitions}, cols) do
    # Match view files whose partition_values equal one of the affected
    # partitions (projected onto the view's partition columns).
    keys = Enum.map(partitions, &project(&1, cols))

    Log.files(view, version - 1, "TRUE", [])
    |> Enum.filter(fn f -> project(f.partition_values, cols) in keys end)
    |> Enum.map(& &1.id)
  end

  # ── SQL assembly ─────────────────────────────────────────────────────────

  # Replace each source alias in the query body with a read_parquet(...) over
  # that source's live files, restricted to the scope's partitions.
  defp build_sql(view, sources, scope) do
    {:ok, query} = Info.view_query(view)

    Enum.reduce(sources, query, fn {alias_name, module}, sql ->
      paths = source_paths(module, scope)

      replacement =
        case paths do
          [] ->
            # No live files in scope → an empty relation with no rows.
            "(SELECT * FROM read_parquet([]) WHERE FALSE) AS #{alias_name}"

          paths ->
            list = Enum.map_join(paths, ", ", &"'#{&1}'")
            "read_parquet([#{list}], union_by_name = true) AS #{alias_name}"
        end

      replace_alias(sql, to_string(alias_name), replacement)
    end)
  end

  # Replace a bare table reference (`FROM source`, `JOIN source`) but not
  # substrings of other identifiers. Word-boundary regex on the alias.
  defp replace_alias(sql, alias_name, replacement) do
    Regex.replace(~r/\b#{Regex.escape(alias_name)}\b/, sql, replacement, global: true)
  end

  defp source_paths(module, :all) do
    Log.snapshot(module) |> Enum.map(& &1.path)
  end

  defp source_paths(module, {:partitions, partitions}) do
    cols = partition_by(module)
    keys = Enum.map(partitions, &project(&1, cols))

    module
    |> Log.snapshot()
    |> Enum.filter(fn f -> project(f.partition_values, cols) in keys end)
    |> Enum.map(& &1.path)
  end

  # ── Affected-partition diff ────────────────────────────────────────────────

  # Distinct partition values across all source files added OR removed in
  # (watermark, current] for every source. Both directions matter: a removed
  # file's partition needs re-aggregation just as much as an added one.
  defp affected_partitions(_view, sources, watermarks) do
    sources
    |> Enum.flat_map(fn {_alias, module} ->
      repo = Log.repo!(module)
      table_id = Log.ensure_table!(module)
      from_v = Map.get(watermarks, table_id, 0)
      {:ok, to_v} = Log.resolve_version(module, %{})

      %{rows: rows} =
        repo.query!(
          """
          SELECT DISTINCT partition_values FROM delta_files
          WHERE table_id = $1
            AND ((added_version > $2 AND added_version <= $3)
              OR (removed_version > $2 AND removed_version <= $3))
          """,
          [table_id, from_v, to_v]
        )

      Enum.map(rows, fn [pv] -> pv end)
    end)
    |> Enum.uniq()
  end

  # ── Watermarks ─────────────────────────────────────────────────────────────

  # targets :: [{module, source_table_id, current_version, name}]
  defp source_targets(sources) do
    Enum.map(sources, fn {_alias, module} ->
      table_id = Log.ensure_table!(module)
      {:ok, version} = Log.resolve_version(module, %{})
      {module, table_id, version, Info.table_name(module)}
    end)
  end

  defp read_watermarks(view, targets) do
    repo = Log.repo!(view)
    view_table_id = Log.ensure_table!(view)
    source_ids = Enum.map(targets, fn {_m, id, _v, _n} -> id end)

    %{rows: rows} =
      repo.query!(
        """
        SELECT source_table_id, consumed_version FROM delta_view_state
        WHERE view_table_id = $1 AND source_table_id = ANY($2)
        """,
        [view_table_id, source_ids]
      )

    Map.new(rows, fn [sid, v] -> {sid, v} end)
  end

  defp write_watermarks(repo, view_table_id, targets) do
    Enum.each(targets, fn {_m, source_id, version, _n} ->
      repo.query!(
        """
        INSERT INTO delta_view_state (view_table_id, source_table_id, consumed_version)
        VALUES ($1, $2, $3)
        ON CONFLICT (view_table_id, source_table_id)
        DO UPDATE SET consumed_version = EXCLUDED.consumed_version, refreshed_at = now()
        """,
        [view_table_id, source_id, version]
      )
    end)

    :ok
  end

  defp advance_only(view, targets) do
    Log.commit(view, :refresh_watermark, %{}, fn repo, table_id, _version ->
      write_watermarks(repo, table_id, targets)
    end)
    |> case do
      {:ok, version} -> {:ok, %{view_version: version, strategy: :watermark_only, rows_written: 0}}
      other -> other
    end
  end

  defp up_to_date?(targets, watermarks) do
    Enum.all?(targets, fn {_m, id, version, _n} -> Map.get(watermarks, id, -1) == version end)
  end

  # ── Validation ──────────────────────────────────────────────────────────────

  defp validate_alignment!(_view, _sources, :recompute), do: :ok

  defp validate_alignment!(view, sources, :partition_incremental) do
    view_parts = MapSet.new(partition_by(view))

    Enum.each(sources, fn {alias_name, module} ->
      source_parts = MapSet.new(partition_by(module))

      unless MapSet.subset?(view_parts, source_parts) do
        raise ArgumentError, """
        #{inspect(view)} uses :partition_incremental but its partition_by \
        #{inspect(MapSet.to_list(view_parts))} is not a subset of source \
        #{alias_name} (#{inspect(module)}) partition_by \
        #{inspect(MapSet.to_list(source_parts))}.

        A view partition's rows could then depend on source rows outside that \
        partition, so incremental recompute would produce wrong results. Use \
        refresh :recompute, or align the partitioning.
        """
      end
    end)
  end

  # ── Helpers ──────────────────────────────────────────────────────────────────

  defp source_modules(view) do
    {:ok, sources} = Info.view_sources(view)

    Enum.each(sources, fn {alias_name, module} ->
      Code.ensure_loaded!(module)

      unless Ash.DataLayer.data_layer(module) in [AshDelta.DataLayer, AshDelta.ViewLayer] do
        raise ArgumentError,
              "view source #{alias_name} (#{inspect(module)}) is not an AshDelta resource"
      end
    end)

    sources
  end

  defp partition_by(resource) do
    {:ok, parts} = Info.partition_by(resource)
    parts
  end

  # Project a partition_values map (string keys) onto a subset of columns.
  defp project(partition_values, cols) do
    Map.new(cols, fn c -> {to_string(c), partition_values[to_string(c)]} end)
  end

  defp refresh_op(:all), do: :refresh_full
  defp refresh_op({:partitions, _}), do: :refresh_incremental

  defp refresh_params(:all, records), do: %{"rows" => length(records)}

  defp refresh_params({:partitions, partitions}, records),
    do: %{"rows" => length(records), "partitions" => length(partitions)}

  defp scope_strategy(:all), do: :recompute
  defp scope_strategy({:partitions, _}), do: :partition_incremental

  defp scope_partition_count(:all), do: :all
  defp scope_partition_count({:partitions, p}), do: length(p)
end

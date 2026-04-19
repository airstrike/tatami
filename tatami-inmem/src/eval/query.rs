//! Query orchestration — Phase 5g of MAP_PLAN.md §5.
//!
//! Takes a [`ResolvedQuery`] and folds it into a public [`Results`] value,
//! following the `Axes → Results` table from §3.3:
//!
//! | `Axes`                         | `Results`                                                |
//! |---                             |---                                                       |
//! | `Scalar`                       | `Scalar`                                                 |
//! | `Series { rows }`              | `Series`                                                 |
//! | `Pivot { rows, columns }`      | `Pivot` — or `Rollup` when `rows` is a top-level, single-rooted `Descendants` |
//! | `Pages { rows, columns, pages }` | `Pivot` with stacked column headers (page × column)    |
//!
//! "Single-rooted" means the flat `Descendants` output shares one
//! top-level ancestor (same dim, same hierarchy, same `path[0]`). A
//! multi-rooted descendants set — e.g. `Set::range(FY2025..FY2030).descendants_to(Quarter)`
//! spanning six fiscal years — can't fit a single-rooted [`rollup::Tree`]
//! and falls back to a flat Pivot. Collapsing every root under one
//! synthetic ancestor would silently drop subtrees (MAP §8 R3).
//!
//! ### Shape assembly
//!
//! 1. Evaluate each axis's [`ResolvedSet`] into a `Vec<ResolvedTuple>`.
//! 2. For every grid position `(row, column, page, slicer)`, merge the
//!    tuples (axis dims are disjoint, enforced at resolve time) and
//!    evaluate every metric via [`crate::eval::metric::evaluate_expr`].
//! 3. Assemble per the §3.3 table.
//!
//! Multi-metric pivots widen `col_headers` so the grid stays rectangular:
//! for `m` metrics and `c` column tuples, `col_headers.len() == c * m`,
//! grouped by column-then-metric. Callers reading `cells[r][c * m + k]`
//! address row `r`'s column `c` metric `k`.
//!
//! ### Options
//!
//! `options.non_empty`, `options.order`, `options.limit` are applied
//! post-assembly to `Series` and `Pivot` results. `Scalar` and `Rollup`
//! ignore them — a single cell has nothing to sort, and v0.1 rollup
//! ordering is out of scope.
//!
//! [`Results`]: tatami::Results
//! [`ResolvedQuery`]: crate::resolve::ResolvedQuery
//! [`ResolvedSet`]: crate::resolve::ResolvedSet

use std::cmp::Ordering;

use tatami::query::{MemberRef, Tuple};
use tatami::schema::metric::Expr;
use tatami::schema::{Metric, Name};
use tatami::{Cell, Results, pivot, rollup, scalar, series};

use crate::Error;
use crate::InMemoryCube;
use crate::eval::metric::evaluate_expr;
use crate::eval::set;
use crate::resolve::{MetricHandle, ResolvedAxes, ResolvedMember, ResolvedQuery, ResolvedTuple};

/// Evaluate a resolved query end-to-end, producing a typed [`Results`]
/// value.
///
/// The outer `Result` surfaces evaluation-time invariant violations
/// (unresolved names defensively threaded through metric eval, polars
/// runtime failures). Successful evaluation always returns a
/// shape-appropriate `Results` variant per §3.3.
pub(crate) fn evaluate<'s>(
    resolved: &ResolvedQuery<'s>,
    cube: &'s InMemoryCube,
) -> Result<Results, Error> {
    match &resolved.axes {
        ResolvedAxes::Scalar => evaluate_scalar(resolved, cube),
        ResolvedAxes::Series { rows } => evaluate_series(resolved, rows, cube),
        ResolvedAxes::Pivot { rows, columns } => evaluate_pivot(resolved, rows, columns, cube),
        ResolvedAxes::Pages {
            rows,
            columns,
            pages,
        } => evaluate_pages(resolved, rows, columns, pages, cube),
    }
}

/// `Axes::Scalar` — no axes. One cell per metric, evaluated at the slicer
/// tuple alone.
fn evaluate_scalar<'s>(
    resolved: &ResolvedQuery<'s>,
    cube: &'s InMemoryCube,
) -> Result<Results, Error> {
    let values = metrics_at(&resolved.metrics, &resolved.slicer, cube)?;
    let tuple = tuple_of(&resolved.slicer);
    Ok(Results::Scalar(scalar::Result::new(tuple, values)))
}

/// `Axes::Series { rows }` — rows × metrics, with the row tuples' members
/// as the shared x-axis. One [`series::Row`] per metric.
fn evaluate_series<'s>(
    resolved: &ResolvedQuery<'s>,
    rows: &crate::resolve::ResolvedSet<'s>,
    cube: &'s InMemoryCube,
) -> Result<Results, Error> {
    let row_tuples = set::evaluate(rows, cube)?;

    // Build the `Vec<MemberRef>` x-axis from each row tuple's sole member.
    // A series-axis tuple may carry any number of members (cross-joined);
    // the renderer expects a single member per x entry, so fall back to
    // the first member when multiple are present. Single-dim row-sets are
    // the v0.1 norm.
    let mut x: Vec<MemberRef> = Vec::with_capacity(row_tuples.len());
    for tuple in &row_tuples {
        let first = tuple
            .members
            .first()
            .ok_or(Error::EvalSetCompositionIllFormed {
                reason: "Series row tuple has no members",
            })?;
        x.push(member_ref_of(first));
    }

    // Assemble per-metric rows. For each metric, walk every row tuple
    // (merged with the slicer) and evaluate.
    let mut series_rows: Vec<series::Row> = Vec::with_capacity(resolved.metrics.len());
    for metric_handle in &resolved.metrics {
        let label = metric_name(metric_handle).as_str().to_owned();
        let mut values: Vec<Cell> = Vec::with_capacity(row_tuples.len());
        for row_tuple in &row_tuples {
            let merged = merge(&resolved.slicer, row_tuple);
            values.push(evaluate_metric(metric_handle, &merged, cube)?);
        }
        series_rows.push(series::Row { label, values });
    }

    // Apply non-empty / order / limit to the rows.
    let (x, series_rows) = apply_options_series(x, series_rows, resolved);

    Ok(Results::Series(series::Result::new(x, series_rows)))
}

/// `Axes::Pivot { rows, columns }` — rows × columns grid, one cell per
/// metric per (row, column). If `rows` is structurally a `Descendants`
/// set **and** its flat output shares a single top-level ancestor, we
/// return a [`Results::Rollup`] tree instead (§3.3 note).
///
/// Multi-root descendants — e.g. `Set::range(FY2025..FY2030).descendants_to(Quarter)`
/// — don't fit a single-rooted [`rollup::Tree`] and fall through to
/// [`Results::Pivot`]. Collapsing them into one synthetic root would
/// silently drop every non-first-root's subtree (MAP §8 R3 category);
/// a flat grid of (quarter × region) is the honest shape.
fn evaluate_pivot<'s>(
    resolved: &ResolvedQuery<'s>,
    rows: &crate::resolve::ResolvedSet<'s>,
    columns: &crate::resolve::ResolvedSet<'s>,
    cube: &'s InMemoryCube,
) -> Result<Results, Error> {
    if is_descendants(rows) {
        let row_tuples = set::evaluate(rows, cube)?;
        if let Some(root) = single_root_member(&row_tuples) {
            return evaluate_rollup(resolved, row_tuples, root, cube);
        }
        // Fall through: multi-root `Descendants` produces a flat pivot
        // with one row per tuple, same as any other set on the rows axis.
        let col_tuples = set::evaluate(columns, cube)?;
        return build_pivot(resolved, row_tuples, col_tuples, cube);
    }
    let row_tuples = set::evaluate(rows, cube)?;
    let col_tuples = set::evaluate(columns, cube)?;
    build_pivot(resolved, row_tuples, col_tuples, cube)
}

/// Whether a resolved set is structurally a top-level `Descendants`. The
/// §3.3 table triggers a `Rollup` return whenever the pivot's rows axis
/// descends a hierarchy — the tree shape is the natural output.
fn is_descendants(set: &crate::resolve::ResolvedSet<'_>) -> bool {
    matches!(set, crate::resolve::ResolvedSet::Descendants { .. })
}

/// If every tuple in `tuples` carries a single member that shares the same
/// `(dim, hierarchy, path[0])` head, synthesize a [`MemberRef`] at that
/// single-segment path and return it — the implicit root of the rollup
/// tree. Returns `None` when the tuples don't share a single top-level
/// ancestor (multi-root `Descendants` output, cross-joined rows, or a
/// degenerate empty set).
fn single_root_member(tuples: &[ResolvedTuple<'_>]) -> Option<MemberRef> {
    let first = tuples.first()?;
    let lead = first.members.first()?;
    let first_segment: &Name = lead.path.segments().next()?;
    let first_dim = &lead.dim.dim.name;
    let first_hierarchy = &lead.hierarchy.hierarchy.name;
    for tuple in tuples {
        // Rollup trees are single-dim; a cross-joined tuple with multiple
        // members doesn't fit the shape and falls through to Pivot.
        if tuple.members.len() != 1 {
            return None;
        }
        let member = &tuple.members[0];
        let segment = member.path.segments().next()?;
        if segment != first_segment
            || &member.dim.dim.name != first_dim
            || &member.hierarchy.hierarchy.name != first_hierarchy
        {
            return None;
        }
    }
    Some(MemberRef::new(
        first_dim.clone(),
        first_hierarchy.clone(),
        tatami::query::Path::of(first_segment.clone()),
    ))
}

/// `Axes::Pages { rows, columns, pages }` — v0.1 collapses pages onto the
/// columns axis by cross-joining every column tuple with every page
/// tuple. This keeps `Pages` as a strict widening of `Pivot`.
fn evaluate_pages<'s>(
    resolved: &ResolvedQuery<'s>,
    rows: &crate::resolve::ResolvedSet<'s>,
    columns: &crate::resolve::ResolvedSet<'s>,
    pages: &crate::resolve::ResolvedSet<'s>,
    cube: &'s InMemoryCube,
) -> Result<Results, Error> {
    let row_tuples = set::evaluate(rows, cube)?;
    let col_tuples = set::evaluate(columns, cube)?;
    let page_tuples = set::evaluate(pages, cube)?;

    // Widen columns by the page axis: for each page, every column tuple
    // paired with it becomes a top-level column header. Order is
    // page-major to match common UX expectations (one page then the next).
    let mut widened: Vec<ResolvedTuple<'s>> =
        Vec::with_capacity(col_tuples.len().saturating_mul(page_tuples.len()));
    for page_tuple in &page_tuples {
        for col_tuple in &col_tuples {
            widened.push(merge(col_tuple, page_tuple));
        }
    }
    build_pivot(resolved, row_tuples, widened, cube)
}

/// Assemble a pivot result from already-evaluated row and column tuple
/// lists. Handles the multi-metric widening of `col_headers`.
fn build_pivot<'s>(
    resolved: &ResolvedQuery<'s>,
    row_tuples: Vec<ResolvedTuple<'s>>,
    col_tuples: Vec<ResolvedTuple<'s>>,
    cube: &'s InMemoryCube,
) -> Result<Results, Error> {
    let n_metrics = resolved.metrics.len();

    // For every row × column × metric, evaluate one cell.
    let mut cells: Vec<Vec<Cell>> = Vec::with_capacity(row_tuples.len());
    for row_tuple in &row_tuples {
        let merged_row = merge(&resolved.slicer, row_tuple);
        let mut row_cells: Vec<Cell> = Vec::with_capacity(col_tuples.len() * n_metrics);
        for col_tuple in &col_tuples {
            let merged = merge(&merged_row, col_tuple);
            for metric_handle in &resolved.metrics {
                row_cells.push(evaluate_metric(metric_handle, &merged, cube)?);
            }
        }
        cells.push(row_cells);
    }

    // Widen col_headers by metric count — each column tuple repeats once
    // per metric, keeping `cells[r][c * m + k]` indexable. Public
    // `Tuple`s; we drop the handles here.
    let row_headers: Vec<Tuple> = row_tuples.iter().map(tuple_of).collect();
    let mut col_headers: Vec<Tuple> = Vec::with_capacity(col_tuples.len() * n_metrics.max(1));
    for col_tuple in &col_tuples {
        let base = tuple_of(col_tuple);
        if n_metrics <= 1 {
            col_headers.push(base);
        } else {
            for _ in 0..n_metrics {
                col_headers.push(base.clone());
            }
        }
    }

    let (row_headers, cells) = apply_options_pivot(row_headers, cells, resolved);

    Ok(Results::Pivot(pivot::Result::new(
        row_headers,
        col_headers,
        cells,
    )))
}

/// `Axes::Pivot { rows: Descendants, … }` with a single-rooted row set →
/// [`Results::Rollup`]. The flat row tuples from set evaluation carry
/// [`tatami::query::Path`]s that encode the tree; we reassemble it by
/// shared path prefix rooted at the synthesized ancestor member.
///
/// The caller is responsible for guaranteeing the tuples share a single
/// top-level ancestor (see [`single_root_member`]); multi-root descendants
/// fall through to [`build_pivot`] instead so we don't collapse multiple
/// subtrees under one root (a silent-wrong-answer bug, MAP §8 R3).
///
/// Multi-metric rollup is v0.2; v0.1 uses the first metric for every
/// node's `value` cell. `columns` is ignored — a rollup tree has no
/// column axis by shape.
fn evaluate_rollup<'s>(
    resolved: &ResolvedQuery<'s>,
    row_tuples: Vec<ResolvedTuple<'s>>,
    root_ref: MemberRef,
    cube: &'s InMemoryCube,
) -> Result<Results, Error> {
    let primary = resolved
        .metrics
        .first()
        .ok_or(Error::EvalSetCompositionIllFormed {
            reason: "Rollup requires at least one metric",
        })?;

    // Evaluate the primary metric at the root itself. The descendants set
    // doesn't include its own source member, so we compute the root cell
    // explicitly — pinning the slicer × the synthesized root member.
    let root_member = synthesize_root_member(&row_tuples, &root_ref)?;
    let root_tuple = ResolvedTuple::from_members(vec![root_member]);
    let merged_root = merge(&resolved.slicer, &root_tuple);
    let root_cell = evaluate_metric(primary, &merged_root, cube)?;

    // Walk each flat tuple, evaluate the primary metric, collect
    // (member_ref, cell) pairs. These are the descendants that slot under
    // the synthesized root.
    let mut descendants: Vec<(MemberRef, Cell)> = Vec::with_capacity(row_tuples.len());
    for row_tuple in &row_tuples {
        let merged = merge(&resolved.slicer, row_tuple);
        let cell = evaluate_metric(primary, &merged, cube)?;
        let first_member = row_tuple
            .members
            .first()
            .ok_or(Error::EvalSetCompositionIllFormed {
                reason: "Rollup row tuple has no members",
            })?;
        descendants.push((member_ref_of(first_member), cell));
    }

    let tree = assemble_rollup(root_ref, root_cell, descendants);
    Ok(Results::Rollup(tree))
}

/// Rebuild a [`crate::resolve::ResolvedMember`] at the `root_ref`'s
/// single-segment path, borrowing dim/hierarchy handles from the first row
/// tuple's member. The caller guarantees `row_tuples` is non-empty and
/// its tuples share a single-dim, single-hierarchy head (enforced by
/// [`single_root_member`]).
fn synthesize_root_member<'s>(
    row_tuples: &[ResolvedTuple<'s>],
    root_ref: &MemberRef,
) -> Result<ResolvedMember<'s>, Error> {
    let lead = row_tuples.first().and_then(|t| t.members.first()).ok_or(
        Error::EvalSetCompositionIllFormed {
            reason: "Rollup row set is empty",
        },
    )?;
    Ok(ResolvedMember {
        dim: lead.dim,
        hierarchy: lead.hierarchy,
        path: root_ref.path.clone(),
    })
}

/// Assemble the rollup tree: start at the synthesized root with its
/// pre-computed cell, then slot every descendant under it by path prefix.
fn assemble_rollup(
    root_ref: MemberRef,
    root_cell: Cell,
    descendants: Vec<(MemberRef, Cell)>,
) -> rollup::Tree {
    let root_depth = root_ref.path.len();
    let mut root = rollup::Tree {
        root: root_ref,
        value: root_cell,
        children: Vec::new(),
    };

    // Since we can't keep simultaneous `&mut` pointers into the tree in
    // Rust without unsafe, the classic workaround is "insert by path": for
    // each incoming node, walk from the root down through `tree.children`
    // matching each prefix segment, creating missing intermediates.
    for (mref, cell) in descendants {
        insert_by_path(&mut root, root_depth, &mref, cell);
    }

    root
}

/// Insert `(mref, cell)` into `root`'s subtree, walking the prefix from
/// depth `root_depth + 1`. Creates intermediate placeholder nodes if the
/// pre-order traversal skipped levels (the catalogue always produces
/// full paths, so this is defensive).
fn insert_by_path(root: &mut rollup::Tree, root_depth: usize, mref: &MemberRef, cell: Cell) {
    let segments: Vec<&Name> = mref.path.segments().collect();
    let mut cursor = root;
    // Skip the root_depth segments (they lead to the root itself).
    for i in root_depth..segments.len() {
        let seg = segments[i];
        // Find or create a child matching this segment.
        let existing = cursor
            .children
            .iter()
            .position(|c| c.root.path.segments().nth(i) == Some(seg));
        match existing {
            Some(idx) => {
                cursor = &mut cursor.children[idx];
            }
            None => {
                // Build the intermediate's path by truncating the input
                // path to `i + 1` segments.
                let path_segments: Vec<Name> =
                    segments.iter().take(i + 1).copied().cloned().collect();
                let path =
                    tatami::query::Path::parse(path_segments).expect("non-empty truncated path");
                let partial_mref = MemberRef::new(mref.dim.clone(), mref.hierarchy.clone(), path);
                let is_terminal = i == segments.len() - 1;
                cursor.children.push(rollup::Tree {
                    root: partial_mref,
                    // Terminal → the incoming cell; intermediate → Missing,
                    // because we didn't receive a cell for this path.
                    value: if is_terminal {
                        cell.clone()
                    } else {
                        Cell::Missing {
                            reason: tatami::missing::Reason::NoFacts,
                        }
                    },
                    children: Vec::new(),
                });
                let last = cursor.children.len() - 1;
                cursor = &mut cursor.children[last];
            }
        }
    }
    // If we walked to an existing node (the exact path was already present
    // as an intermediate), overwrite its value with the real cell.
    cursor.value = cell;
}

/// Apply `options.non_empty`, `options.order`, `options.limit` to a series.
/// Non-empty drops rows where every metric's value is `Missing`; order
/// sorts x by the first OrderBy's metric's values; limit truncates x.
fn apply_options_series(
    mut x: Vec<MemberRef>,
    mut rows: Vec<series::Row>,
    resolved: &ResolvedQuery<'_>,
) -> (Vec<MemberRef>, Vec<series::Row>) {
    let opts = &resolved.options;

    // Non-empty: drop x entries where every row has a Missing cell at that
    // index. `Error` cells are informative and kept.
    if opts.non_empty {
        let keep: Vec<bool> = (0..x.len())
            .map(|i| {
                rows.iter()
                    .any(|r| !matches!(r.values[i], Cell::Missing { .. }))
            })
            .collect();
        x = filter_by_mask(x, &keep);
        for r in rows.iter_mut() {
            r.values = filter_by_mask(std::mem::take(&mut r.values), &keep);
        }
    }

    // Order: sort x and every row's values in tandem by the first ordering
    // metric's row values, if it matches one of the requested metrics.
    if let Some(ob) = opts.order.first()
        && let Some(metric_idx) = resolved
            .metrics
            .iter()
            .position(|h| metric_name(h) == &ob.metric)
    {
        let mut indices: Vec<usize> = (0..x.len()).collect();
        indices.sort_by(|&a, &b| {
            let va = cell_score(&rows[metric_idx].values[a]);
            let vb = cell_score(&rows[metric_idx].values[b]);
            let base = va.partial_cmp(&vb).unwrap_or(Ordering::Equal);
            match ob.direction {
                tatami::query::Direction::Asc => base,
                tatami::query::Direction::Desc => base.reverse(),
                _ => base,
            }
        });
        x = permute(x, &indices);
        for r in rows.iter_mut() {
            r.values = permute(std::mem::take(&mut r.values), &indices);
        }
    }

    // Limit: truncate x and each row's values.
    if let Some(n) = opts.limit {
        let cap = n.get().min(x.len());
        x.truncate(cap);
        for r in rows.iter_mut() {
            r.values.truncate(cap);
        }
    }

    (x, rows)
}

/// Apply `options.non_empty`, `options.order`, `options.limit` to a pivot.
/// Sorting and limit act on rows. Non-empty drops rows where every cell is
/// `Missing`.
fn apply_options_pivot(
    mut row_headers: Vec<Tuple>,
    mut cells: Vec<Vec<Cell>>,
    resolved: &ResolvedQuery<'_>,
) -> (Vec<Tuple>, Vec<Vec<Cell>>) {
    let opts = &resolved.options;

    if opts.non_empty {
        let keep: Vec<bool> = cells
            .iter()
            .map(|row| row.iter().any(|c| !matches!(c, Cell::Missing { .. })))
            .collect();
        row_headers = filter_by_mask(row_headers, &keep);
        cells = filter_by_mask(cells, &keep);
    }

    if let Some(ob) = opts.order.first()
        && let Some(metric_idx) = resolved
            .metrics
            .iter()
            .position(|h| metric_name(h) == &ob.metric)
    {
        // Sort by the first column's value of the chosen metric.
        // `cells[r][0 * n_metrics + metric_idx] == cells[r][metric_idx]`
        // — metric_idx slots in directly because column 0 lies at byte 0
        // of the metric-widened row.
        let mut indices: Vec<usize> = (0..row_headers.len()).collect();
        indices.sort_by(|&a, &b| {
            let va = cell_score(cells[a].get(metric_idx).unwrap_or(&missing_cell()));
            let vb = cell_score(cells[b].get(metric_idx).unwrap_or(&missing_cell()));
            let base = va.partial_cmp(&vb).unwrap_or(Ordering::Equal);
            match ob.direction {
                tatami::query::Direction::Asc => base,
                tatami::query::Direction::Desc => base.reverse(),
                _ => base,
            }
        });
        row_headers = permute(row_headers, &indices);
        cells = permute(cells, &indices);
    }

    if let Some(n) = opts.limit {
        let cap = n.get().min(row_headers.len());
        row_headers.truncate(cap);
        cells.truncate(cap);
    }

    (row_headers, cells)
}

/// Score a cell for ordering. `Valid` returns its value; `Missing` /
/// `Error` score `f64::NEG_INFINITY` so they sink to the bottom on a
/// descending sort and to the top on ascending — a deterministic choice
/// documented here.
fn cell_score(cell: &Cell) -> f64 {
    match cell {
        Cell::Valid { value, .. } => *value,
        _ => f64::NEG_INFINITY,
    }
}

fn missing_cell() -> Cell {
    Cell::Missing {
        reason: tatami::missing::Reason::NoFacts,
    }
}

/// Keep entries of `v` whose mask is `true`; drop the rest.
fn filter_by_mask<T>(v: Vec<T>, mask: &[bool]) -> Vec<T> {
    v.into_iter()
        .zip(mask.iter().copied())
        .filter_map(|(item, keep)| if keep { Some(item) } else { None })
        .collect()
}

/// Reorder `v` by the given index permutation.
fn permute<T: Clone>(v: Vec<T>, indices: &[usize]) -> Vec<T> {
    indices.iter().map(|&i| v[i].clone()).collect()
}

/// Evaluate every metric at a single tuple — the per-cell engine for the
/// `Scalar` variant and the inner loops of `Series` / `Pivot`.
fn metrics_at<'s>(
    metrics: &[MetricHandle<'s>],
    tuple: &ResolvedTuple<'s>,
    cube: &'s InMemoryCube,
) -> Result<Vec<Cell>, Error> {
    metrics
        .iter()
        .map(|h| evaluate_metric(h, tuple, cube))
        .collect()
}

/// Evaluate a single metric handle at a tuple.
///
/// Measures and derived metrics both resolve by name through
/// [`evaluate_expr`] — measures land on `Expr::Ref` that the metric
/// evaluator dispatches to `evaluate_measure`, derived metrics walk
/// their stored expression tree.
fn evaluate_metric<'s>(
    handle: &MetricHandle<'s>,
    tuple: &ResolvedTuple<'s>,
    cube: &'s InMemoryCube,
) -> Result<Cell, Error> {
    let name = metric_name(handle);
    evaluate_expr(&Expr::Ref { name: name.clone() }, tuple, cube)
}

/// The underlying name of a [`MetricHandle`] — either the measure's or
/// the metric's declared [`Name`].
fn metric_name<'h, 's>(handle: &'h MetricHandle<'s>) -> &'h Name {
    match handle {
        MetricHandle::Measure(m) => &m.name,
        MetricHandle::Metric(Metric { name, .. }) => name,
    }
}

/// Concatenate two tuples' members, overriding `base` members whose dim
/// appears in `overlay`. Axis dim-disjointness (from resolve) makes this
/// an honest merge when the caller passes disjoint tuples.
fn merge<'s>(base: &ResolvedTuple<'s>, overlay: &ResolvedTuple<'s>) -> ResolvedTuple<'s> {
    let mut members: Vec<ResolvedMember<'s>> = base
        .members
        .iter()
        .filter(|m| {
            !overlay
                .members
                .iter()
                .any(|o| o.dim.dim.name == m.dim.dim.name)
        })
        .cloned()
        .collect();
    members.extend(overlay.members.iter().cloned());
    ResolvedTuple::from_members(members)
}

/// Project a [`ResolvedTuple`] to a public [`Tuple`] for embedding in
/// result shapes.
fn tuple_of(resolved: &ResolvedTuple<'_>) -> Tuple {
    let members: Vec<MemberRef> = resolved.members.iter().map(member_ref_of).collect();
    // Resolve-time dim-distinctness guarantees `Tuple::of` succeeds; the
    // defensive fallback to `Tuple::empty` keeps the function total.
    Tuple::of(members).unwrap_or_else(|_| Tuple::empty())
}

/// Project a [`ResolvedMember`] to a public [`MemberRef`].
fn member_ref_of(member: &ResolvedMember<'_>) -> MemberRef {
    MemberRef::new(
        member.dim.dim.name.clone(),
        member.hierarchy.hierarchy.name.clone(),
        member.path.clone(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars_core::df;
    use polars_core::prelude::DataFrame;
    use std::num::NonZeroUsize;
    use tatami::query::{Axes, Direction, MemberRef as MRef, OrderBy, Path, Predicate, Set, Tuple};
    use tatami::schema::{
        Aggregation, Calendar, Dimension, Hierarchy, Level, Measure, Metric, Schema, metric::Expr,
    };
    use tatami::{Cube, Query};

    use crate::InMemoryCube;

    fn n(s: &str) -> Name {
        Name::parse(s).expect("valid name")
    }

    fn mr(dim: &str, hier: &str, segs: &[&str]) -> MRef {
        let names: Vec<Name> = segs.iter().copied().map(n).collect();
        MRef::new(n(dim), n(hier), Path::parse(names).expect("non-empty"))
    }

    /// Two-hierarchy cube: Geography (Region → Country) + Scenario + amount.
    fn fixture_cube() -> InMemoryCube {
        let schema =
            Schema::builder()
                .dimension(
                    Dimension::regular(n("Geography")).hierarchy(
                        Hierarchy::new(n("Default"))
                            .level(Level::new(n("Region"), n("region")))
                            .level(Level::new(n("Country"), n("country"))),
                    ),
                )
                .dimension(Dimension::scenario(n("Scenario")).hierarchy(
                    Hierarchy::new(n("Default")).level(Level::new(n("Plan"), n("plan"))),
                ))
                .measure(Measure::new(n("amount"), Aggregation::sum()))
                .measure(Measure::new(n("units"), Aggregation::sum()))
                .build()
                .expect("schema");
        let df: DataFrame = df! {
            "region"  => ["EMEA", "EMEA", "APAC", "APAC"],
            "country" => ["UK",   "FR",   "JP",   "SG"],
            "plan"    => ["Actual", "Actual", "Actual", "Actual"],
            "amount"  => [100.0_f64, 200.0, 300.0, 400.0],
            "units"   => [1.0_f64, 2.0, 3.0, 4.0],
        }
        .expect("frame");
        InMemoryCube::new(df, schema).expect("cube")
    }

    /// Build a runtime and block on `cube.query(&q)`.
    fn run_query(cube: &InMemoryCube, q: Query) -> Results {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        rt.block_on(cube.query(&q)).expect("query ok")
    }

    #[test]
    fn scalar_query_returns_scalar_result_with_one_cell_per_metric() {
        let cube = fixture_cube();
        let q = Query {
            axes: Axes::Scalar,
            slicer: Tuple::single(mr("Scenario", "Default", &["Actual"])),
            metrics: vec![n("amount"), n("units")],
            options: tatami::query::Options::default(),
        };
        let r = run_query(&cube, q);
        match r {
            Results::Scalar(s) => {
                assert_eq!(s.values().len(), 2, "one cell per metric");
                match &s.values()[0] {
                    Cell::Valid { value, .. } => assert_eq!(*value, 1000.0),
                    other => panic!("amount: expected Valid, got {other:?}"),
                }
                match &s.values()[1] {
                    Cell::Valid { value, .. } => assert_eq!(*value, 10.0),
                    other => panic!("units: expected Valid, got {other:?}"),
                }
            }
            other => panic!("expected Scalar, got {other:?}"),
        }
    }

    #[test]
    fn series_query_returns_series_result() {
        let cube = fixture_cube();
        let q = Query {
            axes: Axes::Series {
                rows: Set::members(n("Geography"), n("Default"), n("Region")),
            },
            slicer: Tuple::single(mr("Scenario", "Default", &["Actual"])),
            metrics: vec![n("amount")],
            options: tatami::query::Options::default(),
        };
        let r = run_query(&cube, q);
        match r {
            Results::Series(s) => {
                assert_eq!(s.x().len(), 2, "two regions");
                assert_eq!(s.rows().len(), 1, "one metric");
                assert_eq!(s.rows()[0].values.len(), 2, "one value per x entry");
            }
            other => panic!("expected Series, got {other:?}"),
        }
    }

    #[test]
    fn pivot_query_returns_pivot_result() {
        let cube = fixture_cube();
        let q = Query {
            axes: Axes::Pivot {
                rows: Set::members(n("Geography"), n("Default"), n("Region")),
                columns: Set::members(n("Scenario"), n("Default"), n("Plan")),
            },
            slicer: Tuple::empty(),
            metrics: vec![n("amount")],
            options: tatami::query::Options::default(),
        };
        let r = run_query(&cube, q);
        match r {
            Results::Pivot(p) => {
                assert_eq!(p.row_headers().len(), 2);
                assert_eq!(p.col_headers().len(), 1);
                assert_eq!(p.cells().len(), 2);
                assert_eq!(p.cells()[0].len(), 1);
            }
            other => panic!("expected Pivot, got {other:?}"),
        }
    }

    #[test]
    fn pivot_with_descendants_of_single_member_returns_rollup() {
        // 3-level geography so Descendants(World, to=Country) spans
        // Region + Country — enough depth for a real tree. Region-level
        // source with to_level=Country would only produce the leaves, no
        // intermediate structure.
        let schema = Schema::builder()
            .dimension(
                Dimension::regular(n("Geography")).hierarchy(
                    Hierarchy::new(n("Default"))
                        .level(Level::new(n("World"), n("world")))
                        .level(Level::new(n("Region"), n("region")))
                        .level(Level::new(n("Country"), n("country"))),
                ),
            )
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .build()
            .expect("schema");
        let df: DataFrame = df! {
            "world"   => ["World", "World", "World"],
            "region"  => ["EMEA",  "EMEA",  "APAC"],
            "country" => ["UK",    "FR",    "JP"],
            "amount"  => [100.0_f64, 200.0, 300.0],
        }
        .expect("frame");
        let cube = InMemoryCube::new(df, schema).expect("cube");
        // §3.3 rule: Pivot with `rows = Descendants` of a single-rooted set
        // → Results::Rollup. The columns axis is ignored by the rollup
        // branch.
        let q = Query {
            axes: Axes::Pivot {
                rows: Set::explicit(vec![mr("Geography", "Default", &["World"])])
                    .expect("non-empty")
                    .descendants_to(n("Country")),
                columns: Set::members(n("Geography"), n("Default"), n("World")),
            },
            slicer: Tuple::empty(),
            metrics: vec![n("amount")],
            options: tatami::query::Options::default(),
        };
        let r = run_query(&cube, q);
        match r {
            Results::Rollup(tree) => {
                // Root = the synthesized World member at depth 1; two
                // regions sit as direct children (APAC, EMEA — sorted
                // pre-order by the catalogue), each with its country
                // children.
                assert_eq!(
                    tree.root.path.segments().collect::<Vec<_>>().len(),
                    1,
                    "root is the synthesized ancestor, not a descendant leaf"
                );
                assert_eq!(
                    tree.children.len(),
                    2,
                    "two regions hang off the World root, got {tree:?}"
                );
            }
            other => panic!("expected Rollup, got {other:?}"),
        }
    }

    #[test]
    fn pivot_with_descendants_of_range_returns_pivot_not_rollup() {
        // Multi-root `Set::range(Y1..Y2).descendants_to(Quarter)`: the
        // flat output has multiple top-level path[0]s (one per year) and
        // therefore can't fit a single-rooted `rollup::Tree`. Expect a
        // flat Pivot with one row per (year, quarter) tuple. Today's
        // single-rooted assembler would collapse every non-first-year
        // quarter under the first year's subtree — the silent-wrong-answer
        // shape (MAP §8 R3).
        let schema = Schema::builder()
            .dimension(
                Dimension::time(
                    n("Time"),
                    vec![tatami::schema::Calendar::gregorian(n("Gregorian"))],
                )
                .hierarchy(
                    Hierarchy::new(n("Fiscal"))
                        .level(Level::new(n("Year"), n("year")))
                        .level(Level::new(n("Quarter"), n("quarter"))),
                ),
            )
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .build()
            .expect("schema");
        let df: DataFrame = df! {
            "year"    => ["FY2025", "FY2025", "FY2026", "FY2026"],
            "quarter" => ["Q1",     "Q2",     "Q1",     "Q2"],
            "amount"  => [10.0_f64, 20.0,     30.0,     40.0],
        }
        .expect("frame");
        let cube = InMemoryCube::new(df, schema).expect("cube");
        let q = Query {
            axes: Axes::Pivot {
                rows: Set::range(
                    n("Time"),
                    n("Fiscal"),
                    mr("Time", "Fiscal", &["FY2025"]),
                    mr("Time", "Fiscal", &["FY2026"]),
                )
                .descendants_to(n("Quarter")),
                columns: Set::members(n("Time"), n("Fiscal"), n("Year")),
            },
            slicer: Tuple::empty(),
            metrics: vec![n("amount")],
            options: tatami::query::Options::default(),
        };
        let r = run_query(&cube, q);
        match r {
            Results::Pivot(p) => {
                // Two years × two quarters each = four row tuples.
                assert_eq!(
                    p.row_headers().len(),
                    4,
                    "multi-root descendants produce one row per (year, quarter)"
                );
            }
            other => panic!(
                "expected Pivot for multi-root Descendants, got {other:?} (rollup would collapse cross-year quarters)"
            ),
        }
    }

    #[test]
    fn pivot_with_multiple_metrics_widens_col_headers() {
        let cube = fixture_cube();
        // Two metrics + one column tuple → `col_headers.len() == 2` and
        // each row has two cells wide. Scenario on columns (instead of
        // Geography again) to keep the rows/cols dims distinct.
        let q = Query {
            axes: Axes::Pivot {
                rows: Set::members(n("Geography"), n("Default"), n("Region")),
                columns: Set::members(n("Scenario"), n("Default"), n("Plan")),
            },
            slicer: Tuple::empty(),
            metrics: vec![n("amount"), n("units")],
            options: tatami::query::Options::default(),
        };
        let r = run_query(&cube, q);
        match r {
            Results::Pivot(p) => {
                // 1 column × 2 metrics = 2 col_headers; cells[r] width matches.
                assert_eq!(p.col_headers().len(), 2, "1 col × 2 metrics");
                assert_eq!(p.cells()[0].len(), 2);
            }
            other => panic!("expected Pivot, got {other:?}"),
        }
    }

    #[test]
    fn non_empty_drops_rows_where_every_cell_is_missing() {
        // Fixture shaped so EMEA has no matching fact row for the slicer's
        // Time coordinate — its cells will be Missing. APAC has a matching
        // row and survives. `non_empty = true` must drop EMEA.
        let schema2 = Schema::builder()
            .dimension(
                Dimension::regular(n("Geography")).hierarchy(
                    Hierarchy::new(n("Default"))
                        .level(Level::new(n("Region"), n("region")))
                        .level(Level::new(n("Country"), n("country"))),
                ),
            )
            .dimension(
                Dimension::time(n("Time"), vec![Calendar::gregorian(n("Gregorian"))]).hierarchy(
                    Hierarchy::new(n("Default")).level(Level::new(n("Month"), n("month"))),
                ),
            )
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .build()
            .expect("schema");
        // APAC has a Jan fact; EMEA has a Feb fact; slicer pins Jan →
        // EMEA row all-Missing, APAC row Valid.
        let df2: DataFrame = df! {
            "region"  => ["EMEA", "APAC"],
            "country" => ["UK",   "JP"],
            "month"   => ["2026-02", "2026-01"],
            "amount"  => [10.0_f64, 20.0],
        }
        .expect("frame");
        let cube2 = InMemoryCube::new(df2, schema2).expect("cube");
        let q = Query {
            axes: Axes::Series {
                rows: Set::members(n("Geography"), n("Default"), n("Region")),
            },
            slicer: Tuple::single(mr("Time", "Default", &["2026-01"])),
            metrics: vec![n("amount")],
            options: tatami::query::Options {
                non_empty: true,
                ..tatami::query::Options::default()
            },
        };
        let r = run_query(&cube2, q);
        match r {
            Results::Series(s) => {
                // Only APAC survives — EMEA's cell was Missing (no
                // Jan row).
                assert_eq!(s.x().len(), 1, "EMEA dropped by non_empty");
            }
            other => panic!("expected Series, got {other:?}"),
        }
    }

    #[test]
    fn order_by_metric_desc_sorts_rows() {
        let cube = fixture_cube();
        let q = Query {
            axes: Axes::Series {
                rows: Set::members(n("Geography"), n("Default"), n("Country")),
            },
            slicer: Tuple::single(mr("Scenario", "Default", &["Actual"])),
            metrics: vec![n("amount")],
            options: tatami::query::Options {
                order: vec![OrderBy {
                    metric: n("amount"),
                    direction: Direction::Desc,
                }],
                ..tatami::query::Options::default()
            },
        };
        let r = run_query(&cube, q);
        match r {
            Results::Series(s) => {
                // Country-level amounts: SG=400, JP=300, FR=200, UK=100.
                // Desc order = SG, JP, FR, UK.
                let leaves: Vec<&str> = s
                    .x()
                    .iter()
                    .map(|m| m.path.segments().last().expect("non-empty").as_str())
                    .collect();
                assert_eq!(leaves, vec!["SG", "JP", "FR", "UK"]);
            }
            other => panic!("expected Series, got {other:?}"),
        }
    }

    #[test]
    fn limit_truncates_rows_post_order() {
        let cube = fixture_cube();
        let q = Query {
            axes: Axes::Series {
                rows: Set::members(n("Geography"), n("Default"), n("Country")),
            },
            slicer: Tuple::single(mr("Scenario", "Default", &["Actual"])),
            metrics: vec![n("amount")],
            options: tatami::query::Options {
                order: vec![OrderBy {
                    metric: n("amount"),
                    direction: Direction::Desc,
                }],
                limit: Some(NonZeroUsize::new(2).expect("nonzero")),
                ..tatami::query::Options::default()
            },
        };
        let r = run_query(&cube, q);
        match r {
            Results::Series(s) => {
                assert_eq!(s.x().len(), 2);
                let leaves: Vec<&str> = s
                    .x()
                    .iter()
                    .map(|m| m.path.segments().last().expect("non-empty").as_str())
                    .collect();
                assert_eq!(leaves, vec!["SG", "JP"], "top-2 by amount desc");
            }
            other => panic!("expected Series, got {other:?}"),
        }
    }

    /// Minimal hewton-shaped schema and one fact row, so end-to-end
    /// `Cube::query(Scalar)` against a Revenue metric (= `Expr::Ref` to
    /// the `amount` measure) returns a non-zero value.
    #[test]
    fn hewton_shaped_scalar_query_returns_non_zero_revenue() {
        let schema = Schema::builder()
            .dimension(Dimension::regular(n("Geography")).hierarchy(
                Hierarchy::new(n("Default")).level(Level::new(n("Region"), n("region"))),
            ))
            .dimension(
                Dimension::scenario(n("Scenario")).hierarchy(
                    Hierarchy::new(n("Default")).level(Level::new(n("Plan"), n("plan"))),
                ),
            )
            .dimension(
                Dimension::time(n("Time"), vec![Calendar::gregorian(n("Gregorian"))]).hierarchy(
                    Hierarchy::new(n("Fiscal")).level(Level::new(n("FiscalYear"), n("fy"))),
                ),
            )
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .metric(Metric::new(n("Revenue"), Expr::Ref { name: n("amount") }))
            .build()
            .expect("schema");
        let df: DataFrame = df! {
            "region" => ["NA"],
            "plan"   => ["Actual"],
            "fy"     => ["FY2026"],
            "amount" => [1_000_000.0_f64],
        }
        .expect("frame");
        let cube = InMemoryCube::new(df, schema).expect("cube");
        let q = Query {
            axes: Axes::Scalar,
            slicer: Tuple::of([
                mr("Time", "Fiscal", &["FY2026"]),
                MRef::scenario(n("Actual")),
            ])
            .expect("disjoint"),
            metrics: vec![n("Revenue")],
            options: tatami::query::Options::default(),
        };
        let r = run_query(&cube, q);
        match r {
            Results::Scalar(s) => {
                assert_eq!(s.values().len(), 1);
                match &s.values()[0] {
                    Cell::Valid { value, .. } => {
                        assert!(*value > 0.0, "hewton revenue must be positive");
                        assert_eq!(*value, 1_000_000.0);
                    }
                    other => panic!("expected Valid, got {other:?}"),
                }
            }
            other => panic!("expected Scalar, got {other:?}"),
        }
    }

    #[test]
    fn filter_with_in_predicate_keeps_matching_path_prefix() {
        // Use Filter on a Country-level set with In on Geography prefix
        // = [EMEA]. EMEA's countries (FR, UK) survive; APAC's don't.
        let cube = fixture_cube();
        let q = Query {
            axes: Axes::Series {
                rows: Set::members(n("Geography"), n("Default"), n("Country")).filter(
                    Predicate::In {
                        dim: n("Geography"),
                        path_prefix: Path::of(n("EMEA")),
                    },
                ),
            },
            slicer: Tuple::empty(),
            metrics: vec![n("amount")],
            options: tatami::query::Options::default(),
        };
        let r = run_query(&cube, q);
        match r {
            Results::Series(s) => {
                let leaves: Vec<&str> = s
                    .x()
                    .iter()
                    .map(|m| m.path.segments().last().expect("non-empty").as_str())
                    .collect();
                assert_eq!(leaves, vec!["FR", "UK"]);
            }
            other => panic!("expected Series, got {other:?}"),
        }
    }
}

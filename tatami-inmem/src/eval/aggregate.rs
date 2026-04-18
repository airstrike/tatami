//! Measure aggregation — Phase 5e of MAP_PLAN.md §5.
//!
//! [`evaluate_measure`] computes a single [`Cell`] value for a
//! [`Measure`] at a [`ResolvedTuple`] context. It is the primitive that
//! Phase 5f's metric-expression evaluator sits on: a metric's `Ref`
//! leaf resolves to a measure, which this function evaluates against the
//! fact frame filtered by the current tuple.
//!
//! ## Semi-additive rollup
//!
//! The load-bearing case, and the one MAP §8 R3 flags as
//! **medium-likelihood / high-impact** (silent wrong answers). If a
//! measure's aggregation is [`Aggregation::SemiAdditive`] and the tuple
//! does *not* bind every non-additive dim, the evaluator:
//!
//! 1. Groups the filtered fact frame by the unbound non-additive dims'
//!    leaf-level keys.
//! 2. Sums the measure column within each group (the Pigment / Anaplan
//!    "additive across all other dims" convention).
//! 3. Folds the per-group sums via the measure's [`SemiAgg`] — `First` /
//!    `Last` require an ordered traversal of the group keys; the rest
//!    are order-independent.
//!
//! If every non-additive dim *is* bound by the tuple, the rollup has a
//! single group and reduces to plain `Sum` — no distinction is needed.
//!
//! The skewed-stock test in this file (`semi_additive_stock_rolls_up_as_last_not_sum`)
//! guards against the whole class of silent-wrong-answer bugs.
//!
//! [`Aggregation::SemiAdditive`]: tatami::schema::Aggregation::SemiAdditive
//! [`Cell`]: tatami::Cell
//! [`Measure`]: tatami::schema::Measure
//! [`ResolvedTuple`]: crate::resolve::ResolvedTuple
//! [`SemiAgg`]: tatami::schema::SemiAgg
#![allow(dead_code)]

use std::collections::BTreeMap;

use polars_core::prelude::{AnyValue, DataFrame, Series};
use tatami::schema::{Aggregation, Measure, Name, Schema, SemiAgg};
use tatami::{Cell, missing};

use crate::Error;
use crate::eval::tuple::filter_by_tuple;
use crate::resolve::ResolvedTuple;

/// Evaluate `measure` at the `tuple` context against the fact frame `df`.
///
/// Returns:
/// - [`Cell::Valid`] on success — `value` is the aggregate, `unit` is the
///   measure's declared unit (no format hint: measures carry units; only
///   metrics carry formats).
/// - [`Cell::Missing`] with [`missing::Reason::NoFacts`] when the filtered
///   frame is empty (no rows match the tuple).
/// - [`Cell::Error`] for arithmetic failures polars surfaces as a scalar
///   null (e.g., all-null measure column, divide-by-zero in a mean).
///
/// Returns [`Error`] only for eval-layer invariant violations — a missing
/// measure column (Phase 5a rules this out structurally) or a polars
/// runtime filter / aggregation failure.
pub(crate) fn evaluate_measure(
    measure: &Measure,
    tuple: &ResolvedTuple<'_>,
    df: &DataFrame,
    schema: &Schema,
) -> Result<Cell, Error> {
    let filtered = filter_by_tuple(tuple, df)?;
    if filtered.height() == 0 {
        return Ok(Cell::Missing {
            reason: missing::Reason::NoFacts,
        });
    }

    match &measure.aggregation {
        Aggregation::Sum
        | Aggregation::Avg
        | Aggregation::Min
        | Aggregation::Max
        | Aggregation::Count
        | Aggregation::DistinctCount => additive(&filtered, measure),

        Aggregation::SemiAdditive {
            non_additive_dims,
            over,
        } => semi_additive(&filtered, measure, tuple, schema, non_additive_dims, *over),

        // `Aggregation` is `#[non_exhaustive]`; surface future variants as
        // `EvalAggregateFailed` rather than panicking.
        other => Err(Error::EvalAggregateFailed {
            measure: measure.name.clone(),
            reason: format!("unsupported aggregation variant: {other:?}"),
        }),
    }
}

/// Additive aggregations — `Sum | Avg | Min | Max | Count | DistinctCount`.
fn additive(filtered: &DataFrame, measure: &Measure) -> Result<Cell, Error> {
    match &measure.aggregation {
        Aggregation::Count => {
            // Row count ignores the measure column's content but still
            // requires the column exist (Phase 5a guarantees it).
            Ok(valid(filtered.height() as f64, measure))
        }
        Aggregation::Sum => reduce_with(filtered, measure, |s| s.sum_reduce().map(extract_f64)),
        Aggregation::Avg => reduce_with(filtered, measure, |s| Ok(extract_f64(s.mean_reduce()))),
        Aggregation::Min => reduce_with(filtered, measure, |s| s.min_reduce().map(extract_f64)),
        Aggregation::Max => reduce_with(filtered, measure, |s| s.max_reduce().map(extract_f64)),
        Aggregation::DistinctCount => {
            let series = measure_series(filtered, measure)?;
            // `Series::n_unique` is only implemented for numeric dtypes in
            // default-features polars-core; iterate row-by-row and dedup
            // via a `BTreeSet` keyed by the stringified cell so the path
            // works uniformly for Integer and String measure columns.
            let mut distinct: std::collections::BTreeSet<String> =
                std::collections::BTreeSet::new();
            for row in 0..series.len() {
                let av = series.get(row).map_err(|e| Error::EvalAggregateFailed {
                    measure: measure.name.clone(),
                    reason: e.to_string(),
                })?;
                if av.is_null() {
                    continue;
                }
                distinct.insert(stringify(&av));
            }
            Ok(valid(distinct.len() as f64, measure))
        }
        // Only callable from the matching arm above; any other variant is
        // a routing bug.
        other => Err(Error::EvalAggregateFailed {
            measure: measure.name.clone(),
            reason: format!("additive path hit non-additive variant: {other:?}"),
        }),
    }
}

/// Semi-additive rollup — see module docs.
fn semi_additive(
    filtered: &DataFrame,
    measure: &Measure,
    tuple: &ResolvedTuple<'_>,
    schema: &Schema,
    non_additive_dims: &[Name],
    over: SemiAgg,
) -> Result<Cell, Error> {
    // Which non-additive dims does the tuple already bind? If the tuple
    // pins every one of them, the rollup collapses to a single group and
    // we just sum the measure column.
    let bound_dims: Vec<&Name> = tuple.members.iter().map(|m| &m.dim.dim.name).collect();
    let unbound: Vec<&Name> = non_additive_dims
        .iter()
        .filter(|d| !bound_dims.contains(d))
        .collect();

    if unbound.is_empty() {
        return reduce_with(filtered, measure, |s| s.sum_reduce().map(extract_f64));
    }

    // For each unbound non-additive dim, we group by the dim's leaf-level
    // key column — consistent choice of "the last level of the first
    // hierarchy" per the brief.
    let group_keys: Vec<String> = unbound
        .iter()
        .map(|dim_name| dim_leaf(schema, dim_name))
        .collect::<Result<Vec<_>, _>>()?;

    // Compute per-group sum of the measure column, indexed by the tuple
    // of group-key values (stringified for ordering). BTreeMap gives us a
    // deterministic ascending traversal for `First` / `Last`.
    let measure_series = measure_series(filtered, measure)?;
    let key_serieses: Vec<&Series> = group_keys
        .iter()
        .map(|k| {
            filtered
                .column(k)
                .map(|c| c.as_materialized_series())
                .map_err(|_| Error::EvalColumnMissing { column: k.clone() })
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Row-by-row, accumulate into the group sums.
    let mut groups: BTreeMap<Vec<String>, f64> = BTreeMap::new();
    let height = filtered.height();
    for row in 0..height {
        let key: Vec<String> = key_serieses
            .iter()
            .map(|s| stringify(&s.get(row).unwrap_or(AnyValue::Null)))
            .collect();
        let value = measure_series
            .get(row)
            .ok()
            .as_ref()
            .and_then(numeric)
            .unwrap_or(0.0);
        *groups.entry(key).or_insert(0.0) += value;
    }

    if groups.is_empty() {
        return Ok(Cell::Missing {
            reason: missing::Reason::NoFacts,
        });
    }

    // Fold across the per-group sums per the `over` rule. BTreeMap iterates
    // in ascending key order; `First` takes the smallest key, `Last` the
    // greatest.
    let folded = match over {
        SemiAgg::First => groups
            .values()
            .next()
            .copied()
            .expect("groups non-empty (checked above)"),
        SemiAgg::Last => groups
            .values()
            .next_back()
            .copied()
            .expect("groups non-empty (checked above)"),
        SemiAgg::Min => groups.values().copied().fold(f64::INFINITY, f64::min),
        SemiAgg::Max => groups.values().copied().fold(f64::NEG_INFINITY, f64::max),
        SemiAgg::Avg => {
            let sum: f64 = groups.values().copied().sum();
            sum / groups.len() as f64
        }
        // `SemiAgg` is `#[non_exhaustive]`; reject unknown variants.
        other => {
            return Err(Error::EvalAggregateFailed {
                measure: measure.name.clone(),
                reason: format!("unsupported SemiAgg variant: {other:?}"),
            });
        }
    };

    if folded.is_nan() {
        return Ok(Cell::Error {
            message: format!(
                "semi-additive rollup of {} produced NaN",
                measure.name.as_str()
            ),
        });
    }
    Ok(valid(folded, measure))
}

/// The leaf-level key column name for `dim` (last level of the first
/// hierarchy). Used as the group-by column in the semi-additive rollup.
fn dim_leaf(schema: &Schema, dim_name: &Name) -> Result<String, Error> {
    let dim = schema
        .dimensions
        .iter()
        .find(|d| d.name == *dim_name)
        .ok_or_else(|| Error::EvalAggregateFailed {
            measure: dim_name.clone(),
            reason: format!(
                "non-additive dim {} is absent from schema",
                dim_name.as_str()
            ),
        })?;
    let hierarchy = dim
        .hierarchies
        .first()
        .ok_or_else(|| Error::EvalAggregateFailed {
            measure: dim_name.clone(),
            reason: format!("dim {} has no hierarchies", dim_name.as_str()),
        })?;
    let leaf = hierarchy
        .levels
        .last()
        .ok_or_else(|| Error::EvalAggregateFailed {
            measure: dim_name.clone(),
            reason: format!("dim {}'s first hierarchy has no levels", dim_name.as_str()),
        })?;
    Ok(leaf.key.as_str().to_owned())
}

/// Shared path for `Sum | Avg | Min | Max` — looks up the measure column
/// and applies a reducer that returns an `f64`, wrapping polars errors as
/// [`Error::EvalAggregateFailed`] and null scalars as [`Cell::Error`].
fn reduce_with<F>(filtered: &DataFrame, measure: &Measure, reducer: F) -> Result<Cell, Error>
where
    F: FnOnce(&Series) -> Result<Option<f64>, polars_core::error::PolarsError>,
{
    let series = measure_series(filtered, measure)?;
    let result = reducer(series).map_err(|e| Error::EvalAggregateFailed {
        measure: measure.name.clone(),
        reason: e.to_string(),
    })?;
    match result {
        Some(value) if value.is_nan() => Ok(Cell::Error {
            message: format!("aggregation of {} produced NaN", measure.name.as_str()),
        }),
        Some(value) => Ok(valid(value, measure)),
        None => Ok(Cell::Missing {
            reason: missing::Reason::NoFacts,
        }),
    }
}

/// Look up the measure's fact-frame series. Phase 5a guarantees the column
/// exists; the defensive branch surfaces `EvalColumnMissing` rather than
/// panicking.
fn measure_series<'a>(filtered: &'a DataFrame, measure: &Measure) -> Result<&'a Series, Error> {
    let column = filtered
        .column(measure.name.as_str())
        .map_err(|_| Error::EvalColumnMissing {
            column: measure.name.as_str().to_owned(),
        })?;
    Ok(column.as_materialized_series())
}

/// Build a `Cell::Valid` preserving the measure's unit. Measures do not
/// carry a format hint — only metrics do — so `format` is always `None`.
fn valid(value: f64, measure: &Measure) -> Cell {
    Cell::Valid {
        value,
        unit: measure.unit.clone(),
        format: None,
    }
}

/// Extract an `f64` from a polars [`polars_core::scalar::Scalar`].
///
/// `Scalar::value()` returns an `AnyValue<'static>`; numeric dtypes extract
/// to `f64` via `NumCast`. A null scalar (no rows, or all-null column)
/// returns `None` so the caller can translate it to [`Cell::Missing`].
fn extract_f64(scalar: polars_core::scalar::Scalar) -> Option<f64> {
    numeric(scalar.value())
}

/// Numeric projection of an [`AnyValue`] — `None` for null, `Some(f64)`
/// for any numeric dtype via `NumCast`.
fn numeric(av: &AnyValue<'_>) -> Option<f64> {
    if av.is_null() {
        return None;
    }
    av.extract::<f64>()
}

/// Stringify an [`AnyValue`] for use as a composite group key. The exact
/// rendering doesn't matter for grouping correctness; only that two cells
/// with the same underlying value produce the same string, and that the
/// ordering respects the natural ordering of the dim's leaf-level values
/// (which for string-typed keys like `"2026-01"` means lexicographic —
/// exactly what the skewed-stock test depends on).
fn stringify(av: &AnyValue<'_>) -> String {
    match av {
        AnyValue::Null => String::new(),
        AnyValue::String(s) => (*s).to_owned(),
        AnyValue::StringOwned(s) => s.to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars_core::df;
    use tatami::query::{Axes, MemberRef, Options, Path, Tuple};
    use tatami::schema::{Dimension, Hierarchy, Level, Schema};

    use crate::InMemoryCube;
    use crate::resolve::ResolvedAxes;

    fn n(s: &str) -> Name {
        Name::parse(s).expect("valid name")
    }

    fn mr(dim: &str, hier: &str, segs: &[&str]) -> MemberRef {
        let names: Vec<Name> = segs.iter().copied().map(n).collect();
        MemberRef::new(n(dim), n(hier), Path::parse(names).expect("non-empty"))
    }

    /// Resolve a slicer tuple against a cube and return it by value (the
    /// caller keeps the cube alive). Picks the first measure in the cube's
    /// schema as the resolved query's metric so the resolve step succeeds
    /// regardless of which fixture the caller built.
    fn slicer<'c>(cube: &'c InMemoryCube, t: Tuple) -> ResolvedTuple<'c> {
        let schema = pollster_schema(cube);
        let metric = schema
            .measures
            .first()
            .expect("fixture always declares at least one measure")
            .name
            .clone();
        let q = tatami::Query {
            axes: Axes::Scalar,
            slicer: t,
            metrics: vec![metric],
            options: Options::default(),
        };
        let rq = cube.resolve(&q).expect("resolve ok");
        let ResolvedAxes::Scalar = rq.axes else {
            panic!("scalar expected");
        };
        rq.slicer
    }

    /// Crate-internal side-door for tests: return the cube's schema via
    /// the async trait without depending on the async runtime. We rely on
    /// `InMemoryCube`'s schema field being visible to the crate (it is —
    /// `pub(crate) schema: Schema`).
    fn pollster_schema(cube: &InMemoryCube) -> &Schema {
        &cube.schema
    }

    // ── Semi-additive: the load-bearing test, written first (MAP §8 R3) ──

    /// Fixture for the semi-additive test: one Time dim (single level
    /// Month) and a `stock` measure declared non-additive over Time with
    /// `SemiAgg::Last`. Three monthly snapshots — 100, 120, 80 — and an
    /// unbound tuple. LAST-child rollup picks 80, not the additive sum
    /// (300) the naive path would produce.
    fn stock_fixture() -> (InMemoryCube, DataFrame, Schema, Measure) {
        let stock = Measure::new(
            n("stock"),
            Aggregation::semi_additive(vec![n("Time")], SemiAgg::Last).expect("non-empty"),
        );
        let schema =
            Schema::builder()
                .dimension(Dimension::time(n("Time"), Vec::new()).hierarchy(
                    Hierarchy::new(n("Default")).level(Level::new(n("Month"), n("month"))),
                ))
                .measure(stock.clone())
                .build()
                .expect("schema");
        let df = df! {
            "month" => ["2026-01", "2026-02", "2026-03"],
            "stock" => [100.0_f64, 120.0, 80.0],
        }
        .expect("frame");
        let cube = InMemoryCube::new(df.clone(), schema.clone()).expect("cube");
        (cube, df, schema, stock)
    }

    #[test]
    fn semi_additive_stock_rolls_up_as_last_not_sum() {
        // The critical path — see module-level docs. Naive additive would
        // produce 300.0. Semi-additive LAST picks the 2026-03 snapshot
        // (80). If this test ever goes green with 300, the rollup is
        // broken in exactly the way MAP §8 R3 warns about.
        let (cube, df, schema, stock) = stock_fixture();
        let tuple = slicer(&cube, Tuple::empty());

        let cell = evaluate_measure(&stock, &tuple, &df, &schema).expect("eval ok");

        match cell {
            Cell::Valid { value, .. } => {
                assert_eq!(value, 80.0, "semi-additive LAST must pick 2026-03, not sum");
            }
            other => panic!("expected Valid, got {other:?}"),
        }
    }

    #[test]
    fn semi_additive_stock_with_tuple_pinning_time_acts_additive() {
        // Tuple pins Time=2026-02 → no unbound non-additive dims → the
        // rollup reduces to the single-group sum of 120.
        let (cube, df, schema, stock) = stock_fixture();
        let tuple = slicer(&cube, Tuple::single(mr("Time", "Default", &["2026-02"])));
        let cell = evaluate_measure(&stock, &tuple, &df, &schema).expect("eval ok");
        match cell {
            Cell::Valid { value, .. } => assert_eq!(value, 120.0),
            other => panic!("expected Valid 120, got {other:?}"),
        }
    }

    #[test]
    fn semi_additive_over_first_picks_earliest_chronological() {
        let (cube, df, schema, _) = stock_fixture();
        // Swap `over` to First; same data → 2026-01 snapshot = 100.
        let stock_first = Measure::new(
            n("stock"),
            Aggregation::semi_additive(vec![n("Time")], SemiAgg::First).expect("non-empty"),
        );
        let tuple = slicer(&cube, Tuple::empty());
        let cell = evaluate_measure(&stock_first, &tuple, &df, &schema).expect("eval ok");
        match cell {
            Cell::Valid { value, .. } => assert_eq!(value, 100.0),
            other => panic!("expected Valid 100, got {other:?}"),
        }
    }

    #[test]
    fn semi_additive_over_avg_computes_mean_of_groups() {
        let (cube, df, schema, _) = stock_fixture();
        let stock_avg = Measure::new(
            n("stock"),
            Aggregation::semi_additive(vec![n("Time")], SemiAgg::Avg).expect("non-empty"),
        );
        let tuple = slicer(&cube, Tuple::empty());
        let cell = evaluate_measure(&stock_avg, &tuple, &df, &schema).expect("eval ok");
        match cell {
            Cell::Valid { value, .. } => assert_eq!(value, (100.0 + 120.0 + 80.0) / 3.0),
            other => panic!("expected Valid mean, got {other:?}"),
        }
    }

    #[test]
    fn semi_additive_across_multiple_non_additive_dims_groups_together() {
        // Two non-additive dims — Time (Month) and Scenario (Plan). Stock
        // is "additive across everything else"; the tuple leaves both dims
        // unbound, so the rollup groups by (month, plan) and takes LAST by
        // the composite key order.
        let stock = Measure::new(
            n("stock"),
            Aggregation::semi_additive(vec![n("Time"), n("Scenario")], SemiAgg::Last)
                .expect("non-empty"),
        );
        let schema =
            Schema::builder()
                .dimension(Dimension::time(n("Time"), Vec::new()).hierarchy(
                    Hierarchy::new(n("Default")).level(Level::new(n("Month"), n("month"))),
                ))
                .dimension(Dimension::regular(n("Scenario")).hierarchy(
                    Hierarchy::new(n("Default")).level(Level::new(n("Plan"), n("plan"))),
                ))
                .measure(stock.clone())
                .build()
                .expect("schema");
        let df = df! {
            "month" => ["2026-01", "2026-01", "2026-02", "2026-02"],
            "plan"  => ["Base",    "High",    "Base",    "High"],
            "stock" => [10.0_f64,  15.0,      20.0,      25.0],
        }
        .expect("frame");
        let cube = InMemoryCube::new(df.clone(), schema.clone()).expect("cube");
        let tuple = slicer(&cube, Tuple::empty());

        // BTreeMap order of (month, plan) string keys:
        //   ("2026-01", "Base"), ("2026-01", "High"),
        //   ("2026-02", "Base"), ("2026-02", "High")
        // LAST → ("2026-02", "High") = 25.
        let cell = evaluate_measure(&stock, &tuple, &df, &schema).expect("eval ok");
        match cell {
            Cell::Valid { value, .. } => assert_eq!(value, 25.0),
            other => panic!("expected Valid, got {other:?}"),
        }
    }

    // ── Additive aggregations ──────────────────────────────────────────

    /// Geography × Time fixture with integer and float measure columns.
    fn amount_fixture() -> (InMemoryCube, DataFrame, Schema, Measure) {
        let amount = Measure::new(n("amount"), Aggregation::sum());
        let schema =
            Schema::builder()
                .dimension(
                    Dimension::regular(n("Geography")).hierarchy(
                        Hierarchy::new(n("Default"))
                            .level(Level::new(n("Region"), n("region")))
                            .level(Level::new(n("Country"), n("country"))),
                    ),
                )
                .dimension(Dimension::time(n("Time"), Vec::new()).hierarchy(
                    Hierarchy::new(n("Default")).level(Level::new(n("Month"), n("month"))),
                ))
                .measure(amount.clone())
                .build()
                .expect("schema");
        let df = df! {
            "region"  => ["EMEA", "EMEA", "APAC"],
            "country" => ["UK",   "FR",   "JP"],
            "month"   => ["2026-01", "2026-02", "2026-01"],
            "amount"  => [100.0_f64, 200.0, 300.0],
        }
        .expect("frame");
        let cube = InMemoryCube::new(df.clone(), schema.clone()).expect("cube");
        (cube, df, schema, amount)
    }

    #[test]
    fn sum_over_empty_tuple_sums_all_rows() {
        let (cube, df, schema, amount) = amount_fixture();
        let tuple = slicer(&cube, Tuple::empty());
        let cell = evaluate_measure(&amount, &tuple, &df, &schema).expect("eval ok");
        match cell {
            Cell::Valid { value, .. } => assert_eq!(value, 600.0),
            other => panic!("expected 600, got {other:?}"),
        }
    }

    #[test]
    fn sum_over_single_dim_tuple_filters_then_sums() {
        let (cube, df, schema, amount) = amount_fixture();
        let tuple = slicer(&cube, Tuple::single(mr("Geography", "Default", &["EMEA"])));
        let cell = evaluate_measure(&amount, &tuple, &df, &schema).expect("eval ok");
        match cell {
            Cell::Valid { value, .. } => assert_eq!(value, 300.0),
            other => panic!("expected 300, got {other:?}"),
        }
    }

    #[test]
    fn avg_min_max_return_expected_values_over_fixture() {
        let (cube, df, schema, _) = amount_fixture();
        let tuple = slicer(&cube, Tuple::empty());

        let avg = Measure::new(n("amount"), Aggregation::avg());
        let min = Measure::new(n("amount"), Aggregation::min());
        let max = Measure::new(n("amount"), Aggregation::max());

        match evaluate_measure(&avg, &tuple, &df, &schema).expect("avg") {
            Cell::Valid { value, .. } => assert_eq!(value, 200.0),
            other => panic!("avg: {other:?}"),
        }
        match evaluate_measure(&min, &tuple, &df, &schema).expect("min") {
            Cell::Valid { value, .. } => assert_eq!(value, 100.0),
            other => panic!("min: {other:?}"),
        }
        match evaluate_measure(&max, &tuple, &df, &schema).expect("max") {
            Cell::Valid { value, .. } => assert_eq!(value, 300.0),
            other => panic!("max: {other:?}"),
        }
    }

    #[test]
    fn count_ignores_measure_column_returns_row_count() {
        let (cube, df, schema, _) = amount_fixture();
        let tuple = slicer(&cube, Tuple::empty());
        let count = Measure::new(n("amount"), Aggregation::count());
        match evaluate_measure(&count, &tuple, &df, &schema).expect("count") {
            Cell::Valid { value, .. } => assert_eq!(value, 3.0),
            other => panic!("count: {other:?}"),
        }
    }

    #[test]
    fn distinct_count_tolerates_string_and_int_columns() {
        // Build a fresh fixture with a string-valued measure column so the
        // distinct-count path exercises the string branch of `n_unique`.
        let dc = Measure::new(n("user_id"), Aggregation::distinct_count());
        let schema = Schema::builder()
            .dimension(Dimension::regular(n("Geography")).hierarchy(
                Hierarchy::new(n("Default")).level(Level::new(n("Region"), n("region"))),
            ))
            .measure(dc.clone())
            .build()
            .expect("schema");
        let df = df! {
            "region"  => ["EMEA", "EMEA", "APAC", "EMEA"],
            "user_id" => ["u1",   "u2",   "u1",   "u1"],
        }
        .expect("frame");
        let cube = InMemoryCube::new(df.clone(), schema.clone()).expect("cube");
        let tuple = slicer(&cube, Tuple::empty());
        match evaluate_measure(&dc, &tuple, &df, &schema).expect("distinct count") {
            Cell::Valid { value, .. } => assert_eq!(value, 2.0),
            other => panic!("distinct_count: {other:?}"),
        }
    }

    // ── Edge cases ─────────────────────────────────────────────────────

    #[test]
    fn empty_filtered_frame_returns_missing_no_facts() {
        // Pin Geography×Time to a combination with no matching rows →
        // filtered frame empty → Cell::Missing with NoFacts. Need a frame
        // that contains all referenced coordinates in the catalogue so
        // resolve doesn't reject the tuple up front.
        let amount = Measure::new(n("amount"), Aggregation::sum());
        let schema =
            Schema::builder()
                .dimension(
                    Dimension::regular(n("Geography")).hierarchy(
                        Hierarchy::new(n("Default"))
                            .level(Level::new(n("Region"), n("region")))
                            .level(Level::new(n("Country"), n("country"))),
                    ),
                )
                .dimension(Dimension::time(n("Time"), Vec::new()).hierarchy(
                    Hierarchy::new(n("Default")).level(Level::new(n("Month"), n("month"))),
                ))
                .measure(amount.clone())
                .build()
                .expect("schema");
        let df = df! {
            "region"  => ["EMEA", "EMEA", "APAC", "APAC"],
            "country" => ["UK",   "FR",   "JP",   "JP"],
            "month"   => ["2026-01", "2026-02", "2026-01", "2026-03"],
            "amount"  => [100.0_f64, 200.0, 300.0, 400.0],
        }
        .expect("frame");
        let cube = InMemoryCube::new(df.clone(), schema.clone()).expect("cube");
        // EMEA × 2026-03 — EMEA has no 2026-03 row, though 2026-03 exists
        // in the catalogue under APAC.
        let tuple = slicer(
            &cube,
            Tuple::of(vec![
                mr("Geography", "Default", &["EMEA"]),
                mr("Time", "Default", &["2026-03"]),
            ])
            .expect("disjoint dims"),
        );
        let cell = evaluate_measure(&amount, &tuple, &df, &schema).expect("eval ok");
        assert!(
            matches!(
                cell,
                Cell::Missing {
                    reason: missing::Reason::NoFacts
                }
            ),
            "expected Missing(NoFacts), got {cell:?}"
        );
    }

    #[test]
    fn aggregation_preserves_unit_on_valid() {
        use tatami::schema::Unit;
        let amount = Measure::new(n("amount"), Aggregation::sum())
            .with_unit(Unit::parse("USD").expect("valid unit"));
        let schema = Schema::builder()
            .dimension(Dimension::regular(n("Geography")).hierarchy(
                Hierarchy::new(n("Default")).level(Level::new(n("Region"), n("region"))),
            ))
            .measure(amount.clone())
            .build()
            .expect("schema");
        let df = df! {
            "region" => ["EMEA"],
            "amount" => [42.0_f64],
        }
        .expect("frame");
        let cube = InMemoryCube::new(df.clone(), schema.clone()).expect("cube");
        let tuple = slicer(&cube, Tuple::empty());
        let cell = evaluate_measure(&amount, &tuple, &df, &schema).expect("eval ok");
        match cell {
            Cell::Valid { unit, format, .. } => {
                assert_eq!(unit.expect("unit set").as_str(), "USD");
                assert!(format.is_none(), "measures do not carry format hints");
            }
            other => panic!("expected Valid, got {other:?}"),
        }
    }
}

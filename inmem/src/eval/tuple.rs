//! Tuple evaluation — Phase 5e of MAP_PLAN.md §5.
//!
//! A [`ResolvedTuple`] names a coordinate subset of the cube: each bound
//! member pins a single dim to a single leaf-level-or-above value.
//! [`filter_by_tuple`] is the fact-touching primitive that turns "tuple
//! context" into "the subset of fact rows that live at this coordinate".
//!
//! Phase 5f's metric evaluator and Phase 5g's `Cube::query` both layer on
//! top of this function; it is the only place where eval code consults the
//! fact frame for tuple membership.
//!
//! ## Column matching
//!
//! For each bound member, the filter ANDs one predicate **per level** of
//! the member's path, from root to leaf. A member pinning `FY2026/Q1/Jan`
//! produces three predicates (`year == "FY2026"`, `quarter == "Q1"`,
//! `month == "Jan"`), all combined into the final mask. Leaf-only matching
//! silently returns rows from other years that happen to share the same
//! month key — a silent-wrong-answer bug class MAP §8 R3 warns about.
//!
//! Each per-level predicate compares the *stringified* cell to the path
//! segment at that depth, consistent with how [`crate::catalogue`]
//! discovers members at construction time. This keeps integer-keyed and
//! string-keyed dims behaving the same and avoids routing the comparison
//! through polars's lazy compare machinery (which this crate deliberately
//! does not enable — see `CLAUDE.md`).
//!
//! A member whose path is shorter than the hierarchy's depth pins only the
//! levels the path covers (parent pinning, not leaf pinning) — pinning
//! `FY2026` alone produces `year == "FY2026"` and nothing deeper.
#![allow(dead_code)]

use polars_core::prelude::{BooleanChunked, ChunkFull, DataFrame, PlSmallStr};

use crate::Error;
use crate::resolve::ResolvedTuple;

/// Filter the fact frame to rows that match every bound coordinate in the
/// resolved tuple.
///
/// An empty tuple returns the frame unchanged (every row is compatible
/// with the empty coordinate set). For each member, the filter ANDs one
/// `col(level.key) == lit(segment)` predicate per level of the member's
/// path — root through leaf — so that e.g. pinning `FY2026/Q1/Jan` is
/// interpreted as "the FY2026 Jan in Q1", not "any row whose `month`
/// string happens to equal `Jan`".
///
/// Paths shorter than the hierarchy depth pin only the levels they cover,
/// which is the intended "parent pinning" behaviour for ancestor members.
/// A path longer than the hierarchy depth is rejected at
/// [`crate::resolve`] (Phase 5c); the defensive branch here surfaces
/// [`Error::EvalFilterFailed`] rather than panicking if such a tuple ever
/// reaches eval.
///
/// Returns [`Error::EvalColumnMissing`] if a level key column has vanished
/// between cube construction and eval (Phase 5a makes this structurally
/// impossible; the branch is defensive). Returns [`Error::EvalFilterFailed`]
/// if the polars `filter` call surfaces a runtime error.
pub(crate) fn filter_by_tuple(
    tuple: &ResolvedTuple<'_>,
    df: &DataFrame,
) -> Result<DataFrame, Error> {
    if tuple.members.is_empty() {
        return Ok(df.clone());
    }

    let height = df.height();
    let mut mask = BooleanChunked::full(PlSmallStr::from_static("mask"), true, height);

    for member in &tuple.members {
        let depth = member.path.len();
        let hierarchy_depth = member.hierarchy.hierarchy.levels.len();
        if depth == 0 || depth > hierarchy_depth {
            // `Path` is non-empty by construction and 5c caps depth at the
            // hierarchy's level count; surface a typed error rather than
            // panic on a hand-built tuple that reached eval.
            return Err(Error::EvalFilterFailed {
                reason: format!(
                    "path depth {} out of range for {}/{} hierarchy (levels: {})",
                    depth,
                    member.dim.dim.name.as_str(),
                    member.hierarchy.hierarchy.name.as_str(),
                    hierarchy_depth,
                ),
            });
        }

        // AND one predicate per level, root → leaf. Pinning `FY2026/Q1`
        // covers the first two levels; `FY2026` alone covers only the
        // first (parent pinning). This is what makes tuple filtering
        // hierarchically honest — without it, a month key shared across
        // years would silently match any year.
        for (level, segment) in member
            .hierarchy
            .hierarchy
            .levels
            .iter()
            .zip(member.path.segments())
            .take(depth)
        {
            let column_name = level.key.as_str();
            let column = df
                .column(column_name)
                .map_err(|_| Error::EvalColumnMissing {
                    column: column_name.to_owned(),
                })?;
            let series = column.as_materialized_series();
            let level_mask = column_equals_stringwise(series, segment.as_str(), height);
            mask = (&mask) & (&level_mask);
        }
    }

    df.filter(&mask).map_err(|e| Error::EvalFilterFailed {
        reason: e.to_string(),
    })
}

/// Build a boolean mask `column == target` with stringwise comparison.
///
/// Iterating with `Series::get` is portable across dtypes — the column has
/// already been validated as discrete (Integer or String) by Phase 5a — and
/// keeps us off the `lazy` feature path. Null cells contribute `false`.
fn column_equals_stringwise(
    series: &polars_core::prelude::Series,
    target: &str,
    height: usize,
) -> BooleanChunked {
    (0..height)
        .map(|i| match series.get(i) {
            Ok(av) => any_value_matches(&av, target),
            Err(_) => false,
        })
        .collect()
}

/// Compare an [`polars_core::prelude::AnyValue`] to a target string using
/// the same stringification rule [`crate::catalogue`] uses at construction
/// time, so "member discovered at build" matches "filter at query".
fn any_value_matches(av: &polars_core::prelude::AnyValue<'_>, target: &str) -> bool {
    use polars_core::prelude::AnyValue;
    match av {
        AnyValue::Null => false,
        AnyValue::String(s) => *s == target,
        AnyValue::StringOwned(s) => s.as_str() == target,
        AnyValue::UInt8(v) => v.to_string() == target,
        AnyValue::UInt16(v) => v.to_string() == target,
        AnyValue::UInt32(v) => v.to_string() == target,
        AnyValue::UInt64(v) => v.to_string() == target,
        AnyValue::Int8(v) => v.to_string() == target,
        AnyValue::Int16(v) => v.to_string() == target,
        AnyValue::Int32(v) => v.to_string() == target,
        AnyValue::Int64(v) => v.to_string() == target,
        // Any other dtype would have been rejected by Phase 5a's
        // `Class::is_discrete`; fall back to `Display` for forward-
        // compatibility rather than silently dropping rows.
        other => other.to_string() == target,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars_core::df;
    use tatami::query::{Axes, MemberRef, Options, Path, Tuple};
    use tatami::schema::{Aggregation, Dimension, Hierarchy, Level, Measure, Name, Schema};

    use crate::InMemoryCube;
    use crate::resolve::ResolvedAxes;

    fn n(s: &str) -> Name {
        Name::parse(s).expect("valid name")
    }

    fn mr(dim: &str, hier: &str, segs: &[&str]) -> MemberRef {
        let names: Vec<Name> = segs.iter().copied().map(n).collect();
        MemberRef::new(n(dim), n(hier), Path::parse(names).expect("non-empty"))
    }

    /// Build a cube with Geography (two-level) + Time (one-level) + amount.
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
                .dimension(Dimension::time(n("Time"), Vec::new()).hierarchy(
                    Hierarchy::new(n("Default")).level(Level::new(n("Month"), n("month"))),
                ))
                .measure(Measure::new(n("amount"), Aggregation::sum()))
                .build()
                .expect("schema valid");
        let df = df! {
            "region"  => ["EMEA", "EMEA", "APAC"],
            "country" => ["UK",   "FR",   "JP"],
            "month"   => ["2026-01", "2026-02", "2026-01"],
            "amount"  => [100.0_f64, 200.0, 300.0],
        }
        .expect("frame valid");
        InMemoryCube::new(df, schema).expect("cube")
    }

    /// Resolve a slicer tuple in the fixture context.
    fn resolve_slicer<'c>(cube: &'c InMemoryCube, t: Tuple) -> ResolvedTuple<'c> {
        let q = tatami::Query {
            axes: Axes::Scalar,
            slicer: t,
            metrics: vec![n("amount")],
            options: Options::default(),
        };
        let rq = cube.resolve(&q).expect("resolve ok");
        // We only need the slicer tuple; axes are `Scalar` so `ResolvedAxes`
        // matches the `Scalar` variant but carries no payload.
        let ResolvedAxes::Scalar = rq.axes else {
            panic!("expected scalar axes");
        };
        rq.slicer
    }

    /// Fact frame — shared between tests via a fresh call per test.
    fn fixture_df() -> DataFrame {
        df! {
            "region"  => ["EMEA", "EMEA", "APAC"],
            "country" => ["UK",   "FR",   "JP"],
            "month"   => ["2026-01", "2026-02", "2026-01"],
            "amount"  => [100.0_f64, 200.0, 300.0],
        }
        .expect("frame valid")
    }

    #[test]
    fn empty_tuple_returns_frame_unchanged() {
        let cube = fixture_cube();
        let df = fixture_df();
        let tuple = resolve_slicer(&cube, Tuple::empty());
        let out = filter_by_tuple(&tuple, &df).expect("filter ok");
        assert_eq!(out.height(), df.height());
    }

    #[test]
    fn single_member_filters_to_matching_rows() {
        let cube = fixture_cube();
        let df = fixture_df();
        // Region == EMEA → rows 0 and 1.
        let tuple = resolve_slicer(&cube, Tuple::single(mr("Geography", "Default", &["EMEA"])));
        let out = filter_by_tuple(&tuple, &df).expect("filter ok");
        assert_eq!(out.height(), 2);
    }

    #[test]
    fn deep_path_ands_every_level_root_to_leaf() {
        let cube = fixture_cube();
        let df = fixture_df();
        // EMEA/UK → single row. The filter must AND both region == EMEA
        // and country == UK; the country-only check was the old leaf-only
        // behaviour (MAP §8 R3).
        let tuple = resolve_slicer(
            &cube,
            Tuple::single(mr("Geography", "Default", &["EMEA", "UK"])),
        );
        let out = filter_by_tuple(&tuple, &df).expect("filter ok");
        assert_eq!(out.height(), 1);
        // Sanity — the country column of the filtered frame is ["UK"].
        let country = out
            .column("country")
            .expect("country col")
            .as_materialized_series();
        let got: Vec<String> = (0..country.len())
            .map(|i| country.get(i).expect("get").to_string().replace('"', ""))
            .collect();
        assert_eq!(got, vec!["UK".to_owned()]);
    }

    #[test]
    fn multi_member_tuple_intersects_masks() {
        let cube = fixture_cube();
        let df = fixture_df();
        // EMEA × 2026-01 → only EMEA/UK.
        let tuple = Tuple::of(vec![
            mr("Geography", "Default", &["EMEA"]),
            mr("Time", "Default", &["2026-01"]),
        ])
        .expect("disjoint dims");
        let tuple = resolve_slicer(&cube, tuple);
        let out = filter_by_tuple(&tuple, &df).expect("filter ok");
        assert_eq!(out.height(), 1);
    }

    #[test]
    fn nonexistent_member_leaf_filters_to_empty_frame() {
        // Tuple resolution checks catalogue membership, but filtering the
        // *frame* can still produce zero rows if the caller hand-constructs
        // tuples inside the crate (Phase 5f will). Simulate by pinning
        // Time to 2026-03 (absent from frame after filtering EMEA).
        let cube = fixture_cube();
        let df = fixture_df();
        let tuple = resolve_slicer(
            &cube,
            Tuple::of(vec![
                mr("Geography", "Default", &["APAC"]),
                mr("Time", "Default", &["2026-02"]),
            ])
            .expect("disjoint dims"),
        );
        let out = filter_by_tuple(&tuple, &df).expect("filter ok");
        assert_eq!(out.height(), 0);
    }

    /// Cube + frame with a three-level Time hierarchy, used to demonstrate
    /// that `filter_by_tuple` ANDs every level of the path — not just the
    /// leaf. Without the per-level AND, a `Jan` row from FY2025 would
    /// slip into a filter pinning FY2026/Q1/Jan (MAP §8 R3).
    fn year_quarter_month_cube_and_frame() -> (InMemoryCube, DataFrame) {
        use tatami::schema::Calendar;
        let schema = Schema::builder()
            .dimension(
                Dimension::time(n("Time"), vec![Calendar::gregorian(n("Gregorian"))]).hierarchy(
                    Hierarchy::new(n("Fiscal"))
                        .level(Level::new(n("Year"), n("year")))
                        .level(Level::new(n("Quarter"), n("quarter")))
                        .level(Level::new(n("Month"), n("month"))),
                ),
            )
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .build()
            .expect("schema valid");
        let df = df! {
            "year"    => ["FY2025", "FY2026", "FY2025"],
            "quarter" => ["Q1",     "Q1",     "Q2"],
            "month"   => ["Jan",    "Jan",    "Apr"],
            "amount"  => [10.0_f64, 20.0,     30.0],
        }
        .expect("frame valid");
        let cube = InMemoryCube::new(df.clone(), schema).expect("construct three-level time cube");
        (cube, df)
    }

    #[test]
    fn filter_by_tuple_ands_every_level_of_the_path() {
        // Two rows share `month == "Jan"` but live in different fiscal
        // years. Pinning FY2026/Q1/Jan must return only the FY2026 row,
        // not both. The old leaf-only filter returned both (silent wrong
        // answer — MAP §8 R3).
        let (cube, df) = year_quarter_month_cube_and_frame();
        let tuple = resolve_slicer(
            &cube,
            Tuple::single(mr("Time", "Fiscal", &["FY2026", "Q1", "Jan"])),
        );
        let out = filter_by_tuple(&tuple, &df).expect("filter ok");
        assert_eq!(
            out.height(),
            1,
            "FY2026/Q1/Jan must not match FY2025/Q1/Jan"
        );
        let amount = out
            .column("amount")
            .expect("amount col")
            .as_materialized_series();
        assert_eq!(amount.get(0).expect("cell").to_string(), "20.0");
    }

    #[test]
    fn filter_by_tuple_on_partial_path_pins_only_those_levels() {
        // Pinning just `FY2026` (depth 1 under a 3-level hierarchy) must
        // keep every FY2026 row across quarters / months. No deeper levels
        // should be constrained.
        let (cube, df) = year_quarter_month_cube_and_frame();
        let tuple = resolve_slicer(&cube, Tuple::single(mr("Time", "Fiscal", &["FY2026"])));
        let out = filter_by_tuple(&tuple, &df).expect("filter ok");
        assert_eq!(out.height(), 1, "only the one FY2026 row survives");

        // And pinning just FY2025 keeps both FY2025 rows (Q1/Jan and Q2/Apr).
        let tuple = resolve_slicer(&cube, Tuple::single(mr("Time", "Fiscal", &["FY2025"])));
        let out = filter_by_tuple(&tuple, &df).expect("filter ok");
        assert_eq!(out.height(), 2, "both FY2025 rows survive partial pin");
    }
}

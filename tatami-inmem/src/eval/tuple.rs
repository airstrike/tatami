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
//! For each bound member, the level to filter on is the one at the
//! member's path depth — `hierarchy.levels[path.len() - 1]`. The filter
//! compares the *stringified* cell to the path's leaf segment, consistent
//! with how [`crate::catalogue`] discovers members at construction time.
//! This keeps integer-keyed and string-keyed dims behaving the same and
//! avoids routing the comparison through polars's lazy compare machinery
//! (which this crate deliberately does not enable — see `CLAUDE.md`).
#![allow(dead_code)]

use polars_core::prelude::{BooleanChunked, ChunkFull, DataFrame, PlSmallStr};

use crate::Error;
use crate::resolve::ResolvedTuple;

/// Filter the fact frame to rows that match every bound coordinate in the
/// resolved tuple.
///
/// An empty tuple returns the frame unchanged (every row is compatible
/// with the empty coordinate set). For each member, the filter computes
/// `frame[level_key_col] == leaf_path_segment` (stringwise) and ANDs the
/// resulting masks row-for-row.
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
        // `path.len() >= 1` by construction; the level at depth `len - 1`
        // is the path's leaf level.
        let depth = member.path.len();
        let level_index = depth - 1;
        let level = member
            .hierarchy
            .hierarchy
            .levels
            .get(level_index)
            .ok_or_else(|| Error::EvalColumnMissing {
                column: format!(
                    "{}/{} level at depth {}",
                    member.dim.dim.name.as_str(),
                    member.hierarchy.hierarchy.name.as_str(),
                    depth,
                ),
            })?;
        let column_name = level.key.as_str();
        let column = df
            .column(column_name)
            .map_err(|_| Error::EvalColumnMissing {
                column: column_name.to_owned(),
            })?;
        let series = column.as_materialized_series();

        // `member.path.segments()` visits root → leaf; the leaf is the
        // last segment, and it's what the fact-frame cell must stringify
        // to at `column_name`.
        let target = member
            .path
            .segments()
            .last()
            .expect("path has at least one segment by construction")
            .as_str();

        let member_mask = column_equals_stringwise(series, target, height);
        mask = (&mask) & (&member_mask);
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
    fn deep_path_filters_on_leaf_level_only() {
        let cube = fixture_cube();
        let df = fixture_df();
        // EMEA/UK → single row.
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
}

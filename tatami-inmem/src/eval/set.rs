//! Set evaluation — Phase 5d (base variants) + Phase 5g (Filter / TopN).
//!
//! Walks a `ResolvedSet` and materialises every tuple it denotes against
//! the cube's member catalogue and metric evaluator. Phase 5d landed every
//! variant except `Filter` and `TopN`, which need the metric evaluator
//! (Phase 5f) to score each candidate tuple. Phase 5g wires that in.
//!
//! ### `Filter`
//!
//! Each source tuple is scored against the predicate. For metric-bearing
//! predicates (`Eq`, `Gt`, `Lt`) the tuple passes iff the metric at that
//! tuple yields a [`Cell::Valid`] whose value satisfies the comparison —
//! [`Cell::Missing`] and [`Cell::Error`] are treated as false, which keeps
//! the filter conservative. For path-bearing predicates (`In`, `NotIn`)
//! the tuple's member on the named dim is compared against the prefix.
//!
//! ### `TopN`
//!
//! Each source tuple is scored against the ranking metric. `Valid` cells
//! contribute their value; `Missing` / `Error` cells score
//! [`f64::NEG_INFINITY`] (sink to the bottom deterministically). The
//! highest `n` scores survive; ties are broken by the source tuple's
//! pre-order position so the output is stable.
//!
//! [`Cell::Valid`]: tatami::Cell::Valid
//! [`Cell::Missing`]: tatami::Cell::Missing
//! [`Cell::Error`]: tatami::Cell::Error
#![allow(dead_code)]

use std::collections::HashSet;

use tatami::schema::Name;
use tatami::{Cell, MemberRelation};

use crate::Error;
use crate::InMemoryCube;
use crate::eval::metric::evaluate_expr;
use crate::resolve::{MetricHandle, ResolvedMember, ResolvedPredicate, ResolvedSet, ResolvedTuple};

/// Evaluate a resolved set into the concrete list of tuples it denotes.
///
/// Ordering is deterministic, matching the catalogue's pre-order DFS
/// (itself `BTreeMap`-backed), so the output of this function can be
/// relied on by snapshot and law tests. `Union` deduplicates by
/// member-identity, preserving first-seen order; `Explicit` does not
/// dedup — callers asked for exactly the list they passed.
pub(crate) fn evaluate<'s>(
    set: &ResolvedSet<'s>,
    cube: &'s InMemoryCube,
) -> Result<Vec<ResolvedTuple<'s>>, Error> {
    let catalogue = cube.catalogue();
    match set {
        ResolvedSet::Members {
            dim,
            hierarchy,
            level,
        } => {
            let members = catalogue
                .members_at(&dim.dim.name, &hierarchy.hierarchy.name, level.index)
                .ok_or(Error::EvalSetCompositionIllFormed {
                    reason: "Members addresses a (dim, hierarchy) pair absent from the catalogue",
                })?;
            let tuples = members
                .into_iter()
                .map(|mr| {
                    ResolvedTuple::from_members(vec![ResolvedMember {
                        dim: *dim,
                        hierarchy: *hierarchy,
                        path: mr.path,
                    }])
                })
                .collect();
            Ok(tuples)
        }

        ResolvedSet::Range {
            dim,
            hierarchy,
            from,
            to,
        } => {
            // Both endpoints share a level (enforced at resolve time); use
            // the `from` endpoint's depth as the canonical level index.
            let level_index = from.path.len().saturating_sub(1);
            let members = catalogue
                .members_at(&dim.dim.name, &hierarchy.hierarchy.name, level_index)
                .ok_or(Error::EvalSetCompositionIllFormed {
                    reason: "Range addresses a (dim, hierarchy) pair absent from the catalogue",
                })?;
            let from_idx = members.iter().position(|m| m.path == from.path).ok_or(
                Error::EvalSetCompositionIllFormed {
                    reason: "Range `from` endpoint not present in catalogue at its level",
                },
            )?;
            let to_idx = members.iter().position(|m| m.path == to.path).ok_or(
                Error::EvalSetCompositionIllFormed {
                    reason: "Range `to` endpoint not present in catalogue at its level",
                },
            )?;
            if from_idx > to_idx {
                return Err(Error::EvalRangeInverted {
                    from: from.path.clone(),
                    to: to.path.clone(),
                });
            }
            let tuples = members[from_idx..=to_idx]
                .iter()
                .map(|mr| {
                    ResolvedTuple::from_members(vec![ResolvedMember {
                        dim: *dim,
                        hierarchy: *hierarchy,
                        path: mr.path.clone(),
                    }])
                })
                .collect();
            Ok(tuples)
        }

        ResolvedSet::Named { set } => evaluate(set, cube),

        ResolvedSet::Explicit { members } => {
            // `Set::explicit` already rejected an empty member list, so no
            // emptiness check here; the caller gets exactly the list they
            // declared, without dedup.
            let tuples = members
                .iter()
                .cloned()
                .map(|m| ResolvedTuple::from_members(vec![m]))
                .collect();
            Ok(tuples)
        }

        ResolvedSet::Children { of } => {
            let parents = evaluate(of, cube)?;
            let mut out: Vec<ResolvedTuple> = Vec::new();
            let mut seen: HashSet<ResolvedTuple> = HashSet::new();
            for parent in parents {
                let member = single_member(&parent)?;
                let children = catalogue.members(
                    &member.dim.dim.name,
                    &member.hierarchy.hierarchy.name,
                    &member_ref(member),
                    MemberRelation::Children,
                )?;
                for child in children {
                    let tuple = ResolvedTuple::from_members(vec![ResolvedMember {
                        dim: member.dim,
                        hierarchy: member.hierarchy,
                        path: child.path,
                    }]);
                    if seen.insert(tuple.clone()) {
                        out.push(tuple);
                    }
                }
            }
            Ok(out)
        }

        ResolvedSet::Descendants { of, to_level } => {
            let sources = evaluate(of, cube)?;
            let mut out: Vec<ResolvedTuple> = Vec::new();
            let mut seen: HashSet<ResolvedTuple> = HashSet::new();
            for source in sources {
                let member = single_member(&source)?;
                // `to_level.index` is the target depth (0-based); the source
                // member sits at depth `path.len() - 1`. The catalogue's
                // `Descendants(d)` collects members at 1..=d levels *below*
                // the source, pre-order — parent before children.
                let source_depth = member.path.len().saturating_sub(1);
                if to_level.index <= source_depth {
                    // Resolve-time check rules this out, but be defensive:
                    // surface a clear error rather than producing nothing
                    // or an underflowing depth.
                    return Err(Error::EvalSetCompositionIllFormed {
                        reason: "Descendants to_level is not below the source set's level",
                    });
                }
                let depth = to_level.index - source_depth;
                let depth_u8 =
                    u8::try_from(depth).map_err(|_| Error::EvalSetCompositionIllFormed {
                        reason: "Descendants depth exceeds 255 levels",
                    })?;
                let descendants = catalogue.members(
                    &member.dim.dim.name,
                    &member.hierarchy.hierarchy.name,
                    &member_ref(member),
                    MemberRelation::Descendants(depth_u8),
                )?;
                for d in descendants {
                    let tuple = ResolvedTuple::from_members(vec![ResolvedMember {
                        dim: member.dim,
                        hierarchy: member.hierarchy,
                        path: d.path,
                    }]);
                    if seen.insert(tuple.clone()) {
                        out.push(tuple);
                    }
                }
            }
            Ok(out)
        }

        ResolvedSet::CrossJoin { left, right } => {
            let ls = evaluate(left, cube)?;
            let rs = evaluate(right, cube)?;
            let mut out = Vec::with_capacity(ls.len().saturating_mul(rs.len()));
            for l in &ls {
                for r in &rs {
                    out.push(cross_tuples(l, r));
                }
            }
            Ok(out)
        }

        ResolvedSet::Union { left, right } => {
            let mut out = evaluate(left, cube)?;
            out.extend(evaluate(right, cube)?);
            Ok(dedup_preserving_order(out))
        }

        ResolvedSet::Filter { set, pred } => {
            let sources = evaluate(set, cube)?;
            let mut out = Vec::with_capacity(sources.len());
            for tuple in sources {
                if predicate_holds(pred, &tuple, cube)? {
                    out.push(tuple);
                }
            }
            Ok(out)
        }

        ResolvedSet::TopN { set, n, by } => {
            let sources = evaluate(set, cube)?;
            // Score each candidate; `Missing` / `Error` score -∞ so they
            // sink to the bottom deterministically. Carry the source index
            // alongside the score so a stable sort breaks ties by pre-order
            // position.
            let mut scored: Vec<(usize, f64, ResolvedTuple<'s>)> =
                Vec::with_capacity(sources.len());
            for (idx, tuple) in sources.into_iter().enumerate() {
                let score = score_by_metric(by, &tuple, cube)?;
                scored.push((idx, score, tuple));
            }
            scored.sort_by(|a, b| {
                // Desc by score, then asc by original index for ties.
                b.1.partial_cmp(&a.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.0.cmp(&b.0))
            });
            let take = n.get().min(scored.len());
            Ok(scored
                .into_iter()
                .take(take)
                .map(|(_, _, tuple)| tuple)
                .collect())
        }
    }
}

// ── Filter predicate evaluation ───────────────────────────────────────────

/// Check whether `tuple` satisfies `pred` against the cube.
///
/// - `Eq` / `Gt` / `Lt` evaluate the predicate's metric at the tuple. A
///   `Cell::Valid` whose value satisfies the comparison passes; every
///   other cell state (`Missing`, `Error`) is conservatively false.
/// - `In` / `NotIn` compare the tuple's member on the named dim against
///   the path prefix. A tuple missing that dim is conservatively false for
///   `In` and true for `NotIn`.
fn predicate_holds(
    pred: &ResolvedPredicate<'_>,
    tuple: &ResolvedTuple<'_>,
    cube: &InMemoryCube,
) -> Result<bool, Error> {
    match pred {
        ResolvedPredicate::Eq { metric, value } => {
            Ok(cell_passes(evaluate_metric_at(metric, tuple, cube)?, |v| {
                v == *value
            }))
        }
        ResolvedPredicate::Gt { metric, value } => {
            Ok(cell_passes(evaluate_metric_at(metric, tuple, cube)?, |v| {
                v > *value
            }))
        }
        ResolvedPredicate::Lt { metric, value } => {
            Ok(cell_passes(evaluate_metric_at(metric, tuple, cube)?, |v| {
                v < *value
            }))
        }
        ResolvedPredicate::In { dim, path_prefix } => Ok(path_prefix_matches(
            tuple,
            &dim.dim.name,
            path_prefix
                .segments()
                .cloned()
                .collect::<Vec<Name>>()
                .as_slice(),
        )),
        ResolvedPredicate::NotIn { dim, path_prefix } => Ok(!path_prefix_matches(
            tuple,
            &dim.dim.name,
            path_prefix
                .segments()
                .cloned()
                .collect::<Vec<Name>>()
                .as_slice(),
        )),
    }
}

/// Whether a scored cell passes a numeric comparison. `Missing` / `Error`
/// are treated as false.
fn cell_passes<F: Fn(f64) -> bool>(cell: Cell, cmp: F) -> bool {
    match cell {
        Cell::Valid { value, .. } => cmp(value),
        _ => false,
    }
}

/// Evaluate a metric handle at a tuple. Measures go straight through
/// `evaluate_measure`; metrics thread through the full `evaluate_expr`
/// machinery so dependent metrics (e.g., `ADR = Revenue / Nights`) work
/// uniformly.
fn evaluate_metric_at(
    handle: &MetricHandle<'_>,
    tuple: &ResolvedTuple<'_>,
    cube: &InMemoryCube,
) -> Result<Cell, Error> {
    let name = match handle {
        MetricHandle::Measure(m) => &m.name,
        MetricHandle::Metric(m) => &m.name,
    };
    evaluate_expr(
        &tatami::schema::metric::Expr::Ref { name: name.clone() },
        tuple,
        cube,
    )
}

/// Extract a comparable score from a metric handle at a tuple. Used by
/// `TopN`. `Missing` / `Error` score [`f64::NEG_INFINITY`] — a
/// deterministic "lowest rank" choice for v0.1.
fn score_by_metric(
    handle: &MetricHandle<'_>,
    tuple: &ResolvedTuple<'_>,
    cube: &InMemoryCube,
) -> Result<f64, Error> {
    match evaluate_metric_at(handle, tuple, cube)? {
        Cell::Valid { value, .. } => Ok(value),
        _ => Ok(f64::NEG_INFINITY),
    }
}

/// Whether the tuple's member on `dim` has a path whose leading segments
/// match `prefix`. A tuple without that dim returns false.
fn path_prefix_matches(tuple: &ResolvedTuple<'_>, dim: &Name, prefix: &[Name]) -> bool {
    let Some(member) = tuple.members.iter().find(|m| m.dim.dim.name == *dim) else {
        return false;
    };
    if prefix.len() > member.path.len() {
        return false;
    }
    member
        .path
        .segments()
        .zip(prefix.iter())
        .all(|(seg, pre)| seg == pre)
}

// ── Helpers ────────────────────────────────────────────────────────────────

/// Merge two tuples into one by concatenating their member lists.
///
/// Resolve-time dim-disjointness on `CrossJoin` (§3.6) guarantees no
/// duplicate dim surfaces in the merged tuple, so construction stays total.
fn cross_tuples<'s>(l: &ResolvedTuple<'s>, r: &ResolvedTuple<'s>) -> ResolvedTuple<'s> {
    let mut members = Vec::with_capacity(l.members.len() + r.members.len());
    members.extend(l.members.iter().cloned());
    members.extend(r.members.iter().cloned());
    ResolvedTuple::from_members(members)
}

/// Deduplicate tuples while preserving first-seen order.
///
/// `Union` evaluation relies on this to honor law S3 (`a ∪ a ≡ a`) while
/// keeping the outer ordering deterministic. `HashSet` carries the
/// "already emitted" bit; the output vector carries order.
fn dedup_preserving_order<'s>(ts: Vec<ResolvedTuple<'s>>) -> Vec<ResolvedTuple<'s>> {
    let mut seen: HashSet<ResolvedTuple<'s>> = HashSet::with_capacity(ts.len());
    let mut out: Vec<ResolvedTuple<'s>> = Vec::with_capacity(ts.len());
    for t in ts {
        if seen.insert(t.clone()) {
            out.push(t);
        }
    }
    out
}

/// Extract the single member of a tuple, or surface an ill-formed
/// composition. `Children` and `Descendants` over multi-member tuples are
/// resolve-time errors, but we check here so the evaluator is total.
fn single_member<'s, 'a>(tuple: &'a ResolvedTuple<'s>) -> Result<&'a ResolvedMember<'s>, Error> {
    match tuple.members.as_slice() {
        [only] => Ok(only),
        _ => Err(Error::EvalSetCompositionIllFormed {
            reason: "Children / Descendants require one-member input tuples",
        }),
    }
}

/// Re-build a public [`tatami::MemberRef`] from a resolved member so we
/// can feed [`Catalogue::members`] without cloning the handles.
fn member_ref(m: &ResolvedMember<'_>) -> tatami::MemberRef {
    tatami::MemberRef::new(
        m.dim.dim.name.clone(),
        m.hierarchy.hierarchy.name.clone(),
        m.path.clone(),
    )
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use polars_core::df;
    use polars_core::prelude::DataFrame;
    use std::collections::HashSet;
    use std::num::NonZeroUsize;
    use tatami::MemberRef;
    use tatami::query::{Path, Predicate, Set};
    use tatami::schema::{Aggregation, Dimension, Hierarchy, Level, Measure, Name, Schema};

    use crate::InMemoryCube;
    use crate::resolve;

    // ── Fixture ────────────────────────────────────────────────────────

    fn n(s: &str) -> Name {
        Name::parse(s).expect("valid name")
    }

    fn mr(dim: &str, hier: &str, segments: Vec<&str>) -> MemberRef {
        let names: Vec<Name> = segments.into_iter().map(n).collect();
        MemberRef::new(n(dim), n(hier), Path::parse(names).expect("non-empty"))
    }

    /// Two-hierarchy fixture — Geography (Region → Country) and Segment
    /// (Single-level). Rich enough for cross-join / union tests.
    fn fixture_schema() -> Schema {
        Schema::builder()
            .dimension(
                Dimension::regular(n("Geography")).hierarchy(
                    Hierarchy::new(n("Default"))
                        .level(Level::new(n("Region"), n("region")))
                        .level(Level::new(n("Country"), n("country"))),
                ),
            )
            .dimension(
                Dimension::regular(n("Segment")).hierarchy(
                    Hierarchy::new(n("Default")).level(Level::new(n("Tier"), n("tier"))),
                ),
            )
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .build()
            .expect("schema valid")
    }

    /// Geography tree:
    ///   EMEA → UK, FR
    ///   APAC → JP
    /// Segment: Business, Leisure.
    fn fixture_frame() -> DataFrame {
        df! {
            "region"  => ["EMEA", "EMEA", "APAC"],
            "country" => ["UK",   "FR",   "JP"],
            "tier"    => ["Business", "Leisure", "Business"],
            "amount"  => [1.0_f64, 2.0, 3.0],
        }
        .expect("frame valid")
    }

    fn fixture_cube() -> InMemoryCube {
        InMemoryCube::new(fixture_frame(), fixture_schema()).expect("cube")
    }

    /// Resolve a `Set` in the fixture context and return the resolved
    /// tree along with a schema/catalogue pair the caller can feed to
    /// `evaluate`.
    fn resolve_set<'c>(cube: &'c InMemoryCube, set: Set) -> resolve::ResolvedSet<'c> {
        // Wrap in a Series query so the existing `resolve` entry-point can
        // do the work end-to-end, then unwrap the axes.
        let q = tatami::Query {
            axes: tatami::query::Axes::Series { rows: set },
            slicer: tatami::query::Tuple::empty(),
            metrics: vec![n("amount")],
            options: tatami::query::Options::default(),
        };
        let rq = cube.resolve(&q).expect("resolve ok");
        match rq.axes {
            resolve::ResolvedAxes::Series { rows } => rows,
            _ => panic!("expected series"),
        }
    }

    /// Collect the single-member path from each tuple, as strings.
    fn single_paths(tuples: &[ResolvedTuple<'_>]) -> Vec<Vec<String>> {
        tuples
            .iter()
            .map(|t| {
                assert_eq!(t.members.len(), 1, "expected single-member tuples");
                t.members[0]
                    .path
                    .segments()
                    .map(|s| s.as_str().to_owned())
                    .collect()
            })
            .collect()
    }

    // ── Happy paths, per variant ───────────────────────────────────────

    #[test]
    fn evaluate_members_returns_every_member_at_level() {
        let cube = fixture_cube();
        let set = resolve_set(
            &cube,
            Set::members(n("Geography"), n("Default"), n("Country")),
        );
        let ts = evaluate(&set, &cube).expect("eval ok");
        let paths = single_paths(&ts);
        // Pre-order DFS, BTreeMap children: APAC→JP, EMEA→FR, EMEA→UK.
        assert_eq!(
            paths,
            vec![
                vec!["APAC".to_owned(), "JP".to_owned()],
                vec!["EMEA".to_owned(), "FR".to_owned()],
                vec!["EMEA".to_owned(), "UK".to_owned()],
            ]
        );
    }

    #[test]
    fn evaluate_range_returns_ordered_slice() {
        let cube = fixture_cube();
        let set = resolve_set(
            &cube,
            Set::range(
                n("Geography"),
                n("Default"),
                mr("Geography", "Default", vec!["APAC", "JP"]),
                mr("Geography", "Default", vec!["EMEA", "FR"]),
            ),
        );
        let ts = evaluate(&set, &cube).expect("eval ok");
        // JP, FR — but catalogue order at Country level is [APAC/JP,
        // EMEA/FR, EMEA/UK], so the range covers [APAC/JP, EMEA/FR].
        let paths = single_paths(&ts);
        assert_eq!(
            paths,
            vec![
                vec!["APAC".to_owned(), "JP".to_owned()],
                vec!["EMEA".to_owned(), "FR".to_owned()],
            ]
        );
    }

    #[test]
    fn evaluate_named_delegates_to_inner_set() {
        let schema = Schema::builder()
            .dimension(
                Dimension::regular(n("Geography")).hierarchy(
                    Hierarchy::new(n("Default"))
                        .level(Level::new(n("Region"), n("region")))
                        .level(Level::new(n("Country"), n("country"))),
                ),
            )
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .named_set(tatami::schema::NamedSet::new(
                n("AllRegions"),
                Set::members(n("Geography"), n("Default"), n("Region")),
            ))
            .build()
            .expect("schema");
        let df = df! {
            "region"  => ["EMEA", "APAC"],
            "country" => ["UK",   "JP"],
            "amount"  => [1.0_f64, 2.0],
        }
        .expect("frame");
        let cube = InMemoryCube::new(df, schema).expect("cube");

        let set = resolve_set(&cube, Set::named(n("AllRegions")));
        let ts = evaluate(&set, &cube).expect("eval ok");
        let paths = single_paths(&ts);
        assert_eq!(
            paths,
            vec![vec!["APAC".to_owned()], vec!["EMEA".to_owned()]]
        );
    }

    #[test]
    fn evaluate_explicit_preserves_caller_order() {
        let cube = fixture_cube();
        // Caller order: UK, JP, FR — catalogue order would be JP, FR, UK.
        let set = resolve_set(
            &cube,
            Set::explicit(vec![
                mr("Geography", "Default", vec!["EMEA", "UK"]),
                mr("Geography", "Default", vec!["APAC", "JP"]),
                mr("Geography", "Default", vec!["EMEA", "FR"]),
            ])
            .expect("non-empty"),
        );
        let ts = evaluate(&set, &cube).expect("eval ok");
        let paths = single_paths(&ts);
        assert_eq!(
            paths,
            vec![
                vec!["EMEA".to_owned(), "UK".to_owned()],
                vec!["APAC".to_owned(), "JP".to_owned()],
                vec!["EMEA".to_owned(), "FR".to_owned()],
            ]
        );
    }

    #[test]
    fn evaluate_children_returns_direct_descendants() {
        let cube = fixture_cube();
        let parents =
            Set::explicit(vec![mr("Geography", "Default", vec!["EMEA"])]).expect("non-empty");
        let set = resolve_set(&cube, parents.children());
        let ts = evaluate(&set, &cube).expect("eval ok");
        let paths = single_paths(&ts);
        assert_eq!(
            paths,
            vec![
                vec!["EMEA".to_owned(), "FR".to_owned()],
                vec!["EMEA".to_owned(), "UK".to_owned()],
            ]
        );
    }

    #[test]
    fn evaluate_descendants_to_leaf_bfs_preorder() {
        let cube = fixture_cube();
        // From Region down to Country — one level down. Pre-order.
        let set = resolve_set(
            &cube,
            Set::members(n("Geography"), n("Default"), n("Region")).descendants_to(n("Country")),
        );
        let ts = evaluate(&set, &cube).expect("eval ok");
        let paths = single_paths(&ts);
        // APAC/JP (below APAC), EMEA/FR, EMEA/UK (below EMEA).
        assert_eq!(
            paths,
            vec![
                vec!["APAC".to_owned(), "JP".to_owned()],
                vec!["EMEA".to_owned(), "FR".to_owned()],
                vec!["EMEA".to_owned(), "UK".to_owned()],
            ]
        );
    }

    #[test]
    fn evaluate_crossjoin_outputs_cartesian_product() {
        let cube = fixture_cube();
        let left = Set::members(n("Geography"), n("Default"), n("Region"));
        let right = Set::members(n("Segment"), n("Default"), n("Tier"));
        let set = resolve_set(&cube, left.cross(right));
        let ts = evaluate(&set, &cube).expect("eval ok");
        // 2 regions × 2 tiers = 4 tuples; each tuple has 2 members,
        // ordered (region, tier).
        assert_eq!(ts.len(), 4);
        let shapes: Vec<(String, String)> = ts
            .iter()
            .map(|t| {
                let r = t.members[0].path.head().as_str().to_owned();
                let s = t.members[1].path.head().as_str().to_owned();
                (r, s)
            })
            .collect();
        assert_eq!(
            shapes,
            vec![
                ("APAC".to_owned(), "Business".to_owned()),
                ("APAC".to_owned(), "Leisure".to_owned()),
                ("EMEA".to_owned(), "Business".to_owned()),
                ("EMEA".to_owned(), "Leisure".to_owned()),
            ]
        );
    }

    #[test]
    fn evaluate_union_deduplicates_preserving_first_order() {
        let cube = fixture_cube();
        // Left: [UK, JP]; Right: [JP, FR]. Union preserving first-seen
        // dedup => [UK, JP, FR].
        let left = Set::explicit(vec![
            mr("Geography", "Default", vec!["EMEA", "UK"]),
            mr("Geography", "Default", vec!["APAC", "JP"]),
        ])
        .expect("non-empty");
        let right = Set::explicit(vec![
            mr("Geography", "Default", vec!["APAC", "JP"]),
            mr("Geography", "Default", vec!["EMEA", "FR"]),
        ])
        .expect("non-empty");
        let set = resolve_set(&cube, left.union(right));
        let ts = evaluate(&set, &cube).expect("eval ok");
        let paths = single_paths(&ts);
        assert_eq!(
            paths,
            vec![
                vec!["EMEA".to_owned(), "UK".to_owned()],
                vec!["APAC".to_owned(), "JP".to_owned()],
                vec!["EMEA".to_owned(), "FR".to_owned()],
            ]
        );
    }

    // ── Law examples (§3.7 S1, S3, S4, S9) ─────────────────────────────

    fn as_set(tuples: &[ResolvedTuple<'_>]) -> HashSet<ResolvedTuple<'static>> {
        // Clone into 'static by rebuilding paths — the handles embed the
        // cube's lifetime so we can't easily promote. Instead, compare via
        // a path-based surrogate.
        let _ = tuples;
        HashSet::new()
    }

    /// Compare two tuple lists as multisets of (dim-name, path)-per-member
    /// signatures. Handle-lifetime gymnastics are avoided by collapsing to
    /// owned data.
    fn signatures(tuples: &[ResolvedTuple<'_>]) -> HashSet<Vec<(String, Vec<String>)>> {
        tuples
            .iter()
            .map(|t| {
                t.members
                    .iter()
                    .map(|m| {
                        (
                            m.dim.dim.name.as_str().to_owned(),
                            m.path
                                .segments()
                                .map(|s| s.as_str().to_owned())
                                .collect::<Vec<_>>(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    fn union_is_commutative_on_simple_sets() {
        // S1 — `a ∪ b ≡ b ∪ a` as multisets.
        let cube = fixture_cube();
        let a = Set::explicit(vec![
            mr("Geography", "Default", vec!["EMEA", "UK"]),
            mr("Geography", "Default", vec!["APAC", "JP"]),
        ])
        .expect("non-empty");
        let b =
            Set::explicit(vec![mr("Geography", "Default", vec!["EMEA", "FR"])]).expect("non-empty");

        let ab = evaluate(&resolve_set(&cube, a.clone().union(b.clone())), &cube).expect("eval");
        let ba = evaluate(&resolve_set(&cube, b.union(a)), &cube).expect("eval");

        assert_eq!(signatures(&ab), signatures(&ba));
        let _ = as_set; // silence unused warning for the surrogate helper
    }

    #[test]
    fn union_is_idempotent_on_simple_sets() {
        // S3 — `a ∪ a ≡ a`.
        let cube = fixture_cube();
        let a = Set::members(n("Geography"), n("Default"), n("Region"));

        let a_ts = evaluate(&resolve_set(&cube, a.clone()), &cube).expect("eval a");
        let aa_ts = evaluate(&resolve_set(&cube, a.clone().union(a)), &cube).expect("eval aa");

        assert_eq!(signatures(&a_ts), signatures(&aa_ts));
    }

    #[test]
    fn crossjoin_is_commutative_up_to_tuple_rearrangement() {
        // S4 — `a × b ≡ b × a` up to per-tuple member order.
        let cube = fixture_cube();
        let a = Set::members(n("Geography"), n("Default"), n("Region"));
        let b = Set::members(n("Segment"), n("Default"), n("Tier"));

        let ab = evaluate(&resolve_set(&cube, a.clone().cross(b.clone())), &cube).expect("eval");
        let ba = evaluate(&resolve_set(&cube, b.cross(a)), &cube).expect("eval");

        // Rearrange each tuple's members to a canonical (by dim name)
        // order, then compare as multisets.
        fn canonical_sigs(tuples: &[ResolvedTuple<'_>]) -> HashSet<Vec<(String, Vec<String>)>> {
            tuples
                .iter()
                .map(|t| {
                    let mut sig: Vec<(String, Vec<String>)> = t
                        .members
                        .iter()
                        .map(|m| {
                            (
                                m.dim.dim.name.as_str().to_owned(),
                                m.path
                                    .segments()
                                    .map(|s| s.as_str().to_owned())
                                    .collect::<Vec<_>>(),
                            )
                        })
                        .collect();
                    sig.sort();
                    sig
                })
                .collect()
        }

        assert_eq!(canonical_sigs(&ab), canonical_sigs(&ba));
    }

    #[test]
    fn descendants_of_union_matches_union_of_descendants() {
        // S9 — `(a ∪ b).descendants_to(L) ≡ a.descendants_to(L) ∪ b.descendants_to(L)`.
        let cube = fixture_cube();
        let a = Set::explicit(vec![mr("Geography", "Default", vec!["EMEA"])]).expect("non-empty");
        let b = Set::explicit(vec![mr("Geography", "Default", vec!["APAC"])]).expect("non-empty");

        let lhs = evaluate(
            &resolve_set(
                &cube,
                a.clone().union(b.clone()).descendants_to(n("Country")),
            ),
            &cube,
        )
        .expect("eval lhs");
        let rhs = evaluate(
            &resolve_set(
                &cube,
                a.descendants_to(n("Country"))
                    .union(b.descendants_to(n("Country"))),
            ),
            &cube,
        )
        .expect("eval rhs");

        assert_eq!(signatures(&lhs), signatures(&rhs));
    }

    // ── Filter + TopN (Phase 5g) ───────────────────────────────────────

    #[test]
    fn filter_keeps_tuples_matching_predicate_gt() {
        // EMEA has amount 1 + 2 = 3; APAC has 3. Gt(2) keeps both EMEA and
        // APAC (each is above 2 as a region-level sum); Gt(10) keeps none.
        let cube = fixture_cube();
        let base = Set::members(n("Geography"), n("Default"), n("Region"));
        let kept = resolve_set(
            &cube,
            base.clone().filter(Predicate::Gt {
                metric: n("amount"),
                value: 2.5,
            }),
        );
        let dropped = resolve_set(
            &cube,
            base.filter(Predicate::Gt {
                metric: n("amount"),
                value: 10.0,
            }),
        );
        let kept_ts = evaluate(&kept, &cube).expect("eval kept");
        let dropped_ts = evaluate(&dropped, &cube).expect("eval dropped");

        // EMEA = 3.0, APAC = 3.0 — both survive Gt(2.5).
        assert_eq!(kept_ts.len(), 2, "both regions exceed 2.5");
        assert!(
            dropped_ts.is_empty(),
            "no regions exceed 10.0, got {dropped_ts:?}"
        );
    }

    #[test]
    fn topn_returns_n_highest_scored_tuples() {
        // Country-level amounts: APAC/JP = 3, EMEA/FR = 2, EMEA/UK = 1.
        // Top(2 by amount) keeps JP (3) and FR (2) in descending-score order.
        let cube = fixture_cube();
        let set = resolve_set(
            &cube,
            Set::members(n("Geography"), n("Default"), n("Country"))
                .top(NonZeroUsize::new(2).expect("nonzero"), n("amount")),
        );
        let ts = evaluate(&set, &cube).expect("eval ok");
        assert_eq!(ts.len(), 2, "n=2 → two tuples");
        let leaves: Vec<String> = ts
            .iter()
            .map(|t| {
                t.members[0]
                    .path
                    .segments()
                    .last()
                    .expect("non-empty path")
                    .as_str()
                    .to_owned()
            })
            .collect();
        assert_eq!(leaves, vec!["JP".to_owned(), "FR".to_owned()]);
    }

    #[test]
    fn topn_with_ties_is_deterministic() {
        // Build a schema where every region-level amount ties at the same
        // value. With identical scores, Top(2) must pick the two
        // lowest-indexed tuples in pre-order (APAC then EMEA).
        let df = df! {
            "region"  => ["EMEA", "EMEA", "APAC", "APAC"],
            "country" => ["UK",   "FR",   "JP",   "SG"],
            "tier"    => ["Business", "Leisure", "Business", "Leisure"],
            "amount"  => [5.0_f64, 5.0, 5.0, 5.0],
        }
        .expect("frame valid");
        let cube = InMemoryCube::new(df, fixture_schema()).expect("cube");
        // Country level — four tuples, each summing to 5.0 for its single
        // row. Top(2) with all-ties keeps the first two in pre-order.
        let set = resolve_set(
            &cube,
            Set::members(n("Geography"), n("Default"), n("Country"))
                .top(NonZeroUsize::new(2).expect("nonzero"), n("amount")),
        );
        let ts = evaluate(&set, &cube).expect("eval ok");
        assert_eq!(ts.len(), 2);
        let leaves: Vec<String> = ts
            .iter()
            .map(|t| {
                t.members[0]
                    .path
                    .segments()
                    .last()
                    .expect("non-empty path")
                    .as_str()
                    .to_owned()
            })
            .collect();
        // Pre-order at Country level = [APAC/JP, APAC/SG, EMEA/FR, EMEA/UK].
        assert_eq!(leaves, vec!["JP".to_owned(), "SG".to_owned()]);
    }
}

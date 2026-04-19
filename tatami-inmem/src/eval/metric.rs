//! Metric expression evaluation — Phase 5f of MAP_PLAN.md §5.
//!
//! [`evaluate_expr`] walks a resolved metric's [`Expr`] tree at a
//! [`ResolvedTuple`] context and folds it into a single [`Cell`]. The
//! upstream resolve pass (Phase 5c) has already guaranteed every `Ref`
//! name exists, every `Lag` names a Time dim, every `PeriodsToDate` level
//! lives in some Time hierarchy, and every `At.at` tuple shape resolves.
//! What this phase adds: the arithmetic + navigation that turns the tree
//! into a value, with §3.3 `Cell` semantics for the three kinds of "no
//! answer" (valid / missing / errored).
//!
//! ## Cell propagation (MAP §3.7 M8, M9)
//!
//! Binary ops walk left then right:
//!
//! - both `Valid` → compute and return a new `Valid` (dropping unit/format,
//!   since compound units are out of scope for v0.1).
//! - either side `Error` → `Error` dominates, message propagates.
//! - neither `Error`, either `Missing` → `Missing`, reason from the first
//!   missing operand.
//!
//! `Div` by zero is a targeted `Cell::Error`; any binary op whose result is
//! not finite (NaN / ±Inf) is likewise `Cell::Error` rather than smuggling
//! a non-finite `f64` into the renderer.
//!
//! ## Metric cycles
//!
//! The static dependency graph built at schema-build time is acyclic — the
//! schema builder rejects metric-to-metric cycles on construction — but the
//! evaluator still threads a `HashSet` of visited metric names through the
//! recursion. The cost is trivial and the defensive guarantee keeps the
//! recursion total regardless of which invariant the caller managed to
//! violate upstream.
#![allow(dead_code)]

use std::collections::HashSet;

use tatami::query::{MemberRef, Path, Tuple};
use tatami::schema::metric::{BinOp, Expr};
use tatami::schema::{Name, dimension};
use tatami::{Cell, missing};

use crate::Error;
use crate::InMemoryCube;
use crate::catalogue::Catalogue;
use crate::eval::aggregate::evaluate_measure;
use crate::resolve::{DimHandle, HierarchyHandle, ResolvedMember, ResolvedTuple};

/// Evaluate a metric expression tree at a tuple context against a cube.
///
/// Returns a [`Cell`] — the metric tree may produce any of `Valid`,
/// `Missing`, or `Error` without propagating those upward as `Result::Err`.
/// The outer `Result` reserves `Err` for invariant violations the recursion
/// cannot continue past: an unresolved `Ref` (Phase 5c should have caught
/// this, so it is defensive) or a metric cycle.
///
/// Non-panicking by construction: division-by-zero, non-finite
/// intermediates, and unbound dims on `Lag` / `PeriodsToDate` all turn into
/// `Cell::Error` / `Cell::Missing`.
pub(crate) fn evaluate_expr<'s>(
    expr: &Expr,
    tuple: &ResolvedTuple<'s>,
    cube: &'s InMemoryCube,
) -> Result<Cell, Error> {
    let mut visited: HashSet<Name> = HashSet::new();
    walk(expr, tuple, cube, &mut visited)
}

/// Inner recursion carrying the metric-cycle visited-set.
fn walk<'s>(
    expr: &Expr,
    tuple: &ResolvedTuple<'s>,
    cube: &'s InMemoryCube,
    visited: &mut HashSet<Name>,
) -> Result<Cell, Error> {
    match expr {
        Expr::Const { value } => Ok(const_cell(*value)),
        Expr::Ref { name } => eval_ref(name, tuple, cube, visited),
        Expr::Binary { bin_op, l, r } => eval_binary(*bin_op, l, r, tuple, cube, visited),
        Expr::Lag { of, dim, n } => eval_lag(of, dim, *n, tuple, cube, visited),
        Expr::PeriodsToDate { of, level } => eval_ptd(of, level, tuple, cube, visited),
        Expr::At { of, at } => eval_at(of, at, tuple, cube, visited),
        // `Expr` is `#[non_exhaustive]`; future variants surface as a
        // typed error rather than a panic.
        _ => Err(Error::EvalUnresolvedRef {
            name: Name::parse("unknown-expr").expect("literal name"),
        }),
    }
}

fn const_cell(value: f64) -> Cell {
    if value.is_finite() {
        Cell::Valid {
            value,
            unit: None,
            format: None,
        }
    } else {
        Cell::Error {
            message: "non-finite constant".to_owned(),
        }
    }
}

fn eval_ref<'s>(
    name: &Name,
    tuple: &ResolvedTuple<'s>,
    cube: &'s InMemoryCube,
    visited: &mut HashSet<Name>,
) -> Result<Cell, Error> {
    if let Some(measure) = cube.schema.measures.iter().find(|m| m.name == *name) {
        return evaluate_measure(measure, tuple, cube.df(), &cube.schema);
    }
    if let Some(metric) = cube.schema.metrics.iter().find(|m| m.name == *name) {
        if !visited.insert(metric.name.clone()) {
            return Err(Error::EvalMetricCycle {
                name: metric.name.clone(),
            });
        }
        let cell = walk(&metric.expr, tuple, cube, visited)?;
        visited.remove(&metric.name);
        // Propagate the metric's declared unit / format onto a `Valid`
        // result (the inner expression drops them during binary ops).
        let cell = match cell {
            Cell::Valid {
                value,
                unit,
                format,
            } => Cell::Valid {
                value,
                unit: unit.or_else(|| metric.unit.clone()),
                format: format.or_else(|| metric.format.clone()),
            },
            other => other,
        };
        return Ok(cell);
    }
    Err(Error::EvalUnresolvedRef { name: name.clone() })
}

fn eval_binary<'s>(
    op: BinOp,
    l: &Expr,
    r: &Expr,
    tuple: &ResolvedTuple<'s>,
    cube: &'s InMemoryCube,
    visited: &mut HashSet<Name>,
) -> Result<Cell, Error> {
    let lc = walk(l, tuple, cube, visited)?;
    let rc = walk(r, tuple, cube, visited)?;
    Ok(combine(op, lc, rc))
}

/// Fold two operand cells through a binary op, honouring the §3.7 M9
/// propagation rule — `Error` dominates, then `Missing`, then compute.
fn combine(op: BinOp, l: Cell, r: Cell) -> Cell {
    // `Error` dominates regardless of side.
    if let Cell::Error { message } = &l {
        return Cell::Error {
            message: message.clone(),
        };
    }
    if let Cell::Error { message } = &r {
        return Cell::Error {
            message: message.clone(),
        };
    }
    // Then `Missing` — take the left reason first.
    if let Cell::Missing { reason } = &l {
        return Cell::Missing {
            reason: reason.clone(),
        };
    }
    if let Cell::Missing { reason } = &r {
        return Cell::Missing {
            reason: reason.clone(),
        };
    }
    // Both must be `Valid` here — destructure safely.
    let lv = match l {
        Cell::Valid { value, .. } => value,
        _ => {
            return Cell::Error {
                message: "binary: unexpected non-valid left operand".to_owned(),
            };
        }
    };
    let rv = match r {
        Cell::Valid { value, .. } => value,
        _ => {
            return Cell::Error {
                message: "binary: unexpected non-valid right operand".to_owned(),
            };
        }
    };
    let value = match op {
        BinOp::Add => lv + rv,
        BinOp::Sub => lv - rv,
        BinOp::Mul => lv * rv,
        BinOp::Div => {
            if rv == 0.0 {
                return Cell::Error {
                    message: "divide by zero".to_owned(),
                };
            }
            lv / rv
        }
        // `BinOp` is `#[non_exhaustive]`; future variants surface as an
        // error cell rather than silently returning zero.
        _ => {
            return Cell::Error {
                message: "unsupported binary operator".to_owned(),
            };
        }
    };
    if !value.is_finite() {
        return Cell::Error {
            message: "non-finite binary result".to_owned(),
        };
    }
    Cell::Valid {
        value,
        unit: None,
        format: None,
    }
}

fn eval_lag<'s>(
    of: &Expr,
    dim: &Name,
    n: i32,
    tuple: &ResolvedTuple<'s>,
    cube: &'s InMemoryCube,
    visited: &mut HashSet<Name>,
) -> Result<Cell, Error> {
    if n == 0 {
        return walk(of, tuple, cube, visited);
    }

    // Find the tuple's member on `dim`; if unbound, lag is not applicable.
    let position = tuple.members.iter().position(|m| m.dim.dim.name == *dim);
    let Some(idx) = position else {
        return Ok(Cell::Missing {
            reason: missing::Reason::NotApplicable,
        });
    };
    let member = &tuple.members[idx];

    // Shift along the catalogue's level ordering. `n > 0` → backward (older,
    // earlier index); `n < 0` → forward (newer, later index).
    let offset = -(n as isize);
    let shifted = sibling_offset(
        cube.catalogue(),
        &member.dim.dim.name,
        &member.hierarchy.hierarchy.name,
        &member.path,
        offset,
    );
    let Some(new_path) = shifted else {
        return Ok(Cell::Missing {
            reason: missing::Reason::NoFacts,
        });
    };

    // Rebuild the tuple with the shifted coordinate in place of the
    // original. `dim` / `hierarchy` handles stay pointing into the cube's
    // schema; only `path` changes.
    let new_member = ResolvedMember {
        dim: member.dim,
        hierarchy: member.hierarchy,
        path: new_path,
    };
    let mut new_members = tuple.members.clone();
    new_members[idx] = new_member;
    let new_tuple = ResolvedTuple::from_members(new_members);
    walk(of, &new_tuple, cube, visited)
}

/// Find the member `offset` positions away from `current_path` at the same
/// level within `(dim, hierarchy)`, using the catalogue's pre-order
/// enumeration of that level as the canonical ordering. `offset = 0` is
/// `current_path` itself; positive moves later in the list, negative moves
/// earlier. `None` if the offset lands outside the level's member range.
fn sibling_offset(
    catalogue: &Catalogue,
    dim: &Name,
    hierarchy: &Name,
    current_path: &Path,
    offset: isize,
) -> Option<Path> {
    let level_index = current_path.len().checked_sub(1)?;
    let members = catalogue.members_at(dim, hierarchy, level_index)?;
    let here = members.iter().position(|m| m.path == *current_path)?;
    let shifted = (here as isize).checked_add(offset)?;
    if shifted < 0 {
        return None;
    }
    let shifted = shifted as usize;
    members.get(shifted).map(|m| m.path.clone())
}

fn eval_ptd<'s>(
    of: &Expr,
    level: &Name,
    tuple: &ResolvedTuple<'s>,
    cube: &'s InMemoryCube,
    visited: &mut HashSet<Name>,
) -> Result<Cell, Error> {
    // Find the tuple's member on some Time dim whose hierarchy contains
    // `level`. Time detection is by `dimension::Kind::Time`.
    let Some((idx, member)) = tuple.members.iter().enumerate().find(|(_, m)| {
        matches!(m.dim.dim.kind, dimension::Kind::Time { .. })
            && m.hierarchy
                .hierarchy
                .levels
                .iter()
                .any(|l| l.name == *level)
    }) else {
        return Ok(Cell::Missing {
            reason: missing::Reason::NotApplicable,
        });
    };

    // Navigate to the ancestor at `level`. `member.path` sits at its own
    // depth; `level` sits at some index within the hierarchy. If the level
    // is deeper than the member's path, the ancestor doesn't exist — the
    // member is above the window level, which makes PTD a no-op (the
    // resolver should catch most such cases but defensively return
    // `NotApplicable`).
    let hierarchy = member.hierarchy.hierarchy;
    let Some(level_index) = hierarchy.levels.iter().position(|l| l.name == *level) else {
        return Ok(Cell::Missing {
            reason: missing::Reason::NotApplicable,
        });
    };
    let member_depth = member.path.len();
    if level_index + 1 > member_depth {
        // `level` is below the tuple's coordinate — no ancestor at that
        // depth. Return the inner expr unchanged at the tuple.
        return walk(of, tuple, cube, visited);
    }
    // Ancestor path = member.path segments truncated to `level_index + 1`.
    let ancestor_segments: Vec<Name> = member
        .path
        .segments()
        .take(level_index + 1)
        .cloned()
        .collect();
    let ancestor_path = Path::parse(ancestor_segments).expect("at least one segment");

    // Enumerate every member at the current member's level that descends
    // from the ancestor AND appears at-or-before the current member in the
    // catalogue's pre-order.
    let current_level_index = member_depth - 1;
    let Some(level_members) = cube.catalogue().members_at(
        &member.dim.dim.name,
        &member.hierarchy.hierarchy.name,
        current_level_index,
    ) else {
        return Ok(Cell::Missing {
            reason: missing::Reason::NoFacts,
        });
    };

    // Walk until we find `member.path`; the window is every descendant of
    // `ancestor_path` seen up to and including that point.
    let mut window: Vec<Path> = Vec::new();
    for m in level_members {
        let descends = m
            .path
            .segments()
            .zip(ancestor_path.segments())
            .take(level_index + 1)
            .all(|(a, b)| a == b)
            && m.path.len() > level_index;
        if descends {
            window.push(m.path.clone());
        }
        if m.path == member.path {
            break;
        }
    }
    if window.is_empty() {
        return Ok(Cell::Missing {
            reason: missing::Reason::NoFacts,
        });
    }

    // Sum `of` evaluated at the tuple with the Time coordinate replaced by
    // each period-member. `Missing` skips, `Error` aborts.
    let mut total = 0.0;
    let mut unit = None;
    let mut format = None;
    for path in window {
        let mut members = tuple.members.clone();
        members[idx] = ResolvedMember {
            dim: member.dim,
            hierarchy: member.hierarchy,
            path,
        };
        let sub_tuple = ResolvedTuple::from_members(members);
        match walk(of, &sub_tuple, cube, visited)? {
            Cell::Valid {
                value,
                unit: u,
                format: f,
            } => {
                total += value;
                if unit.is_none() {
                    unit = u;
                }
                if format.is_none() {
                    format = f;
                }
            }
            Cell::Missing { .. } => continue,
            Cell::Error { message } => return Ok(Cell::Error { message }),
            // `Cell` is `#[non_exhaustive]`; treat future variants as
            // missing so the PTD sum can keep progressing.
            _ => continue,
        }
    }
    if !total.is_finite() {
        return Ok(Cell::Error {
            message: "non-finite periods-to-date total".to_owned(),
        });
    }
    Ok(Cell::Valid {
        value: total,
        unit,
        format,
    })
}

fn eval_at<'s>(
    of: &Expr,
    at: &Tuple,
    tuple: &ResolvedTuple<'s>,
    cube: &'s InMemoryCube,
    visited: &mut HashSet<Name>,
) -> Result<Cell, Error> {
    // Resolve the `at` tuple against the cube. Phase 5c already verified
    // the shape statically; this call actually locates each member's
    // handles. An `at` with no members is a no-op — evaluate `of` at the
    // outer tuple unchanged.
    let at_members = resolve_overlay(at, cube)?;
    let merged = merge_tuples(tuple, &at_members);
    walk(of, &merged, cube, visited)
}

/// Resolve every [`MemberRef`] in a [`Tuple`] against the cube's schema and
/// catalogue, producing a vector of [`ResolvedMember`]s.
fn resolve_overlay<'s>(
    tuple: &Tuple,
    cube: &'s InMemoryCube,
) -> Result<Vec<ResolvedMember<'s>>, Error> {
    tuple
        .members()
        .iter()
        .map(|m| resolve_member(m, cube))
        .collect()
}

/// Resolve a single [`MemberRef`] against the cube.
///
/// Returns the same [`Error`] variants `crate::resolve::resolve_member_ref`
/// would — we reuse the variant names (`ResolveUnknownDimension`, etc.) so
/// eval-time failures and resolve-time failures share one vocabulary.
fn resolve_member<'s>(m: &MemberRef, cube: &'s InMemoryCube) -> Result<ResolvedMember<'s>, Error> {
    let dim = cube
        .schema
        .dimensions
        .iter()
        .find(|d| d.name == m.dim)
        .map(|dim| DimHandle { dim })
        .ok_or_else(|| Error::ResolveUnknownDimension { dim: m.dim.clone() })?;
    let hierarchy = dim
        .dim
        .hierarchies
        .iter()
        .find(|h| h.name == m.hierarchy)
        .map(|hierarchy| HierarchyHandle { hierarchy })
        .ok_or_else(|| Error::ResolveUnknownHierarchy {
            dim: m.dim.clone(),
            hierarchy: m.hierarchy.clone(),
        })?;
    if m.path.len() > hierarchy.hierarchy.levels.len() {
        return Err(Error::ResolveUnknownMember {
            dim: m.dim.clone(),
            hierarchy: m.hierarchy.clone(),
            path: m.path.clone(),
        });
    }
    let found = cube
        .catalogue()
        .path_exists(&m.dim, &m.hierarchy, &m.path)
        .unwrap_or(false);
    if !found {
        return Err(Error::ResolveUnknownMember {
            dim: m.dim.clone(),
            hierarchy: m.hierarchy.clone(),
            path: m.path.clone(),
        });
    }
    Ok(ResolvedMember {
        dim,
        hierarchy,
        path: m.path.clone(),
    })
}

/// Merge `base` with `overlay`: every dim in `overlay` overrides that dim
/// in `base`; dims only in `base` pass through; dims only in `overlay` are
/// appended. Preserves `base`'s dim order for passthrough dims.
fn merge_tuples<'s>(base: &ResolvedTuple<'s>, overlay: &[ResolvedMember<'s>]) -> ResolvedTuple<'s> {
    let mut members: Vec<ResolvedMember<'s>> = base
        .members
        .iter()
        .filter(|m| !overlay.iter().any(|o| o.dim.dim.name == m.dim.dim.name))
        .cloned()
        .collect();
    members.extend(overlay.iter().cloned());
    ResolvedTuple::from_members(members)
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars_core::df;
    use polars_core::prelude::DataFrame;
    use tatami::query::{Axes, Options, Path, Tuple};
    use tatami::schema::{
        Aggregation, Calendar, Dimension, Hierarchy, Level, Measure, Metric, Schema,
    };

    fn n(s: &str) -> Name {
        Name::parse(s).expect("valid name")
    }

    fn mr(dim: &str, hier: &str, segs: &[&str]) -> MemberRef {
        let names: Vec<Name> = segs.iter().copied().map(n).collect();
        MemberRef::new(n(dim), n(hier), Path::parse(names).expect("non-empty"))
    }

    /// Resolve a slicer tuple in the given cube context and return it.
    fn slicer<'c>(cube: &'c InMemoryCube, t: Tuple) -> ResolvedTuple<'c> {
        let metric = cube
            .schema
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
        rq.slicer
    }

    /// Time (Year → Month) + Scenario fixture, used by Lag / PTD / At tests.
    /// Amount values laid out so monthly subtotals are easy to spot:
    /// Actual 2026-01 → 10, 2026-02 → 20, 2026-03 → 30.
    /// Plan   2026-01 → 100, 2026-02 → 200, 2026-03 → 300.
    fn time_fixture() -> InMemoryCube {
        let schema = Schema::builder()
            .dimension(
                Dimension::time(n("Time"), vec![Calendar::gregorian(n("Gregorian"))]).hierarchy(
                    Hierarchy::new(n("Default"))
                        .level(Level::new(n("Year"), n("year")))
                        .level(Level::new(n("Month"), n("month"))),
                ),
            )
            .dimension(
                Dimension::regular(n("Scenario")).hierarchy(
                    Hierarchy::new(n("Default")).level(Level::new(n("Plan"), n("plan"))),
                ),
            )
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .build()
            .expect("schema");
        let df = df! {
            "year"   => ["2026"; 6],
            "month"  => ["2026-01","2026-02","2026-03","2026-01","2026-02","2026-03"],
            "plan"   => ["Actual","Actual","Actual","Plan","Plan","Plan"],
            "amount" => [10.0_f64, 20.0, 30.0, 100.0, 200.0, 300.0],
        }
        .expect("frame");
        InMemoryCube::new(df, schema).expect("cube")
    }

    #[test]
    fn const_returns_literal() {
        let cube = time_fixture();
        let tuple = slicer(&cube, Tuple::empty());
        let cell = evaluate_expr(&Expr::Const { value: 3.5 }, &tuple, &cube).expect("eval ok");
        assert!(
            matches!(cell, Cell::Valid { value, .. } if value == 3.5),
            "expected Valid(3.5), got {cell:?}"
        );
    }

    #[test]
    fn ref_to_measure_evaluates_via_aggregate() {
        let cube = time_fixture();
        let tuple = slicer(&cube, Tuple::empty());
        let cell = evaluate_expr(&Expr::Ref { name: n("amount") }, &tuple, &cube).expect("eval ok");
        // 10 + 20 + 30 + 100 + 200 + 300 = 660.
        assert!(matches!(cell, Cell::Valid { value, .. } if value == 660.0));
    }

    #[test]
    fn ref_to_metric_recurses() {
        // Define a metric that wraps `amount`; ref-by-name must walk
        // through it.
        let schema = Schema::builder()
            .dimension(
                Dimension::time(n("Time"), vec![Calendar::gregorian(n("Gregorian"))]).hierarchy(
                    Hierarchy::new(n("Default")).level(Level::new(n("Month"), n("month"))),
                ),
            )
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .metric(Metric::new(n("alias"), Expr::Ref { name: n("amount") }))
            .build()
            .expect("schema");
        let df = df! {
            "month"  => ["2026-01", "2026-02"],
            "amount" => [5.0_f64, 7.0],
        }
        .expect("frame");
        let cube = InMemoryCube::new(df, schema).expect("cube");
        let tuple = slicer(&cube, Tuple::empty());
        let cell = evaluate_expr(&Expr::Ref { name: n("alias") }, &tuple, &cube).expect("eval ok");
        assert!(matches!(cell, Cell::Valid { value, .. } if value == 12.0));
    }

    #[test]
    fn binary_add_sub_mul_div_return_expected() {
        let cube = time_fixture();
        let tuple = slicer(&cube, Tuple::empty());
        let cases = [
            (BinOp::Add, 9.0, 5.0, 14.0),
            (BinOp::Sub, 9.0, 5.0, 4.0),
            (BinOp::Mul, 9.0, 5.0, 45.0),
            (BinOp::Div, 9.0, 5.0, 9.0 / 5.0),
        ];
        for (op, lv, rv, want) in cases {
            let expr = Expr::Binary {
                bin_op: op,
                l: Box::new(Expr::Const { value: lv }),
                r: Box::new(Expr::Const { value: rv }),
            };
            let cell = evaluate_expr(&expr, &tuple, &cube).expect("eval ok");
            match cell {
                Cell::Valid { value, .. } => assert!(
                    (value - want).abs() < 1e-12,
                    "{op:?} expected {want}, got {value}"
                ),
                other => panic!("{op:?}: expected Valid, got {other:?}"),
            }
        }
    }

    #[test]
    fn binary_divide_by_zero_yields_error_cell() {
        let cube = time_fixture();
        let tuple = slicer(&cube, Tuple::empty());
        let expr = Expr::Binary {
            bin_op: BinOp::Div,
            l: Box::new(Expr::Const { value: 1.0 }),
            r: Box::new(Expr::Const { value: 0.0 }),
        };
        let cell = evaluate_expr(&expr, &tuple, &cube).expect("eval ok");
        match cell {
            Cell::Error { message } => {
                assert!(message.contains("divide by zero"), "got: {message}");
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn binary_missing_operand_propagates_as_missing() {
        // amount + Lag(amount, Time, 1) — lag lands before 2026-01 =>
        // Missing(NoFacts). Add of Valid + Missing => Missing.
        let cube = time_fixture();
        let tuple = slicer(
            &cube,
            Tuple::of(vec![
                mr("Time", "Default", &["2026", "2026-01"]),
                mr("Scenario", "Default", &["Actual"]),
            ])
            .expect("disjoint"),
        );
        let expr = Expr::Binary {
            bin_op: BinOp::Add,
            l: Box::new(Expr::Ref { name: n("amount") }),
            r: Box::new(Expr::Lag {
                of: Box::new(Expr::Ref { name: n("amount") }),
                dim: n("Time"),
                n: 1,
            }),
        };
        let cell = evaluate_expr(&expr, &tuple, &cube).expect("eval ok");
        assert!(
            matches!(cell, Cell::Missing { .. }),
            "expected Missing, got {cell:?}"
        );
    }

    #[test]
    fn binary_error_dominates_missing() {
        let cube = time_fixture();
        let tuple = slicer(
            &cube,
            Tuple::of(vec![
                mr("Time", "Default", &["2026", "2026-01"]),
                mr("Scenario", "Default", &["Actual"]),
            ])
            .expect("disjoint"),
        );
        // Error (div by zero) + Missing (lag out of range) => Error.
        let error_side = Expr::Binary {
            bin_op: BinOp::Div,
            l: Box::new(Expr::Const { value: 1.0 }),
            r: Box::new(Expr::Const { value: 0.0 }),
        };
        let missing_side = Expr::Lag {
            of: Box::new(Expr::Ref { name: n("amount") }),
            dim: n("Time"),
            n: 12,
        };
        let expr = Expr::Binary {
            bin_op: BinOp::Add,
            l: Box::new(missing_side),
            r: Box::new(error_side),
        };
        let cell = evaluate_expr(&expr, &tuple, &cube).expect("eval ok");
        assert!(
            matches!(cell, Cell::Error { .. }),
            "expected Error (dominates Missing), got {cell:?}"
        );
    }

    #[test]
    fn const_nan_and_inf_yield_error_cells() {
        let cube = time_fixture();
        let tuple = slicer(&cube, Tuple::empty());
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let cell = evaluate_expr(&Expr::Const { value: bad }, &tuple, &cube).expect("eval ok");
            assert!(
                matches!(cell, Cell::Error { .. }),
                "expected Error for {bad}, got {cell:?}"
            );
        }
    }

    #[test]
    fn lag_one_period_backward_returns_previous_month() {
        let cube = time_fixture();
        // Pin Time=2026-02, Scenario=Actual → amount=20. Lag(amount, 1)
        // navigates to 2026-01 → 10.
        let tuple = slicer(
            &cube,
            Tuple::of(vec![
                mr("Time", "Default", &["2026", "2026-02"]),
                mr("Scenario", "Default", &["Actual"]),
            ])
            .expect("disjoint"),
        );
        let expr = Expr::Lag {
            of: Box::new(Expr::Ref { name: n("amount") }),
            dim: n("Time"),
            n: 1,
        };
        let cell = evaluate_expr(&expr, &tuple, &cube).expect("eval ok");
        assert!(
            matches!(cell, Cell::Valid { value, .. } if value == 10.0),
            "expected 10, got {cell:?}"
        );
    }

    #[test]
    fn lag_zero_is_noop() {
        let cube = time_fixture();
        let tuple = slicer(
            &cube,
            Tuple::of(vec![
                mr("Time", "Default", &["2026", "2026-02"]),
                mr("Scenario", "Default", &["Actual"]),
            ])
            .expect("disjoint"),
        );
        let inner = Expr::Ref { name: n("amount") };
        let lagged = Expr::Lag {
            of: Box::new(inner.clone()),
            dim: n("Time"),
            n: 0,
        };
        let a = evaluate_expr(&inner, &tuple, &cube).expect("eval inner");
        let b = evaluate_expr(&lagged, &tuple, &cube).expect("eval lagged");
        assert_eq!(a, b, "lag-0 must equal the inner expression");
    }

    #[test]
    fn lag_out_of_bounds_returns_missing_no_facts() {
        let cube = time_fixture();
        let tuple = slicer(
            &cube,
            Tuple::of(vec![
                mr("Time", "Default", &["2026", "2026-01"]),
                mr("Scenario", "Default", &["Actual"]),
            ])
            .expect("disjoint"),
        );
        let expr = Expr::Lag {
            of: Box::new(Expr::Ref { name: n("amount") }),
            dim: n("Time"),
            n: 1,
        };
        let cell = evaluate_expr(&expr, &tuple, &cube).expect("eval ok");
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
    fn lag_of_const_unchanged() {
        let cube = time_fixture();
        let tuple = slicer(
            &cube,
            Tuple::of(vec![
                mr("Time", "Default", &["2026", "2026-02"]),
                mr("Scenario", "Default", &["Actual"]),
            ])
            .expect("disjoint"),
        );
        let expr = Expr::Lag {
            of: Box::new(Expr::Const { value: 5.0 }),
            dim: n("Time"),
            n: 1,
        };
        let cell = evaluate_expr(&expr, &tuple, &cube).expect("eval ok");
        assert!(
            matches!(cell, Cell::Valid { value, .. } if value == 5.0),
            "expected 5.0 (Const survives Lag), got {cell:?}"
        );
    }

    #[test]
    fn lag_over_unbound_time_returns_missing_not_applicable() {
        let cube = time_fixture();
        // Tuple does NOT pin Time — only Scenario.
        let tuple = slicer(&cube, Tuple::single(mr("Scenario", "Default", &["Actual"])));
        let expr = Expr::Lag {
            of: Box::new(Expr::Ref { name: n("amount") }),
            dim: n("Time"),
            n: 1,
        };
        let cell = evaluate_expr(&expr, &tuple, &cube).expect("eval ok");
        assert!(
            matches!(
                cell,
                Cell::Missing {
                    reason: missing::Reason::NotApplicable
                }
            ),
            "expected Missing(NotApplicable), got {cell:?}"
        );
    }

    #[test]
    fn periods_to_date_sums_months_in_current_year() {
        let cube = time_fixture();
        // Pin Time to 2026-03 (Month), Scenario=Actual. YTD at Year level:
        // 10 + 20 + 30 = 60.
        let tuple = slicer(
            &cube,
            Tuple::of(vec![
                mr("Time", "Default", &["2026", "2026-03"]),
                mr("Scenario", "Default", &["Actual"]),
            ])
            .expect("disjoint"),
        );
        let expr = Expr::PeriodsToDate {
            of: Box::new(Expr::Ref { name: n("amount") }),
            level: n("Year"),
        };
        let cell = evaluate_expr(&expr, &tuple, &cube).expect("eval ok");
        assert!(
            matches!(cell, Cell::Valid { value, .. } if value == 60.0),
            "expected 60, got {cell:?}"
        );
    }

    #[test]
    fn periods_to_date_idempotence() {
        // M6 holds strictly only at the first period of the window: the
        // cumulative sum of a single cumulative sum collapses to that
        // single cumulative sum (triangular-vs-linear accumulation
        // coincides at n = 1). Past the first period, straightforward sum
        // semantics make `PTD(PTD(x, L), L)` the triangular sum — that
        // divergence is a §3.7 conjecture-level concern (the M6 law is
        // listed as "conjectures, not v0.1 commitments") and will be
        // revisited at Phase 5i. Pin to 2026-01 where the equality is
        // arithmetically exact.
        let cube = time_fixture();
        let tuple = slicer(
            &cube,
            Tuple::of(vec![
                mr("Time", "Default", &["2026", "2026-01"]),
                mr("Scenario", "Default", &["Actual"]),
            ])
            .expect("disjoint"),
        );
        let inner = Expr::PeriodsToDate {
            of: Box::new(Expr::Ref { name: n("amount") }),
            level: n("Year"),
        };
        let outer = Expr::PeriodsToDate {
            of: Box::new(inner.clone()),
            level: n("Year"),
        };
        let a = evaluate_expr(&inner, &tuple, &cube).expect("inner");
        let b = evaluate_expr(&outer, &tuple, &cube).expect("outer");
        assert_eq!(
            a, b,
            "PTD(PTD(x, Year), Year) ≡ PTD(x, Year) at the first period of the window"
        );
    }

    #[test]
    fn at_overrides_tuple_coordinate() {
        let cube = time_fixture();
        // Outer tuple pins Scenario=Actual; At pins Scenario=Plan. The Plan
        // value at Time=2026-01 is 100, not 10.
        let tuple = slicer(
            &cube,
            Tuple::of(vec![
                mr("Time", "Default", &["2026", "2026-01"]),
                mr("Scenario", "Default", &["Actual"]),
            ])
            .expect("disjoint"),
        );
        let at = Tuple::single(mr("Scenario", "Default", &["Plan"]));
        let expr = Expr::At {
            of: Box::new(Expr::Ref { name: n("amount") }),
            at,
        };
        let cell = evaluate_expr(&expr, &tuple, &cube).expect("eval ok");
        assert!(
            matches!(cell, Cell::Valid { value, .. } if value == 100.0),
            "expected Plan=100, got {cell:?}"
        );
    }

    #[test]
    fn at_of_unchanged_value_is_value() {
        let cube = time_fixture();
        let tuple = slicer(&cube, Tuple::empty());
        let at = Tuple::single(mr("Scenario", "Default", &["Plan"]));
        let expr = Expr::At {
            of: Box::new(Expr::Const { value: 3.0 }),
            at,
        };
        let cell = evaluate_expr(&expr, &tuple, &cube).expect("eval ok");
        assert!(
            matches!(cell, Cell::Valid { value, .. } if value == 3.0),
            "expected 3.0 regardless of context, got {cell:?}"
        );
    }

    #[test]
    fn metric_refs_another_metric_end_to_end() {
        // ADR = revenue / room_nights_sold. Pin a tuple and assert the
        // division.
        let schema = Schema::builder()
            .dimension(Dimension::regular(n("Geography")).hierarchy(
                Hierarchy::new(n("Default")).level(Level::new(n("Region"), n("region"))),
            ))
            .measure(Measure::new(n("revenue"), Aggregation::sum()))
            .measure(Measure::new(n("room_nights_sold"), Aggregation::sum()))
            .metric(Metric::new(
                n("ADR"),
                Expr::Binary {
                    bin_op: BinOp::Div,
                    l: Box::new(Expr::Ref { name: n("revenue") }),
                    r: Box::new(Expr::Ref {
                        name: n("room_nights_sold"),
                    }),
                },
            ))
            .build()
            .expect("schema");
        let df = df! {
            "region"           => ["EMEA", "EMEA"],
            "revenue"          => [1000.0_f64, 500.0],
            "room_nights_sold" => [10.0_f64, 5.0],
        }
        .expect("frame");
        let cube = InMemoryCube::new(df, schema).expect("cube");
        let tuple = slicer(&cube, Tuple::single(mr("Geography", "Default", &["EMEA"])));
        let cell = evaluate_expr(&Expr::Ref { name: n("ADR") }, &tuple, &cube).expect("eval ok");
        // (1000 + 500) / (10 + 5) = 1500 / 15 = 100.
        assert!(
            matches!(cell, Cell::Valid { value, .. } if value == 100.0),
            "expected ADR=100, got {cell:?}"
        );
    }

    #[test]
    fn metric_cycle_returns_typed_error() {
        // The schema builder rejects metric-to-metric cycles on
        // construction, so we build a schema with a trivial identity
        // metric and then mutate it post-construction to create a 1-hop
        // self-cycle. `Schema`'s fields are `pub` within the crate-root
        // re-export; `InMemoryCube::schema` is `pub(crate)`, so this lives
        // inside the crate.
        let schema = Schema::builder()
            .dimension(Dimension::regular(n("Geography")).hierarchy(
                Hierarchy::new(n("Default")).level(Level::new(n("Region"), n("region"))),
            ))
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .metric(Metric::new(n("Loop"), Expr::Ref { name: n("amount") }))
            .build()
            .expect("schema");
        let df: DataFrame = df! {
            "region" => ["EMEA"],
            "amount" => [1.0_f64],
        }
        .expect("frame");
        let mut cube = InMemoryCube::new(df, schema).expect("cube");
        cube.schema.metrics = vec![Metric::new(n("Loop"), Expr::Ref { name: n("Loop") })];
        let tuple = slicer(&cube, Tuple::empty());
        let err = evaluate_expr(&Expr::Ref { name: n("Loop") }, &tuple, &cube)
            .expect_err("cycle detected");
        match err {
            Error::EvalMetricCycle { name } => assert_eq!(name.as_str(), "Loop"),
            other => panic!("wrong variant: {other:?}"),
        }
    }
}

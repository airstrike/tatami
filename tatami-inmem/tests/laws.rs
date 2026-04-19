//! Phase 5i law proptests — §3.7 of MAP_PLAN.md.
//!
//! Property-based verification of the set / metric algebra conjectures. Each
//! law from §3.7's S1–S10 and M1–M9 tables is encoded as a single proptest
//! that generates two queries related by the law, drives them through
//! [`InMemoryCube::query`], and asserts observational equality of the
//! resulting [`Results`].
//!
//! ## Driving path
//!
//! The spec calls for zero production-code changes. Every law is therefore
//! verified through the public [`Cube::query`] surface:
//!
//! - **Set laws** wrap the set-under-test as the rows axis of an
//!   [`Axes::Series`] or [`Axes::Pivot`] query and compare the resulting
//!   tuple / member sets via [`HashSet`] (order-agnostic, multiset-after-dedup
//!   as §3.7 phrases it).
//! - **CrossJoin-related laws** (S4, S5, S7) can't observe a 2-dim product
//!   through [`Axes::Series`]' flattened x-axis — Series collapses each
//!   tuple to its first member. They use [`Axes::Pivot`] with a trivial
//!   single-element columns axis and compare `row_headers` as a
//!   `HashSet<Vec<MemberRef>>` after sorting each tuple's members by dim
//!   name ([`Tuple`] itself is not `Hash`/`Eq`, so the sorted member
//!   vector is the canonical key).
//! - **Metric laws** wrap the expression-under-test as an ad-hoc [`Metric`]
//!   added to the fixture schema and compare [`scalar::Result`] cell values
//!   at a pinned slicer tuple.
//!
//! ## Case count
//!
//! Every proptest uses [`ProptestConfig::default`] with `cases = 64`. The
//! fixture is tiny (~16 fact rows) so 64 cases per law stay well under a
//! second total while still generating meaningful structural diversity.

use std::collections::HashSet;

use polars_core::df;
use polars_core::prelude::DataFrame;
use proptest::prelude::*;
use tatami::query::{Axes, MemberRef, Options, Path, Predicate, Set, Tuple};
use tatami::schema::metric::{BinOp, Expr};
use tatami::schema::{
    Aggregation, Calendar, Dimension, Hierarchy, Level, Measure, Metric, Name, Schema,
};
use tatami::{Cell, Cube, Query, Results};
use tatami_inmem::InMemoryCube;

mod fixture {
    use super::*;

    /// Convenience `Name::parse` — tests freely unwrap.
    pub fn n(s: &str) -> Name {
        Name::parse(s).expect("valid name")
    }

    /// Geography member ref, Default hierarchy.
    pub fn geo(path: &[&str]) -> MemberRef {
        MemberRef::new(n("Geography"), n("Default"), path_of(path))
    }

    /// Time member ref, Fiscal hierarchy.
    pub fn time_mr(path: &[&str]) -> MemberRef {
        MemberRef::new(n("Time"), n("Fiscal"), path_of(path))
    }

    /// Scenario member ref, Default hierarchy.
    pub fn scen(path: &[&str]) -> MemberRef {
        MemberRef::new(n("Scenario"), n("Default"), path_of(path))
    }

    fn path_of(segs: &[&str]) -> Path {
        let parts: Vec<Name> = segs.iter().copied().map(n).collect();
        Path::parse(parts).expect("non-empty path")
    }

    /// The fact DataFrame shared by [`cube`] and [`cube_with_probe`].
    ///
    /// 4 countries × 3 months × 2 scenarios = 24 rows. Amounts chosen so
    /// Actual and Plan differ and country totals are ordered
    /// US > CA > UK > FR — TopN / Filter-by-metric proptests can rank
    /// the four countries unambiguously.
    fn fact_frame() -> DataFrame {
        df! {
            "region"  => [
                // Month = Jan
                "North","North","South","South","North","North","South","South",
                // Month = Feb
                "North","North","South","South","North","North","South","South",
                // Month = Mar
                "North","North","South","South","North","North","South","South",
            ],
            "country" => [
                "US","CA","UK","FR","US","CA","UK","FR",
                "US","CA","UK","FR","US","CA","UK","FR",
                "US","CA","UK","FR","US","CA","UK","FR",
            ],
            "year"    => [
                "FY2026","FY2026","FY2026","FY2026","FY2026","FY2026","FY2026","FY2026",
                "FY2026","FY2026","FY2026","FY2026","FY2026","FY2026","FY2026","FY2026",
                "FY2026","FY2026","FY2026","FY2026","FY2026","FY2026","FY2026","FY2026",
            ],
            "month"   => [
                "Jan","Jan","Jan","Jan","Jan","Jan","Jan","Jan",
                "Feb","Feb","Feb","Feb","Feb","Feb","Feb","Feb",
                "Mar","Mar","Mar","Mar","Mar","Mar","Mar","Mar",
            ],
            "plan"    => [
                "Actual","Actual","Actual","Actual","Plan","Plan","Plan","Plan",
                "Actual","Actual","Actual","Actual","Plan","Plan","Plan","Plan",
                "Actual","Actual","Actual","Actual","Plan","Plan","Plan","Plan",
            ],
            "amount"  => [
                // Jan Actual
                100.0_f64, 60.0, 40.0, 20.0,
                // Jan Plan
                110.0,     70.0, 50.0, 30.0,
                // Feb Actual
                120.0,     75.0, 45.0, 25.0,
                // Feb Plan
                130.0,     85.0, 55.0, 35.0,
                // Mar Actual
                140.0,     90.0, 50.0, 30.0,
                // Mar Plan
                150.0,    100.0, 60.0, 40.0,
            ],
        }
        .expect("laws fact frame")
    }

    /// Build the fixture schema. `extra_metric` is spliced in alongside
    /// `Revenue` when supplied, giving the metric-law tests a `Probe`
    /// metric to resolve against without mutating shared state.
    fn build_schema(extra_metric: Option<(Name, Expr)>) -> Schema {
        let b = Schema::builder()
            .dimension(
                Dimension::regular(n("Geography")).hierarchy(
                    Hierarchy::new(n("Default"))
                        .level(Level::new(n("Region"), n("region")))
                        .level(Level::new(n("Country"), n("country"))),
                ),
            )
            .dimension(
                Dimension::time(n("Time"), vec![Calendar::gregorian(n("Gregorian"))]).hierarchy(
                    Hierarchy::new(n("Fiscal"))
                        .level(Level::new(n("Year"), n("year")))
                        .level(Level::new(n("Month"), n("month"))),
                ),
            )
            .dimension(
                Dimension::scenario(n("Scenario")).hierarchy(
                    Hierarchy::new(n("Default")).level(Level::new(n("Plan"), n("plan"))),
                ),
            )
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .metric(Metric::new(n("Revenue"), Expr::Ref { name: n("amount") }));
        let b = match extra_metric {
            Some((name, expr)) => b.metric(Metric::new(name, expr)),
            None => b,
        };
        b.build().expect("schema valid")
    }

    /// The small laws fixture. Two regions × two countries = four leaf
    /// Geography members; three months × one year = three leaf Time
    /// members; two scenarios. Small enough to hit 64× per proptest and
    /// still exercise the structural variants.
    ///
    /// Metrics:
    /// - `Revenue` = Ref(amount) — for Filter / TopN predicates and metric
    ///   laws that need a concrete numeric operand.
    pub fn cube() -> InMemoryCube {
        InMemoryCube::new(fact_frame(), build_schema(None)).expect("construct laws cube")
    }

    /// Cube with the metric-under-test spliced in as `Probe`. Each metric
    /// law rebuilds the cube with its chosen [`Expr`] so the proptest can
    /// resolve against the named `Probe` metric without mutating shared
    /// state.
    pub fn cube_with_probe(probe: Expr) -> InMemoryCube {
        InMemoryCube::new(fact_frame(), build_schema(Some((n("Probe"), probe))))
            .expect("reconstruct with probe")
    }

    /// `(region, country)` leaf paths the fixture knows about.
    /// Strategies pick subsets from this list. The fixture has
    /// `Region → Country`, so every Country-level member is a two-segment
    /// path `[region, country]` — not just the country name alone.
    pub const COUNTRY_PATHS: &[(&str, &str)] = &[
        ("North", "US"),
        ("North", "CA"),
        ("South", "UK"),
        ("South", "FR"),
    ];
}

/// Drive a set through [`Axes::Series`] and return the x-axis members as a
/// `HashSet<MemberRef>`. Suitable for single-dim set laws (S1–S3, S6, S8,
/// S9, S10). CrossJoin-related laws use [`evaluate_pivot_rows`] instead.
fn evaluate_series_x(cube: &InMemoryCube, set: Set) -> HashSet<MemberRef> {
    // No ordering constraint — convert to a set for multiset-after-dedup
    // semantics per §3.7. Using an async-aware runtime is cheaper than
    // spinning up tokio::test per proptest case.
    let q = Query {
        axes: Axes::Series { rows: set },
        slicer: Tuple::empty(),
        metrics: vec![fixture::n("Revenue")],
        options: Options::default(),
    };
    let r = futures_block_on(cube.query(&q)).expect("series query ok");
    match r {
        Results::Series(s) => s.x().iter().cloned().collect(),
        other => panic!("expected Series, got {other:?}"),
    }
}

/// Drive a set through the rows axis of an [`Axes::Pivot`] query with a
/// trivial single-tuple columns axis, returning the row-header tuples as
/// a `Vec<Tuple>`. Captures cross-joined 2-dim tuples without the
/// single-member flattening that [`Axes::Series`] imposes. Callers push
/// this through [`normalize_tuples`] to get an order-agnostic hash set.
fn evaluate_pivot_rows(cube: &InMemoryCube, rows: Set) -> Vec<Tuple> {
    // The columns side must be non-empty. Pick a one-member set that
    // addresses a dim disjoint from every axis we generate on `rows` — the
    // Scenario dim is never referenced by the Geography / Time set
    // strategies below.
    let columns = Set::explicit([fixture::scen(&["Actual"])]).expect("non-empty");
    let q = Query {
        axes: Axes::Pivot { rows, columns },
        slicer: Tuple::empty(),
        metrics: vec![fixture::n("Revenue")],
        options: Options::default(),
    };
    let r = futures_block_on(cube.query(&q)).expect("pivot query ok");
    match r {
        Results::Pivot(p) => p.row_headers().to_vec(),
        // `Descendants` on rows can trip the Rollup path — flatten it
        // back to a list of single-member tuples. Set-law strategies
        // below don't hit this branch, so it's defensive.
        Results::Rollup(tree) => {
            let mut out = Vec::new();
            collect_rollup_members(&tree, &mut out);
            out
        }
        other => panic!("expected Pivot/Rollup, got {other:?}"),
    }
}

/// Flatten a rollup tree's every node into a list of single-member
/// tuples. Used only by the defensive branch in [`evaluate_pivot_rows`].
fn collect_rollup_members(tree: &tatami::rollup::Tree, out: &mut Vec<Tuple>) {
    out.push(Tuple::single(tree.root.clone()));
    for child in &tree.children {
        collect_rollup_members(child, out);
    }
}

/// Evaluate a single metric expression at a pinned slicer and return the
/// single-cell `Cell`. Used by the metric-law tests.
fn evaluate_probe_scalar(cube: &InMemoryCube, slicer: Tuple) -> Cell {
    let q = Query {
        axes: Axes::Scalar,
        slicer,
        metrics: vec![fixture::n("Probe")],
        options: Options::default(),
    };
    let r = futures_block_on(cube.query(&q)).expect("scalar query ok");
    match r {
        Results::Scalar(s) => s.values()[0].clone(),
        other => panic!("expected Scalar, got {other:?}"),
    }
}

/// Minimal single-threaded block_on for the proptest bodies. `tokio`
/// macros expand to a multi-threaded runtime per test, which is overkill
/// for 64 cases × ~17 laws. Pull in `std::future` manually.
fn futures_block_on<F: std::future::Future>(fut: F) -> F::Output {
    // Reuse the tokio dev-dep's current-thread runtime — already in the
    // dev deps via `tokio = { workspace = true }`, so this does not pull
    // any new crate.
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime")
        .block_on(fut)
}

/// Pick a 1–4-element subset of [`fixture::COUNTRY_PATHS`] and return it
/// as an `Explicit` Geography/Country set. Each member is a fully-qualified
/// two-segment path `[region, country]` — the Country-level leaf.
fn country_subset() -> impl Strategy<Value = Set> {
    // Sample a bitmask over 4 countries; reject the empty mask so the
    // resulting Explicit set always has ≥1 member (Set::explicit requires
    // non-empty). The upper bound `1..=(1 << 4) - 1` = 1..=15.
    (1usize..=15).prop_map(|mask| {
        let mut members = Vec::new();
        for (i, (region, country)) in fixture::COUNTRY_PATHS.iter().enumerate() {
            if mask & (1 << i) != 0 {
                members.push(fixture::geo(&[region, country]));
            }
        }
        Set::explicit(members).expect("non-empty")
    })
}

/// Pick one of the fixture's month names, as a Time/Fiscal member ref at
/// the Month level (path = `[FY2026, <month>]`).
fn month_member() -> impl Strategy<Value = MemberRef> {
    prop::sample::select(vec!["Jan", "Feb", "Mar"]).prop_map(|m| fixture::time_mr(&["FY2026", m]))
}

/// Strategy over small finite-value `Expr`s suitable as operands in M1/M2.
/// Depth is capped implicitly — no nested Binary — to keep case count
/// predictable.
fn simple_expr() -> impl Strategy<Value = Expr> {
    prop_oneof![
        // `Ref("Revenue")` — resolves to `amount` at the pinned tuple.
        Just(Expr::Ref {
            name: fixture::n("Revenue"),
        }),
        // `Const(v)` with v in a finite, non-zero range so M2 identity
        // tests don't degenerate (Mul by Const-zero collapses to zero,
        // which breaks the x * 1 == x identity when x is the Const).
        (1.0f64..=1_000.0).prop_map(|v| Expr::Const { value: v }),
    ]
}

/// Strategy over small predicate values for S6 / S7 filter laws. Predicates
/// are metric-bearing so the Filter eval exercises its `Cell::Valid` path.
fn simple_predicate() -> impl Strategy<Value = Predicate> {
    (50.0f64..=500.0).prop_map(|threshold| Predicate::Gt {
        metric: fixture::n("Revenue"),
        value: threshold,
    })
}

/// A path-bearing predicate over the Geography dim — scoped to the `a`
/// side in S7's "filter only touches a's dims" antecedent.
fn geo_path_predicate() -> impl Strategy<Value = Predicate> {
    prop::sample::select(vec!["North", "South"]).prop_map(|region| Predicate::In {
        dim: fixture::n("Geography"),
        path_prefix: Path::of(fixture::n(region)),
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// S1. `a.union(b) ≡ b.union(a)` as multisets-after-dedup.
    #[test]
    fn set_union_is_commutative(
        a in country_subset(),
        b in country_subset(),
    ) {
        let cube = fixture::cube();
        let lhs = evaluate_series_x(&cube, a.clone().union(b.clone()));
        let rhs = evaluate_series_x(&cube, b.union(a));
        prop_assert_eq!(lhs, rhs);
    }

    /// S2. `(a ∪ b) ∪ c ≡ a ∪ (b ∪ c)`.
    #[test]
    fn set_union_is_associative(
        a in country_subset(),
        b in country_subset(),
        c in country_subset(),
    ) {
        let cube = fixture::cube();
        let lhs = evaluate_series_x(
            &cube,
            a.clone().union(b.clone()).union(c.clone()),
        );
        let rhs = evaluate_series_x(&cube, a.union(b.union(c)));
        prop_assert_eq!(lhs, rhs);
    }

    /// S3. `a ∪ a ≡ a` — Union deduplicates.
    #[test]
    fn set_union_is_idempotent(a in country_subset()) {
        let cube = fixture::cube();
        let doubled = evaluate_series_x(&cube, a.clone().union(a.clone()));
        let single = evaluate_series_x(&cube, a);
        prop_assert_eq!(doubled, single);
    }

    /// S4. `a × b ≡ b × a` as tuples up to member-order within each
    /// tuple. Compared via [`HashSet<Tuple>`] — but Tuple equality is
    /// order-sensitive on members, so we normalize each tuple's members
    /// to a `BTreeMap<dim, MemberRef>` before hashing via a canonical
    /// `Vec`.
    #[test]
    fn set_crossjoin_is_commutative(
        a in country_subset(),
        b in month_subset(),
    ) {
        let cube = fixture::cube();
        let lhs = normalize_tuples(evaluate_pivot_rows(&cube, a.clone().cross(b.clone())));
        let rhs = normalize_tuples(evaluate_pivot_rows(&cube, b.cross(a)));
        prop_assert_eq!(lhs, rhs);
    }

    /// S5. `(a × b) × c ≡ a × (b × c)`.
    #[test]
    fn set_crossjoin_is_associative(
        a in country_subset(),
        b in month_subset(),
        c in scenario_subset(),
    ) {
        let cube = fixture::cube();
        let lhs = normalize_tuples(evaluate_pivot_rows(
            &cube,
            a.clone().cross(b.clone()).cross(c.clone()),
        ));
        let rhs = normalize_tuples(evaluate_pivot_rows(&cube, a.cross(b.cross(c))));
        prop_assert_eq!(lhs, rhs);
    }

    /// S6. `a.filter(p).filter(p) ≡ a.filter(p)`.
    #[test]
    fn set_filter_is_idempotent(
        a in country_subset(),
        p in simple_predicate(),
    ) {
        let cube = fixture::cube();
        let once = evaluate_series_x(&cube, a.clone().filter(p.clone()));
        let twice = evaluate_series_x(&cube, a.filter(p.clone()).filter(p));
        prop_assert_eq!(once, twice);
    }

    /// S7. Filter push-down: if predicate `p` addresses only `a`'s dim,
    /// then `(a × b).filter(p) ≡ a.filter(p) × b`.
    #[test]
    fn set_filter_over_crossjoin_pushes_down(
        a in country_subset(),
        b in month_subset(),
        p in geo_path_predicate(),
    ) {
        let cube = fixture::cube();
        let lhs = normalize_tuples(evaluate_pivot_rows(
            &cube,
            a.clone().cross(b.clone()).filter(p.clone()),
        ));
        let rhs = normalize_tuples(evaluate_pivot_rows(&cube, a.filter(p).cross(b)));
        prop_assert_eq!(lhs, rhs);
    }

    /// S8. `a.top(n, m).top(k, m) ≡ a.top(min(n, k), m)`.
    #[test]
    fn set_topn_collapses(
        a in country_subset(),
        n in 1u16..=4,
        k in 1u16..=4,
    ) {
        use std::num::NonZeroUsize;
        let cube = fixture::cube();
        let m = fixture::n("Revenue");
        let n_nz = NonZeroUsize::new(usize::from(n)).expect("nonzero");
        let k_nz = NonZeroUsize::new(usize::from(k)).expect("nonzero");
        let min_nz = NonZeroUsize::new(usize::from(n.min(k))).expect("nonzero");
        let lhs = evaluate_series_x(
            &cube,
            a.clone().top(n_nz, m.clone()).top(k_nz, m.clone()),
        );
        let rhs = evaluate_series_x(&cube, a.top(min_nz, m));
        prop_assert_eq!(lhs, rhs);
    }

    /// S9. `(a ∪ b).descendants_to(L) ≡ a.descendants_to(L) ∪ b.descendants_to(L)`.
    ///
    /// Driven at the Region-level inputs with `to_level = Country`, so
    /// descendants produces leaf countries under each named region.
    #[test]
    fn set_descendants_distributes_over_union(
        a in region_subset(),
        b in region_subset(),
    ) {
        let cube = fixture::cube();
        let to = fixture::n("Country");
        let lhs = evaluate_series_x(
            &cube,
            a.clone().union(b.clone()).descendants_to(to.clone()),
        );
        let rhs = evaluate_series_x(
            &cube,
            a.descendants_to(to.clone()).union(b.descendants_to(to)),
        );
        prop_assert_eq!(lhs, rhs);
    }

    /// S10. `a.children() ≡ a.descendants_to(L+1)` where L is `a`'s
    /// level. For Region-level `a`, L+1 is Country — so
    /// `regions.children()` equals `regions.descendants_to(Country)`.
    #[test]
    fn set_children_equals_depth_one_descendants(a in region_subset()) {
        let cube = fixture::cube();
        let lhs = evaluate_series_x(&cube, a.clone().children());
        let rhs = evaluate_series_x(&cube, a.descendants_to(fixture::n("Country")));
        prop_assert_eq!(lhs, rhs);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// M1. `Add` and `Mul` commute — the left and right operand roles are
    /// interchangeable. `Sub` and `Div` intentionally excluded.
    #[test]
    fn metric_binary_add_mul_commute(
        l in simple_expr(),
        r in simple_expr(),
        op in prop_oneof![Just(BinOp::Add), Just(BinOp::Mul)],
    ) {
        let slicer = fixed_slicer();
        let cube_lr = fixture::cube_with_probe(Expr::Binary {
            bin_op: op,
            l: Box::new(l.clone()),
            r: Box::new(r.clone()),
        });
        let cube_rl = fixture::cube_with_probe(Expr::Binary {
            bin_op: op,
            l: Box::new(r),
            r: Box::new(l),
        });
        let lhs = evaluate_probe_scalar(&cube_lr, slicer.clone());
        let rhs = evaluate_probe_scalar(&cube_rl, slicer);
        prop_assert!(cells_eq(&lhs, &rhs), "{lhs:?} != {rhs:?}");
    }

    /// M2. `x + 0 ≡ x` and `x * 1 ≡ x` — the additive / multiplicative
    /// identities. `x` is drawn from `simple_expr`, which is either a
    /// `Ref` or a finite non-zero `Const` — both evaluate to a finite
    /// [`Cell::Valid`] at the fixed slicer.
    #[test]
    fn metric_binary_identity(x in simple_expr()) {
        let slicer = fixed_slicer();
        let baseline = fixture::cube_with_probe(x.clone());
        let via_zero_add = fixture::cube_with_probe(Expr::Binary {
            bin_op: BinOp::Add,
            l: Box::new(x.clone()),
            r: Box::new(Expr::Const { value: 0.0 }),
        });
        let via_one_mul = fixture::cube_with_probe(Expr::Binary {
            bin_op: BinOp::Mul,
            l: Box::new(x),
            r: Box::new(Expr::Const { value: 1.0 }),
        });
        let b = evaluate_probe_scalar(&baseline, slicer.clone());
        let plus_zero = evaluate_probe_scalar(&via_zero_add, slicer.clone());
        let times_one = evaluate_probe_scalar(&via_one_mul, slicer);
        prop_assert!(cells_eq(&b, &plus_zero), "x+0 != x: {b:?} vs {plus_zero:?}");
        prop_assert!(cells_eq(&b, &times_one), "x*1 != x: {b:?} vs {times_one:?}");
    }

    /// M3. `Lag(Lag(x, Time, n), Time, m) ≡ Lag(x, Time, n+m)` when
    /// every intermediate member exists. The fixture's Time dim has
    /// three months Jan/Feb/Mar; sliced at `Mar` with n=1 and m=1 the
    /// composed lag lands at `Jan` — in-range for both forms.
    #[test]
    fn metric_lag_composes(
        n in 0i32..=1,
        m in 0i32..=1,
    ) {
        // Pin the slicer to March so n + m ≤ 2 stays in-range (Jan).
        let slicer = Tuple::of([
            fixture::time_mr(&["FY2026", "Mar"]),
            fixture::scen(&["Actual"]),
            fixture::geo(&["North", "US"]),
        ])
        .expect("distinct dims");

        let x = Expr::Ref {
            name: fixture::n("Revenue"),
        };
        let nested = Expr::Lag {
            of: Box::new(Expr::Lag {
                of: Box::new(x.clone()),
                dim: fixture::n("Time"),
                n,
            }),
            dim: fixture::n("Time"),
            n: m,
        };
        let flat = Expr::Lag {
            of: Box::new(x),
            dim: fixture::n("Time"),
            n: n + m,
        };

        let cube_nested = fixture::cube_with_probe(nested);
        let cube_flat = fixture::cube_with_probe(flat);
        let nested_cell = evaluate_probe_scalar(&cube_nested, slicer.clone());
        let flat_cell = evaluate_probe_scalar(&cube_flat, slicer);
        prop_assert!(
            cells_eq(&nested_cell, &flat_cell),
            "nested={nested_cell:?}, flat={flat_cell:?}"
        );
    }

    /// M4. `Lag(Const(v), D, n) ≡ Const(v)` — a constant is invariant
    /// under time shifts. The catalogue orders the fixture's months
    /// alphabetically (`Feb < Jan < Mar`), so pinning at `Jan`
    /// (position 1 of 3) lets `n ∈ [-1, 1]` stay in range. The
    /// reference impl's `Lag` evaluator short-circuits to `Missing`
    /// when the shift falls outside the level — a defensible
    /// conservative choice but it means M4 only holds on in-range
    /// shifts.
    #[test]
    fn metric_lag_of_const_is_const(
        v in -1_000.0f64..=1_000.0,
        n in -1i32..=1,
    ) {
        let slicer = Tuple::of([
            fixture::time_mr(&["FY2026", "Jan"]),
            fixture::scen(&["Actual"]),
        ])
        .expect("distinct dims");

        let cube_lagged = fixture::cube_with_probe(Expr::Lag {
            of: Box::new(Expr::Const { value: v }),
            dim: fixture::n("Time"),
            n,
        });
        let cube_const = fixture::cube_with_probe(Expr::Const { value: v });
        let lagged = evaluate_probe_scalar(&cube_lagged, slicer.clone());
        let plain = evaluate_probe_scalar(&cube_const, slicer);
        prop_assert!(cells_eq(&lagged, &plain));
    }

    /// M5. `Lag(x, D, 0) ≡ x` — the zero shift is identity.
    #[test]
    fn metric_lag_zero_is_identity(x in simple_expr()) {
        let slicer = Tuple::of([
            fixture::time_mr(&["FY2026", "Feb"]),
            fixture::scen(&["Actual"]),
            fixture::geo(&["North", "US"]),
        ])
        .expect("distinct dims");

        let cube_lag0 = fixture::cube_with_probe(Expr::Lag {
            of: Box::new(x.clone()),
            dim: fixture::n("Time"),
            n: 0,
        });
        let cube_plain = fixture::cube_with_probe(x);
        let lag0 = evaluate_probe_scalar(&cube_lag0, slicer.clone());
        let plain = evaluate_probe_scalar(&cube_plain, slicer);
        prop_assert!(cells_eq(&lag0, &plain));
    }

    // M6: struck — see MAP §3.7 (PTD idempotence fails under sum
    // semantics; the conjecture itself was wrong). No test.

    /// M7. `At(x, t)` evaluated at any `t'` equals `x` evaluated at `t`
    /// — `At` pins the context, discarding the outer tuple.
    #[test]
    fn metric_at_pins_context(
        outer_month in month_member(),
        pinned_month in month_member(),
    ) {
        // The outer slicer we evaluate at — varies.
        let outer_slicer = Tuple::of([outer_month.clone(), fixture::scen(&["Actual"])])
            .expect("distinct dims");
        // The pinned tuple — also varies.
        let pinned = Tuple::of([pinned_month, fixture::scen(&["Actual"])])
            .expect("distinct dims");

        let x = Expr::Ref {
            name: fixture::n("Revenue"),
        };
        let cube_at = fixture::cube_with_probe(Expr::At {
            of: Box::new(x.clone()),
            at: pinned.clone(),
        });
        let cube_direct = fixture::cube_with_probe(x);
        let at_outer = evaluate_probe_scalar(&cube_at, outer_slicer);
        let direct = evaluate_probe_scalar(&cube_direct, pinned);
        prop_assert!(
            cells_eq(&at_outer, &direct),
            "At(outer)={at_outer:?}, direct(pinned)={direct:?}"
        );
    }

    /// M8. `Div(x, Const(0))` yields [`Cell::Error`].
    #[test]
    fn metric_divide_by_zero_is_error(x in simple_expr()) {
        let slicer = fixed_slicer();
        let cube = fixture::cube_with_probe(Expr::Binary {
            bin_op: BinOp::Div,
            l: Box::new(x),
            r: Box::new(Expr::Const { value: 0.0 }),
        });
        let cell = evaluate_probe_scalar(&cube, slicer);
        prop_assert!(matches!(cell, Cell::Error { .. }), "got {cell:?}");
    }

    /// M9. Missing propagates through Binary — `Binary(op, Missing, _)`
    /// yields `Missing`. Synthesize a `Missing` operand via an
    /// out-of-range `Lag` (shift beyond the catalogue's 3-month Time
    /// level), then assert `Add(missing, other)` is `Missing`.
    #[test]
    fn metric_missing_propagates_through_binary(other in simple_expr()) {
        // Pin at Feb (catalogue-position 0 alphabetically); Lag with
        // n=5 shifts offset=-5 which falls outside the three-month
        // range → [`Cell::Missing`].
        let slicer = Tuple::of([
            fixture::geo(&["North", "US"]),
            fixture::time_mr(&["FY2026", "Feb"]),
            fixture::scen(&["Actual"]),
        ])
        .expect("distinct dims");

        let missing_expr = Expr::Lag {
            of: Box::new(Expr::Ref {
                name: fixture::n("Revenue"),
            }),
            dim: fixture::n("Time"),
            n: 5,
        };

        // Sanity-check the antecedent: the lagged expression really does
        // evaluate to `Missing` at this slicer. If the catalogue grew a
        // fifth month tomorrow the antecedent would fail and this law
        // would vacuously pass — `prop_assume!` fails the case in that
        // world so we notice.
        let cube_missing = fixture::cube_with_probe(missing_expr.clone());
        let baseline = evaluate_probe_scalar(&cube_missing, slicer.clone());
        prop_assume!(matches!(baseline, Cell::Missing { .. }));

        let cube_sum = fixture::cube_with_probe(Expr::Binary {
            bin_op: BinOp::Add,
            l: Box::new(missing_expr),
            r: Box::new(other),
        });
        let summed = evaluate_probe_scalar(&cube_sum, slicer);
        prop_assert!(matches!(summed, Cell::Missing { .. }), "got {summed:?}");
    }
}

/// 1..=3 regions as an `Explicit` Geography/Region set. Two regions in
/// the fixture (North, South); at least one must be selected.
fn region_subset() -> impl Strategy<Value = Set> {
    (1usize..=3).prop_map(|mask| {
        let mut members = Vec::new();
        if mask & 0b01 != 0 {
            members.push(fixture::geo(&["North"]));
        }
        if mask & 0b10 != 0 {
            members.push(fixture::geo(&["South"]));
        }
        Set::explicit(members).expect("non-empty")
    })
}

/// 1..=3 months as an `Explicit` Time/Month set.
fn month_subset() -> impl Strategy<Value = Set> {
    (1usize..=7).prop_map(|mask| {
        let mut members = Vec::new();
        if mask & 0b001 != 0 {
            members.push(fixture::time_mr(&["FY2026", "Jan"]));
        }
        if mask & 0b010 != 0 {
            members.push(fixture::time_mr(&["FY2026", "Feb"]));
        }
        if mask & 0b100 != 0 {
            members.push(fixture::time_mr(&["FY2026", "Mar"]));
        }
        Set::explicit(members).expect("non-empty")
    })
}

/// 1..=2 scenarios as an `Explicit` Scenario set.
fn scenario_subset() -> impl Strategy<Value = Set> {
    (1usize..=3).prop_map(|mask| {
        let mut members = Vec::new();
        if mask & 0b01 != 0 {
            members.push(fixture::scen(&["Actual"]));
        }
        if mask & 0b10 != 0 {
            members.push(fixture::scen(&["Plan"]));
        }
        Set::explicit(members).expect("non-empty")
    })
}

/// Canonicalize a list of [`Tuple`] values into an order-agnostic
/// [`HashSet`]. [`Tuple`] is not `Hash`/`Eq` (its members carry an
/// arbitrary order), but §3.7 S4 / S5 want "as tuples up to axis
/// order" — sort the inner members by dim name and key the hash set on
/// the resulting `Vec<MemberRef>`.
fn normalize_tuples(tuples: Vec<Tuple>) -> HashSet<Vec<MemberRef>> {
    tuples
        .into_iter()
        .map(|t| {
            let mut ms: Vec<MemberRef> = t.members().to_vec();
            ms.sort_by(|a, b| a.dim.as_str().cmp(b.dim.as_str()));
            ms
        })
        .collect()
}

/// Slicer used by metric laws that do not themselves vary the outer
/// tuple. Pins all three dims to a single valid cell in the fixture.
fn fixed_slicer() -> Tuple {
    Tuple::of([
        fixture::geo(&["North", "US"]),
        fixture::time_mr(&["FY2026", "Feb"]),
        fixture::scen(&["Actual"]),
    ])
    .expect("distinct dims")
}

/// Observational cell equality: two cells match when
/// - both `Valid` and values are close (avoids f64 drift under
///   associative rearrangement);
/// - both `Missing` (reason not compared — the M9 law only asserts the
///   propagation, not which reason wins);
/// - both `Error` (message not compared — M8 only asserts the error
///   classification).
fn cells_eq(l: &Cell, r: &Cell) -> bool {
    match (l, r) {
        (Cell::Valid { value: lv, .. }, Cell::Valid { value: rv, .. }) => {
            (lv - rv).abs() <= 1e-9 * lv.abs().max(rv.abs()).max(1.0)
        }
        (Cell::Missing { .. }, Cell::Missing { .. }) => true,
        (Cell::Error { .. }, Cell::Error { .. }) => true,
        _ => false,
    }
}

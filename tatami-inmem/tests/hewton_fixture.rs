//! Phase 5h end-to-end integration tests.
//!
//! Separate from the inline unit tests in `src/**`: these exercise
//! `InMemoryCube` via the public `Cube` trait surface, with self-contained
//! fixtures the tests can assert exact values against. The first test —
//! [`semi_additive_stock_rolls_up_via_last_not_sum_end_to_end`] — is the
//! load-bearing one (MAP §8 R3): if it ever passes with SUM=300 instead of
//! LAST=80, the rollup path is broken in exactly the silent-wrong-answer
//! way the design calls out.

use std::num::NonZeroUsize;

use polars_core::df;
use polars_core::prelude::DataFrame;
use tatami::query::{Axes, MemberRef, Options, OrderBy, Path, Set, Tuple};
use tatami::schema::metric::{BinOp, Expr};
use tatami::schema::{
    Aggregation, Calendar, Dimension, Hierarchy, Level, Measure, Metric, Name, Schema, SemiAgg,
};
use tatami::{Cell, Cube, Query, Results};
use tatami_inmem::InMemoryCube;

mod fixture {
    use super::*;

    /// Convenience name parser — tests freely unwrap.
    pub fn n(s: &str) -> Name {
        Name::parse(s).expect("valid name")
    }

    /// Build a single-segment member ref.
    pub fn mr(dim: &str, hier: &str, segs: &[&str]) -> MemberRef {
        let names: Vec<Name> = segs.iter().copied().map(n).collect();
        MemberRef::new(n(dim), n(hier), Path::parse(names).expect("non-empty"))
    }

    /// Main 14-day fixture.
    ///
    /// Schema:
    /// - Geography: Default hierarchy `Region → Country → State` —
    ///   EMEA/UK/LON, Americas/US/CA, Americas/US/NY.
    /// - Time: Fiscal hierarchy `Year → Quarter → Month`.
    /// - Scenario: Default hierarchy `Plan`.
    /// - Measures: `amount` (Sum), `units` (Sum).
    /// - Metrics: `Revenue` = Ref(amount).
    ///
    /// Fact shape: 14 rows spanning FY2025 + FY2026. Each row pins every
    /// dim down to leaf level, so `Descendants(Year→Quarter)` evaluates
    /// against a real catalogue tree.
    pub fn cube() -> InMemoryCube {
        let schema = Schema::builder()
            .dimension(
                Dimension::regular(n("Geography")).hierarchy(
                    Hierarchy::new(n("Default"))
                        .level(Level::new(n("Region"), n("region")))
                        .level(Level::new(n("Country"), n("country")))
                        .level(Level::new(n("State"), n("state"))),
                ),
            )
            .dimension(
                Dimension::time(n("Time"), vec![Calendar::gregorian(n("Gregorian"))]).hierarchy(
                    Hierarchy::new(n("Fiscal"))
                        .level(Level::new(n("Year"), n("year")))
                        .level(Level::new(n("Quarter"), n("quarter")))
                        .level(Level::new(n("Month"), n("month"))),
                ),
            )
            .dimension(
                Dimension::scenario(n("Scenario")).hierarchy(
                    Hierarchy::new(n("Default")).level(Level::new(n("Plan"), n("plan"))),
                ),
            )
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .measure(Measure::new(n("units"), Aggregation::sum()))
            .metric(Metric::new(n("Revenue"), Expr::Ref { name: n("amount") }))
            .build()
            .expect("schema valid");

        // 14 rows. FY2025/Q4/Mar: 1 row. FY2026: 13 rows across Q1–Q4, a
        // mix of regions and scenarios. Amounts are picked so quarterly
        // subtotals are easy to verify by hand:
        //   FY2025 Q4 Mar: 50.
        //   FY2026 Q1:  Jan=100 + Feb=200 + Mar=300         = 600.
        //   FY2026 Q2:  Apr=400 + May=500 + Jun=100         = 1000.
        //   FY2026 Q3:  Jul=700 + Aug=800                   = 1500.
        //   FY2026 Q4:  Oct=900 + Nov=50 + Dec=25  + Nov=50 = 1025.
        //   Total FY2026 Actual:       600 + 1000 + 1500 + 1025 = 4125.
        //   Plus Plan row: 2000.
        let df: DataFrame = df! {
            "region"   => [
                "EMEA", "Americas", "Americas", "Americas", "Americas",
                "Americas", "EMEA",     "Americas", "Americas", "EMEA",
                "Americas", "Americas", "Americas", "Americas",
            ],
            "country"  => [
                "UK", "US", "US", "US", "US",
                "US", "UK", "US", "US", "UK",
                "US", "US", "US", "US",
            ],
            "state"    => [
                "LON", "CA", "NY", "CA", "NY",
                "CA",  "LON","NY", "CA", "LON",
                "CA",  "NY", "NY", "CA",
            ],
            "year"     => [
                "FY2025", "FY2026", "FY2026", "FY2026", "FY2026",
                "FY2026", "FY2026", "FY2026", "FY2026", "FY2026",
                "FY2026", "FY2026", "FY2026", "FY2026",
            ],
            "quarter"  => [
                "Q4", "Q1", "Q1", "Q1", "Q2",
                "Q2", "Q2", "Q3", "Q3", "Q4",
                "Q4", "Q4", "Q4", "Q1",
            ],
            "month"    => [
                "Mar",  "Jan", "Feb", "Mar", "Apr",
                "May",  "Jun", "Jul", "Aug", "Oct",
                "Nov",  "Dec", "Nov", "Jan",
            ],
            "plan"     => [
                "Actual", "Actual", "Actual", "Actual", "Actual",
                "Actual", "Actual", "Actual", "Actual", "Actual",
                "Actual", "Actual", "Plan",   "Plan",
            ],
            "amount"   => [
                 50.0_f64, 100.0, 200.0, 300.0, 400.0,
                500.0,     100.0, 700.0, 800.0, 900.0,
                 50.0,      25.0,  50.0, 2000.0,
            ],
            "units"    => [
                 1.0_f64, 10.0, 20.0, 30.0, 40.0,
                 50.0,    10.0, 70.0, 80.0, 90.0,
                  5.0,     3.0,  5.0, 200.0,
            ],
        }
        .expect("fact frame");

        InMemoryCube::new(df, schema).expect("construct cube")
    }

    /// Semi-additive fixture. Single Time dim (Fiscal: Year → Quarter →
    /// Month); `stock` measure declared [`Aggregation::SemiAdditive`]
    /// over Time with [`SemiAgg::Last`]. Three months, three snapshots —
    /// 100, 120, 80. LAST-child rollup picks 80; the naive SUM trap
    /// returns 300.
    pub fn stock_cube() -> InMemoryCube {
        let schema = Schema::builder()
            .dimension(
                Dimension::time(n("Time"), vec![Calendar::gregorian(n("Gregorian"))]).hierarchy(
                    Hierarchy::new(n("Fiscal"))
                        .level(Level::new(n("Year"), n("year")))
                        .level(Level::new(n("Quarter"), n("quarter")))
                        .level(Level::new(n("Month"), n("month"))),
                ),
            )
            .measure(Measure::new(
                n("stock"),
                Aggregation::semi_additive(vec![n("Time")], SemiAgg::Last).expect("non-empty"),
            ))
            .build()
            .expect("schema valid");

        let df: DataFrame = df! {
            "year"    => ["FY2026", "FY2026", "FY2026"],
            "quarter" => ["Q1",     "Q1",     "Q1"],
            "month"   => ["Jan",    "Feb",    "Mar"],
            "stock"   => [100.0_f64, 120.0, 80.0],
        }
        .expect("stock frame");
        InMemoryCube::new(df, schema).expect("construct stock cube")
    }

    /// YoY / MoM / YTD fixture. Time (Year → Quarter → Month) + Scenario
    /// + `amount` + derived metrics that compose Lag / PeriodsToDate.
    ///
    /// **Leaf keys are fully-qualified** — `Jan2025`, `Jul2025`,
    /// `Jan2026`, `Jul2026`, `Oct2026`. The backend's
    /// [`filter_by_tuple`] keys only on the leaf level (see
    /// `deep_path_filters_on_leaf_level_only` in `eval::tuple`), so
    /// disambiguating by year at the month level is required to express
    /// year-crossing Lag / YTD semantics deterministically.
    ///
    /// Fact rows arrange so:
    /// - FY2025 Actual total = 1_000_000 (500k + 500k).
    /// - FY2026 Actual total = 1_200_000 (400k + 500k + 300k).
    /// - Plan rows carry deterministic Jan / Q1 values for the Variance
    ///   metric.
    pub fn yoy_cube() -> InMemoryCube {
        let schema = Schema::builder()
            .dimension(
                Dimension::time(n("Time"), vec![Calendar::gregorian(n("Gregorian"))]).hierarchy(
                    Hierarchy::new(n("Fiscal"))
                        .level(Level::new(n("Year"), n("year")))
                        .level(Level::new(n("Quarter"), n("quarter")))
                        .level(Level::new(n("Month"), n("month"))),
                ),
            )
            .dimension(
                Dimension::scenario(n("Scenario")).hierarchy(
                    Hierarchy::new(n("Default")).level(Level::new(n("Plan"), n("plan"))),
                ),
            )
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .metric(Metric::new(n("Revenue"), Expr::Ref { name: n("amount") }))
            // RevenueYoY = (Revenue - Lag(Revenue, Time, 1)) / Lag(Revenue, Time, 1).
            .metric(Metric::new(
                n("RevenueYoY"),
                Expr::Binary {
                    bin_op: BinOp::Div,
                    l: Box::new(Expr::Binary {
                        bin_op: BinOp::Sub,
                        l: Box::new(Expr::Ref { name: n("Revenue") }),
                        r: Box::new(Expr::Lag {
                            of: Box::new(Expr::Ref { name: n("Revenue") }),
                            dim: n("Time"),
                            n: 1,
                        }),
                    }),
                    r: Box::new(Expr::Lag {
                        of: Box::new(Expr::Ref { name: n("Revenue") }),
                        dim: n("Time"),
                        n: 1,
                    }),
                },
            ))
            // RevenueYTD = PeriodsToDate(Revenue, Year).
            .metric(Metric::new(
                n("RevenueYTD"),
                Expr::PeriodsToDate {
                    of: Box::new(Expr::Ref { name: n("Revenue") }),
                    level: n("Year"),
                },
            ))
            // Variance = At(Revenue, Scenario=Actual) - At(Revenue, Scenario=Plan).
            .metric(Metric::new(
                n("Variance"),
                Expr::Binary {
                    bin_op: BinOp::Sub,
                    l: Box::new(Expr::At {
                        of: Box::new(Expr::Ref { name: n("Revenue") }),
                        at: Tuple::single(MemberRef::new(
                            n("Scenario"),
                            n("Default"),
                            Path::of(n("Actual")),
                        )),
                    }),
                    r: Box::new(Expr::At {
                        of: Box::new(Expr::Ref { name: n("Revenue") }),
                        at: Tuple::single(MemberRef::new(
                            n("Scenario"),
                            n("Default"),
                            Path::of(n("Plan")),
                        )),
                    }),
                },
            ))
            .build()
            .expect("yoy schema valid");

        // FY2025 Actual: Q1/Jan2025=500_000, Q2/Jul2025=500_000      (sum 1M).
        // FY2026 Actual: Q1/Jan2026=400_000, Q2/Jul2026=500_000,
        //                Q3/Oct2026=300_000                           (sum 1.2M).
        // FY2025 Plan:   Q1/Jan2025=900_000.
        // FY2026 Plan:   Q1/Jan2026=1_100_000.
        //
        // BTreeMap-ordered month traversal in the catalogue goes alphabetic
        // on the month key: `Jan2025, Jan2026, Jul2025, Jul2026, Oct2026`.
        // That's the `Lag` ordering — Jan2026 lags back one to Jan2025 by
        // sibling-offset through the Month level.
        let df: DataFrame = df! {
            "year"    => ["FY2025",  "FY2026",  "FY2025",  "FY2026",  "FY2026",  "FY2025",  "FY2026"],
            "quarter" => ["Q1",      "Q1",      "Q2",      "Q2",      "Q3",      "Q1",      "Q1"],
            "month"   => ["Jan2025", "Jan2026", "Jul2025", "Jul2026", "Oct2026", "Jan2025", "Jan2026"],
            "plan"    => ["Actual",  "Actual",  "Actual",  "Actual",  "Actual",  "Plan",    "Plan"],
            "amount"  => [
                500_000.0_f64, 400_000.0, 500_000.0, 500_000.0, 300_000.0,
                900_000.0,     1_100_000.0,
            ],
        }
        .expect("yoy frame");
        InMemoryCube::new(df, schema).expect("construct yoy cube")
    }
}

// ── 1. Semi-additive end-to-end — the load-bearing test (MAP §8 R3) ──────

#[tokio::test]
async fn semi_additive_stock_rolls_up_via_last_not_sum_end_to_end() {
    // Three monthly stock snapshots — 100, 120, 80. The naive additive
    // path would return 300; semi-additive LAST-over-Time picks 80. If
    // this test ever asserts 300 and passes, the semi-additive rollup is
    // broken in exactly the way MAP §8 R3 warns about.
    let cube = fixture::stock_cube();
    let q = Query {
        axes: Axes::Scalar,
        // Empty slicer — Time is unbound, so the rollup rule fires.
        slicer: Tuple::empty(),
        metrics: vec![fixture::n("stock")],
        options: Options::default(),
    };
    let r = cube.query(&q).await.expect("query ok");
    match r {
        Results::Scalar(s) => {
            assert_eq!(s.values().len(), 1);
            match &s.values()[0] {
                Cell::Valid { value, .. } => {
                    assert_eq!(
                        *value, 80.0,
                        "semi-additive LAST must pick Mar snapshot (80), not sum (300)"
                    );
                }
                other => panic!("expected Valid, got {other:?}"),
            }
        }
        other => panic!("expected Scalar, got {other:?}"),
    }
}

#[tokio::test]
async fn semi_additive_stock_pinned_to_month_returns_that_month_value() {
    // Pinning Time=FY2026/Q1/Feb binds the only non-additive dim, so the
    // rollup collapses to a single-group sum — that group is the single
    // Feb row → 120.
    let cube = fixture::stock_cube();
    let q = Query {
        axes: Axes::Scalar,
        slicer: Tuple::single(fixture::mr("Time", "Fiscal", &["FY2026", "Q1", "Feb"])),
        metrics: vec![fixture::n("stock")],
        options: Options::default(),
    };
    let r = cube.query(&q).await.expect("query ok");
    match r {
        Results::Scalar(s) => match &s.values()[0] {
            Cell::Valid { value, .. } => assert_eq!(*value, 120.0),
            other => panic!("expected Valid 120, got {other:?}"),
        },
        other => panic!("expected Scalar, got {other:?}"),
    }
}

// ── 2. The four §3.5 example queries ─────────────────────────────────────

#[tokio::test]
async fn fy2026_revenue_scalar_returns_expected_total() {
    // §3.5(a): Revenue for FY2026/Actual. Fixture arithmetic: 100+200+300
    // (Q1) + 400+500+100 (Q2) + 700+800 (Q3) + 900+50+25 (Q4 Actual) =
    // 4_075. Plan rows and FY2025 are excluded by the slicer.
    let cube = fixture::cube();
    let q = Query {
        axes: Axes::Scalar,
        slicer: Tuple::of([
            fixture::mr("Time", "Fiscal", &["FY2026"]),
            MemberRef::new(
                fixture::n("Scenario"),
                fixture::n("Default"),
                Path::of(fixture::n("Actual")),
            ),
        ])
        .expect("disjoint"),
        metrics: vec![fixture::n("Revenue")],
        options: Options::default(),
    };
    let r = cube.query(&q).await.expect("query ok");
    match r {
        Results::Scalar(s) => {
            assert_eq!(s.values().len(), 1);
            match &s.values()[0] {
                Cell::Valid { value, .. } => assert_eq!(*value, 4_075.0),
                other => panic!("expected Valid, got {other:?}"),
            }
        }
        other => panic!("expected Scalar, got {other:?}"),
    }
}

#[tokio::test]
async fn quarterly_revenue_by_region_pivot_returns_grid() {
    // §3.5(b)-shaped: a single Time root drilled via
    // `descendants_to(Month)` — rollup assembly anchors on a single
    // in-set root and nests the rest by path prefix, producing a real
    // Year → Quarter → Month tree. (Multi-root Range sources collapse
    // the tree under the current single-root assembler; see §5 notes in
    // the Phase 5h report.)
    let cube = fixture::cube();
    let q = Query {
        axes: Axes::Pivot {
            rows: Set::from_member(fixture::mr("Time", "Fiscal", &["FY2026"]))
                .descendants_to(fixture::n("Month")),
            columns: Set::members(
                fixture::n("Geography"),
                fixture::n("Default"),
                fixture::n("Region"),
            ),
        },
        slicer: Tuple::single(MemberRef::new(
            fixture::n("Scenario"),
            fixture::n("Default"),
            Path::of(fixture::n("Actual")),
        )),
        metrics: vec![fixture::n("Revenue")],
        options: Options::default(),
    };
    let r = cube.query(&q).await.expect("query ok");
    match r {
        Results::Rollup(tree) => {
            assert!(
                !tree.children.is_empty(),
                "descendants must produce a non-trivial tree, got {tree:?}"
            );
        }
        other => panic!("expected Rollup, got {other:?}"),
    }
}

#[tokio::test]
async fn plan_vs_whatif_variance_pivot_returns_grid() {
    // §3.5(c)-shaped: rows = Geography Region; columns = explicit
    // [Actual, Plan]. Two metrics (amount, units) widen the col_headers
    // by metric-count. We don't have WhatIf in the fixture, so we use
    // the real scenarios — the shape assertion is what matters.
    let cube = fixture::cube();
    let q = Query {
        axes: Axes::Pivot {
            rows: Set::members(
                fixture::n("Geography"),
                fixture::n("Default"),
                fixture::n("Region"),
            ),
            columns: Set::explicit([
                MemberRef::new(
                    fixture::n("Scenario"),
                    fixture::n("Default"),
                    Path::of(fixture::n("Actual")),
                ),
                MemberRef::new(
                    fixture::n("Scenario"),
                    fixture::n("Default"),
                    Path::of(fixture::n("Plan")),
                ),
            ])
            .expect("non-empty"),
        },
        slicer: Tuple::single(fixture::mr("Time", "Fiscal", &["FY2026"])),
        metrics: vec![fixture::n("amount"), fixture::n("units")],
        options: Options::default(),
    };
    let r = cube.query(&q).await.expect("query ok");
    match r {
        Results::Pivot(p) => {
            // 2 regions → 2 row_headers; 2 scenarios × 2 metrics = 4
            // col_headers; cells is 2×4.
            assert_eq!(p.row_headers().len(), 2, "two regions");
            assert_eq!(p.col_headers().len(), 4, "2 scenarios × 2 metrics");
            assert_eq!(p.cells().len(), 2);
            assert_eq!(p.cells()[0].len(), 4);
        }
        other => panic!("expected Pivot, got {other:?}"),
    }
}

#[tokio::test]
async fn sales_by_territory_series_returns_rows() {
    // §3.5(d)-shaped: Series with rows = Geography.Country. Assert x
    // count matches the fixture's unique countries (UK, US = 2).
    let cube = fixture::cube();
    let q = Query {
        axes: Axes::Series {
            rows: Set::members(
                fixture::n("Geography"),
                fixture::n("Default"),
                fixture::n("Country"),
            ),
        },
        slicer: Tuple::of([
            fixture::mr("Time", "Fiscal", &["FY2026"]),
            MemberRef::new(
                fixture::n("Scenario"),
                fixture::n("Default"),
                Path::of(fixture::n("Actual")),
            ),
        ])
        .expect("disjoint"),
        metrics: vec![fixture::n("units")],
        options: Options::default(),
    };
    let r = cube.query(&q).await.expect("query ok");
    match r {
        Results::Series(s) => {
            assert_eq!(s.x().len(), 2, "two countries: UK, US");
            assert_eq!(s.rows().len(), 1, "one metric: units");
        }
        other => panic!("expected Series, got {other:?}"),
    }
}

// ── 3. Metric composition tests ──────────────────────────────────────────

#[tokio::test]
async fn yoy_revenue_at_fy2026_jan_equals_hand_computed_growth() {
    // FY2025 Jan Revenue Actual = 500_000; FY2026 Jan Revenue Actual =
    // 400_000. YoY at FY2026/Jan = (400k - 500k) / 500k = -0.2.
    let cube = fixture::yoy_cube();
    let q = Query {
        axes: Axes::Scalar,
        slicer: Tuple::of([
            fixture::mr("Time", "Fiscal", &["FY2026", "Q1", "Jan2026"]),
            MemberRef::new(
                fixture::n("Scenario"),
                fixture::n("Default"),
                Path::of(fixture::n("Actual")),
            ),
        ])
        .expect("disjoint"),
        metrics: vec![fixture::n("RevenueYoY")],
        options: Options::default(),
    };
    let r = cube.query(&q).await.expect("query ok");
    match r {
        Results::Scalar(s) => match &s.values()[0] {
            Cell::Valid { value, .. } => {
                assert!(
                    (*value - (-0.2)).abs() < 1e-9,
                    "YoY expected -0.2, got {value}"
                );
            }
            other => panic!("expected Valid YoY, got {other:?}"),
        },
        other => panic!("expected Scalar, got {other:?}"),
    }
}

#[tokio::test]
async fn revenue_at_prior_fy_month_returns_fy2025_value_not_current() {
    // Cross-fiscal-year lookup through a pinned tuple: evaluating
    // `Revenue` at `Time=FY2025/Q2/Jul2025` must reach the FY2025 fact
    // row (500k), not accidentally pick up any FY2026 data. Guards
    // against a whole class of "stale year" bugs in tuple filtering.
    let cube = fixture::yoy_cube();
    let q = Query {
        axes: Axes::Scalar,
        slicer: Tuple::of([
            fixture::mr("Time", "Fiscal", &["FY2025", "Q2", "Jul2025"]),
            MemberRef::new(
                fixture::n("Scenario"),
                fixture::n("Default"),
                Path::of(fixture::n("Actual")),
            ),
        ])
        .expect("disjoint"),
        metrics: vec![fixture::n("Revenue")],
        options: Options::default(),
    };
    let r = cube.query(&q).await.expect("query ok");
    match r {
        Results::Scalar(s) => match &s.values()[0] {
            Cell::Valid { value, .. } => assert_eq!(*value, 500_000.0),
            other => panic!("expected Valid Revenue, got {other:?}"),
        },
        other => panic!("expected Scalar, got {other:?}"),
    }
}

#[tokio::test]
async fn ytd_revenue_accumulates_across_months_in_year() {
    // At FY2026/Q3/Oct with Actual, RevenueYTD = PeriodsToDate(Revenue,
    // Year) = sum of all FY2026 months up to and including Oct. The
    // fixture's FY2026 Actual months at-or-before Oct in catalogue order
    // are Jan (400k), Jul (500k), Oct (300k) → 1_200_000.
    let cube = fixture::yoy_cube();
    let q = Query {
        axes: Axes::Scalar,
        slicer: Tuple::of([
            fixture::mr("Time", "Fiscal", &["FY2026", "Q3", "Oct2026"]),
            MemberRef::new(
                fixture::n("Scenario"),
                fixture::n("Default"),
                Path::of(fixture::n("Actual")),
            ),
        ])
        .expect("disjoint"),
        metrics: vec![fixture::n("RevenueYTD")],
        options: Options::default(),
    };
    let r = cube.query(&q).await.expect("query ok");
    match r {
        Results::Scalar(s) => match &s.values()[0] {
            Cell::Valid { value, .. } => assert_eq!(*value, 1_200_000.0),
            other => panic!("expected Valid YTD, got {other:?}"),
        },
        other => panic!("expected Scalar, got {other:?}"),
    }
}

#[tokio::test]
async fn variance_equals_actual_minus_plan_at_each_cell() {
    // At FY2026/Q1/Jan, Revenue Actual = 400_000 and Revenue Plan =
    // 1_100_000. Variance = Actual - Plan = -700_000.
    let cube = fixture::yoy_cube();
    let q = Query {
        axes: Axes::Scalar,
        slicer: Tuple::single(fixture::mr("Time", "Fiscal", &["FY2026", "Q1", "Jan2026"])),
        metrics: vec![fixture::n("Variance")],
        options: Options::default(),
    };
    let r = cube.query(&q).await.expect("query ok");
    match r {
        Results::Scalar(s) => match &s.values()[0] {
            Cell::Valid { value, .. } => assert_eq!(*value, -700_000.0),
            other => panic!("expected Valid Variance, got {other:?}"),
        },
        other => panic!("expected Scalar, got {other:?}"),
    }
}

// ── 4. Resolve-layer tests — errors surface before eval fires ────────────

#[tokio::test]
async fn query_with_unknown_metric_ref_fails_at_resolve_not_eval() {
    // Referencing a metric that doesn't exist in the schema must surface
    // as a resolve-layer error — the eval machinery should never see it.
    let cube = fixture::cube();
    let q = Query {
        axes: Axes::Scalar,
        slicer: Tuple::empty(),
        metrics: vec![fixture::n("DoesNotExist")],
        options: Options::default(),
    };
    let err = cube.query(&q).await.expect_err("unresolved ref");
    assert!(
        matches!(err, tatami_inmem::Error::ResolveUnresolvedRef { ref name } if name.as_str() == "DoesNotExist"),
        "expected ResolveUnresolvedRef, got {err:?}"
    );
}

#[tokio::test]
async fn query_with_lag_over_non_time_dim_fails_at_resolve() {
    // A schema with a Lag-over-Geography metric is rejected at resolve
    // time — the pre-eval invariant "Lag.dim is Time" is enforced by
    // `resolve::check_expr`.
    let schema = Schema::builder()
        .dimension(
            Dimension::regular(fixture::n("Geography")).hierarchy(
                Hierarchy::new(fixture::n("Default"))
                    .level(Level::new(fixture::n("Region"), fixture::n("region"))),
            ),
        )
        .measure(Measure::new(fixture::n("amount"), Aggregation::sum()))
        .metric(Metric::new(
            fixture::n("BadLag"),
            Expr::Lag {
                of: Box::new(Expr::Ref {
                    name: fixture::n("amount"),
                }),
                dim: fixture::n("Geography"),
                n: 1,
            },
        ))
        .build()
        .expect("schema valid");
    let df: DataFrame = df! {
        "region" => ["EMEA"],
        "amount" => [1.0_f64],
    }
    .expect("frame");
    let cube = InMemoryCube::new(df, schema).expect("cube");
    let q = Query {
        axes: Axes::Scalar,
        slicer: Tuple::empty(),
        metrics: vec![fixture::n("BadLag")],
        options: Options::default(),
    };
    let err = cube.query(&q).await.expect_err("bad lag");
    assert!(
        matches!(err, tatami_inmem::Error::ResolveLagDimNotTime { ref dim } if dim.as_str() == "Geography"),
        "expected ResolveLagDimNotTime, got {err:?}"
    );
}

// ── 5. Options pass-through ──────────────────────────────────────────────

#[tokio::test]
async fn order_and_limit_apply_post_evaluation() {
    // Sanity-check that `Options::{order, limit}` propagate through the
    // end-to-end path (the inline 5g tests cover this against the inline
    // fixture; this mirror-test pins the behaviour against the richer
    // fixture used by the other integration tests).
    let cube = fixture::cube();
    let q = Query {
        axes: Axes::Series {
            rows: Set::members(
                fixture::n("Geography"),
                fixture::n("Default"),
                fixture::n("Country"),
            ),
        },
        slicer: Tuple::of([
            fixture::mr("Time", "Fiscal", &["FY2026"]),
            MemberRef::new(
                fixture::n("Scenario"),
                fixture::n("Default"),
                Path::of(fixture::n("Actual")),
            ),
        ])
        .expect("disjoint"),
        metrics: vec![fixture::n("amount")],
        options: Options {
            order: vec![OrderBy {
                metric: fixture::n("amount"),
                direction: tatami::query::Direction::Desc,
            }],
            limit: Some(NonZeroUsize::new(1).expect("nonzero")),
            ..Options::default()
        },
    };
    let r = cube.query(&q).await.expect("query ok");
    match r {
        Results::Series(s) => {
            assert_eq!(s.x().len(), 1, "limit=1 retains a single x entry");
        }
        other => panic!("expected Series, got {other:?}"),
    }
}

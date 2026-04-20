//! Integration tests: the four example queries from MAP §3.5.
//!
//! Each test constructs a query programmatically, serializes to JSON,
//! deserializes back, and asserts byte-stable roundtrip equality against
//! the original value.
//!
//! §3.5(b) — "Quarterly Revenue by Region, FY2025–FY2030" — uses the
//! tidy-form `Set::range(...).descendants_to(Quarter)`, which is only
//! legal because `Set::Descendants { of: Box<Set> }` is closed under the
//! algebra. An extra `descendants_of_union_roundtrips` test below
//! demonstrates the same closure with `Union`.

use tatami::query::{self, Set, Tuple};
use tatami::schema::Name;
use tatami::{Axes, MemberRef, OrderBy, Path, Query};

fn n(s: &str) -> Name {
    Name::parse(s).expect("valid name")
}

fn roundtrip(q: &Query) -> Query {
    let json = serde_json::to_string_pretty(q).expect("serialize");
    let back: Query = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(q, &back, "query did not roundtrip byte-stable");
    back
}

#[test]
fn fy2026_revenue_with_mom_delta_scalar_roundtrips() {
    let q = Query {
        axes: Axes::Scalar,
        slicer: Tuple::of([
            MemberRef::new(n("Time"), n("Fiscal"), Path::of(n("FY2026"))),
            MemberRef::new(n("Scenario"), n("Default"), Path::of(n("Actual"))),
        ])
        .expect("distinct dims"),
        metrics: vec![n("Revenue"), n("RevenueMoM")],
        options: query::Options::default(),
    };
    roundtrip(&q);
}

#[test]
fn quarterly_revenue_by_region_pivot_roundtrips() {
    // MAP §3.5(b) — "Descendants of a Time range FY2025..FY2030".
    // Expressible directly now that `Set::Descendants.of` is a full `Set`.
    let q = Query {
        axes: Axes::Pivot {
            rows: Set::range(
                n("Time"),
                n("Fiscal"),
                MemberRef::time(n("FY2025")),
                MemberRef::time(n("FY2030")),
            )
            .descendants_to(n("Quarter")),
            columns: Set::Members {
                dim: n("Geography"),
                hierarchy: n("Default"),
                level: n("Region"),
            },
        },
        slicer: Tuple::of([MemberRef::scenario(n("Actual"))]).expect("distinct dims"),
        metrics: vec![n("Revenue")],
        options: query::Options {
            non_empty: true,
            ..Default::default()
        },
    };
    roundtrip(&q);
}

#[test]
fn aop_plan_vs_whatif_pivot_roundtrips() {
    let q = Query {
        axes: Axes::Pivot {
            rows: Set::Members {
                dim: n("Account"),
                hierarchy: n("PnL"),
                level: n("LineItem"),
            },
            columns: Set::explicit([
                MemberRef::scenario(n("Plan")),
                MemberRef::scenario(n("WhatIf_A")),
            ])
            .expect("non-empty members"),
        },
        slicer: Tuple::of([MemberRef::time(n("FY2026"))]).expect("distinct dims"),
        metrics: vec![n("Amount"), n("Variance"), n("VariancePct")],
        options: query::Options::default(),
    };
    roundtrip(&q);
}

#[test]
fn sales_volume_by_territory_series_roundtrips() {
    let q = Query {
        axes: Axes::Series {
            // MAP §3.5(d) — tidy form: `MemberRef::world().descendants_to(Country)`.
            rows: MemberRef::world().descendants_to(n("Country")),
        },
        slicer: Tuple::of([
            MemberRef::time(n("FY2026")),
            MemberRef::scenario(n("Actual")),
        ])
        .expect("distinct dims"),
        metrics: vec![n("Units")],
        options: query::Options::default(),
    };
    roundtrip(&q);
}

#[test]
fn descendants_of_union_roundtrips() {
    // Closure demonstration: `Set::Descendants { of: Box<Set> }` admits
    // a `Union` under it — previously impossible when `of: MemberRef`.
    let q = Query {
        axes: Axes::Series {
            rows: Set::union(
                MemberRef::time(n("FY2025")).descendants_to(n("Quarter")),
                MemberRef::time(n("FY2026")).descendants_to(n("Quarter")),
            ),
        },
        slicer: Tuple::of([MemberRef::scenario(n("Actual"))]).expect("distinct dims"),
        metrics: vec![n("Revenue")],
        options: query::Options::default(),
    };
    roundtrip(&q);
}

#[test]
fn query_with_order_and_limit_roundtrips() {
    // Exercises the `query::Options` fields beyond defaults, which the four
    // §3.5 examples don't all cover.
    use std::num::NonZeroUsize;
    use tatami::Direction;

    let q = Query {
        axes: Axes::Series {
            rows: Set::Members {
                dim: n("Geography"),
                hierarchy: n("Default"),
                level: n("Country"),
            },
        },
        slicer: Tuple::empty(),
        metrics: vec![n("Revenue")],
        options: query::Options {
            order: vec![OrderBy {
                metric: n("Revenue"),
                direction: Direction::Desc,
            }],
            limit: Some(NonZeroUsize::new(10).expect("nonzero")),
            non_empty: true,
        },
    };
    roundtrip(&q);
}

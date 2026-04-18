//! Integration tests: the four example queries from MAP §3.5.
//!
//! Each test constructs a query programmatically, serializes to JSON,
//! deserializes back, and asserts byte-stable roundtrip equality against
//! the original value. Where MAP §3.5's literal example relies on a
//! signature that the types don't support (e.g. using
//! `MemberRef::range` — which returns `(MemberRef, MemberRef)` — as a
//! single `MemberRef`), the test uses the closest legal form and a
//! comment notes the adjustment.

use tatami::query::{Set, Tuple};
use tatami::schema::Name;
use tatami::{Axes, MemberRef, OrderBy, Path, Query, QueryOptions};

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
        options: QueryOptions::default(),
    };
    roundtrip(&q);
}

#[test]
fn quarterly_revenue_by_region_pivot_roundtrips() {
    // MAP §3.5(b) illustrates "Descendants over a Time range FY2025..FY2030".
    // `Set::Descendants.of` is a single `MemberRef`, so we use the range's
    // lower endpoint here; the resolve stage (§3.6) is where range-shaped
    // drill-down will be expressed via `Set::CrossJoin(Set::Range, …)`.
    let q = Query {
        axes: Axes::Pivot {
            rows: Set::Descendants {
                of: MemberRef::time(n("FY2025")),
                to_level: n("Quarter"),
            },
            columns: Set::Members {
                dim: n("Geography"),
                hierarchy: n("Default"),
                level: n("Region"),
            },
        },
        slicer: Tuple::of([MemberRef::scenario(n("Actual"))]).expect("distinct dims"),
        metrics: vec![n("Revenue")],
        options: QueryOptions {
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
        options: QueryOptions::default(),
    };
    roundtrip(&q);
}

#[test]
fn sales_volume_by_territory_series_roundtrips() {
    let q = Query {
        axes: Axes::Series {
            rows: Set::Descendants {
                of: MemberRef::world(),
                to_level: n("Country"),
            },
        },
        slicer: Tuple::of([
            MemberRef::time(n("FY2026")),
            MemberRef::scenario(n("Actual")),
        ])
        .expect("distinct dims"),
        metrics: vec![n("Units")],
        options: QueryOptions::default(),
    };
    roundtrip(&q);
}

#[test]
fn query_with_order_and_limit_roundtrips() {
    // Exercises the `QueryOptions` fields beyond defaults, which the four
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
        options: QueryOptions {
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

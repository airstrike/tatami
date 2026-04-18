//! The four example queries from §3.5 of the plan as a **closed enum** —
//! each variant carries its own [`Query`], heading, and subtitle so the
//! list of examples and the list of labels can't drift out of sync.

use tatami::query::{self, Set, Tuple};
use tatami::schema::Name;
use tatami::{Axes, MemberRef, Path, Query};

/// Closed set of example queries Hewton demonstrates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ExampleQuery {
    /// FY2026 Revenue with MoM delta — single-cell KPI.
    FyRevenue,
    /// Quarterly Revenue by Region across FY2025–FY2030.
    QuarterlyByRegion,
    /// Plan vs What-If variance across scenarios.
    PlanVsWhatIf,
    /// Sales volume by territory, World → Region → Country.
    WorldToCountry,
}

impl ExampleQuery {
    pub const ALL: [Self; 4] = [
        Self::FyRevenue,
        Self::QuarterlyByRegion,
        Self::PlanVsWhatIf,
        Self::WorldToCountry,
    ];

    pub fn heading(self) -> &'static str {
        match self {
            Self::FyRevenue => "FY2026 Revenue",
            Self::QuarterlyByRegion => "Quarterly Revenue by Region",
            Self::PlanVsWhatIf => "Plan vs What-If",
            Self::WorldToCountry => "Sales by Territory",
        }
    }

    pub fn subtitle(self) -> &'static str {
        match self {
            Self::FyRevenue => "Single-cell KPI with MoM delta — Results::Scalar",
            Self::QuarterlyByRegion => "FY2025–FY2030 × regions — Results::Pivot",
            Self::PlanVsWhatIf => "Variance table across scenarios — Results::Pivot",
            Self::WorldToCountry => "World → Region → Country drilldown — Results::Rollup",
        }
    }

    pub fn query(self) -> Query {
        match self {
            Self::FyRevenue => scalar_kpi(),
            Self::QuarterlyByRegion => pivot_by_region(),
            Self::PlanVsWhatIf => variance_pivot(),
            Self::WorldToCountry => rollup_by_territory(),
        }
    }
}

// ── §3.5(a) Scalar ─────────────────────────────────────────────────────────

fn scalar_kpi() -> Query {
    Query {
        axes: Axes::Scalar,
        slicer: Tuple::of([
            MemberRef::new(n("Time"), n("Fiscal"), Path::of(n("FY2026"))),
            MemberRef::scenario(n("Actual")),
        ])
        .expect("distinct dims"),
        metrics: vec![n("Revenue"), n("RevenueMoM")],
        options: query::Options::default(),
    }
}

// ── §3.5(b) Pivot — Descendants over a range ───────────────────────────────

fn pivot_by_region() -> Query {
    Query {
        axes: Axes::Pivot {
            // Descendants of a Set::Range — this is what the Children /
            // Descendants `of: Box<Set>` lift buys us. The "all quarters
            // across 6 fiscal years" shape is one expression.
            rows: Set::range(
                n("Time"),
                n("Fiscal"),
                MemberRef::time(n("FY2025")),
                MemberRef::time(n("FY2030")),
            )
            .descendants_to(n("Quarter")),
            columns: Set::members(n("Geography"), n("Default"), n("Region")),
        },
        slicer: Tuple::of([MemberRef::scenario(n("Actual"))]).expect("distinct dims"),
        metrics: vec![n("Revenue")],
        options: query::Options {
            non_empty: true,
            ..query::Options::default()
        },
    }
}

// ── §3.5(c) Variance pivot ─────────────────────────────────────────────────

fn variance_pivot() -> Query {
    Query {
        axes: Axes::Pivot {
            rows: Set::members(n("Segment"), n("Default"), n("Segment")),
            columns: Set::explicit([
                MemberRef::scenario(n("Plan")),
                MemberRef::scenario(n("WhatIf_A")),
            ])
            .expect("non-empty members"),
        },
        slicer: Tuple::of([MemberRef::time(n("FY2026"))]).expect("distinct dims"),
        metrics: vec![n("Revenue"), n("ADR"), n("Occupancy")],
        options: query::Options::default(),
    }
}

// ── §3.5(d) Rollup — descendants of a member ───────────────────────────────

fn rollup_by_territory() -> Query {
    Query {
        axes: Axes::Series {
            // MemberRef → Set sugar: `.descendants_to(level)` wraps the
            // member in Set::Explicit automatically. Reads as
            // `world.descendants_to("Country")`.
            rows: MemberRef::world().descendants_to(n("Country")),
        },
        slicer: Tuple::of([
            MemberRef::time(n("FY2026")),
            MemberRef::scenario(n("Actual")),
        ])
        .expect("distinct dims"),
        metrics: vec![n("room_nights_sold")],
        options: query::Options::default(),
    }
}

fn n(s: &str) -> Name {
    Name::parse(s).expect("hewton identifiers are valid")
}

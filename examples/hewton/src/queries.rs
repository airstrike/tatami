//! The four example queries from §3.5 of the plan as a **closed enum** —
//! each variant carries its own [`Query`], heading, and subtitle so the
//! list of examples and the list of labels can't drift out of sync.

use tatami::query::{self, Set, Tuple};
use tatami::schema::Name;
use tatami::{Axes, MemberRef, Path, Query};

use crate::scenario::Scenario;

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

    /// Construct the `Query` for this example against the selected
    /// scenario. `PlanVsWhatIf` ignores `scenario` by design — its columns
    /// axis *is* the scenario enumeration (Plan vs WhatIf_A), so a picker
    /// value would be meaningless.
    pub fn query(self, scenario: Scenario) -> Query {
        match self {
            Self::FyRevenue => scalar_kpi(scenario),
            Self::QuarterlyByRegion => pivot_by_region(scenario),
            Self::PlanVsWhatIf => variance_pivot(),
            Self::WorldToCountry => rollup_by_territory(scenario),
        }
    }
}

// ── §3.5(a) Scalar ─────────────────────────────────────────────────────────

fn scalar_kpi(scenario: Scenario) -> Query {
    Query {
        axes: Axes::Scalar,
        slicer: Tuple::of([
            MemberRef::new(n("Time"), n("Fiscal"), Path::of(n("FY2026"))),
            MemberRef::scenario(n(scenario.name())),
        ])
        .expect("distinct dims"),
        metrics: vec![n("Revenue"), n("RevenueMoM")],
        options: query::Options::default(),
    }
}

// ── §3.5(b) Pivot — Descendants over a range ───────────────────────────────

fn pivot_by_region(scenario: Scenario) -> Query {
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
        slicer: Tuple::of([MemberRef::scenario(n(scenario.name()))]).expect("distinct dims"),
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

// ── §3.5(d) Rollup — Descendants rows + single-column Pivot ────────────────

fn rollup_by_territory(scenario: Scenario) -> Query {
    // Pivot-with-Descendants-rows hits the rollup trigger in
    // eval/query.rs::evaluate_pivot: a single-root Descendants set
    // collapses to `Results::Rollup(Tree)` rather than a flat pivot grid.
    // The active scenario moves from the slicer onto the columns axis so
    // the picker still parameterises the query without clashing with the
    // slicer's dim set.
    Query {
        axes: Axes::Pivot {
            rows: MemberRef::world().descendants_to(n("Country")),
            columns: Set::explicit([MemberRef::scenario(n(scenario.name()))])
                .expect("single-scenario column is non-empty"),
        },
        slicer: Tuple::of([MemberRef::time(n("FY2026"))]).expect("distinct dims"),
        metrics: vec![n("room_nights_sold")],
        options: query::Options::default(),
    }
}

fn n(s: &str) -> Name {
    Name::parse(s).expect("hewton identifiers are valid")
}

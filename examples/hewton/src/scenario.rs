//! The scenario picker's closed enum — drives the slicer for every
//! scenario-aware example query. `PlanVsWhatIf` is the one exception
//! (its columns axis *is* the scenarios, so the picker is irrelevant).
//!
//! Two sides to each variant:
//! - [`Scenario::name`] — the schema-side identifier (matches the CSV's
//!   `scenario` column values: `Actual`, `Plan`, `WhatIf_A`).
//! - [`Display`] — the UI-side label shown in the `pick_list`.

/// Closed set of scenarios the picker offers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Scenario {
    Actual,
    Plan,
    WhatIfA,
}

impl Scenario {
    pub const ALL: [Self; 3] = [Self::Actual, Self::Plan, Self::WhatIfA];

    /// Schema-side name — matches the `scenario` column values in the CSV.
    pub fn name(self) -> &'static str {
        match self {
            Self::Actual => "Actual",
            Self::Plan => "Plan",
            Self::WhatIfA => "WhatIf_A",
        }
    }
}

impl std::fmt::Display for Scenario {
    /// UI-side label — the exact string rendered in the `pick_list`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Actual => "Actual",
            Self::Plan => "Plan",
            Self::WhatIfA => "What-If (A)",
        })
    }
}

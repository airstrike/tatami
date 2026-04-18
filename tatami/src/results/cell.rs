//! [`Cell`] — the three states a single result cell can be in.
//!
//! Sum type over `Valid` / `Missing` / `Error`, so consumers must handle each
//! state explicitly: no silent `Option<f64>` collapsing `Missing` into
//! `Error`, no NaN-bearing `f64` sneaking into render code.
//!
//! The reason for a `Missing` cell lives in the nested [`missing`] module as
//! [`missing::Reason`] — per `RUST_STYLE.md`, the module path carries the
//! qualifier (`missing::Reason`, not `MissingReason`).

use serde::{Deserialize, Serialize};

use crate::schema::{Format, Unit};

/// The three states a single result cell can be in.
///
/// `Valid` carries the numeric value plus optional unit/format hints for
/// renderers. `Missing` carries a [`missing::Reason`] so a consumer can
/// choose to style "no facts" differently from "not applicable". `Error`
/// carries a human-readable message for cells that could not be evaluated
/// (division by zero, overflow, NaN input, etc.).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Cell {
    /// A successfully-evaluated numeric cell.
    Valid {
        /// The numeric value.
        value: f64,
        /// Optional unit (e.g. `USD`, `units`).
        unit: Option<Unit>,
        /// Optional renderer format hint (e.g. `0.0%`).
        format: Option<Format>,
    },
    /// The cell has no value; `reason` tells the consumer why.
    Missing {
        /// Why this cell has no value.
        reason: missing::Reason,
    },
    /// The cell could not be evaluated. `message` is human-readable.
    Error {
        /// Human-readable description of the evaluation failure.
        message: String,
    },
}

/// Reasons a [`Cell`] may be in the `Missing` state.
///
/// Lives in a nested module so the public type name is `missing::Reason`
/// rather than `MissingReason` — `RUST_STYLE.md` §Naming requires the
/// module path carry the qualifier. A crate-root re-export makes
/// `tatami::missing::Reason` the stable import path.
pub mod missing {
    use serde::{Deserialize, Serialize};

    /// Why a [`super::Cell`] is in the `Missing` state.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "kind", rename_all = "snake_case")]
    #[non_exhaustive]
    pub enum Reason {
        /// No fact rows matched the tuple.
        NoFacts,
        /// The scenario dim is unbound and not cross-joined onto an axis.
        UnboundScenario,
        /// The measure does not apply at this granularity.
        NotApplicable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cell_roundtrips_all_three_variants() {
        let cases = vec![
            Cell::Valid {
                value: 1.5,
                unit: None,
                format: None,
            },
            Cell::Missing {
                reason: missing::Reason::NoFacts,
            },
            Cell::Error {
                message: "div by zero".into(),
            },
        ];
        for c in cases {
            let s = serde_json::to_string(&c).expect("serialize");
            let back: Cell = serde_json::from_str(&s).expect("deserialize");
            assert_eq!(c, back);
        }
    }

    #[test]
    fn missing_reason_roundtrips_each_variant() {
        for r in [
            missing::Reason::NoFacts,
            missing::Reason::UnboundScenario,
            missing::Reason::NotApplicable,
        ] {
            let s = serde_json::to_string(&r).expect("serialize");
            let back: missing::Reason = serde_json::from_str(&s).expect("deserialize");
            assert_eq!(r, back);
        }
    }
}

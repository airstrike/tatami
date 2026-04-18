//! Scalar-shape [`Result`] — one cell tuple paired with a `Vec<Cell>`,
//! one entry per requested metric.
//!
//! Opaque fields; the only constructor is [`Result::new`]. In the v0.1
//! scaffold that constructor is total — Phase 5's `resolve` step pairs the
//! metric count from `ResolvedQuery` with the constructor to enforce
//! `values.len() == metrics.len()` as a typed precondition.

use serde::{Deserialize, Serialize};

use crate::Cell;
use crate::query::Tuple;

/// Scalar-shape result: the single tuple the scalar query pinned plus one
/// cell per requested metric, in metric order.
///
/// Lives at `tatami::scalar::Result` via the crate-root re-export — no
/// composite `ScalarResult` name per `RUST_STYLE.md`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Result {
    tuple: Tuple,
    values: Vec<Cell>,
}

impl Result {
    /// Construct a scalar result from its tuple and per-metric values.
    ///
    /// v0.1 scaffold: total. Phase 5 tightens this to check
    /// `values.len() == metrics.len()` once `ResolvedQuery` is wired in.
    #[must_use]
    pub fn new(tuple: Tuple, values: Vec<Cell>) -> Self {
        Self { tuple, values }
    }

    /// Read-only view of the tuple this result was evaluated at.
    #[must_use]
    pub fn tuple(&self) -> &Tuple {
        &self.tuple
    }

    /// Read-only view of the per-metric cell values, in metric order.
    #[must_use]
    pub fn values(&self) -> &[Cell] {
        &self.values
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::results::cell::missing;

    #[test]
    fn scalar_result_preserves_tuple_and_values() {
        let v = Result::new(
            Tuple::empty(),
            vec![Cell::Valid {
                value: 1.5,
                unit: None,
                format: None,
            }],
        );
        assert!(v.tuple().is_empty());
        assert_eq!(v.values().len(), 1);
    }

    #[test]
    fn scalar_result_roundtrips_via_serde() {
        let v = Result::new(
            Tuple::empty(),
            vec![
                Cell::Valid {
                    value: 1.5,
                    unit: None,
                    format: None,
                },
                Cell::Missing {
                    reason: missing::Reason::NoFacts,
                },
            ],
        );
        let s = serde_json::to_string(&v).expect("serialize");
        let back: Result = serde_json::from_str(&s).expect("deserialize");
        assert_eq!(v, back);
    }
}

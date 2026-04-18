//! Result types — typed per widget shape, total sum at the outer `Results`.
//!
//! Each shape lives in its own submodule:
//! - [`scalar::Result`] — KPI tile, one cell tuple + one cell per metric.
//! - [`series::Result`] — line / bar chart, shared x + one row per metric.
//! - [`pivot::Result`] — variance / heatmap table, 2-D cell grid.
//! - [`rollup::Tree`] — hierarchical / map-ready, recursive subtree.
//!
//! The closed [`Results`] sum wraps the four so backends hand one typed
//! value back per query (the `Axes → Results` mapping is total; see
//! `.claude/map/v0-1/MAP_PLAN.md` §3.3).

pub mod cell;
pub mod error;
pub mod pivot;
pub mod rollup;
pub mod scalar;
pub mod series;

pub use cell::Cell;
pub use error::Error;

use serde::{Deserialize, Serialize};

/// Closed sum of the four result shapes. The `Axes` variant of the original
/// [`crate::Query`] determines which variant the backend returns (mapping
/// documented in MAP §3.3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum Results {
    /// Single cell tuple + one cell per metric — KPI tile shape.
    Scalar(scalar::Result),
    /// Shared x-axis + one row per metric — line/bar chart shape.
    Series(series::Result),
    /// Row/column headers + 2-D cell grid — pivot/variance table shape.
    Pivot(pivot::Result),
    /// Recursive `(root, value, children)` tree — rollup/choropleth shape.
    Rollup(rollup::Tree),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{MemberRef, Path, Tuple};
    use crate::schema::Name;

    fn mr(dim: &str, head: &str) -> MemberRef {
        MemberRef::new(
            Name::parse(dim).expect("valid"),
            Name::parse("Default").expect("valid"),
            Path::of(Name::parse(head).expect("valid")),
        )
    }

    fn one_valid() -> Cell {
        Cell::Valid {
            value: 1.0,
            unit: None,
            format: None,
        }
    }

    #[test]
    fn results_scalar_variant_roundtrips() {
        let r = Results::Scalar(scalar::Result::new(Tuple::empty(), vec![one_valid()]));
        let s = serde_json::to_string(&r).expect("serialize");
        let back: Results = serde_json::from_str(&s).expect("deserialize");
        assert_eq!(r, back);
    }

    #[test]
    fn results_series_variant_roundtrips() {
        let r = Results::Series(series::Result::new(
            vec![mr("Time", "Q1")],
            vec![series::Row {
                label: "Revenue".into(),
                values: vec![one_valid()],
            }],
        ));
        let s = serde_json::to_string(&r).expect("serialize");
        let back: Results = serde_json::from_str(&s).expect("deserialize");
        assert_eq!(r, back);
    }

    #[test]
    fn results_pivot_variant_roundtrips() {
        let r = Results::Pivot(pivot::Result::new(
            vec![Tuple::empty()],
            vec![Tuple::empty()],
            vec![vec![one_valid()]],
        ));
        let s = serde_json::to_string(&r).expect("serialize");
        let back: Results = serde_json::from_str(&s).expect("deserialize");
        assert_eq!(r, back);
    }

    #[test]
    fn results_rollup_variant_roundtrips() {
        let r = Results::Rollup(rollup::Tree {
            root: mr("Geography", "World"),
            value: one_valid(),
            children: Vec::new(),
        });
        let s = serde_json::to_string(&r).expect("serialize");
        let back: Results = serde_json::from_str(&s).expect("deserialize");
        assert_eq!(r, back);
    }
}

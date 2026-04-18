//! [`Axes`] — the closed set of query axis shapes.
//!
//! The variant of `Axes` uniquely determines the variant of `Results`
//! (Phase 3). Invalid shapes (`[Rows, Rows]`, `Columns` without `Rows`)
//! cannot be constructed.

use serde::{Deserialize, Serialize};

use crate::query::Set;

/// The shape of a query's axis projection.
///
/// Four total shapes:
/// - [`Axes::Scalar`] — zero axes (KPI tile).
/// - [`Axes::Series`] — one axis (line / bar chart).
/// - [`Axes::Pivot`] — two axes (table / heatmap / variance).
/// - [`Axes::Pages`] — three axes (scenario toggle above a pivot).
///
/// The variants differ in size — `Pages` owns three `Set` trees, `Pivot`
/// owns two, `Series` owns one, `Scalar` owns none. This is intentional:
/// MAP §3.2 specifies the shape verbatim as unboxed fields and `Axes`
/// values are constructed once per query rather than in hot loops, so the
/// ergonomic win over `Box<Set>` outweighs the layout cost.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "shape", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Axes {
    /// Zero axes — a single value per metric, rendered as a KPI tile.
    Scalar,
    /// One axis — rows of tuples, rendered as a line / bar chart.
    Series {
        /// Rows axis.
        rows: Set,
    },
    /// Two axes — rows × columns, rendered as a table / heatmap.
    Pivot {
        /// Rows axis.
        rows: Set,
        /// Columns axis.
        columns: Set,
    },
    /// Three axes — rows × columns × pages, for scenario toggles above a
    /// pivot.
    Pages {
        /// Rows axis.
        rows: Set,
        /// Columns axis.
        columns: Set,
        /// Pages axis.
        pages: Set,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{MemberRef, Path};
    use crate::schema::Name;

    fn n(s: &str) -> Name {
        Name::parse(s).expect("valid")
    }

    #[test]
    fn scalar_roundtrips() {
        let a = Axes::Scalar;
        let json = serde_json::to_string(&a).expect("serialize");
        assert_eq!(json, r#"{"shape":"scalar"}"#);
        let back: Axes = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(a, back);
    }

    #[test]
    fn series_roundtrips() {
        let a = Axes::Series {
            rows: Set::Children {
                of: MemberRef::new(n("Geography"), n("Default"), Path::of(n("World"))),
            },
        };
        let json = serde_json::to_string(&a).expect("serialize");
        let back: Axes = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(a, back);
    }

    #[test]
    fn pivot_roundtrips() {
        let a = Axes::Pivot {
            rows: Set::Members {
                dim: n("Time"),
                hierarchy: n("Fiscal"),
                level: n("Quarter"),
            },
            columns: Set::Members {
                dim: n("Geography"),
                hierarchy: n("Default"),
                level: n("Region"),
            },
        };
        let json = serde_json::to_string(&a).expect("serialize");
        let back: Axes = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(a, back);
    }

    #[test]
    fn pages_roundtrips() {
        let a = Axes::Pages {
            rows: Set::Members {
                dim: n("Time"),
                hierarchy: n("Fiscal"),
                level: n("Quarter"),
            },
            columns: Set::Members {
                dim: n("Geography"),
                hierarchy: n("Default"),
                level: n("Region"),
            },
            pages: Set::Members {
                dim: n("Scenario"),
                hierarchy: n("Default"),
                level: n("Name"),
            },
        };
        let json = serde_json::to_string(&a).expect("serialize");
        let back: Axes = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(a, back);
    }
}

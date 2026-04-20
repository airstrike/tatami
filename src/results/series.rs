//! Series-shape [`Result`] — an x-axis of member references plus one [`Row`]
//! per requested metric.
//!
//! Opaque fields; the only constructor is [`Result::new`]. v0.1 scaffold is
//! total — Phase 5 tightens the invariant
//! `rows[i].values.len() == x.len()` once row widths are plumbed through.

use serde::{Deserialize, Serialize};

use crate::Cell;
use crate::query::MemberRef;

/// Series-shape result.
///
/// `x` is the shared domain (every row shares the same x-axis members).
/// Each [`Row`] is one metric's values across that domain, in x order.
///
/// Lives at `tatami::series::Result` via the crate-root re-export.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Result {
    x: Vec<MemberRef>,
    rows: Vec<Row>,
}

/// One metric's values across the series' shared x-axis, with a label for
/// display.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Row {
    /// Display label — typically the metric name.
    pub label: String,
    /// One [`Cell`] per x-axis member, in the same order as [`Result::x`].
    pub values: Vec<Cell>,
}

impl Result {
    /// Construct a series result.
    ///
    /// v0.1 scaffold: total. Phase 5 enforces
    /// `rows[i].values.len() == x.len()` once `ResolvedQuery` exposes the
    /// row width.
    #[must_use]
    pub fn new(x: Vec<MemberRef>, rows: Vec<Row>) -> Self {
        Self { x, rows }
    }

    /// Read-only view of the x-axis members.
    #[must_use]
    pub fn x(&self) -> &[MemberRef] {
        &self.x
    }

    /// Read-only view of the rows, one per metric.
    #[must_use]
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::Path;
    use crate::schema::Name;

    fn mr(head: &str) -> MemberRef {
        MemberRef::new(
            Name::parse("Time").expect("valid"),
            Name::parse("Fiscal").expect("valid"),
            Path::of(Name::parse(head).expect("valid")),
        )
    }

    #[test]
    fn series_result_preserves_x_and_rows() {
        let v = Result::new(
            vec![mr("Q1"), mr("Q2")],
            vec![Row {
                label: "Revenue".into(),
                values: vec![
                    Cell::Valid {
                        value: 1.0,
                        unit: None,
                        format: None,
                    },
                    Cell::Valid {
                        value: 2.0,
                        unit: None,
                        format: None,
                    },
                ],
            }],
        );
        assert_eq!(v.x().len(), 2);
        assert_eq!(v.rows().len(), 1);
        assert_eq!(v.rows()[0].values.len(), 2);
    }

    #[test]
    fn series_result_roundtrips_via_serde() {
        let v = Result::new(
            vec![mr("Q1")],
            vec![Row {
                label: "Revenue".into(),
                values: vec![Cell::Valid {
                    value: 1.0,
                    unit: None,
                    format: None,
                }],
            }],
        );
        let s = serde_json::to_string(&v).expect("serialize");
        let back: Result = serde_json::from_str(&s).expect("deserialize");
        assert_eq!(v, back);
    }
}

//! [`Metric`], [`MetricExpr`], [`BinOp`].

use serde::{Deserialize, Serialize};

use crate::query::Tuple;
use crate::schema::{Format, Name, Unit};

/// A named formula over measures and other metrics. Compare with
/// `schema::Measure`, which is a stored column with an aggregator.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Metric {
    /// Metric name; unique within a schema.
    pub name: Name,
    /// The formula tree.
    pub expr: MetricExpr,
    /// Optional display unit.
    pub unit: Option<Unit>,
    /// Optional display format.
    pub format: Option<Format>,
}

impl Metric {
    /// Construct a metric with no declared unit or format.
    #[must_use]
    pub fn new(name: Name, expr: MetricExpr) -> Self {
        Self {
            name,
            expr,
            unit: None,
            format: None,
        }
    }

    /// Fluent: set the unit.
    #[must_use]
    pub fn with_unit(mut self, unit: Unit) -> Self {
        self.unit = Some(unit);
        self
    }

    /// Fluent: set the format.
    #[must_use]
    pub fn with_format(mut self, format: Format) -> Self {
        self.format = Some(format);
        self
    }
}

/// A metric formula tree.
///
/// Internal-tag JSON so nested formulas read cleanly:
/// `{"op": "binary", "bin_op": "div", "l": {…}, "r": {…}}`. Struct variants
/// only — internal tagging requires it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
#[non_exhaustive]
pub enum MetricExpr {
    /// Reference to a measure or metric by name.
    Ref {
        /// The name being referenced.
        name: Name,
    },
    /// A numeric literal.
    Const {
        /// The literal value.
        value: f64,
    },
    /// Binary operation over two sub-expressions.
    Binary {
        /// The operator.
        bin_op: BinOp,
        /// Left operand.
        l: Box<MetricExpr>,
        /// Right operand.
        r: Box<MetricExpr>,
    },
    /// Lag along a time dimension — `YoY` with `n = 12` months, `MoM` with
    /// `n = 1`. The `dim` must resolve to a `DimKind::Time` dim (checked
    /// in the Phase 5 resolve stage, not here).
    Lag {
        /// Sub-expression to lag.
        of: Box<MetricExpr>,
        /// The time dimension along which to lag.
        dim: Name,
        /// Lag offset (negative for lead).
        n: i32,
    },
    /// Periods-to-date — YTD / QTD / MTD depending on `level`.
    PeriodsToDate {
        /// Sub-expression to aggregate.
        of: Box<MetricExpr>,
        /// The level whose current period defines the window.
        level: Name,
    },
    /// Pin a coordinate — evaluate `of` at the tuple `at`.
    At {
        /// Sub-expression to evaluate at the fixed coordinate.
        of: Box<MetricExpr>,
        /// The coordinate to pin.
        at: Tuple,
    },
}

/// Binary operator for [`MetricExpr::Binary`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum BinOp {
    /// `a + b`
    Add,
    /// `a - b`
    Sub,
    /// `a * b`
    Mul,
    /// `a / b`
    Div,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{MemberRef, Path};

    fn n(s: &str) -> Name {
        Name::parse(s).expect("valid")
    }

    #[test]
    fn metric_ref_roundtrip_stable() {
        let expr = MetricExpr::Ref { name: n("amount") };
        let json = serde_json::to_string(&expr).expect("serialize");
        assert_eq!(json, r#"{"op":"ref","name":"amount"}"#);
        let back: MetricExpr = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(expr, back);
    }

    #[test]
    fn metric_const_roundtrip_stable() {
        let expr = MetricExpr::Const { value: 0.5 };
        let json = serde_json::to_string(&expr).expect("serialize");
        let back: MetricExpr = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(expr, back);
    }

    #[test]
    fn metric_binary_div_roundtrip_stable() {
        let expr = MetricExpr::Binary {
            bin_op: BinOp::Div,
            l: Box::new(MetricExpr::Ref { name: n("revenue") }),
            r: Box::new(MetricExpr::Ref { name: n("cogs") }),
        };
        let json = serde_json::to_string(&expr).expect("serialize");
        let back: MetricExpr = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(expr, back);
    }

    #[test]
    fn metric_yoy_tree_roundtrips() {
        let expr = MetricExpr::Binary {
            bin_op: BinOp::Div,
            l: Box::new(MetricExpr::Binary {
                bin_op: BinOp::Sub,
                l: Box::new(MetricExpr::Ref { name: n("Revenue") }),
                r: Box::new(MetricExpr::Lag {
                    of: Box::new(MetricExpr::Ref { name: n("Revenue") }),
                    dim: n("Time"),
                    n: 12,
                }),
            }),
            r: Box::new(MetricExpr::Lag {
                of: Box::new(MetricExpr::Ref { name: n("Revenue") }),
                dim: n("Time"),
                n: 12,
            }),
        };
        let json = serde_json::to_string(&expr).expect("serialize");
        let back: MetricExpr = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(expr, back);
    }

    #[test]
    fn metric_at_roundtrips_with_real_tuple() {
        let expr = MetricExpr::At {
            of: Box::new(MetricExpr::Ref { name: n("Revenue") }),
            at: Tuple::single(MemberRef::new(
                n("Scenario"),
                n("Default"),
                Path::of(n("Plan")),
            )),
        };
        let json = serde_json::to_string(&expr).expect("serialize");
        let back: MetricExpr = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(expr, back);
    }

    #[test]
    fn metric_at_with_empty_tuple_roundtrips() {
        let expr = MetricExpr::At {
            of: Box::new(MetricExpr::Ref { name: n("Revenue") }),
            at: Tuple::empty(),
        };
        let json = serde_json::to_string(&expr).expect("serialize");
        let back: MetricExpr = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(expr, back);
    }

    #[test]
    fn metric_periods_to_date_roundtrips() {
        let expr = MetricExpr::PeriodsToDate {
            of: Box::new(MetricExpr::Ref { name: n("Revenue") }),
            level: n("Year"),
        };
        let json = serde_json::to_string(&expr).expect("serialize");
        let back: MetricExpr = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(expr, back);
    }

    #[test]
    fn metric_with_unit_and_format_roundtrips() {
        let m = Metric::new(
            n("Occupancy"),
            MetricExpr::Binary {
                bin_op: BinOp::Div,
                l: Box::new(MetricExpr::Ref {
                    name: n("room_nights_sold"),
                }),
                r: Box::new(MetricExpr::Ref {
                    name: n("rooms_available"),
                }),
            },
        )
        .with_format(Format::parse("0.0%").expect("valid"));
        let json = serde_json::to_string(&m).expect("serialize");
        let back: Metric = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(m, back);
    }

    #[test]
    fn bin_op_roundtrips_snake_case() {
        let json = serde_json::to_string(&BinOp::Add).expect("serialize");
        assert_eq!(json, r#""add""#);
    }
}

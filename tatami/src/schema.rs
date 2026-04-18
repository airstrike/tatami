//! Schema types — dimensions, measures, metrics, named sets.
//!
//! Every public enum here is `#[non_exhaustive]`; every serializable type
//! roundtrips JSON via serde. See [`Schema::builder`] for typestate
//! construction.

pub mod builder;
pub mod dimension;
pub mod error;
pub mod format;
pub mod measure;
pub mod metric;
pub mod month_day;
pub mod name;
pub mod named_set;
pub mod unit;

use serde::{Deserialize, Serialize};

pub use builder::Builder;
pub use dimension::{Calendar, DimKind, Dimension, Hierarchy, Level};
pub use error::Error;
pub use format::Format;
pub use measure::{Aggregation, Measure, SemiAgg};
pub use metric::{BinOp, Metric, MetricExpr};
pub use month_day::{Month, MonthDay};
pub use name::Name;
pub use named_set::NamedSet;
pub use unit::Unit;

/// Top-level schema — the declarative description of a cube.
///
/// Construct via [`Schema::builder`]. Name uniqueness within each collection
/// and `MetricExpr::Ref` resolution are checked once at `.build()` time.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    /// Declared dimensions of the cube.
    pub dimensions: Vec<Dimension>,
    /// Declared stored measures.
    pub measures: Vec<Measure>,
    /// Declared metrics (named formulas over measures / metrics).
    pub metrics: Vec<Metric>,
    /// Declared named sets — reusable `Set` expressions referenceable via
    /// [`crate::query::Set::Named`]. Defaults to empty for back-compat
    /// with Phase 1 JSON payloads that predate this field.
    #[serde(default)]
    pub named_sets: Vec<NamedSet>,
}

impl Schema {
    /// Enter the typestate builder. `.build()` is defined only on the
    /// terminal state `Builder<HasDims, HasMeasures>` — partial builders fail
    /// to compile.
    ///
    /// ```
    /// use tatami::schema::{Aggregation, Dimension, Measure, Name, Schema};
    ///
    /// let schema = Schema::builder()
    ///     .dimension(Dimension::regular(Name::parse("Geography").unwrap()))
    ///     .measure(Measure::new(Name::parse("amount").unwrap(), Aggregation::sum()))
    ///     .build()
    ///     .unwrap();
    /// assert_eq!(schema.dimensions.len(), 1);
    /// assert_eq!(schema.measures.len(), 1);
    /// ```
    #[must_use]
    pub fn builder() -> Builder<builder::NoDims, builder::NoMeasures> {
        Builder::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(s: &str) -> Name {
        Name::parse(s).expect("valid name")
    }

    fn sample_schema() -> Schema {
        Schema::builder()
            .dimension(Dimension::regular(n("Geography")))
            .dimension(Dimension::time(
                n("Time"),
                vec![Calendar::gregorian(n("Gregorian"))],
            ))
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .measure(Measure::new(
                n("stock"),
                Aggregation::semi_additive(vec![n("Time")], SemiAgg::Last).expect("non-empty dims"),
            ))
            .metric(Metric::new(
                n("Revenue"),
                MetricExpr::Ref { name: n("amount") },
            ))
            .build()
            .expect("valid schema")
    }

    #[test]
    fn schema_roundtrips_via_serde() {
        let schema = sample_schema();
        let s = serde_json::to_string_pretty(&schema).expect("serialize");
        let back: Schema = serde_json::from_str(&s).expect("deserialize");
        assert_eq!(schema, back);
    }

    #[test]
    fn schema_deserializes_without_named_sets_field() {
        // Legacy JSON that predates the `named_sets` field. Should still
        // deserialize, yielding an empty named-sets list.
        let json = r#"{
            "dimensions": [{"name":"Geography","hierarchies":[],"kind":{"kind":"regular"}}],
            "measures":   [{"name":"amount","aggregation":{"kind":"sum"},"unit":null}],
            "metrics":    []
        }"#;
        let schema: Schema = serde_json::from_str(json).expect("deserialize");
        assert!(schema.named_sets.is_empty());
    }

    #[test]
    fn builder_rejects_duplicate_measure_names() {
        let err = Schema::builder()
            .dimension(Dimension::regular(n("Geography")))
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .measure(Measure::new(n("amount"), Aggregation::avg()))
            .build()
            .expect_err("duplicate measure names");
        assert!(matches!(err, Error::DuplicateMeasureName(_)));
    }

    #[test]
    fn builder_rejects_duplicate_dimension_names() {
        let err = Schema::builder()
            .dimension(Dimension::regular(n("Geography")))
            .dimension(Dimension::regular(n("Geography")))
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .build()
            .expect_err("duplicate dimension names");
        assert!(matches!(err, Error::DuplicateDimensionName(_)));
    }

    #[test]
    fn builder_rejects_unresolved_metric_ref() {
        let err = Schema::builder()
            .dimension(Dimension::regular(n("Geography")))
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .metric(Metric::new(
                n("Nope"),
                MetricExpr::Ref {
                    name: n("NonExistent"),
                },
            ))
            .build()
            .expect_err("unresolved ref");
        assert!(matches!(err, Error::UnresolvedMetricRef { .. }));
    }

    #[test]
    fn builder_resolves_nested_metric_refs() {
        let schema = Schema::builder()
            .dimension(Dimension::regular(n("Geography")))
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .measure(Measure::new(n("cogs"), Aggregation::sum()))
            .metric(Metric::new(
                n("GrossMargin"),
                MetricExpr::Binary {
                    bin_op: BinOp::Sub,
                    l: Box::new(MetricExpr::Ref { name: n("amount") }),
                    r: Box::new(MetricExpr::Ref { name: n("cogs") }),
                },
            ))
            .build()
            .expect("valid schema");
        assert_eq!(schema.metrics.len(), 1);
    }
}

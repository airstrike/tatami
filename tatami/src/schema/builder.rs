//! Typestate builder for [`crate::Schema`].
//!
//! Two phantom-type axes track "have we seen a dimension yet?" and "have we
//! seen a measure yet?". `.build()` is defined only on the terminal state
//! `Builder<HasDims, HasMeasures>` — calling it before adding at least one
//! dimension and one measure fails to *compile*, not at runtime.
//!
//! ```compile_fail
//! use tatami::Schema;
//! // .build() is not defined on Builder<NoDims, NoMeasures>.
//! let _ = Schema::builder().build();
//! ```
//!
//! ```compile_fail
//! use tatami::schema::{Dimension, Name, Schema};
//! // .build() is not defined on Builder<HasDims, NoMeasures>.
//! let _ = Schema::builder()
//!     .dimension(Dimension::regular(Name::parse("Geography").unwrap()))
//!     .build();
//! ```
//!
//! ```compile_fail
//! use tatami::schema::{Aggregation, Measure, Name, Schema};
//! // .measure is not defined on Builder<NoDims, _> — measures require at
//! // least one dimension to have been declared first.
//! let _ = Schema::builder()
//!     .measure(Measure::new(Name::parse("amount").unwrap(), Aggregation::sum()));
//! ```

use std::collections::HashSet;
use std::marker::PhantomData;

use crate::schema::{Dimension, Error, Measure, Metric, MetricExpr, Name, NamedSet, Schema};

/// Typestate marker: no dimensions added yet.
#[derive(Debug)]
pub struct NoDims;

/// Typestate marker: at least one dimension has been added.
#[derive(Debug)]
pub struct HasDims;

/// Typestate marker: no measures added yet.
#[derive(Debug)]
pub struct NoMeasures;

/// Typestate marker: at least one measure has been added.
#[derive(Debug)]
pub struct HasMeasures;

/// Typestate [`crate::Schema`] builder.
///
/// The two type parameters track which of the two required-collection
/// preconditions have been satisfied. `Builder::new()` starts at
/// `(NoDims, NoMeasures)`; calling `.dimension` advances the first axis,
/// `.measure` advances the second. `.build()` exists only when both have
/// advanced.
#[derive(Debug)]
pub struct Builder<Dims, Measures> {
    dimensions: Vec<Dimension>,
    measures: Vec<Measure>,
    metrics: Vec<Metric>,
    named_sets: Vec<NamedSet>,
    _state: PhantomData<(Dims, Measures)>,
}

impl Builder<NoDims, NoMeasures> {
    /// Create a fresh builder.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            dimensions: Vec::new(),
            measures: Vec::new(),
            metrics: Vec::new(),
            named_sets: Vec::new(),
            _state: PhantomData,
        }
    }
}

// `.dimension(...)` is callable on the two "pre-HasDims" states. It always
// advances to `HasDims` and preserves the measures axis (which in these
// states is always `NoMeasures`, since measure-adding requires `HasDims`).
impl Builder<NoDims, NoMeasures> {
    /// Append a dimension. Advances the dims axis to [`HasDims`].
    #[must_use]
    pub fn dimension(self, dimension: Dimension) -> Builder<HasDims, NoMeasures> {
        let Builder {
            mut dimensions,
            measures,
            metrics,
            named_sets,
            _state,
        } = self;
        dimensions.push(dimension);
        Builder {
            dimensions,
            measures,
            metrics,
            named_sets,
            _state: PhantomData,
        }
    }
}

impl Builder<HasDims, NoMeasures> {
    /// Append a dimension. Always available once at least one dim has been
    /// declared.
    #[must_use]
    pub fn dimension(mut self, dimension: Dimension) -> Self {
        self.dimensions.push(dimension);
        self
    }

    /// Append a measure. Available only once at least one dimension has been
    /// declared. Advances the measures axis to [`HasMeasures`].
    #[must_use]
    pub fn measure(self, measure: Measure) -> Builder<HasDims, HasMeasures> {
        let Builder {
            dimensions,
            mut measures,
            metrics,
            named_sets,
            _state,
        } = self;
        measures.push(measure);
        Builder {
            dimensions,
            measures,
            metrics,
            named_sets,
            _state: PhantomData,
        }
    }
}

impl Builder<HasDims, HasMeasures> {
    /// Append a dimension.
    #[must_use]
    pub fn dimension(mut self, dimension: Dimension) -> Self {
        self.dimensions.push(dimension);
        self
    }

    /// Append a measure.
    #[must_use]
    pub fn measure(mut self, measure: Measure) -> Self {
        self.measures.push(measure);
        self
    }

    /// Append a metric. Available only in the terminal state — metrics
    /// reference measures and/or other metrics, so both prerequisites must
    /// be in place.
    #[must_use]
    pub fn metric(mut self, metric: Metric) -> Self {
        self.metrics.push(metric);
        self
    }

    /// Append a named set. Available only in the terminal state — named
    /// sets live in the same reference namespace as measures and metrics.
    #[must_use]
    pub fn named_set(mut self, named_set: NamedSet) -> Self {
        self.named_sets.push(named_set);
        self
    }

    /// Validate and produce the [`crate::Schema`].
    ///
    /// Checks:
    /// - Dimension names are unique.
    /// - Measure names are unique.
    /// - Metric names are unique.
    /// - Named set names are unique.
    /// - No name is shared between a measure / metric / named set
    ///   (they share one reference namespace).
    /// - Every `MetricExpr::Ref { name }` resolves to a declared measure or
    ///   metric.
    ///
    /// This is the single `Result` site in the schema pipeline.
    pub fn build(self) -> Result<Schema, Error> {
        let Builder {
            dimensions,
            measures,
            metrics,
            named_sets,
            ..
        } = self;

        // Uniqueness within each collection.
        let mut dim_names: HashSet<&Name> = HashSet::new();
        for d in &dimensions {
            if !dim_names.insert(&d.name) {
                return Err(Error::DuplicateDimensionName(d.name.clone()));
            }
        }
        let mut measure_names: HashSet<&Name> = HashSet::new();
        for m in &measures {
            if !measure_names.insert(&m.name) {
                return Err(Error::DuplicateMeasureName(m.name.clone()));
            }
        }
        let mut metric_names: HashSet<&Name> = HashSet::new();
        for m in &metrics {
            if !metric_names.insert(&m.name) {
                return Err(Error::DuplicateMetricName(m.name.clone()));
            }
        }
        let mut named_set_names: HashSet<&Name> = HashSet::new();
        for ns in &named_sets {
            if !named_set_names.insert(&ns.name) {
                return Err(Error::DuplicateNamedSetName(ns.name.clone()));
            }
        }

        // Measures, metrics, and named sets share one reference namespace —
        // a bare `Ref { name }` or `Set::Named { name }` resolves against the
        // union, so collisions would be ambiguous.
        for m in &metrics {
            if measure_names.contains(&m.name) {
                return Err(Error::MeasureMetricNameCollision(m.name.clone()));
            }
        }
        for ns in &named_sets {
            if measure_names.contains(&ns.name) || metric_names.contains(&ns.name) {
                return Err(Error::NamedSetNameCollision(ns.name.clone()));
            }
        }

        // Every `Ref` must resolve.
        let known: HashSet<&Name> = measure_names
            .iter()
            .chain(metric_names.iter())
            .copied()
            .collect();
        for m in &metrics {
            check_refs(&m.name, &m.expr, &known)?;
        }

        Ok(Schema {
            dimensions,
            measures,
            metrics,
            named_sets,
        })
    }
}

fn check_refs(metric: &Name, expr: &MetricExpr, known: &HashSet<&Name>) -> Result<(), Error> {
    match expr {
        MetricExpr::Ref { name } => {
            if !known.contains(name) {
                return Err(Error::UnresolvedMetricRef {
                    metric: metric.clone(),
                    referenced: name.clone(),
                });
            }
            Ok(())
        }
        MetricExpr::Const { .. } => Ok(()),
        MetricExpr::Binary { l, r, .. } => {
            check_refs(metric, l, known)?;
            check_refs(metric, r, known)
        }
        MetricExpr::Lag { of, .. } | MetricExpr::PeriodsToDate { of, .. } => {
            check_refs(metric, of, known)
        }
        MetricExpr::At { of, .. } => check_refs(metric, of, known),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{MemberRef, Path};
    use crate::schema::{Aggregation, BinOp, Dimension};

    fn n(s: &str) -> Name {
        Name::parse(s).expect("valid")
    }

    #[test]
    fn builder_happy_path_produces_schema() {
        let schema = Schema::builder()
            .dimension(Dimension::regular(n("Geography")))
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .metric(Metric::new(
                n("Revenue"),
                MetricExpr::Ref { name: n("amount") },
            ))
            .build()
            .expect("valid");
        assert_eq!(schema.dimensions.len(), 1);
        assert_eq!(schema.measures.len(), 1);
        assert_eq!(schema.metrics.len(), 1);
        assert!(schema.named_sets.is_empty());
    }

    #[test]
    fn builder_resolves_binary_metric_refs() {
        Schema::builder()
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
            .expect("valid");
    }

    #[test]
    fn builder_rejects_duplicate_metric_names() {
        let err = Schema::builder()
            .dimension(Dimension::regular(n("Geography")))
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .metric(Metric::new(
                n("Revenue"),
                MetricExpr::Ref { name: n("amount") },
            ))
            .metric(Metric::new(
                n("Revenue"),
                MetricExpr::Ref { name: n("amount") },
            ))
            .build()
            .expect_err("duplicate metric names");
        assert!(matches!(err, Error::DuplicateMetricName(_)));
    }

    #[test]
    fn builder_rejects_measure_metric_name_collision() {
        let err = Schema::builder()
            .dimension(Dimension::regular(n("Geography")))
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .metric(Metric::new(n("amount"), MetricExpr::Const { value: 0.0 }))
            .build()
            .expect_err("name collision");
        assert!(matches!(err, Error::MeasureMetricNameCollision(_)));
    }

    #[test]
    fn builder_detects_unresolved_ref_deep_in_tree() {
        let err = Schema::builder()
            .dimension(Dimension::regular(n("Geography")))
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .metric(Metric::new(
                n("Bad"),
                MetricExpr::Binary {
                    bin_op: BinOp::Div,
                    l: Box::new(MetricExpr::Ref { name: n("amount") }),
                    r: Box::new(MetricExpr::Lag {
                        of: Box::new(MetricExpr::Ref { name: n("NotHere") }),
                        dim: n("Time"),
                        n: 1,
                    }),
                },
            ))
            .build()
            .expect_err("unresolved ref");
        assert!(matches!(err, Error::UnresolvedMetricRef { .. }));
    }

    #[test]
    fn builder_accepts_named_set() {
        let schema = Schema::builder()
            .dimension(Dimension::regular(n("Geography")))
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .named_set(NamedSet::new(
                n("TopRegions"),
                MemberRef::new(n("Geography"), n("Default"), Path::of(n("World"))).children(),
            ))
            .build()
            .expect("valid");
        assert_eq!(schema.named_sets.len(), 1);
    }

    #[test]
    fn builder_rejects_duplicate_named_set_names() {
        let ns = || {
            NamedSet::new(
                n("TopRegions"),
                MemberRef::new(n("Geography"), n("Default"), Path::of(n("World"))).children(),
            )
        };
        let err = Schema::builder()
            .dimension(Dimension::regular(n("Geography")))
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .named_set(ns())
            .named_set(ns())
            .build()
            .expect_err("duplicate named set");
        assert!(matches!(err, Error::DuplicateNamedSetName(_)));
    }

    #[test]
    fn builder_rejects_named_set_name_collision_with_measure() {
        let err = Schema::builder()
            .dimension(Dimension::regular(n("Geography")))
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .named_set(NamedSet::new(
                n("amount"),
                MemberRef::new(n("Geography"), n("Default"), Path::of(n("World"))).children(),
            ))
            .build()
            .expect_err("named set collides with measure");
        assert!(matches!(err, Error::NamedSetNameCollision(_)));
    }
}

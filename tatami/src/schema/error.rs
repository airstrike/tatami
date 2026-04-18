//! Crate-level schema errors — composes sub-module errors and the
//! builder-stage validation failures.

use crate::schema::{Name, measure, month_day, name};

/// Errors produced when constructing a [`crate::Schema`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Two or more dimensions shared a name.
    #[error("duplicate dimension name: {0}")]
    DuplicateDimensionName(Name),
    /// Two or more measures shared a name.
    #[error("duplicate measure name: {0}")]
    DuplicateMeasureName(Name),
    /// Two or more metrics shared a name.
    #[error("duplicate metric name: {0}")]
    DuplicateMetricName(Name),
    /// A measure and a metric shared a name (would make a bare `Ref`
    /// ambiguous at resolve time).
    #[error("name {0} is used by both a measure and a metric")]
    MeasureMetricNameCollision(Name),
    /// A `MetricExpr::Ref { name }` did not resolve to any measure or
    /// metric.
    #[error("metric {metric} references unknown name {referenced}")]
    UnresolvedMetricRef {
        /// The metric carrying the unresolved reference.
        metric: Name,
        /// The name the reference pointed at.
        referenced: Name,
    },
    /// Name validation failed (should not happen during `.build()` since
    /// names arrive already-parsed, but composed for completeness).
    #[error(transparent)]
    InvalidName(#[from] name::Error),
    /// `MonthDay` validation failed.
    #[error(transparent)]
    InvalidMonthDay(#[from] month_day::Error),
    /// Aggregation construction failed (e.g., empty `non_additive_dims`).
    #[error(transparent)]
    InvalidAggregation(#[from] measure::Error),
}

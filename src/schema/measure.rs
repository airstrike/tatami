//! [`Measure`], [`Aggregation`], [`SemiAgg`].

use serde::{Deserialize, Serialize};

use crate::schema::{Name, Unit};

/// A stored numeric column plus the rule for combining its values along each
/// dimension. Compare with `schema::Metric`, which is a named formula.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Measure {
    /// Measure name; unique within a schema.
    pub name: Name,
    /// Aggregation rule — fully determines how this measure rolls up.
    pub aggregation: Aggregation,
    /// Optional unit of measure.
    pub unit: Option<Unit>,
}

impl Measure {
    /// Construct a measure with no declared unit.
    #[must_use]
    pub fn new(name: Name, aggregation: Aggregation) -> Self {
        Self {
            name,
            aggregation,
            unit: None,
        }
    }

    /// Fluent: set the unit.
    #[must_use]
    pub fn with_unit(mut self, unit: Unit) -> Self {
        self.unit = Some(unit);
        self
    }
}

/// The aggregation rule for a [`Measure`].
///
/// Previously two fields (`agg` + `additivity`); now one sum type so that
/// nonsense combinations (`Sum` + `SemiAdditive`, `DistinctCount` +
/// `Additive`, `First` as a top-level aggregator) are unrepresentable.
///
/// See [`Aggregation::semi_additive`] for the only fallible constructor —
/// the non-empty constraint on `non_additive_dims` is enforced at
/// construction.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Aggregation {
    /// Additive sum.
    Sum,
    /// Arithmetic mean.
    Avg,
    /// Minimum.
    Min,
    /// Maximum.
    Max,
    /// Count of fact rows.
    Count,
    /// Distinct count of a key column.
    DistinctCount,
    /// Semi-additive: additive along every dimension *except* the listed
    /// `non_additive_dims`; along those, apply `over`. Canonical example:
    /// `Stock` rolled up over `Time` using [`SemiAgg::Last`] (last child).
    SemiAdditive {
        /// Dims along which this measure is non-additive. Non-empty by
        /// construction — use [`Aggregation::semi_additive`].
        non_additive_dims: Vec<Name>,
        /// How to fold across the non-additive dims.
        over: SemiAgg,
    },
}

impl Aggregation {
    /// Total constructor for additive sum.
    #[must_use]
    pub fn sum() -> Self {
        Self::Sum
    }

    /// Total constructor for average.
    #[must_use]
    pub fn avg() -> Self {
        Self::Avg
    }

    /// Total constructor for minimum.
    #[must_use]
    pub fn min() -> Self {
        Self::Min
    }

    /// Total constructor for maximum.
    #[must_use]
    pub fn max() -> Self {
        Self::Max
    }

    /// Total constructor for row count.
    #[must_use]
    pub fn count() -> Self {
        Self::Count
    }

    /// Total constructor for distinct count.
    #[must_use]
    pub fn distinct_count() -> Self {
        Self::DistinctCount
    }

    /// Fallible constructor for semi-additive aggregation.
    ///
    /// Rejects empty `non_additive_dims` — that would be identical to full
    /// additive aggregation, and a caller who wanted that should write
    /// [`Aggregation::sum`] instead.
    ///
    /// ```
    /// use tatami::schema::{Aggregation, Name, SemiAgg};
    /// let agg = Aggregation::semi_additive(
    ///     vec![Name::parse("Time").unwrap()],
    ///     SemiAgg::Last,
    /// ).unwrap();
    /// assert!(matches!(agg, Aggregation::SemiAdditive { .. }));
    /// ```
    pub fn semi_additive(non_additive_dims: Vec<Name>, over: SemiAgg) -> Result<Self, Error> {
        if non_additive_dims.is_empty() {
            return Err(Error::EmptyNonAdditiveDims);
        }
        Ok(Self::SemiAdditive {
            non_additive_dims,
            over,
        })
    }
}

/// How a [`Aggregation::SemiAdditive`] measure folds across its non-additive
/// dimensions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SemiAgg {
    /// First child along the non-additive dim.
    First,
    /// Last child along the non-additive dim (stock / balance).
    Last,
    /// Average across children.
    Avg,
    /// Minimum across children.
    Min,
    /// Maximum across children.
    Max,
}

/// Errors produced by [`Aggregation`] smart constructors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// `non_additive_dims` was empty in [`Aggregation::semi_additive`].
    #[error("semi-additive aggregation requires at least one non-additive dim")]
    EmptyNonAdditiveDims,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(s: &str) -> Name {
        Name::parse(s).expect("valid")
    }

    #[test]
    fn aggregation_sum_roundtrip_stable() {
        let agg = Aggregation::sum();
        let json = serde_json::to_string(&agg).expect("serialize");
        assert_eq!(json, r#"{"kind":"sum"}"#);
        let back: Aggregation = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(agg, back);
    }

    #[test]
    fn aggregation_semi_additive_roundtrip_stable() {
        let agg =
            Aggregation::semi_additive(vec![n("Time")], SemiAgg::Last).expect("non-empty dims");
        let json = serde_json::to_string(&agg).expect("serialize");
        let back: Aggregation = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(agg, back);
    }

    #[test]
    fn aggregation_semi_additive_rejects_empty_dims() {
        assert!(matches!(
            Aggregation::semi_additive(vec![], SemiAgg::Last),
            Err(Error::EmptyNonAdditiveDims)
        ));
    }

    #[test]
    fn measure_with_unit_roundtrips() {
        let m = Measure::new(n("amount"), Aggregation::sum())
            .with_unit(crate::schema::Unit::parse("USD").expect("valid"));
        let json = serde_json::to_string(&m).expect("serialize");
        let back: Measure = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(m, back);
    }
}

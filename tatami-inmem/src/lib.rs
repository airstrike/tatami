//! Polars-backed reference implementation of [`tatami::Cube`].
//!
//! v0.1 scaffold — Phase 5 of MAP_PLAN.md fills in real evaluation. Phase 5a
//! adds structural validation at [`InMemoryCube::new`] time: every measure
//! and every dimension-level key must have a column of a sensible dtype in
//! the fact [`DataFrame`]. [`InMemoryCube::query`] and
//! [`InMemoryCube::members`] still short-circuit with
//! [`Error::NotImplemented`] until Phase 5c–g land.

use polars_core::prelude::{Column, DataFrame, DataType};
use tatami::schema::{Aggregation, Dimension, Measure, Name, Schema};
use tatami::{Cube, MemberRef, MemberRelation, Query, Results};

/// In-memory cube backed by a Polars [`DataFrame`].
///
/// Construct via [`InMemoryCube::new`], which validates that the fact frame's
/// columns match the schema's measures and dimension levels (see Phase 5a of
/// MAP_PLAN.md §5). Evaluation lands in Phase 5c–g; today
/// [`InMemoryCube::query`] and [`InMemoryCube::members`] return
/// [`Error::NotImplemented`].
#[derive(Debug)]
pub struct InMemoryCube {
    schema: Schema,
    // Prefixed with `_` to silence dead-code warnings until Phase 5c–g wires
    // the fact frame into evaluation. Renamed to `df` at that point.
    _df: DataFrame,
}

impl InMemoryCube {
    /// Construct an in-memory cube from a fact frame and its schema.
    ///
    /// Validates that every [`Measure`] and every level in every
    /// [`tatami::schema::Hierarchy`] of every [`Dimension`] has a
    /// correspondingly-named column of a sensible dtype in `df`. Returns
    /// [`Error::MissingMeasureColumn`], [`Error::MissingLevelColumn`],
    /// [`Error::MeasureDtypeMismatch`], or [`Error::LevelDtypeMismatch`] on
    /// the first violation found.
    ///
    /// Semantic checks that depend on the resolved query pipeline — metric
    /// ref resolution, `Lag` dim-kind, `PeriodsToDate` level membership —
    /// are deferred to Phase 5c.
    pub fn new(df: DataFrame, schema: Schema) -> Result<Self, Error> {
        validate(&df, &schema)?;
        Ok(Self { schema, _df: df })
    }
}

impl Cube for InMemoryCube {
    type Error = Error;

    async fn schema(&self) -> Result<Schema, Self::Error> {
        Ok(self.schema.clone())
    }

    async fn query(&self, _q: &Query) -> Result<Results, Self::Error> {
        Err(Error::NotImplemented)
    }

    async fn members(
        &self,
        _dim: &Name,
        _hierarchy: &Name,
        _at: &MemberRef,
        _relation: MemberRelation,
    ) -> Result<Vec<MemberRef>, Self::Error> {
        Err(Error::NotImplemented)
    }
}

/// Errors produced by [`InMemoryCube`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Evaluation not yet implemented (Phase 5 sub-steps 5c–g light this up).
    #[error("tatami-inmem: evaluation not yet implemented (Phase 5 of MAP_PLAN.md)")]
    NotImplemented,

    /// A measure's column is missing from the fact frame.
    #[error("measure {measure} references column {column} which is not in the fact frame")]
    MissingMeasureColumn {
        /// Name of the measure whose column is missing.
        measure: Name,
        /// Expected column name (mirrors the measure name today).
        column: Name,
    },

    /// A dimension level's key column is missing from the fact frame.
    #[error(
        "dimension {dim}, hierarchy {hierarchy}, level {level}: key column {column} is not in the fact frame"
    )]
    MissingLevelColumn {
        /// Dimension name.
        dim: Name,
        /// Hierarchy name within the dimension.
        hierarchy: Name,
        /// Level name within the hierarchy.
        level: Name,
        /// The level's key column name.
        column: Name,
    },

    /// A measure's column is not a supported dtype for its aggregation.
    #[error(
        "measure {measure}: column {column} has dtype {dtype}, which is not valid for {aggregation}"
    )]
    MeasureDtypeMismatch {
        /// Name of the offending measure.
        measure: Name,
        /// Column name as declared by the measure.
        column: Name,
        /// Stringified fact-frame dtype (human-readable; does not expose
        /// [`polars_core::datatypes::DataType`] in the public API).
        dtype: String,
        /// Short tag for the measure's aggregation — `"Sum"`, `"Avg"`,
        /// `"DistinctCount"`, `"SemiAdditive"`, etc.
        aggregation: String,
    },

    /// A dimension level's key column dtype is not discrete-friendly.
    ///
    /// Floats are rejected outright — float equality is a footgun for
    /// hierarchy keys.
    #[error(
        "dimension {dim}, level {level}: column {column} has dtype {dtype}, which is not valid as a hierarchy key"
    )]
    LevelDtypeMismatch {
        /// Dimension name.
        dim: Name,
        /// Level name.
        level: Name,
        /// Level's key column name.
        column: Name,
        /// Stringified fact-frame dtype (human-readable).
        dtype: String,
    },
}

// ── Validation ─────────────────────────────────────────────────────────────

fn validate(df: &DataFrame, schema: &Schema) -> Result<(), Error> {
    validate_measures(df, &schema.measures)?;
    validate_dimensions(df, &schema.dimensions)?;
    Ok(())
}

fn validate_measures(df: &DataFrame, measures: &[Measure]) -> Result<(), Error> {
    for measure in measures {
        let column =
            find_column(df, measure.name.as_str()).ok_or_else(|| Error::MissingMeasureColumn {
                measure: measure.name.clone(),
                column: measure.name.clone(),
            })?;
        let dtype = column.dtype();
        if !dtype_class(dtype).accepted_by(&measure.aggregation) {
            return Err(Error::MeasureDtypeMismatch {
                measure: measure.name.clone(),
                column: measure.name.clone(),
                dtype: format_dtype(dtype),
                aggregation: aggregation_tag(&measure.aggregation).to_owned(),
            });
        }
    }
    Ok(())
}

fn validate_dimensions(df: &DataFrame, dimensions: &[Dimension]) -> Result<(), Error> {
    for dim in dimensions {
        for hierarchy in &dim.hierarchies {
            for level in &hierarchy.levels {
                let column = find_column(df, level.key.as_str()).ok_or_else(|| {
                    Error::MissingLevelColumn {
                        dim: dim.name.clone(),
                        hierarchy: hierarchy.name.clone(),
                        level: level.name.clone(),
                        column: level.key.clone(),
                    }
                })?;
                let dtype = column.dtype();
                if !dtype_class(dtype).is_discrete() {
                    return Err(Error::LevelDtypeMismatch {
                        dim: dim.name.clone(),
                        level: level.name.clone(),
                        column: level.key.clone(),
                        dtype: format_dtype(dtype),
                    });
                }
            }
        }
    }
    Ok(())
}

/// Find a column by name without bubbling Polars's typed error — this layer
/// only cares about presence, and handles the "not present" case explicitly.
fn find_column<'a>(df: &'a DataFrame, name: &str) -> Option<&'a Column> {
    df.get_columns().iter().find(|c| c.name().as_str() == name)
}

/// Coarse classification of a [`DataType`] for validation purposes.
///
/// The concrete `DataType` enum is large and feature-gated; flattening the
/// cases we care about into this small enum keeps validation logic tight
/// and the split between "numeric", "discrete", and "other" explicit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Class {
    /// Signed or unsigned integer — numeric *and* discrete-friendly.
    Integer,
    /// 32- or 64-bit float — numeric, *not* discrete (float equality is a
    /// footgun for member identity).
    Float,
    /// Utf-8 string — discrete-friendly; acceptable for `DistinctCount`.
    String,
    /// Anything else (booleans, binary, nested, temporal, nulls, …).
    Other,
}

impl Class {
    fn is_numeric(self) -> bool {
        matches!(self, Self::Integer | Self::Float)
    }

    fn is_discrete(self) -> bool {
        matches!(self, Self::Integer | Self::String)
    }

    /// Whether this column class is acceptable for the given aggregation.
    ///
    /// `DistinctCount` additionally tolerates strings. `Count` is a
    /// row-count and does not inspect the column's dtype (the column must
    /// still exist).
    fn accepted_by(self, agg: &Aggregation) -> bool {
        match agg {
            Aggregation::Sum
            | Aggregation::Avg
            | Aggregation::Min
            | Aggregation::Max
            | Aggregation::SemiAdditive { .. } => self.is_numeric(),
            Aggregation::DistinctCount => self.is_numeric() || matches!(self, Self::String),
            Aggregation::Count => true,
            // `Aggregation` is `#[non_exhaustive]`; be conservative for
            // any future variant and require a numeric column until Phase
            // 5a is revisited alongside the new variant's semantics.
            _ => self.is_numeric(),
        }
    }
}

fn dtype_class(dt: &DataType) -> Class {
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Class::Integer,
        DataType::Float32 | DataType::Float64 => Class::Float,
        DataType::String => Class::String,
        _ => Class::Other,
    }
}

/// Short human-readable tag for an [`Aggregation`] — drops the inner detail
/// of `SemiAdditive` so error messages stay compact.
fn aggregation_tag(agg: &Aggregation) -> &'static str {
    match agg {
        Aggregation::Sum => "Sum",
        Aggregation::Avg => "Avg",
        Aggregation::Min => "Min",
        Aggregation::Max => "Max",
        Aggregation::Count => "Count",
        Aggregation::DistinctCount => "DistinctCount",
        Aggregation::SemiAdditive { .. } => "SemiAdditive",
        // `Aggregation` is `#[non_exhaustive]`; surface the fallback
        // plainly rather than panicking on a future variant.
        _ => "Aggregation",
    }
}

/// Stringify a [`DataType`] for error messages without exposing the polars
/// type in the public `Error` API.
fn format_dtype(dt: &DataType) -> String {
    dt.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars_core::df;
    use tatami::schema::{Hierarchy, Level, SemiAgg};

    fn n(s: &str) -> Name {
        Name::parse(s).expect("valid name")
    }

    /// Minimal hewton-shaped schema: one regular dim with a two-level
    /// hierarchy, one time dim with one level, one sum measure.
    fn small_schema() -> Schema {
        Schema::builder()
            .dimension(
                Dimension::regular(n("Geography")).hierarchy(
                    Hierarchy::new(n("Default"))
                        .level(Level::new(n("Region"), n("region")))
                        .level(Level::new(n("Country"), n("country"))),
                ),
            )
            .dimension(
                Dimension::time(n("Time"), Vec::new()).hierarchy(
                    Hierarchy::new(n("Default")).level(Level::new(n("Month"), n("month"))),
                ),
            )
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .build()
            .expect("schema valid")
    }

    fn small_frame() -> DataFrame {
        df! {
            "region"  => ["West", "East"],
            "country" => ["US", "US"],
            "month"   => ["2026-01", "2026-02"],
            "amount"  => [100.0_f64, 200.0],
        }
        .expect("frame valid")
    }

    #[test]
    fn new_accepts_hewton_shaped_schema_and_frame() {
        let cube = InMemoryCube::new(small_frame(), small_schema()).expect("construct cube");
        drop(cube);
    }

    #[test]
    fn new_rejects_missing_measure_column() {
        let schema = Schema::builder()
            .dimension(Dimension::regular(n("Geography")).hierarchy(
                Hierarchy::new(n("Default")).level(Level::new(n("Region"), n("region"))),
            ))
            .measure(Measure::new(n("foo"), Aggregation::sum()))
            .build()
            .expect("schema valid");
        let df = df! {
            "region" => ["West"],
            // no "foo" column
        }
        .expect("frame valid");

        let err = InMemoryCube::new(df, schema).expect_err("missing measure column");
        match err {
            Error::MissingMeasureColumn { measure, column } => {
                assert_eq!(measure.as_str(), "foo");
                assert_eq!(column.as_str(), "foo");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn new_rejects_missing_level_column() {
        let schema =
            Schema::builder()
                .dimension(Dimension::regular(n("Geography")).hierarchy(
                    Hierarchy::new(n("Default")).level(Level::new(n("Region"), n("bar"))),
                ))
                .measure(Measure::new(n("amount"), Aggregation::sum()))
                .build()
                .expect("schema valid");
        let df = df! {
            "amount" => [1.0_f64],
            // no "bar" column
        }
        .expect("frame valid");

        let err = InMemoryCube::new(df, schema).expect_err("missing level column");
        match err {
            Error::MissingLevelColumn {
                dim,
                hierarchy,
                level,
                column,
            } => {
                assert_eq!(dim.as_str(), "Geography");
                assert_eq!(hierarchy.as_str(), "Default");
                assert_eq!(level.as_str(), "Region");
                assert_eq!(column.as_str(), "bar");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn new_rejects_wrong_measure_dtype() {
        let schema = Schema::builder()
            .dimension(Dimension::regular(n("Geography")).hierarchy(
                Hierarchy::new(n("Default")).level(Level::new(n("Region"), n("region"))),
            ))
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .build()
            .expect("schema valid");
        let df = df! {
            "region" => ["West"],
            "amount" => ["not-a-number"],
        }
        .expect("frame valid");

        let err = InMemoryCube::new(df, schema).expect_err("wrong measure dtype");
        match err {
            Error::MeasureDtypeMismatch {
                measure,
                column,
                aggregation,
                ..
            } => {
                assert_eq!(measure.as_str(), "amount");
                assert_eq!(column.as_str(), "amount");
                assert_eq!(aggregation, "Sum");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn new_rejects_float_dimension_key() {
        let schema =
            Schema::builder()
                .dimension(Dimension::regular(n("Product")).hierarchy(
                    Hierarchy::new(n("Default")).level(Level::new(n("Price"), n("price"))),
                ))
                .measure(Measure::new(n("amount"), Aggregation::sum()))
                .build()
                .expect("schema valid");
        let df = df! {
            "price"  => [9.99_f64, 19.99],
            "amount" => [1.0_f64, 2.0],
        }
        .expect("frame valid");

        let err = InMemoryCube::new(df, schema).expect_err("float dim key");
        match err {
            Error::LevelDtypeMismatch {
                dim, level, column, ..
            } => {
                assert_eq!(dim.as_str(), "Product");
                assert_eq!(level.as_str(), "Price");
                assert_eq!(column.as_str(), "price");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn new_accepts_distinct_count_on_string_column() {
        let schema = Schema::builder()
            .dimension(Dimension::regular(n("Geography")).hierarchy(
                Hierarchy::new(n("Default")).level(Level::new(n("Region"), n("region"))),
            ))
            .measure(Measure::new(n("user_id"), Aggregation::distinct_count()))
            .build()
            .expect("schema valid");
        let df = df! {
            "region"  => ["West", "East"],
            "user_id" => ["u1", "u2"],
        }
        .expect("frame valid");

        InMemoryCube::new(df, schema).expect("distinct count tolerates strings");
    }

    #[test]
    fn semi_additive_measure_requires_numeric_column() {
        let schema =
            Schema::builder()
                .dimension(Dimension::time(n("Time"), Vec::new()).hierarchy(
                    Hierarchy::new(n("Default")).level(Level::new(n("Month"), n("month"))),
                ))
                .measure(Measure::new(
                    n("stock"),
                    Aggregation::semi_additive(vec![n("Time")], SemiAgg::Last).expect("non-empty"),
                ))
                .build()
                .expect("schema valid");
        let df = df! {
            "month" => ["2026-01"],
            "stock" => ["oops"],
        }
        .expect("frame valid");

        let err = InMemoryCube::new(df, schema).expect_err("semi-additive needs numeric");
        assert!(matches!(
            err,
            Error::MeasureDtypeMismatch { aggregation, .. } if aggregation == "SemiAdditive"
        ));
    }
}

//! Polars-backed reference implementation of [`tatami::Cube`].
//!
//! [`InMemoryCube`] wraps a fact-source [`DataFrame`] plus a [`Schema`] and
//! answers [`tatami::Query`] values end-to-end — set evaluation, tuple
//! filtering, measure aggregation (including semi-additive rollup), metric
//! expression evaluation (`Lag`, `PeriodsToDate`, `At`, arithmetic), and
//! result assembly into the `Axes`-determined [`Results`] shape.
//!
//! # Validation: construction vs. query time
//!
//! [`InMemoryCube::new`] is fail-fast on everything the schema + frame
//! can jointly decide up front:
//!
//! - Every [`tatami::schema::Measure`] has a same-named column with a
//!   dtype acceptable to its [`tatami::schema::Aggregation`] (numeric
//!   for `Sum`/`Avg`/`Min`/`Max`/`SemiAdditive`; `DistinctCount` also
//!   tolerates strings).
//! - Every level in every [`tatami::schema::Hierarchy`] has a key column
//!   of a discrete-friendly dtype (integers or strings; floats are
//!   rejected — equality is a footgun for hierarchy keys).
//! - A one-shot scan builds a per-`(dim, hierarchy)` member catalogue,
//!   surfacing `MalformedMemberValue` on any cell that fails
//!   [`tatami::schema::Name::parse`].
//!
//! Query-dependent checks are deferred to [`InMemoryCube::query`], where
//! a `Query → ResolvedQuery` step binds refs to schema handles: metric /
//! measure name resolution, `Lag`'s dim must be time, `PeriodsToDate`'s
//! level must appear in a time hierarchy, cross-join sides must address
//! disjoint dims, named-set references must resolve without cycles.
//! These surface as `ResolveUnresolved*` / `ResolveUnknown*` / `Resolve…`
//! variants of [`Error`].
//!
//! # See also
//!
//! `examples/hewton/` exercises the full pipeline against ~2,300 rows of
//! synthetic hotel-sales data — the target surface every change to this
//! crate is measured against.

mod catalogue;
mod eval;
mod resolve;

use polars_core::prelude::{Column, DataFrame, DataType};
use tatami::query::Path;
use tatami::schema::{Aggregation, Dimension, Measure, Name, Schema};
use tatami::{Cube, MemberRef, MemberRelation, Query, Results};

use crate::catalogue::Catalogue;

/// In-memory cube backed by a Polars [`DataFrame`].
///
/// Construct via [`InMemoryCube::new`], which validates that the fact frame's
/// columns match the schema's measures and dimension levels (Phase 5a of
/// MAP_PLAN.md §5) and then builds a member catalogue used by
/// [`InMemoryCube::members`] (Phase 5b). [`InMemoryCube::query`] evaluates
/// resolved queries end-to-end — resolve (5c) → set / metric eval (5d–f)
/// → assemble results (5g).
#[derive(Debug)]
pub struct InMemoryCube {
    pub(crate) schema: Schema,
    pub(crate) catalogue: Catalogue,
    pub(crate) df: DataFrame,
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
    /// After validation, scans `df` once per `(dim, hierarchy)` pair to build
    /// the per-hierarchy member tree used by [`InMemoryCube::members`].
    /// Surfaces [`Error::MalformedMemberValue`] if any non-null level cell
    /// fails [`Name::parse`].
    ///
    /// Semantic checks that depend on the resolved query pipeline — metric
    /// ref resolution, `Lag` dim-kind, `PeriodsToDate` level membership —
    /// are deferred to Phase 5c.
    pub fn new(df: DataFrame, schema: Schema) -> Result<Self, Error> {
        validate(&df, &schema)?;
        let catalogue = Catalogue::build(&df, &schema)?;
        Ok(Self {
            schema,
            catalogue,
            df,
        })
    }

    /// Crate-internal accessor for the member catalogue. Phase 5d's
    /// `Set::Members` evaluation consumes this directly; the `allow` clears
    /// the dead-code warning until that wiring lands.
    #[allow(dead_code)]
    pub(crate) fn catalogue(&self) -> &Catalogue {
        &self.catalogue
    }

    /// Crate-internal accessor for the fact frame. Phase 5f's metric
    /// evaluator reaches through this to resolve `Ref` leaves via
    /// [`crate::eval::aggregate::evaluate_measure`].
    #[allow(dead_code)]
    pub(crate) fn df(&self) -> &DataFrame {
        &self.df
    }

    /// Lift a public [`Query`] into a crate-internal `ResolvedQuery` bound
    /// to this cube's schema and catalogue — Phase 5c of MAP_PLAN.md §5.
    ///
    /// `Cube::query` does not yet invoke this; the wiring lands in Phase
    /// 5g alongside set / tuple / metric evaluation. Exposed as
    /// `pub(crate)` so 5d–g can reach it.
    #[allow(dead_code)]
    pub(crate) fn resolve<'s>(&'s self, q: &Query) -> Result<resolve::ResolvedQuery<'s>, Error> {
        resolve::resolve(q, &self.schema, &self.catalogue)
    }

    /// Enumerate every member at a named level within a named hierarchy of
    /// a named dimension.
    ///
    /// The [`Cube`] trait's [`Cube::members`] takes an `at: &MemberRef` that
    /// is structurally non-empty, so it cannot express "every top-level
    /// member of this hierarchy" without first constructing a full
    /// [`Query`]. This inherent method closes that gap for composers /
    /// slicer UIs that want to offer a top-of-hierarchy picker without
    /// building a set expression just to list members.
    ///
    /// Returns:
    /// - [`Error::ResolveUnknownDimension`] if `dim` doesn't exist.
    /// - [`Error::ResolveUnknownHierarchy`] if `hierarchy` doesn't exist
    ///   within `dim`.
    /// - [`Error::ResolveUnknownLevel`] if `level` doesn't exist within
    ///   `hierarchy`.
    ///
    /// The returned members are in pre-order DFS traversal order (the same
    /// order the catalogue's iteration uses elsewhere), so output is
    /// deterministic.
    pub fn level_members(
        &self,
        dim: &Name,
        hierarchy: &Name,
        level: &Name,
    ) -> Result<Vec<MemberRef>, Error> {
        let dim_def = self
            .schema
            .dimensions
            .iter()
            .find(|d| d.name == *dim)
            .ok_or_else(|| Error::ResolveUnknownDimension { dim: dim.clone() })?;
        let hierarchy_def = dim_def
            .hierarchies
            .iter()
            .find(|h| h.name == *hierarchy)
            .ok_or_else(|| Error::ResolveUnknownHierarchy {
                dim: dim.clone(),
                hierarchy: hierarchy.clone(),
            })?;
        let level_index = hierarchy_def
            .levels
            .iter()
            .position(|l| l.name == *level)
            .ok_or_else(|| Error::ResolveUnknownLevel {
                dim: dim.clone(),
                hierarchy: hierarchy.clone(),
                level: level.clone(),
            })?;
        // `members_at` returns `None` only when the `(dim, hierarchy)` pair
        // isn't catalogued — `new` catalogues every schema hierarchy, so
        // reaching that branch would mean internal corruption. Surface as
        // `UnknownHierarchy` defensively rather than panicking.
        self.catalogue
            .members_at(dim, hierarchy, level_index)
            .ok_or_else(|| Error::ResolveUnknownHierarchy {
                dim: dim.clone(),
                hierarchy: hierarchy.clone(),
            })
    }
}

impl Cube for InMemoryCube {
    type Error = Error;

    async fn schema(&self) -> Result<Schema, Self::Error> {
        Ok(self.schema.clone())
    }

    async fn query(&self, q: &Query) -> Result<Results, Self::Error> {
        let resolved = resolve::resolve(q, &self.schema, &self.catalogue)?;
        eval::query::evaluate(&resolved, self)
    }

    async fn members(
        &self,
        dim: &Name,
        hierarchy: &Name,
        at: &MemberRef,
        relation: MemberRelation,
    ) -> Result<Vec<MemberRef>, Self::Error> {
        self.catalogue.members(dim, hierarchy, at, relation)
    }
}

/// Errors produced by [`InMemoryCube`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
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

    /// A non-null cell in a level-key column could not be parsed as a
    /// [`Name`] (e.g., empty string after a cast, or a value with leading
    /// whitespace).
    ///
    /// Surfaced at [`InMemoryCube::new`] time — the catalogue build is
    /// fail-fast so callers see bad data up front, not on the first query.
    #[error("dimension {dim}, level {level}: cell value {value:?} is not a valid member name")]
    MalformedMemberValue {
        /// Dimension name.
        dim: Name,
        /// Level name within the dimension.
        level: Name,
        /// The raw cell value, as stringified from the fact frame.
        value: String,
    },

    /// `members()` was called for a `(dim, hierarchy)` pair that is not in
    /// the schema's catalogue.
    #[error("dimension {dim} has no hierarchy named {hierarchy}")]
    UnknownHierarchy {
        /// Dimension name.
        dim: Name,
        /// Hierarchy name that was not found.
        hierarchy: Name,
    },

    /// The `at` [`MemberRef`] passed to `members()` refers to a different
    /// `(dim, hierarchy)` than the pair the call is asking about.
    #[error(
        "member reference points at ({actual_dim}, {actual_hierarchy}) but members() was called for ({expected_dim}, {expected_hierarchy})"
    )]
    MemberRefHierarchyMismatch {
        /// Dimension the call was made against.
        expected_dim: Name,
        /// Hierarchy the call was made against.
        expected_hierarchy: Name,
        /// Dimension carried by the `at` [`MemberRef`].
        actual_dim: Name,
        /// Hierarchy carried by the `at` [`MemberRef`].
        actual_hierarchy: Name,
    },

    /// The `at` path does not exist in the catalogue — some segment along
    /// the path has no matching member.
    #[error("dimension {dim}, hierarchy {hierarchy}: no member at path {path}")]
    UnknownMember {
        /// Dimension name.
        dim: Name,
        /// Hierarchy name.
        hierarchy: Name,
        /// Requested path.
        path: Path,
    },

    /// A [`MemberRelation`] variant this backend has not yet learnt to
    /// handle — `MemberRelation` is `#[non_exhaustive]`, so forward
    /// compatibility requires a fallible case rather than a panic.
    #[error("unsupported member relation: {0:?}")]
    UnsupportedRelation(MemberRelation),

    // ── Phase 5c: resolve (Query → ResolvedQuery) ──────────────────────
    //
    // These variants surface from `crate::resolve::resolve`. Prefixed
    // `Resolve*` so message text reads naturally when bubbled up through
    // the shared `Error` type.
    /// A name in [`tatami::Query::metrics`], a predicate, or an expression
    /// `Ref` did not resolve to any measure or metric.
    #[error("unresolved metric reference: {name}")]
    ResolveUnresolvedRef {
        /// The name the reference pointed at.
        name: Name,
    },

    /// A ref matched both a measure and a metric — schema construction
    /// rejects collisions at build time, so this variant is defensive.
    #[error("ambiguous metric reference: {name} matches both a measure and a metric")]
    ResolveAmbiguousRef {
        /// The ambiguous name.
        name: Name,
    },

    /// A dimension name in the query did not appear in the schema.
    #[error("resolve: unknown dimension {dim}")]
    ResolveUnknownDimension {
        /// The unknown dimension name.
        dim: Name,
    },

    /// A hierarchy name did not exist within the named dimension.
    #[error("resolve: dimension {dim} has no hierarchy named {hierarchy}")]
    ResolveUnknownHierarchy {
        /// Dimension name.
        dim: Name,
        /// Hierarchy name that was not found.
        hierarchy: Name,
    },

    /// A level name did not exist within the named hierarchy.
    #[error("resolve: dimension {dim}, hierarchy {hierarchy}: no level named {level}")]
    ResolveUnknownLevel {
        /// Dimension name.
        dim: Name,
        /// Hierarchy name.
        hierarchy: Name,
        /// Level name that was not found.
        level: Name,
    },

    /// A `MemberRef`'s path does not structurally fit in the hierarchy
    /// (too long). Full catalogue existence is checked at eval time.
    #[error("resolve: dimension {dim}, hierarchy {hierarchy}: unknown member at path {path}")]
    ResolveUnknownMember {
        /// Dimension name.
        dim: Name,
        /// Hierarchy name.
        hierarchy: Name,
        /// The offending path.
        path: Path,
    },

    /// `Expr::Lag` named a dimension whose kind is not Time.
    #[error("resolve: Lag over dimension {dim} — not a time dimension")]
    ResolveLagDimNotTime {
        /// The offending dimension.
        dim: Name,
    },

    /// `Expr::PeriodsToDate` named a level that does not appear in any
    /// Time-kind hierarchy.
    #[error("resolve: PeriodsToDate level {level} does not appear in any time hierarchy")]
    ResolvePeriodsToDateLevelNotInTime {
        /// The offending level name.
        level: Name,
    },

    /// `Set::Named` referenced a named set not declared in the schema.
    #[error("resolve: unknown named set {name}")]
    ResolveUnknownNamedSet {
        /// The offending name.
        name: Name,
    },

    /// A named set transitively references itself.
    #[error("resolve: named set {name} participates in a cycle")]
    ResolveNamedSetCycle {
        /// The named set at which the cycle was detected.
        name: Name,
    },

    /// `Set::CrossJoin` sides addressed a common dimension.
    #[error("resolve: cross-join sides overlap on dimension {dim}")]
    ResolveCrossJoinDimsOverlap {
        /// The shared dimension.
        dim: Name,
    },

    /// `Set::Union` sides addressed different dimensions.
    #[error("resolve: union sides address different dimensions: {left_dims:?} vs {right_dims:?}")]
    ResolveUnionDimsMismatch {
        /// Dimensions addressed by the left side.
        left_dims: Vec<Name>,
        /// Dimensions addressed by the right side.
        right_dims: Vec<Name>,
    },

    /// `Set::Descendants.to_level` was at or above the source set's output
    /// level — descendants must go deeper.
    #[error(
        "resolve: descendants to_level {to_level} is not below the source set's level {set_level}"
    )]
    ResolveDescendantsLevelNotBelow {
        /// The source set's output level.
        set_level: Name,
        /// The requested target level.
        to_level: Name,
    },

    /// `Set::Range.from` and `Set::Range.to` are at different levels.
    #[error("resolve: range endpoints at different levels: {from_level} vs {to_level}")]
    ResolveRangeMembersAtDifferentLevels {
        /// The `from` member's level.
        from_level: Name,
        /// The `to` member's level.
        to_level: Name,
    },

    /// A set composition could not be resolved because the inner set's
    /// shape is ambiguous (for example, `Children` of a cross-join).
    #[error("resolve: set composition is not well-formed: {reason}")]
    ResolveSetCompositionIllFormed {
        /// Human-readable reason.
        reason: &'static str,
    },

    // ── Phase 5d: set evaluation ───────────────────────────────────────
    /// A set evaluator reached a `ResolvedSet` whose shape it could not
    /// evaluate — for example, a cross-join as the argument of `Children`,
    /// or an `Explicit` set with members spanning multiple dims under
    /// `Children`.
    #[error("set evaluation: ill-formed composition: {reason}")]
    EvalSetCompositionIllFormed {
        /// Human-readable reason.
        reason: &'static str,
    },

    /// A `Range` endpoint comparison in the catalogue indicated the `from`
    /// path sorts after the `to` path. `from <= to` is a structural
    /// precondition of `Set::Range`; this variant surfaces the violation
    /// when the catalogue's traversal order disagrees with the caller's
    /// declared direction.
    #[error("set evaluation: range endpoints inverted: {from} sorts after {to}")]
    EvalRangeInverted {
        /// Offending lower endpoint.
        from: Path,
        /// Offending upper endpoint.
        to: Path,
    },

    // ── Phase 5e: tuple + aggregate evaluation ─────────────────────────
    //
    // The filter and aggregate primitives in `crate::eval::tuple` and
    // `crate::eval::aggregate` run inside the polars runtime. Errors from
    // that runtime are the one internal boundary where `Result` is
    // appropriate inside the eval pipeline (MAP §0.5).
    /// A polars filter call surfaced an error while materialising the
    /// boolean mask for a resolved tuple.
    #[error("tuple evaluation: polars filter failed: {reason}")]
    EvalFilterFailed {
        /// The polars error text.
        reason: String,
    },

    /// A level-key or measure column disappeared between cube
    /// construction and eval time. Phase 5a rules this out structurally,
    /// so the variant is a defensive surfacing of "polars runtime
    /// invariant violation" rather than a user-reachable error — the
    /// §0.5 "Result at the boundary" rule applies because the polars
    /// runtime is the boundary.
    #[error("eval: expected column {column} to exist at query time")]
    EvalColumnMissing {
        /// The missing column name (measure name or level key).
        column: String,
    },

    /// An aggregation call against polars surfaced an error — a sum /
    /// mean / min / max returned a non-numeric scalar, or the measure
    /// column iteration hit a polars runtime failure.
    #[error("aggregate evaluation: measure {measure}: {reason}")]
    EvalAggregateFailed {
        /// The measure whose aggregation failed.
        measure: Name,
        /// The polars error text (or internal reason string).
        reason: String,
    },

    // ── Phase 5f: metric expression evaluation ─────────────────────────
    //
    // The metric-tree evaluator (`crate::eval::metric::evaluate_expr`) is
    // mostly total — arithmetic failures and unbound dims surface as
    // `Cell::Error` / `Cell::Missing` rather than `Result::Err`. The two
    // genuine error paths are defensive: a name that resolve (5c) did not
    // flag, and a metric-to-metric cycle (5c checks name existence, not
    // recursion shape).
    /// A metric expression's `Ref` did not resolve to any measure or
    /// metric at eval time. Phase 5c should rule this out structurally;
    /// the variant is defensive so the recursion stays total.
    #[error("metric evaluation: unresolved reference {name}")]
    EvalUnresolvedRef {
        /// The name the reference pointed at.
        name: Name,
    },

    /// A metric's expression transitively references itself through one or
    /// more metric-to-metric hops. Surfaced the first time the evaluator
    /// would revisit an already-entered metric.
    #[error("metric evaluation: metric {name} participates in a cycle")]
    EvalMetricCycle {
        /// The metric at which the cycle was detected.
        name: Name,
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

    // ── Phase 5b: member catalogue + navigation ─────────────────────────

    /// Two-level Geography schema: root `Region`, then `Country`.
    /// Reused by several 5b tests that only need a single hierarchy.
    fn geo_schema() -> Schema {
        Schema::builder()
            .dimension(
                Dimension::regular(n("Geography")).hierarchy(
                    Hierarchy::new(n("Default"))
                        .level(Level::new(n("Region"), n("region")))
                        .level(Level::new(n("Country"), n("country"))),
                ),
            )
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .build()
            .expect("schema valid")
    }

    /// Three-level Geography schema: `World` → `Country` → `City`. Used by
    /// the leaves / descendants-depth tests.
    fn geo3_schema() -> Schema {
        Schema::builder()
            .dimension(
                Dimension::regular(n("Geography")).hierarchy(
                    Hierarchy::new(n("Default"))
                        .level(Level::new(n("World"), n("world")))
                        .level(Level::new(n("Country"), n("country")))
                        .level(Level::new(n("City"), n("city"))),
                ),
            )
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .build()
            .expect("schema valid")
    }

    fn world_ref(segments: Vec<Name>) -> MemberRef {
        MemberRef::new(
            n("Geography"),
            n("Default"),
            Path::parse(segments).expect("non-empty path"),
        )
    }

    #[tokio::test]
    async fn catalogue_discovers_distinct_members_per_hierarchy() {
        let df = df! {
            "region"  => ["World", "World", "World"],
            "country" => ["US", "US", "UK"],
            "amount"  => [1.0_f64, 2.0, 3.0],
        }
        .expect("frame valid");
        let cube = InMemoryCube::new(df, geo_schema()).expect("construct cube");

        let roots = cube
            .members(
                &n("Geography"),
                &n("Default"),
                &world_ref(vec![n("World")]),
                MemberRelation::Parent,
            )
            .await
            .expect("parent query");
        assert!(roots.is_empty(), "root-level members have no parent");

        let countries = cube
            .members(
                &n("Geography"),
                &n("Default"),
                &world_ref(vec![n("World")]),
                MemberRelation::Children,
            )
            .await
            .expect("children query");
        let tails: Vec<&str> = countries
            .iter()
            .map(|m| m.path.tail().last().expect("has tail").as_str())
            .collect();
        assert_eq!(tails, vec!["UK", "US"], "BTreeMap-ordered children");
    }

    #[tokio::test]
    async fn catalogue_skips_rows_with_null_level_values() {
        // "country" has a null in the first row — that row must not produce a
        // synthetic null child under "World".
        let df = df! {
            "region"  => [Some("World"),         Some("World"), Some("World")],
            "country" => [None::<&str>,          Some("US"),    Some("UK")],
            "amount"  => [1.0_f64,               2.0,           3.0],
        }
        .expect("frame valid");
        let cube = InMemoryCube::new(df, geo_schema()).expect("construct cube");

        let countries = cube
            .members(
                &n("Geography"),
                &n("Default"),
                &world_ref(vec![n("World")]),
                MemberRelation::Children,
            )
            .await
            .expect("children query");
        let tails: Vec<&str> = countries
            .iter()
            .map(|m| m.path.tail().last().expect("has tail").as_str())
            .collect();
        assert_eq!(tails, vec!["UK", "US"]);
    }

    #[tokio::test]
    async fn catalogue_deduplicates_rows() {
        // Ten identical rows → one member per distinct path.
        let df = df! {
            "region"  => ["World"; 10],
            "country" => ["US"; 10],
            "amount"  => [1.0_f64; 10],
        }
        .expect("frame valid");
        let cube = InMemoryCube::new(df, geo_schema()).expect("construct cube");

        let countries = cube
            .members(
                &n("Geography"),
                &n("Default"),
                &world_ref(vec![n("World")]),
                MemberRelation::Children,
            )
            .await
            .expect("children query");
        assert_eq!(countries.len(), 1);
        assert_eq!(
            countries[0].path.tail().last().expect("has tail").as_str(),
            "US"
        );
    }

    #[test]
    fn new_rejects_malformed_member_value() {
        // Empty string at a level-key cell fails `Name::parse`.
        let df = df! {
            "region"  => ["World", ""],
            "country" => ["US", "UK"],
            "amount"  => [1.0_f64, 2.0],
        }
        .expect("frame valid");

        let err = InMemoryCube::new(df, geo_schema()).expect_err("empty member value");
        match err {
            Error::MalformedMemberValue { dim, level, value } => {
                assert_eq!(dim.as_str(), "Geography");
                assert_eq!(level.as_str(), "Region");
                assert_eq!(value, "");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[tokio::test]
    async fn members_returns_children_in_btree_order() {
        // Input order on disk is { A, C, B }; output must be [A, B, C].
        let df = df! {
            "region"  => ["World", "World", "World"],
            "country" => ["A", "C", "B"],
            "amount"  => [1.0_f64, 2.0, 3.0],
        }
        .expect("frame valid");
        let cube = InMemoryCube::new(df, geo_schema()).expect("construct cube");

        let members = cube
            .members(
                &n("Geography"),
                &n("Default"),
                &world_ref(vec![n("World")]),
                MemberRelation::Children,
            )
            .await
            .expect("children query");
        let tails: Vec<&str> = members
            .iter()
            .map(|m| m.path.tail().last().expect("has tail").as_str())
            .collect();
        assert_eq!(tails, vec!["A", "B", "C"]);
    }

    #[tokio::test]
    async fn members_returns_siblings_excluding_self() {
        let df = df! {
            "region"  => ["World"; 4],
            "country" => ["Q1", "Q2", "Q3", "Q4"],
            "amount"  => [1.0_f64, 2.0, 3.0, 4.0],
        }
        .expect("frame valid");
        let cube = InMemoryCube::new(df, geo_schema()).expect("construct cube");

        let siblings = cube
            .members(
                &n("Geography"),
                &n("Default"),
                &world_ref(vec![n("World"), n("Q1")]),
                MemberRelation::Siblings,
            )
            .await
            .expect("siblings query");
        let tails: Vec<&str> = siblings
            .iter()
            .map(|m| m.path.tail().last().expect("has tail").as_str())
            .collect();
        assert_eq!(tails, vec!["Q2", "Q3", "Q4"]);
    }

    #[tokio::test]
    async fn members_returns_parent() {
        let df = df! {
            "region"  => ["World"],
            "country" => ["US"],
            "amount"  => [1.0_f64],
        }
        .expect("frame valid");
        let cube = InMemoryCube::new(df, geo_schema()).expect("construct cube");

        let parents = cube
            .members(
                &n("Geography"),
                &n("Default"),
                &world_ref(vec![n("World"), n("US")]),
                MemberRelation::Parent,
            )
            .await
            .expect("parent query");
        assert_eq!(parents.len(), 1);
        assert_eq!(parents[0].path.head().as_str(), "World");
        assert!(parents[0].path.tail().is_empty());
    }

    #[tokio::test]
    async fn members_returns_leaves_via_dfs() {
        // World → US → {CA, NY}. Leaves of US must be [CA, NY] in BTreeMap
        // order.
        let df = df! {
            "world"   => ["World", "World"],
            "country" => ["US", "US"],
            "city"    => ["NY", "CA"],
            "amount"  => [1.0_f64, 2.0],
        }
        .expect("frame valid");
        let cube = InMemoryCube::new(df, geo3_schema()).expect("construct cube");

        let leaves = cube
            .members(
                &n("Geography"),
                &n("Default"),
                &world_ref(vec![n("World"), n("US")]),
                MemberRelation::Leaves,
            )
            .await
            .expect("leaves query");
        let tails: Vec<&str> = leaves
            .iter()
            .map(|m| m.path.tail().last().expect("has tail").as_str())
            .collect();
        assert_eq!(tails, vec!["CA", "NY"]);
    }

    #[tokio::test]
    async fn members_descendants_respects_depth() {
        // World → US → {CA, NY}.
        let df = df! {
            "world"   => ["World", "World"],
            "country" => ["US", "US"],
            "city"    => ["CA", "NY"],
            "amount"  => [1.0_f64, 2.0],
        }
        .expect("frame valid");
        let cube = InMemoryCube::new(df, geo3_schema()).expect("construct cube");

        let d1 = cube
            .members(
                &n("Geography"),
                &n("Default"),
                &world_ref(vec![n("World")]),
                MemberRelation::Descendants(1),
            )
            .await
            .expect("depth-1 query");
        assert_eq!(d1.len(), 1, "only the single Country member at depth 1");

        let d2 = cube
            .members(
                &n("Geography"),
                &n("Default"),
                &world_ref(vec![n("World")]),
                MemberRelation::Descendants(2),
            )
            .await
            .expect("depth-2 query");
        // Pre-order DFS: US, CA, NY.
        let tails: Vec<&str> = d2
            .iter()
            .map(|m| m.path.tail().last().expect("has tail").as_str())
            .collect();
        assert_eq!(tails, vec!["US", "CA", "NY"]);
    }

    #[tokio::test]
    async fn members_rejects_unknown_hierarchy() {
        let cube = InMemoryCube::new(small_frame(), small_schema()).expect("construct cube");
        let at = world_ref(vec![n("West")]);
        // `Geography` exists, but `NoSuch` does not.
        let err = cube
            .members(&n("Geography"), &n("NoSuch"), &at, MemberRelation::Children)
            .await
            .expect_err("unknown hierarchy");
        match err {
            Error::UnknownHierarchy { dim, hierarchy } => {
                assert_eq!(dim.as_str(), "Geography");
                assert_eq!(hierarchy.as_str(), "NoSuch");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[tokio::test]
    async fn members_rejects_hierarchy_mismatched_memberref() {
        let cube = InMemoryCube::new(small_frame(), small_schema()).expect("construct cube");
        // Call asks about Geography/Default, but the ref points to Time/Default.
        let at = MemberRef::new(n("Time"), n("Default"), Path::of(n("2026-01")));
        let err = cube
            .members(
                &n("Geography"),
                &n("Default"),
                &at,
                MemberRelation::Children,
            )
            .await
            .expect_err("ref/hierarchy mismatch");
        match err {
            Error::MemberRefHierarchyMismatch {
                expected_dim,
                expected_hierarchy,
                actual_dim,
                actual_hierarchy,
            } => {
                assert_eq!(expected_dim.as_str(), "Geography");
                assert_eq!(expected_hierarchy.as_str(), "Default");
                assert_eq!(actual_dim.as_str(), "Time");
                assert_eq!(actual_hierarchy.as_str(), "Default");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[tokio::test]
    async fn members_rejects_unknown_path() {
        let cube = InMemoryCube::new(small_frame(), small_schema()).expect("construct cube");
        let at = world_ref(vec![n("NoSuchRegion")]);
        let err = cube
            .members(
                &n("Geography"),
                &n("Default"),
                &at,
                MemberRelation::Children,
            )
            .await
            .expect_err("unknown path");
        match err {
            Error::UnknownMember {
                dim,
                hierarchy,
                path,
            } => {
                assert_eq!(dim.as_str(), "Geography");
                assert_eq!(hierarchy.as_str(), "Default");
                assert_eq!(path.head().as_str(), "NoSuchRegion");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    // ── level_members ──────────────────────────────────────────────────

    #[test]
    fn level_members_returns_every_top_level_member() {
        let df = df! {
            "region"  => ["East", "West", "West"],
            "country" => ["UK", "US", "CA"],
            "month"   => ["2026-01", "2026-01", "2026-02"],
            "amount"  => [1.0_f64, 2.0, 3.0],
        }
        .expect("frame valid");
        let cube = InMemoryCube::new(df, small_schema()).expect("construct cube");
        let members = cube
            .level_members(&n("Geography"), &n("Default"), &n("Region"))
            .expect("level members");
        let tails: Vec<&str> = members.iter().map(|m| m.path.head().as_str()).collect();
        assert_eq!(tails, vec!["East", "West"]);
    }

    #[test]
    fn level_members_returns_deeper_level_members() {
        let df = df! {
            "world"   => ["World", "World", "World"],
            "country" => ["US", "US", "UK"],
            "city"    => ["NY", "CA", "London"],
            "amount"  => [1.0_f64, 2.0, 3.0],
        }
        .expect("frame valid");
        let cube = InMemoryCube::new(df, geo3_schema()).expect("construct cube");
        let members = cube
            .level_members(&n("Geography"), &n("Default"), &n("Country"))
            .expect("level members");
        let heads: Vec<&str> = members
            .iter()
            .map(|m| m.path.tail().last().expect("has tail").as_str())
            .collect();
        assert_eq!(heads, vec!["UK", "US"]);
    }

    #[test]
    fn level_members_rejects_unknown_dim() {
        let cube = InMemoryCube::new(small_frame(), small_schema()).expect("construct cube");
        let err = cube
            .level_members(&n("NoSuchDim"), &n("Default"), &n("Region"))
            .expect_err("unknown dim");
        match err {
            Error::ResolveUnknownDimension { dim } => assert_eq!(dim.as_str(), "NoSuchDim"),
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn level_members_rejects_unknown_hierarchy() {
        let cube = InMemoryCube::new(small_frame(), small_schema()).expect("construct cube");
        let err = cube
            .level_members(&n("Geography"), &n("NoSuch"), &n("Region"))
            .expect_err("unknown hierarchy");
        match err {
            Error::ResolveUnknownHierarchy { dim, hierarchy } => {
                assert_eq!(dim.as_str(), "Geography");
                assert_eq!(hierarchy.as_str(), "NoSuch");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn level_members_rejects_unknown_level() {
        let cube = InMemoryCube::new(small_frame(), small_schema()).expect("construct cube");
        let err = cube
            .level_members(&n("Geography"), &n("Default"), &n("NoSuchLevel"))
            .expect_err("unknown level");
        match err {
            Error::ResolveUnknownLevel {
                dim,
                hierarchy,
                level,
            } => {
                assert_eq!(dim.as_str(), "Geography");
                assert_eq!(hierarchy.as_str(), "Default");
                assert_eq!(level.as_str(), "NoSuchLevel");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }
}

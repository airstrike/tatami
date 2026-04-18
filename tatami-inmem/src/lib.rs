//! Polars-backed reference implementation of [`tatami::Cube`].
//!
//! v0.1 scaffold — Phase 5 of MAP_PLAN.md fills in real evaluation. Until
//! then [`InMemoryCube::query`] and [`InMemoryCube::members`] short-circuit
//! with [`Error::NotImplemented`]; [`InMemoryCube::schema`] already returns
//! the validated schema passed at construction.

use polars_core::prelude::DataFrame;
use tatami::schema::{Name, Schema};
use tatami::{Cube, MemberRef, MemberRelation, Query, Results};

/// In-memory cube backed by a Polars [`DataFrame`].
///
/// Construct via [`InMemoryCube::new`]. Phase 5 adds full column/dtype
/// validation and per-dimension member-catalogue build at construction
/// time; today's scaffold stores both values verbatim.
pub struct InMemoryCube {
    schema: Schema,
    // Prefixed with `_` to silence dead-code warnings until Phase 5 wires
    // the fact frame into evaluation. Renamed to `df` at that point.
    _df: DataFrame,
}

impl InMemoryCube {
    /// Construct an in-memory cube from a fact frame and its schema.
    ///
    /// v0.1 scaffold: stores both without validation. Phase 5a adds full
    /// column/dtype checks and per-dimension member catalogue build. A
    /// mismatch today goes undetected; in Phase 5 it becomes an
    /// [`Error::SchemaValidation`] at construction.
    pub fn new(df: DataFrame, schema: Schema) -> Result<Self, Error> {
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
    /// Phase 5 lights this up. Until then queries short-circuit here so
    /// hewton's view shows a visible "not implemented yet" card instead of
    /// silently hanging.
    #[error("tatami-inmem: evaluation not yet implemented (Phase 5 of MAP_PLAN.md)")]
    NotImplemented,

    /// Placeholder for schema/column validation failures (Phase 5a).
    #[error("tatami-inmem: schema validation — {0}")]
    SchemaValidation(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tatami::schema::{Aggregation, Dimension, Measure, Name};

    fn empty_schema() -> Schema {
        Schema::builder()
            .dimension(Dimension::regular(Name::parse("Geography").expect("valid")))
            .measure(Measure::new(
                Name::parse("amount").expect("valid"),
                Aggregation::sum(),
            ))
            .build()
            .expect("schema is valid")
    }

    #[test]
    fn new_returns_cube_for_nonempty_schema() {
        let df = DataFrame::default();
        let cube = InMemoryCube::new(df, empty_schema()).expect("construct cube");
        // compile-smoke: the cube can be held and dropped.
        drop(cube);
    }
}

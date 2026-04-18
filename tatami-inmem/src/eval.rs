//! Crate-internal evaluation pipeline for Phase 5 of MAP_PLAN.md §5.
//!
//! Every query that reaches an evaluator has already been lifted into a
//! `ResolvedQuery` (Phase 5c); the submodules below consume resolved-tree
//! fragments and produce concrete results without needing to fail for
//! schema-binding reasons. Errors surfaced from this module therefore
//! signal **eval-time** issues (catalogue mismatches, ill-formed
//! compositions, or deferred-phase short-circuits) — never unresolved
//! names.
//!
//! - [`set`] (Phase 5d) turns a `ResolvedSet` into a `Vec<ResolvedTuple>`
//!   by walking the member catalogue. `Filter` and `TopN` are stubbed out
//!   with typed errors until Phase 5g wires the metric evaluator through.
//! - [`tuple`] (Phase 5e) filters a fact [`DataFrame`] by a resolved tuple
//!   — the fact-touching primitive that aggregation builds on.
//! - [`aggregate`] (Phase 5e) evaluates a single [`Measure`] at a
//!   [`ResolvedTuple`] context, including the semi-additive rollup rule
//!   that guards against silent wrong answers (MAP §8 R3).
//!
//! Phase 5f (metric tree eval) and 5g (query execution) land as additional
//! submodules here.
//!
//! [`DataFrame`]: polars_core::prelude::DataFrame
//! [`Measure`]: tatami::schema::Measure
//! [`ResolvedTuple`]: crate::resolve::ResolvedTuple

pub(crate) mod aggregate;
pub(crate) mod metric;
pub(crate) mod set;
pub(crate) mod tuple;

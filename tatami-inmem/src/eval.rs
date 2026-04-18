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
//!
//! Phase 5e (tuple evaluation), 5f (metric evaluation), and 5g (query
//! execution) land as additional submodules here.

pub(crate) mod set;

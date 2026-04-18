//! Placeholder module for `NamedSet`.
//!
//! `NamedSet` depends on `query::set::Set` (Phase 2). This module intentionally
//! contains no public items in Phase 1; it exists so the module tree from
//! `MAP_PLAN.md §4` is already carved out. Phase 2 will add:
//!
//! ```ignore
//! pub struct NamedSet {
//!     pub name: crate::schema::Name,
//!     pub set:  crate::query::set::Set,
//! }
//! ```
//!
//! along with a corresponding `Vec<NamedSet>` field on [`crate::Schema`] and
//! a `.named_set(NamedSet)` method on the typestate builder.
//
// TODO(phase-2): replace with real `NamedSet` (and wire it into `Schema`
// plus the builder's terminal state).

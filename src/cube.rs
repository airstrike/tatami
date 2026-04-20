//! The [`Cube`] trait — the backend-agnostic OLAP surface every tatami
//! backend implements.
//!
//! Three async methods per MAP §3.4:
//!
//! - [`Cube::schema`] — schema discovery (dims, measures, metrics, named sets).
//! - [`Cube::query`] — evaluate an axis-projection `Query` into the
//!   `Axes`-determined `Results` variant (mapping per MAP §3.3).
//! - [`Cube::members`] — hierarchy navigation escape hatch used by drill
//!   controls without constructing a full query.
//!
//! Plus the [`MemberRelation`] enum — the navigation verb fed to
//! [`Cube::members`].
//!
//! The trait uses native `async fn` (Rust 2024). Calls return
//! `std::result::Result<_, Self::Error>` — we qualify `std::result::Result`
//! because the crate re-exports [`Results`] at the root, so the
//! unqualified name `Result` would collide.

use crate::schema::{Name, Schema};
use crate::{MemberRef, Query, Results};

/// The OLAP cube surface. Implementations are `Send + Sync` so callers can
/// share a cube across async tasks via `Arc`.
///
/// See MAP §3.4 for the three-method contract and §3.3 for the
/// `Axes → Results` mapping [`Cube::query`] must respect.
///
/// # `async fn` in traits
///
/// MAP §3.4 specifies native `async fn` (Rust 2024) for the three methods.
/// Rust warns on this because the returned future's auto traits can't be
/// named at the trait; we silence the warning at the trait level because
/// the design intentionally trades that naming for the terser signatures.
/// Callers that need `Send` futures can bound `impl Future<..> + Send`
/// themselves where they hold a concrete backend.
#[allow(async_fn_in_trait)]
pub trait Cube: Send + Sync {
    /// Per-backend error type. Bound so callers can propagate it through
    /// `Box<dyn std::error::Error>` or `anyhow::Error`.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Schema discovery — dimensions, measures, metrics, named sets.
    ///
    /// Backends typically return a clone of a `Schema` validated at
    /// construction time. See MAP §3.1 for the schema types.
    async fn schema(&self) -> std::result::Result<Schema, Self::Error>;

    /// Evaluate an axis-projection query.
    ///
    /// The returned [`Results`] variant is determined by the `Axes` variant
    /// of `q` per the total mapping in MAP §3.3. Backends internally call
    /// the `resolve(query, schema)` step (§3.6) to lift the public
    /// [`Query`] into the crate-internal `ResolvedQuery` before evaluation.
    async fn query(&self, q: &Query) -> std::result::Result<Results, Self::Error>;

    /// Hierarchy navigation — answer "what are the `relation` of `at` in
    /// `(dim, hierarchy)`?".
    ///
    /// Used by drill-down controls and by interactive UI that doesn't want
    /// to construct a full [`Query`] just to list a member's children.
    async fn members(
        &self,
        dim: &Name,
        hierarchy: &Name,
        at: &MemberRef,
        relation: MemberRelation,
    ) -> std::result::Result<Vec<MemberRef>, Self::Error>;
}

/// The navigation verb passed to [`Cube::members`]. See MAP §3.4.
///
/// `#[non_exhaustive]` — adding a relation (e.g. `Ancestors`, `Self`) is a
/// non-breaking change.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum MemberRelation {
    /// Direct children of the given member.
    Children,
    /// All descendants down to the given depth (`1 == Children`).
    Descendants(u8),
    /// Other members at the same level under the same parent.
    Siblings,
    /// The immediate parent.
    Parent,
    /// All leaf descendants.
    Leaves,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compile-only test: prove the trait can be implemented with `todo!()`
    /// bodies. Ensures the signatures are self-consistent (no hidden
    /// `Send`/`Sync`/`'static` conflicts).
    struct StubCube;

    impl Cube for StubCube {
        type Error = crate::results::Error;

        async fn schema(&self) -> std::result::Result<Schema, Self::Error> {
            todo!()
        }

        async fn query(&self, _q: &Query) -> std::result::Result<Results, Self::Error> {
            todo!()
        }

        async fn members(
            &self,
            _dim: &Name,
            _hierarchy: &Name,
            _at: &MemberRef,
            _relation: MemberRelation,
        ) -> std::result::Result<Vec<MemberRef>, Self::Error> {
            todo!()
        }
    }

    #[test]
    fn cube_trait_can_be_implemented() {
        let _ = StubCube;
    }

    #[test]
    fn member_relation_variants_are_copy() {
        // Smoke: MemberRelation is Copy so call sites don't have to clone.
        let r = MemberRelation::Children;
        let _ = r;
        let _ = r;
    }
}

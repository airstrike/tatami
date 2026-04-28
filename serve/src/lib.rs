//! Run any [`tatami::Cube`] as a [`runway::Module`] over HTTP.
//!
//! The wire format is tatami's existing serde encoding verbatim — no
//! envelopes, no DSL translation. Three endpoints are registered:
//!
//! | Method | Path                       | Body in                  | Body out                |
//! |--------|----------------------------|--------------------------|-------------------------|
//! | GET    | `/api/v1/cube/schema`      | —                        | [`tatami::Schema`]      |
//! | POST   | `/api/v1/cube/query`       | [`tatami::Query`]        | [`tatami::Results`]     |
//! | POST   | `/api/v1/cube/members`     | `MembersRequest` (local) | `Vec<tatami::MemberRef>`|
//!
//! See `MAP_PLAN.md` §1 for the canonical wire format and `MAP_PHASE_L1.md`
//! for the design rationale. The crate intentionally does not derive
//! `JsonSchema` on tatami types; OpenAPI generation is a downstream
//! concern that lands later, alongside the facade split.

#![warn(missing_docs)]

mod error;
mod handler;

use std::sync::Arc;

pub use error::Error;

/// HTTP-side wrapper for a [`tatami::Cube`].
///
/// Construct with [`Service::new`] and register against a
/// [`runway::Router`] via the [`runway::Module`] impl. The cube lives
/// behind an `Arc` so route handlers clone and share it cheaply across
/// concurrent requests; runway's threadpool dispatches them in parallel.
pub struct Service<C> {
    cube: Arc<C>,
}

impl<C> Service<C>
where
    C: tatami::Cube + Send + Sync + 'static,
{
    /// Wrap a cube as a runway-compatible HTTP service.
    pub fn new(cube: Arc<C>) -> Self {
        Self { cube }
    }
}

impl<C> runway::Module for Service<C>
where
    C: tatami::Cube + Send + Sync + 'static,
    C::Error: std::error::Error + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        "cube"
    }

    fn routes(&self, router: &mut runway::Router) {
        let schema_cube = self.cube.clone();
        let query_cube = self.cube.clone();
        let members_cube = self.cube.clone();

        router.get("/api/v1/cube/schema", move |_ctx| {
            let cube = schema_cube.clone();
            async move { handler::schema(cube).await }
        });
        router.post("/api/v1/cube/query", move |ctx| {
            let cube = query_cube.clone();
            async move { handler::query(cube, ctx).await }
        });
        router.post("/api/v1/cube/members", move |ctx| {
            let cube = members_cube.clone();
            async move { handler::members(cube, ctx).await }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compile-only check: prove `Service<InMemoryCube>` satisfies the
    /// `runway::Module` bounds for the reference cube implementation.
    /// The function never runs; the type-check is the assertion.
    #[allow(dead_code)]
    fn service_compiles_for_inmem_cube(
        cube: Arc<tatami_inmem::InMemoryCube>,
    ) -> impl runway::Module {
        Service::new(cube)
    }
}

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
//!
//! # Concurrency model
//!
//! Each [`Service`] owns a dedicated worker OS thread running a
//! current-thread `tokio` runtime. Inbound requests cross from the
//! runway server's threadpool into the worker via a `tokio::sync::mpsc`
//! channel; the worker calls the cube's async methods inline, then
//! returns each result via a `tokio::sync::oneshot`.
//!
//! This indirection exists because [`tatami::Cube`] uses native
//! `async fn` (Rust 2024) without `Send` bounds on the returned
//! futures. `runway::Router` requires handler futures to be `Send +
//! 'static` — a generic call to `cube.schema().await` inside a routed
//! closure cannot satisfy that without Return-Type-Notation, which is
//! still unstable on Rust 1.95. Confining the cube to a single thread
//! sidesteps the missing bound entirely; channels are `Send` regardless
//! of the cube's future shape. When the trait gains Send-bound futures
//! (or RTN stabilises), the worker thread becomes a no-op and can be
//! removed without an API change.

#![warn(missing_docs)]

mod error;
mod handler;

use std::sync::Arc;
use std::thread;

use tokio::runtime;
use tokio::sync;

pub use error::Error;

use handler::{Handle, Request};

/// Channel depth for the cube-worker mailbox. Picked to absorb a small
/// burst of concurrent requests without blocking the runway threadpool;
/// callers experiencing back-pressure should scale by running multiple
/// services rather than tuning this.
const WORKER_MAILBOX: usize = 128;

/// HTTP-side wrapper for a [`tatami::Cube`].
///
/// Construct with [`Service::new`] and register against a
/// [`runway::Router`] via the [`runway::Module`] impl. The cube lives on
/// a dedicated worker thread; the [`Service`] value is a cheaply cloneable
/// channel handle into that worker.
pub struct Service<C> {
    handle: Handle,
    /// Drives the generic parameter without storing the cube directly —
    /// the cube has been moved to the worker thread.
    _phantom: std::marker::PhantomData<fn() -> C>,
}

impl<C> Service<C>
where
    C: tatami::Cube + Send + Sync + 'static,
{
    /// Spawn a worker thread that owns `cube` and return a handle to it.
    ///
    /// The worker runs a current-thread `tokio` runtime and processes
    /// requests serially. Tearing down the [`Service`] (and any clones
    /// of its handle) closes the request channel, which lets the worker
    /// exit cleanly on the next loop iteration.
    pub fn new(cube: Arc<C>) -> Self {
        let (tx, rx) = sync::mpsc::channel::<Request>(WORKER_MAILBOX);
        thread::Builder::new()
            .name("tatami-serve-cube".into())
            .spawn(move || {
                let rt = runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("current-thread runtime builds");
                rt.block_on(handler::run_worker(cube, rx));
            })
            .expect("OS allocates the worker thread");
        Self {
            handle: Handle::new(tx),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<C> runway::Module for Service<C>
where
    C: tatami::Cube + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        "cube"
    }

    fn routes(&self, router: &mut runway::Router) {
        let schema_handle = self.handle.clone();
        let query_handle = self.handle.clone();
        let members_handle = self.handle.clone();

        router.get("/api/v1/cube/schema", move |_ctx| {
            let h = schema_handle.clone();
            async move { handler::schema(h).await }
        });
        router.post("/api/v1/cube/query", move |ctx| {
            let h = query_handle.clone();
            async move { handler::query(h, ctx).await }
        });
        router.post("/api/v1/cube/members", move |ctx| {
            let h = members_handle.clone();
            async move { handler::members(h, ctx).await }
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

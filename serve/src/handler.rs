//! Handler implementations for the three cube endpoints, plus the worker
//! task that owns the cube on a dedicated thread.
//!
//! See the crate-level docs for the rationale behind the worker thread.
//! In short: `tatami::Cube`'s async-fn-in-trait return types are not
//! statically `Send`-bound, so we cannot await them from inside the
//! `Send + 'static` futures `runway::Router` requires. Confining the cube
//! to one thread and ferrying requests through `Send` channels is the
//! stable-Rust workaround.

use std::sync::Arc;

use runway::Context;
use serde::Deserialize;
use tokio::sync;

use crate::Error;

/// Cheap, clonable handle to the cube worker. Each handler clones the
/// handle, sends a request, and awaits the worker's reply.
#[derive(Clone)]
pub(crate) struct Handle {
    tx: sync::mpsc::Sender<Request>,
}

impl Handle {
    pub(crate) fn new(tx: sync::mpsc::Sender<Request>) -> Self {
        Self { tx }
    }

    /// Send a request to the worker and await its reply, mapping every
    /// failure mode (worker gone, channel closed, cube error) into a
    /// `runway::Error`.
    async fn call<T>(
        &self,
        build: impl FnOnce(sync::oneshot::Sender<Result<T, String>>) -> Request,
    ) -> runway::Result<T> {
        let (reply_tx, reply_rx) = sync::oneshot::channel();
        self.tx
            .send(build(reply_tx))
            .await
            .map_err(|_| Error::Cube("cube worker stopped".into()))?;
        let result = reply_rx
            .await
            .map_err(|_| Error::Cube("cube worker dropped reply".into()))?;
        result.map_err(|msg| Error::Cube(msg).into())
    }
}

/// Messages sent from request handlers to the cube worker.
///
/// Each variant carries a `oneshot::Sender` that the worker uses to ship
/// the result back to the awaiting handler. Errors from the cube are
/// stringified before transit because `tatami::Cube::Error` is an
/// associated type the worker side doesn't know how to forward typed.
pub(crate) enum Request {
    /// Resolve [`tatami::Cube::schema`].
    Schema(sync::oneshot::Sender<Result<tatami::Schema, String>>),
    /// Resolve [`tatami::Cube::query`] for `query`.
    Query(
        tatami::Query,
        sync::oneshot::Sender<Result<tatami::Results, String>>,
    ),
    /// Resolve [`tatami::Cube::members`] for `(dim, hierarchy, at, relation)`.
    Members(
        MembersArgs,
        sync::oneshot::Sender<Result<Vec<tatami::MemberRef>, String>>,
    ),
}

/// Members navigation arguments, bundled so [`Request::Members`] stays a
/// single-payload variant rather than five positional fields.
pub(crate) struct MembersArgs {
    pub(crate) dim: tatami::schema::Name,
    pub(crate) hierarchy: tatami::schema::Name,
    pub(crate) at: tatami::MemberRef,
    pub(crate) relation: tatami::MemberRelation,
}

/// Worker loop. Owns the cube, services requests one at a time, exits
/// cleanly when every handle has been dropped.
pub(crate) async fn run_worker<C>(cube: Arc<C>, mut rx: sync::mpsc::Receiver<Request>)
where
    C: tatami::Cube + Send + Sync + 'static,
{
    while let Some(req) = rx.recv().await {
        match req {
            Request::Schema(reply) => {
                let r = cube.schema().await.map_err(|e| e.to_string());
                let _ = reply.send(r);
            }
            Request::Query(q, reply) => {
                let r = cube.query(&q).await.map_err(|e| e.to_string());
                let _ = reply.send(r);
            }
            Request::Members(args, reply) => {
                let r = cube
                    .members(&args.dim, &args.hierarchy, &args.at, args.relation)
                    .await
                    .map_err(|e| e.to_string());
                let _ = reply.send(r);
            }
        }
    }
}

/// `GET /api/v1/cube/schema` — return the cube's [`tatami::Schema`].
pub(crate) async fn schema(handle: Handle) -> runway::Result<runway::response::HttpResponse> {
    let schema = handle.call(Request::Schema).await?;
    runway::response::ok(&schema)
}

/// `POST /api/v1/cube/query` — body is a [`tatami::Query`] JSON; returns
/// the [`tatami::Results`] variant determined by the query's `Axes`.
pub(crate) async fn query(
    handle: Handle,
    ctx: Context,
) -> runway::Result<runway::response::HttpResponse> {
    let q: tatami::Query = ctx.json().map_err(into_bad_body)?;
    let results = handle.call(|reply| Request::Query(q, reply)).await?;
    runway::response::ok(&results)
}

/// `POST /api/v1/cube/members` — request is a [`MembersRequest`]; returns
/// `Vec<tatami::MemberRef>` resolved by the cube's hierarchy walk.
pub(crate) async fn members(
    handle: Handle,
    ctx: Context,
) -> runway::Result<runway::response::HttpResponse> {
    let req: MembersRequest = ctx.json().map_err(into_bad_body)?;
    let args = MembersArgs {
        dim: req.dim,
        hierarchy: req.hierarchy,
        at: req.at,
        relation: req.relation.into(),
    };
    let members = handle.call(|reply| Request::Members(args, reply)).await?;
    runway::response::ok(&members)
}

/// Body shape for `POST /api/v1/cube/members`. Mirrors the four arguments of
/// [`tatami::Cube::members`] one-for-one.
#[derive(Debug, Deserialize)]
pub(crate) struct MembersRequest {
    /// Dimension name to navigate within.
    pub(crate) dim: tatami::schema::Name,
    /// Hierarchy name within `dim`.
    pub(crate) hierarchy: tatami::schema::Name,
    /// Anchor member the navigation is relative to.
    pub(crate) at: tatami::MemberRef,
    /// Navigation verb (children, descendants, siblings, parent, leaves).
    pub(crate) relation: Relation,
}

/// Wire-side mirror of [`tatami::MemberRelation`].
///
/// `tatami::MemberRelation` does not yet derive `Serialize`/`Deserialize`
/// (see MAP §1 — the wire format treats it as serde-roundtrippable, but
/// the derives haven't landed in `src/cube.rs`). We define a parallel enum
/// here, deserialise into it, and convert at the seam. When tatami gains
/// the derives upstream, this type and the `From` impl below are deleted
/// in one move; the JSON shape stays identical.
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Relation {
    /// Direct children of the anchor.
    Children,
    /// All descendants down to the given depth (`1 == Children`).
    Descendants {
        /// Depth bound, in levels.
        depth: u8,
    },
    /// Other members at the same level under the same parent.
    Siblings,
    /// The immediate parent.
    Parent,
    /// All leaf descendants.
    Leaves,
}

impl From<Relation> for tatami::MemberRelation {
    fn from(r: Relation) -> Self {
        match r {
            Relation::Children => tatami::MemberRelation::Children,
            Relation::Descendants { depth } => tatami::MemberRelation::Descendants(depth),
            Relation::Siblings => tatami::MemberRelation::Siblings,
            Relation::Parent => tatami::MemberRelation::Parent,
            Relation::Leaves => tatami::MemberRelation::Leaves,
        }
    }
}

/// Map a runway-level body parse error into our `Error::BadBody` (400). This
/// preserves the original message but reroutes it through `tatami_serve::Error`
/// so the wire-side mapping stays in one place.
fn into_bad_body(e: runway::Error) -> runway::Error {
    Error::BadBody(e.to_string()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_body_path_maps_to_400() {
        // Simulate the body-parse failure runway hands back when the JSON is
        // unparseable. We funnel it through `into_bad_body` exactly as the
        // handlers do, then check the resulting HTTP status.
        let upstream = runway::Error::BadRequest("Invalid request body: expected `{`".into());
        let mapped = into_bad_body(upstream);
        assert_eq!(mapped.status_code().as_u16(), 400);
        assert!(mapped.to_string().contains("invalid request body"));
    }

    #[test]
    fn members_request_deserializes_from_canonical_json() {
        // Defensive: the on-the-wire shape stays in lockstep with
        // tatami's serde encoding. If `MemberRef`'s serde shape ever
        // drifts, this test breaks loudly. We round-trip through a JSON
        // string because `tatami::schema::Name` deserialises from a
        // borrowed `&str`, which `serde_json::from_value` cannot supply.
        let json = r#"{
            "dim": "Geography",
            "hierarchy": "World",
            "at": {
                "dim": "Geography",
                "hierarchy": "World",
                "path": ["NAM"]
            },
            "relation": { "kind": "children" }
        }"#;
        let req: MembersRequest =
            serde_json::from_str(json).expect("canonical members JSON parses");
        assert_eq!(req.dim.as_str(), "Geography");
        assert_eq!(req.hierarchy.as_str(), "World");
        let mapped: tatami::MemberRelation = req.relation.into();
        assert!(matches!(mapped, tatami::MemberRelation::Children));
    }

    #[test]
    fn members_request_handles_descendants_with_depth() {
        let json = r#"{
            "dim": "Geography",
            "hierarchy": "World",
            "at": {
                "dim": "Geography",
                "hierarchy": "World",
                "path": ["NAM"]
            },
            "relation": { "kind": "descendants", "depth": 3 }
        }"#;
        let req: MembersRequest = serde_json::from_str(json).expect("descendants JSON parses");
        let mapped: tatami::MemberRelation = req.relation.into();
        assert!(matches!(mapped, tatami::MemberRelation::Descendants(3)));
    }

    /// End-to-end exercise of the worker-thread plumbing: spin up a worker
    /// over a stub cube whose `schema()` always errors, send a request via
    /// the handle, verify the failure surfaces as a 500.
    #[tokio::test]
    async fn cube_error_surfaces_as_500_through_worker() {
        use std::fmt;

        struct ExplodingCube;
        #[derive(Debug)]
        struct Boom;
        impl fmt::Display for Boom {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("kaboom")
            }
        }
        impl std::error::Error for Boom {}
        impl tatami::Cube for ExplodingCube {
            type Error = Boom;
            async fn schema(&self) -> Result<tatami::Schema, Self::Error> {
                Err(Boom)
            }
            async fn query(&self, _q: &tatami::Query) -> Result<tatami::Results, Self::Error> {
                Err(Boom)
            }
            async fn members(
                &self,
                _dim: &tatami::schema::Name,
                _hierarchy: &tatami::schema::Name,
                _at: &tatami::MemberRef,
                _relation: tatami::MemberRelation,
            ) -> Result<Vec<tatami::MemberRef>, Self::Error> {
                Err(Boom)
            }
        }

        // Spawn a worker locally on this test's runtime.
        let (tx, rx) = sync::mpsc::channel::<Request>(8);
        let handle = Handle::new(tx);
        let cube = Arc::new(ExplodingCube);
        tokio::spawn(async move {
            run_worker(cube, rx).await;
        });

        // `runway::response::HttpResponse` does not derive Debug, so we
        // cannot use `expect_err` directly — match the variant by hand.
        let err = match schema(handle).await {
            Ok(_) => panic!("cube error must surface, got Ok response"),
            Err(e) => e,
        };
        assert_eq!(err.status_code().as_u16(), 500);
        assert!(err.to_string().contains("kaboom"));
    }
}

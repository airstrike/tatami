//! Handler implementations for the three cube endpoints.
//!
//! Each handler clones the cube `Arc` (cheap), parses the request body
//! (if any), calls the cube method directly, and serialises the response.
//! The cube's async methods return `Send` futures (per the trait), so the
//! routed closures satisfy `runway::Router`'s `Send + 'static` bound
//! without any per-request thread or channel.

use std::sync::Arc;

use runway::Context;
use serde::Deserialize;

use crate::Error;

/// `GET /api/v1/cube/schema` — return the cube's [`tatami::Schema`].
pub(crate) async fn schema<C>(cube: Arc<C>) -> runway::Result<runway::response::HttpResponse>
where
    C: tatami::Cube + Send + Sync + 'static,
    C::Error: std::error::Error + Send + Sync + 'static,
{
    let schema = cube.schema().await.map_err(map_cube_err)?;
    runway::response::ok(&schema)
}

/// `POST /api/v1/cube/query` — body is a [`tatami::Query`] JSON; returns
/// the [`tatami::Results`] variant determined by the query's `Axes`.
pub(crate) async fn query<C>(
    cube: Arc<C>,
    ctx: Context,
) -> runway::Result<runway::response::HttpResponse>
where
    C: tatami::Cube + Send + Sync + 'static,
    C::Error: std::error::Error + Send + Sync + 'static,
{
    let q: tatami::Query = ctx.json().map_err(into_bad_body)?;
    let results = cube.query(&q).await.map_err(map_cube_err)?;
    runway::response::ok(&results)
}

/// `POST /api/v1/cube/members` — request is a [`MembersRequest`]; returns
/// `Vec<tatami::MemberRef>` resolved by the cube's hierarchy walk.
pub(crate) async fn members<C>(
    cube: Arc<C>,
    ctx: Context,
) -> runway::Result<runway::response::HttpResponse>
where
    C: tatami::Cube + Send + Sync + 'static,
    C::Error: std::error::Error + Send + Sync + 'static,
{
    let req: MembersRequest = ctx.json().map_err(into_bad_body)?;
    let members = cube
        .members(&req.dim, &req.hierarchy, &req.at, req.relation.into())
        .await
        .map_err(map_cube_err)?;
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

/// Map a `runway::Error` (typically a body-parse failure) into our
/// `Error::BadBody` (400). Funnels every wire-side parse error through
/// `tatami_serve::Error` so the HTTP mapping stays in one place.
fn into_bad_body(e: runway::Error) -> runway::Error {
    Error::BadBody(e.to_string()).into()
}

/// Convert a typed cube error into a `runway::Error` (500). Stringifies
/// the message because `tatami::Cube::Error` is an associated type the
/// HTTP layer cannot forward typed.
fn map_cube_err<E: std::error::Error + 'static>(e: E) -> runway::Error {
    Error::Cube(e.to_string()).into()
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

    /// End-to-end exercise: invoke the schema handler against a stub cube
    /// whose `schema()` always errors, verify the failure surfaces as a 500.
    #[tokio::test]
    async fn cube_error_surfaces_as_500() {
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

        let cube = Arc::new(ExplodingCube);
        // `runway::HttpResponse` does not derive Debug, so we cannot use
        // `expect_err` directly — match the variant by hand.
        let err = match schema(cube).await {
            Ok(_) => panic!("cube error must surface, got Ok response"),
            Err(e) => e,
        };
        assert_eq!(err.status_code().as_u16(), 500);
        assert!(err.to_string().contains("kaboom"));
    }
}

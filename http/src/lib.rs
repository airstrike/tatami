//! `tatami::Cube` over HTTP.
//!
//! [`Remote::new`] returns a struct that implements [`tatami::Cube`] by
//! talking to a remote `tatami-serve` instance. Schema is fetched once and
//! cached for the lifetime of the [`Remote`]; query and members are one
//! round-trip each.
//!
//! Wire format: tatami's serde JSON, verbatim. No DSL translation, no
//! envelope. See `MAP_PLAN.md` §1 in the `adapter-tonight` MAP for the
//! canonical wire shape and the three endpoints we hit:
//!
//! | Method | Path                       | Body in           | Body out                  |
//! |--------|----------------------------|-------------------|---------------------------|
//! | GET    | `/api/v1/cube/schema`      | —                 | [`tatami::Schema`]        |
//! | POST   | `/api/v1/cube/query`       | [`tatami::Query`] | [`tatami::Results`]       |
//! | POST   | `/api/v1/cube/members`     | members request   | `Vec<tatami::MemberRef>`  |

#![warn(missing_docs)]

mod error;

use std::sync::Arc;

pub use error::Error;

use tokio::sync::OnceCell;

/// `tatami::Cube` backed by a remote `tatami-serve` endpoint.
///
/// Cheap to clone the underlying [`reqwest::Client`] across instances; the
/// schema cache is per-`Remote`. The struct holds no live connection — TLS
/// and HTTP/2 are negotiated on the first call.
#[derive(Debug)]
pub struct Remote {
    base_url: reqwest::Url,
    client: reqwest::Client,
    schema: OnceCell<Arc<tatami::Schema>>,
}

impl Remote {
    /// Connect to a `tatami-serve` endpoint at `base_url` (e.g.
    /// `http://localhost:8080`).
    ///
    /// Returns immediately — the schema is fetched lazily on the first
    /// [`tatami::Cube::schema`] call. Use [`Remote::warm_up`] to pre-fetch.
    pub fn new(base_url: impl reqwest::IntoUrl) -> Result<Self, Error> {
        Self::with_client(base_url, reqwest::Client::new())
    }

    /// Like [`Remote::new`] but reuses an existing `reqwest::Client` (e.g.
    /// to share connection pools or set custom timeouts).
    pub fn with_client(
        base_url: impl reqwest::IntoUrl,
        client: reqwest::Client,
    ) -> Result<Self, Error> {
        let base_url = base_url.into_url().map_err(Error::Build)?;
        Ok(Self {
            base_url,
            client,
            schema: OnceCell::new(),
        })
    }

    /// Eagerly fetch and cache the schema. Optional — [`tatami::Cube::schema`]
    /// does this lazily on first call.
    pub async fn warm_up(&self) -> Result<(), Error> {
        let _ = self.fetch_schema_cached().await?;
        Ok(())
    }

    /// The base URL this `Remote` is bound to.
    pub fn base_url(&self) -> &reqwest::Url {
        &self.base_url
    }

    fn endpoint(&self, path: &str) -> Result<reqwest::Url, Error> {
        self.base_url.join(path).map_err(Error::InvalidUrl)
    }

    async fn fetch_schema_cached(&self) -> Result<&Arc<tatami::Schema>, Error> {
        self.schema
            .get_or_try_init(|| async {
                let url = self.endpoint("/api/v1/cube/schema")?;
                let s: tatami::Schema = self
                    .client
                    .get(url)
                    .send()
                    .await
                    .map_err(Error::Transport)?
                    .error_for_status()
                    .map_err(Error::Status)?
                    .json()
                    .await
                    .map_err(Error::Decode)?;
                Ok::<Arc<tatami::Schema>, Error>(Arc::new(s))
            })
            .await
    }
}

/// Convenience constructor: connect to `base_url` and pre-fetch the schema.
///
/// Equivalent to `Remote::new(base_url)?.warm_up().await?` followed by
/// returning the [`Remote`]. Surfaces unreachable endpoints at construction
/// time rather than on first query.
pub async fn connect(base_url: impl reqwest::IntoUrl) -> Result<Remote, Error> {
    let remote = Remote::new(base_url)?;
    remote.warm_up().await?;
    Ok(remote)
}

impl tatami::Cube for Remote {
    type Error = Error;

    async fn schema(&self) -> Result<tatami::Schema, Self::Error> {
        let arc = self.fetch_schema_cached().await?;
        // `tatami::Schema: Clone`. We hand back an owned copy because the
        // trait returns owned, not borrowed; the cache keeps the Arc.
        Ok((**arc).clone())
    }

    async fn query(&self, q: &tatami::Query) -> Result<tatami::Results, Self::Error> {
        let url = self.endpoint("/api/v1/cube/query")?;
        let res: tatami::Results = self
            .client
            .post(url)
            .json(q)
            .send()
            .await
            .map_err(Error::Transport)?
            .error_for_status()
            .map_err(Error::Status)?
            .json()
            .await
            .map_err(Error::Decode)?;
        Ok(res)
    }

    async fn members(
        &self,
        dim: &tatami::schema::Name,
        hierarchy: &tatami::schema::Name,
        at: &tatami::MemberRef,
        relation: tatami::MemberRelation,
    ) -> Result<Vec<tatami::MemberRef>, Self::Error> {
        let url = self.endpoint("/api/v1/cube/members")?;
        let body = members::Request {
            dim,
            hierarchy,
            at,
            relation: members::Relation::try_from(relation)?,
        };
        let res: Vec<tatami::MemberRef> = self
            .client
            .post(url)
            .json(&body)
            .send()
            .await
            .map_err(Error::Transport)?
            .error_for_status()
            .map_err(Error::Status)?
            .json()
            .await
            .map_err(Error::Decode)?;
        Ok(res)
    }
}

/// Wire-side mirrors for the `members` endpoint.
///
/// `tatami::MemberRelation` does not yet derive `Serialize` (a debt flagged
/// in `serve/src/handler.rs`). `tatami-serve` defines a private mirror enum
/// for *deserialisation*; this module defines its symmetric counterpart for
/// *serialisation*. Both halves share one wire shape:
///
/// ```json
/// { "kind": "children" }
/// { "kind": "descendants", "depth": 3 }
/// ```
///
/// When the upstream derives land, both mirrors disappear in lockstep and
/// the JSON shape is unchanged.
mod members {
    use serde::Serialize;

    use crate::Error;

    /// Request body for `POST /api/v1/cube/members` — mirrors the four
    /// arguments of [`tatami::Cube::members`] one-for-one. Borrows from the
    /// caller's values to avoid clones on the hot path.
    #[derive(Debug, Serialize)]
    pub(super) struct Request<'a> {
        pub(super) dim: &'a tatami::schema::Name,
        pub(super) hierarchy: &'a tatami::schema::Name,
        pub(super) at: &'a tatami::MemberRef,
        pub(super) relation: Relation,
    }

    /// Wire-shape mirror of [`tatami::MemberRelation`] used solely so we can
    /// derive `Serialize` until the upstream type does. Tagged JSON with
    /// `kind` matches the server-side deserialiser in `tatami-serve`.
    #[derive(Debug, Serialize)]
    #[serde(tag = "kind", rename_all = "snake_case")]
    pub(super) enum Relation {
        Children,
        Descendants { depth: u8 },
        Siblings,
        Parent,
        Leaves,
    }

    impl TryFrom<tatami::MemberRelation> for Relation {
        type Error = Error;

        // `tatami::MemberRelation` is `#[non_exhaustive]`. We surface unknown
        // variants as a typed error rather than panic or guess; the catch-all
        // arm is forced by the `non_exhaustive` attribute, not laziness.
        fn try_from(r: tatami::MemberRelation) -> Result<Self, Self::Error> {
            Ok(match r {
                tatami::MemberRelation::Children => Self::Children,
                tatami::MemberRelation::Descendants(depth) => Self::Descendants { depth },
                tatami::MemberRelation::Siblings => Self::Siblings,
                tatami::MemberRelation::Parent => Self::Parent,
                tatami::MemberRelation::Leaves => Self::Leaves,
                _ => return Err(Error::UnsupportedRelation),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tatami::Cube;
    use tatami::query::{Axes, Options, Tuple};
    use tatami::results::{Cell, scalar};
    use tatami::schema::{Aggregation, Dimension, Measure, Name, Schema};
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// `Remote::new` accepts well-formed URLs.
    #[test]
    fn new_accepts_valid_url() {
        assert!(Remote::new("http://localhost:8080").is_ok());
    }

    /// Compile-time assertion: the futures returned by [`tatami::Cube`] on
    /// `Remote` are `Send`, which the `runway::Router` handler bound
    /// requires. Failure shows up at build time.
    #[allow(dead_code)]
    fn cube_futures_are_send(r: &Remote, q: &tatami::Query) {
        fn assert_send<T: Send>(_: &T) {}
        assert_send(&r.schema());
        assert_send(&r.query(q));
    }

    /// `schema()` round-trips against a mock server and caches the result —
    /// the second call must not hit the wire.
    #[tokio::test]
    async fn schema_round_trip_caches() {
        let server = MockServer::start().await;
        let payload = sample_schema_json();
        Mock::given(method("GET"))
            .and(path("/api/v1/cube/schema"))
            .respond_with(ResponseTemplate::new(200).set_body_string(payload))
            .expect(1) // exactly one hit — the second call must use the cache
            .mount(&server)
            .await;

        let r = Remote::new(server.uri()).unwrap();
        let first = r.schema().await.unwrap();
        let second = r.schema().await.unwrap();
        assert_eq!(first, second);
        // wiremock's `.expect(1)` verifies on `MockServer::drop`.
    }

    /// `query()` POSTs the body and decodes a `Results` from the response.
    #[tokio::test]
    async fn query_posts_body_and_decodes() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/cube/query"))
            .respond_with(ResponseTemplate::new(200).set_body_string(sample_results_json()))
            .expect(1)
            .mount(&server)
            .await;

        let r = Remote::new(server.uri()).unwrap();
        let q = sample_query();
        let _ = r.query(&q).await.unwrap();
    }

    /// 5xx response from the server is reported as `Error::Status`, not eaten.
    #[tokio::test]
    async fn server_5xx_is_reported() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/cube/schema"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let r = Remote::new(server.uri()).unwrap();
        let err = r.schema().await.unwrap_err();
        assert!(matches!(err, Error::Status(_)));
    }

    /// 2xx with a malformed body surfaces as `Error::Decode`.
    #[tokio::test]
    async fn malformed_body_is_decode_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/cube/query"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json at all"))
            .mount(&server)
            .await;

        let r = Remote::new(server.uri()).unwrap();
        let q = sample_query();
        let err = r.query(&q).await.unwrap_err();
        assert!(matches!(err, Error::Decode(_)));
    }

    /// `members()` serialises `MemberRelation::Descendants(depth)` using the
    /// `{ kind, depth }` wire shape that `tatami-serve` deserialises. If
    /// either side drifts this test fails — the contract is symmetric.
    #[tokio::test]
    async fn members_serialises_relation_with_kind_tag() {
        let server = MockServer::start().await;
        let dim = Name::parse("Geography").unwrap();
        let hierarchy = Name::parse("World").unwrap();
        let at = tatami::MemberRef::new(
            dim.clone(),
            hierarchy.clone(),
            tatami::Path::of(Name::parse("NAM").unwrap()),
        );
        let expected_body = serde_json::json!({
            "dim": "Geography",
            "hierarchy": "World",
            "at": {
                "dim": "Geography",
                "hierarchy": "World",
                "path": ["NAM"],
            },
            "relation": { "kind": "descendants", "depth": 2 }
        });
        Mock::given(method("POST"))
            .and(path("/api/v1/cube/members"))
            .and(body_json(&expected_body))
            .respond_with(ResponseTemplate::new(200).set_body_string("[]"))
            .expect(1)
            .mount(&server)
            .await;

        let r = Remote::new(server.uri()).unwrap();
        let out = r
            .members(
                &dim,
                &hierarchy,
                &at,
                tatami::MemberRelation::Descendants(2),
            )
            .await
            .unwrap();
        assert!(out.is_empty());
    }

    // --- fixtures -----------------------------------------------------

    fn n(s: &str) -> Name {
        Name::parse(s).unwrap()
    }

    fn minimal_schema() -> Schema {
        Schema::builder()
            .dimension(Dimension::regular(n("Geography")))
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .build()
            .unwrap()
    }

    fn minimal_results() -> tatami::Results {
        tatami::Results::Scalar(scalar::Result::new(
            Tuple::empty(),
            vec![Cell::Valid {
                value: 42.0,
                unit: None,
                format: None,
            }],
        ))
    }

    fn minimal_query() -> tatami::Query {
        tatami::Query {
            axes: Axes::Scalar,
            slicer: Tuple::empty(),
            metrics: vec![n("amount")],
            options: Options::default(),
        }
    }

    fn sample_schema_json() -> String {
        serde_json::to_string(&minimal_schema()).unwrap()
    }

    fn sample_results_json() -> String {
        serde_json::to_string(&minimal_results()).unwrap()
    }

    fn sample_query() -> tatami::Query {
        minimal_query()
    }
}

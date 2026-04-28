//! Error type for `tatami-http`.
//!
//! Each variant captures one stage of the HTTP round-trip so callers can
//! distinguish "the server isn't there" from "the server replied with
//! garbage". `reqwest::Error` happens to be the source for `Transport`,
//! `Status`, and `Decode` because reqwest models all three as a single error
//! type — we keep them apart at the variant level so consumers don't have
//! to inspect kind flags.

use thiserror::Error;

/// Errors surfaced by the [`crate::Remote`] HTTP client.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Failed to parse the base URL or join an endpoint path.
    #[error("invalid URL: {0}")]
    InvalidUrl(#[source] url::ParseError),

    /// Failed to construct an HTTP request (e.g. URL conversion via
    /// `reqwest::IntoUrl`). Distinct from [`Error::Transport`] because no
    /// network I/O has happened yet.
    #[error("request build error: {0}")]
    Build(#[source] reqwest::Error),

    /// Network / transport failure (DNS, connect, TLS, etc.).
    #[error("transport error: {0}")]
    Transport(#[source] reqwest::Error),

    /// Server returned an HTTP error status (4xx / 5xx).
    #[error("server returned status: {0}")]
    Status(#[source] reqwest::Error),

    /// Server returned a 2xx but the body could not be deserialized.
    #[error("response decode error: {0}")]
    Decode(#[source] reqwest::Error),

    /// A [`tatami::MemberRelation`] variant added upstream isn't yet known
    /// to this client. Surfaces only after a tatami upgrade until this
    /// crate catches up; never on stable builds where the workspace is
    /// in lockstep.
    #[error("unsupported MemberRelation variant — tatami upgrade required")]
    UnsupportedRelation,
}

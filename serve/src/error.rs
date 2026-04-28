//! Error types for `tatami-serve`.
//!
//! The crate maps both backend (cube) errors and request-shape errors into
//! runway HTTP responses. We do not preserve typed Rust errors over the
//! wire — clients see HTTP status + a string message.

use thiserror::Error;

/// Errors surfaced by the HTTP wrapper around a [`tatami::Cube`].
///
/// Each variant carries the originating message as a string; we deliberately
/// flatten typed backend errors here because the wire format is JSON and the
/// client has no way to reconstruct the cube's `Error` enum.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Request body could not be deserialized into the expected shape.
    #[error("invalid request body: {0}")]
    BadBody(String),
    /// The cube backend returned an error.
    #[error("cube backend error: {0}")]
    Cube(String),
}

impl From<Error> for runway::Error {
    fn from(e: Error) -> Self {
        let message = e.to_string();
        match e {
            Error::BadBody(_) => runway::Error::BadRequest(message),
            Error::Cube(_) => runway::Error::Internal(message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bad_body_maps_to_400() {
        let e: runway::Error = Error::BadBody("malformed".into()).into();
        assert_eq!(e.status_code().as_u16(), 400);
    }

    #[test]
    fn cube_error_maps_to_500() {
        let e: runway::Error = Error::Cube("backend died".into()).into();
        assert_eq!(e.status_code().as_u16(), 500);
    }

    #[test]
    fn error_display_includes_inner_message() {
        let e = Error::Cube("OUT_OF_MEMORY".into());
        assert!(e.to_string().contains("OUT_OF_MEMORY"));
    }
}

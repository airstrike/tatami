//! Top-level query-construction [`Error`] — composes sub-module errors.

use crate::query::{path, set, tuple};

/// Errors produced by query-layer constructors.
///
/// Each variant wraps a sub-module's error so the crate-level surface
/// remains a single composed enum while keeping per-module failure cases
/// local.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A [`crate::query::Tuple`] construction failure.
    #[error("tuple: {0}")]
    Tuple(#[from] tuple::Error),
    /// A [`crate::query::Path`] construction failure.
    #[error("path: {0}")]
    Path(#[from] path::Error),
    /// A [`crate::query::Set`] construction failure.
    #[error("set: {0}")]
    Set(#[from] set::Error),
}

//! Top-level [`Error`] for the `results` module.
//!
//! v0.1 scaffold — carries a single `Internal` variant so Phase 5 can route
//! shape-invariant violations through a typed path once `ResolvedQuery`
//! wires metric counts into the opaque constructors.

/// Errors produced while constructing or manipulating result values.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Internal invariant violation — carries a human-readable message.
    ///
    /// v0.1 scaffold: this is the only variant. Phase 5 splits it into
    /// shape-specific variants (`MetricCountMismatch`, `RowWidthMismatch`,
    /// `CellGridMismatch`) once the counts are plumbed through.
    #[error("internal: {0}")]
    Internal(String),
}

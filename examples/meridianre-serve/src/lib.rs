//! Library half of the meridianre-serve example.
//!
//! Re-exports [`cube::build`] so sibling examples (notably `meridianre-board`)
//! can construct the same Polars-backed [`tatami_inmem::InMemoryCube`] the
//! `meridianre-serve` binary boots — no duplication of the schema or
//! region-derivation logic.

#![warn(missing_docs)]

pub mod cube;

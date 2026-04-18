//! Query types — axis projection, slicer, set algebra.
//!
//! The public surface is re-exported here; each semantic type lives in its
//! own submodule per §4 of the MAP. See `MAP_PLAN.md §3.2` for the
//! type-level contract every public type in this module must match.

pub mod axes;
pub mod error;
pub mod member_ref;
pub mod options;
pub mod path;
pub mod predicate;
pub mod set;
pub mod tuple;

use serde::{Deserialize, Serialize};

pub use axes::Axes;
pub use error::Error;
pub use member_ref::MemberRef;
pub use options::{Direction, OrderBy, QueryOptions};
pub use path::Path;
pub use predicate::Predicate;
pub use set::Set;
pub use tuple::Tuple;

use crate::schema::Name;

/// A complete query: an axis projection, a slicer, a list of requested
/// metrics, and tuning options.
///
/// Construct programmatically using the types in this module, or
/// deserialize from JSON — both yield shape-valid values that still need
/// `Schema`-based resolution before evaluation (Phase 5 / §3.6).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Query {
    /// Axis projection shape.
    pub axes: Axes,
    /// The slicer — a tuple that pins the query to a subspace.
    pub slicer: Tuple,
    /// The metrics to evaluate at each cell, in the order returned.
    pub metrics: Vec<Name>,
    /// Optional ordering, limit, and non-empty flag.
    pub options: QueryOptions,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Name;

    fn n(s: &str) -> Name {
        Name::parse(s).expect("valid")
    }

    #[test]
    fn scalar_query_roundtrips() {
        let q = Query {
            axes: Axes::Scalar,
            slicer: Tuple::empty(),
            metrics: vec![n("Revenue")],
            options: QueryOptions::default(),
        };
        let json = serde_json::to_string(&q).expect("serialize");
        let back: Query = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(q, back);
    }
}

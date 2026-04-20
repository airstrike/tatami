//! [`Predicate`] — a Boolean constraint used inside [`crate::query::Set::Filter`].
//!
//! Internal-tag serde on struct variants, consistent with the rest of the
//! query tree. The numeric variants hold `f64`, so `Predicate` derives
//! `PartialEq` but not `Eq`.

use serde::{Deserialize, Serialize};

use crate::query::Path;
use crate::schema::Name;

/// A predicate over cells or dim coordinates, used by
/// [`crate::query::Set::Filter`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Predicate {
    /// `metric == value` at the filtered cell.
    Eq {
        /// The metric whose value is compared.
        metric: Name,
        /// The comparison target.
        value: f64,
    },
    /// `metric > value` at the filtered cell.
    Gt {
        /// The metric whose value is compared.
        metric: Name,
        /// The comparison target.
        value: f64,
    },
    /// `metric < value` at the filtered cell.
    Lt {
        /// The metric whose value is compared.
        metric: Name,
        /// The comparison target.
        value: f64,
    },
    /// The filtered member's coordinate on `dim` starts with `path_prefix`.
    In {
        /// The dimension to test.
        dim: Name,
        /// The path prefix members must match.
        path_prefix: Path,
    },
    /// The filtered member's coordinate on `dim` does *not* start with
    /// `path_prefix`.
    NotIn {
        /// The dimension to test.
        dim: Name,
        /// The path prefix members must not match.
        path_prefix: Path,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(s: &str) -> Name {
        Name::parse(s).expect("valid")
    }

    #[test]
    fn predicate_gt_roundtrips() {
        let p = Predicate::Gt {
            metric: n("Revenue"),
            value: 1_000.0,
        };
        let json = serde_json::to_string(&p).expect("serialize");
        let back: Predicate = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(p, back);
    }

    #[test]
    fn predicate_in_roundtrips() {
        let p = Predicate::In {
            dim: n("Geography"),
            path_prefix: Path::of(n("EMEA")),
        };
        let json = serde_json::to_string(&p).expect("serialize");
        let back: Predicate = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(p, back);
    }

    #[test]
    fn predicate_not_in_roundtrips() {
        let p = Predicate::NotIn {
            dim: n("Geography"),
            path_prefix: Path::of(n("EMEA")),
        };
        let json = serde_json::to_string(&p).expect("serialize");
        let back: Predicate = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(p, back);
    }
}

//! [`Set`] — the set-algebra tree that defines an axis of a query.
//!
//! Internal-tag serde (`#[serde(tag = "op")]`) requires every variant be a
//! struct variant. All ten variants from MAP §3.2 are represented.
//!
//! # Design notes
//!
//! - [`Set::Explicit`] carries a public `members: Vec<MemberRef>` so
//!   consumers can pattern-match on it, but the only supported constructor
//!   is [`Set::explicit`], which rejects an empty list. Callers who build
//!   `Set::Explicit { members: vec![] }` directly produce a shape-valid
//!   value that the resolve stage in Phase 5 is expected to reject. This
//!   trade-off (easy matching vs. strict non-empty invariant) is explicit
//!   in the v0.1 design.
//! - [`Set::CrossJoin`] does not check dim-disjointness at construction —
//!   doing so would require walking two set trees. Disjointness is enforced
//!   in the resolve stage (§3.6).
//! - [`Set::Children`] and [`Set::Descendants`] carry `Box<Set>` under
//!   `of`, not a single [`MemberRef`]. This lifts the drill-down
//!   combinators so the algebra is closed under itself: "descendants of a
//!   range" and "children of a named set" are both expressible without
//!   Union-nesting workarounds. The scalar-member case is a one-line
//!   wrapper — see [`Set::from_member`] and the tidy methods on
//!   [`MemberRef`].
//!
//! # Combinator methods (tidy style)
//!
//! Struct variants stay public for pattern matching; the methods below
//! compose them as a pipeable surface. Every method is total — wrapping
//! in [`Box`] never fails — so none return [`Result`]. The one
//! [`Result`]-returning constructor is [`Set::explicit`], which rejects
//! empty member lists at the boundary.

use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

use crate::query::{MemberRef, Predicate};
use crate::schema::Name;

/// A set of tuples — the building block of a query axis.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Set {
    /// All members of `dim`'s `hierarchy` at a given `level`.
    Members {
        /// The dimension.
        dim: Name,
        /// The hierarchy within the dimension.
        hierarchy: Name,
        /// The level within the hierarchy.
        level: Name,
    },
    /// The immediate children of every member produced by `of`.
    ///
    /// `of` is a full [`Set`] so drill-down composes with every other
    /// combinator: `range.children()`, `explicit.children()`,
    /// `named.children()`, etc.
    Children {
        /// The parent set. For the scalar "children of a single member"
        /// case, wrap the member via [`Set::from_member`] or
        /// [`MemberRef::children`].
        of: Box<Set>,
    },
    /// All descendants of every member in `of`, down to `to_level`
    /// (inclusive).
    Descendants {
        /// The ancestor set. Use [`Set::from_member`] for the scalar case.
        of: Box<Set>,
        /// The target depth.
        to_level: Name,
    },
    /// Inclusive range between two members in the same hierarchy.
    Range {
        /// The dimension.
        dim: Name,
        /// The hierarchy within the dimension.
        hierarchy: Name,
        /// Lower endpoint (inclusive).
        from: MemberRef,
        /// Upper endpoint (inclusive).
        to: MemberRef,
    },
    /// Reference to a named set declared in the schema.
    Named {
        /// The named set's name.
        name: Name,
    },
    /// Cartesian product of two sets on disjoint dims. Disjointness is
    /// verified at resolve time (§3.6), not here.
    CrossJoin {
        /// Left operand.
        left: Box<Set>,
        /// Right operand.
        right: Box<Set>,
    },
    /// Filter a set by a predicate over a metric or dim coordinate.
    Filter {
        /// The set to filter.
        set: Box<Set>,
        /// The predicate to apply.
        pred: Predicate,
    },
    /// Top-N tuples from `set` ranked by `by`.
    TopN {
        /// The source set.
        set: Box<Set>,
        /// The number of tuples to retain.
        n: NonZeroUsize,
        /// The metric to rank by.
        by: Name,
    },
    /// Union of two sets.
    Union {
        /// Left operand.
        left: Box<Set>,
        /// Right operand.
        right: Box<Set>,
    },
    /// Explicit list of members. Use [`Set::explicit`] to construct.
    Explicit {
        /// The members listed directly. Should be non-empty — enforced at
        /// construction by [`Set::explicit`]; see module-level note.
        members: Vec<MemberRef>,
    },
}

impl Set {
    // --- Atom constructors -------------------------------------------------

    /// Total constructor for [`Set::Members`].
    #[must_use]
    pub fn members(dim: Name, hierarchy: Name, level: Name) -> Self {
        Self::Members {
            dim,
            hierarchy,
            level,
        }
    }

    /// Total constructor for [`Set::Range`].
    #[must_use]
    pub fn range(dim: Name, hierarchy: Name, from: MemberRef, to: MemberRef) -> Self {
        Self::Range {
            dim,
            hierarchy,
            from,
            to,
        }
    }

    /// Total constructor for [`Set::Named`] — references a named set from
    /// the schema.
    #[must_use]
    pub fn named(name: Name) -> Self {
        Self::Named { name }
    }

    /// Total constructor: wrap a single [`MemberRef`] as a one-element
    /// explicit set. This is the bridge from "I have a member" to "I have
    /// a Set" for the scalar case of [`Set::children`] /
    /// [`Set::descendants_to`].
    #[must_use]
    pub fn from_member(member: MemberRef) -> Self {
        Self::Explicit {
            members: vec![member],
        }
    }

    /// Boundary constructor for [`Set::Explicit`] — rejects an empty member
    /// list.
    pub fn explicit<I>(members: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = MemberRef>,
    {
        let members: Vec<MemberRef> = members.into_iter().collect();
        if members.is_empty() {
            return Err(Error::EmptyExplicit);
        }
        Ok(Self::Explicit { members })
    }

    // --- Unary combinators -------------------------------------------------

    /// Drill down one level: wraps `self` in [`Set::Children`].
    #[must_use]
    pub fn children(self) -> Self {
        Self::Children { of: Box::new(self) }
    }

    /// Drill down to `to_level`: wraps `self` in [`Set::Descendants`].
    #[must_use]
    pub fn descendants_to(self, to_level: Name) -> Self {
        Self::Descendants {
            of: Box::new(self),
            to_level,
        }
    }

    /// Filter: wraps `self` in [`Set::Filter`] with the given predicate.
    #[must_use]
    pub fn filter(self, pred: Predicate) -> Self {
        Self::Filter {
            set: Box::new(self),
            pred,
        }
    }

    /// Top-N: wraps `self` in [`Set::TopN`], ranking by `by` and keeping
    /// `n` tuples.
    #[must_use]
    pub fn top(self, n: NonZeroUsize, by: Name) -> Self {
        Self::TopN {
            set: Box::new(self),
            n,
            by,
        }
    }

    // --- Binary combinators ------------------------------------------------

    /// Cartesian product: wraps `self` and `other` in [`Set::CrossJoin`].
    /// Dim-disjointness is checked at resolve time, not here.
    #[must_use]
    pub fn cross(self, other: Self) -> Self {
        Self::CrossJoin {
            left: Box::new(self),
            right: Box::new(other),
        }
    }

    /// Union: wraps `self` and `other` in [`Set::Union`].
    #[must_use]
    pub fn union(self, other: Self) -> Self {
        Self::Union {
            left: Box::new(self),
            right: Box::new(other),
        }
    }
}

/// Errors produced by [`Set`] constructors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// [`Set::explicit`] was called with an empty member list.
    #[error("explicit set must contain at least one member")]
    EmptyExplicit,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::Path;

    fn n(s: &str) -> Name {
        Name::parse(s).expect("valid")
    }

    fn mr(dim: &str, head: &str) -> MemberRef {
        MemberRef::new(n(dim), n("Default"), Path::of(n(head)))
    }

    #[test]
    fn members_roundtrips() {
        let s = Set::Members {
            dim: n("Geography"),
            hierarchy: n("Default"),
            level: n("Region"),
        };
        let json = serde_json::to_string(&s).expect("serialize");
        let back: Set = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(s, back);
    }

    #[test]
    fn children_roundtrips() {
        let s = Set::Children {
            of: Box::new(Set::from_member(mr("Geography", "EMEA"))),
        };
        let json = serde_json::to_string(&s).expect("serialize");
        let back: Set = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(s, back);
    }

    #[test]
    fn descendants_roundtrips() {
        let s = Set::Descendants {
            of: Box::new(Set::from_member(mr("Geography", "World"))),
            to_level: n("Country"),
        };
        let json = serde_json::to_string(&s).expect("serialize");
        let back: Set = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(s, back);
    }

    #[test]
    fn range_roundtrips() {
        let s = Set::Range {
            dim: n("Time"),
            hierarchy: n("Fiscal"),
            from: mr("Time", "FY2025"),
            to: mr("Time", "FY2030"),
        };
        let json = serde_json::to_string(&s).expect("serialize");
        let back: Set = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(s, back);
    }

    #[test]
    fn cross_join_roundtrips() {
        let s = Set::CrossJoin {
            left: Box::new(Set::Members {
                dim: n("Geography"),
                hierarchy: n("Default"),
                level: n("Region"),
            }),
            right: Box::new(Set::Members {
                dim: n("Time"),
                hierarchy: n("Fiscal"),
                level: n("Quarter"),
            }),
        };
        let json = serde_json::to_string(&s).expect("serialize");
        let back: Set = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(s, back);
    }

    #[test]
    fn top_n_roundtrips() {
        let s = Set::TopN {
            set: Box::new(Set::Members {
                dim: n("Geography"),
                hierarchy: n("Default"),
                level: n("Country"),
            }),
            n: NonZeroUsize::new(10).expect("nonzero"),
            by: n("Revenue"),
        };
        let json = serde_json::to_string(&s).expect("serialize");
        let back: Set = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(s, back);
    }

    #[test]
    fn explicit_constructor_accepts_members() {
        let s = Set::explicit([mr("Scenario", "Plan"), mr("Scenario", "WhatIf_A")])
            .expect("non-empty members");
        let json = serde_json::to_string(&s).expect("serialize");
        let back: Set = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(s, back);
    }

    #[test]
    fn explicit_constructor_rejects_empty() {
        let err = Set::explicit::<Vec<MemberRef>>(vec![]).expect_err("empty");
        assert!(matches!(err, Error::EmptyExplicit));
    }

    #[test]
    fn named_set_reference_roundtrips() {
        let s = Set::Named {
            name: n("TopRegions"),
        };
        let json = serde_json::to_string(&s).expect("serialize");
        let back: Set = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(s, back);
    }

    #[test]
    fn union_roundtrips() {
        let s = Set::Union {
            left: Box::new(Set::from_member(mr("Geography", "EMEA")).children()),
            right: Box::new(Set::from_member(mr("Geography", "APAC")).children()),
        };
        let json = serde_json::to_string(&s).expect("serialize");
        let back: Set = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(s, back);
    }

    #[test]
    fn filter_roundtrips() {
        let s = Set::Filter {
            set: Box::new(Set::Members {
                dim: n("Geography"),
                hierarchy: n("Default"),
                level: n("Country"),
            }),
            pred: Predicate::Gt {
                metric: n("Revenue"),
                value: 1_000.0,
            },
        };
        let json = serde_json::to_string(&s).expect("serialize");
        let back: Set = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(s, back);
    }

    #[test]
    fn children_lifts_member_to_set() {
        // The tidy form: MemberRef -> Set::from_member -> .children()
        // roundtrips as `Children { of: Explicit { members: [...] } }`.
        let via_method = Set::from_member(mr("Geography", "EMEA")).children();
        let via_struct = Set::Children {
            of: Box::new(Set::Explicit {
                members: vec![mr("Geography", "EMEA")],
            }),
        };
        assert_eq!(via_method, via_struct);
        let json = serde_json::to_string(&via_method).expect("serialize");
        let back: Set = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(via_method, back);
    }

    #[test]
    fn descendants_of_range_roundtrips() {
        // §3.5(b) shape: drill into a time range. Expressible only
        // because `Descendants.of` is a full `Set`.
        let s = Set::range(
            n("Time"),
            n("Fiscal"),
            mr("Time", "FY2025"),
            mr("Time", "FY2030"),
        )
        .descendants_to(n("Quarter"));
        let json = serde_json::to_string(&s).expect("serialize");
        let back: Set = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(s, back);
        assert!(matches!(s, Set::Descendants { .. }));
    }

    #[test]
    fn cross_and_union_compose() {
        let a = Set::Members {
            dim: n("Geography"),
            hierarchy: n("Default"),
            level: n("Region"),
        };
        let b = Set::Members {
            dim: n("Time"),
            hierarchy: n("Fiscal"),
            level: n("Quarter"),
        };
        let c = Set::Members {
            dim: n("Geography"),
            hierarchy: n("Default"),
            level: n("Country"),
        };
        let d = Set::Members {
            dim: n("Time"),
            hierarchy: n("Fiscal"),
            level: n("Month"),
        };
        let composed = a.clone().cross(b.clone()).union(c.clone().cross(d.clone()));
        let json = serde_json::to_string(&composed).expect("serialize");
        let back: Set = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(composed, back);
        assert!(matches!(composed, Set::Union { .. }));
    }
}

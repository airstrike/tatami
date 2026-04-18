//! Opaque [`Tuple`] — a coordinate across a set of dimensions, each bound
//! at most once.
//!
//! Serde shape is a flat JSON array of [`MemberRef`] values. The hand-rolled
//! [`Deserialize`] implementation re-runs the distinct-dim check on incoming
//! JSON so nobody can smuggle a duplicate-dim tuple in over the wire.

use std::collections::HashSet;

use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::query::MemberRef;
use crate::schema::Name;

/// Coordinate across zero or more dims, each bound at most once.
///
/// - [`Tuple::empty`] — total constructor for the zero-length tuple.
/// - [`Tuple::single`] — total constructor (a single member trivially has
///   distinct dims).
/// - [`Tuple::of`] — boundary constructor; rejects duplicate dims.
///
/// The inner `Vec<MemberRef>` is private; callers read members via
/// [`Tuple::members`].
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Tuple {
    members: Vec<MemberRef>,
}

impl Tuple {
    /// Total constructor — the zero-length tuple.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            members: Vec::new(),
        }
    }

    /// Total constructor — a single-member tuple.
    #[must_use]
    pub fn single(member: MemberRef) -> Self {
        Self {
            members: vec![member],
        }
    }

    /// Boundary constructor. Rejects any collection that binds the same dim
    /// more than once.
    pub fn of<I>(iter: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = MemberRef>,
    {
        let members: Vec<MemberRef> = iter.into_iter().collect();
        check_distinct_dims(&members)?;
        Ok(Self { members })
    }

    /// Read-only view of the members, in insertion order.
    #[must_use]
    pub fn members(&self) -> &[MemberRef] {
        &self.members
    }

    /// Number of bound dims (0 if [`Tuple::empty`]).
    #[must_use]
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// `true` for the zero-length tuple.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
}

fn check_distinct_dims(members: &[MemberRef]) -> Result<(), Error> {
    let mut seen: HashSet<&Name> = HashSet::with_capacity(members.len());
    for m in members {
        if !seen.insert(&m.dim) {
            return Err(Error::DuplicateDim(m.dim.clone()));
        }
    }
    Ok(())
}

impl Serialize for Tuple {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.members.len()))?;
        for m in &self.members {
            seq.serialize_element(m)?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for Tuple {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let members: Vec<MemberRef> = Vec::deserialize(deserializer)?;
        check_distinct_dims(&members).map_err(serde::de::Error::custom)?;
        Ok(Self { members })
    }
}

/// Errors produced by [`Tuple::of`] (and the hand-rolled deserialiser).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The same dim was bound more than once in the input.
    #[error("tuple binds dim {0} more than once")]
    DuplicateDim(Name),
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
    fn empty_tuple_has_zero_members() {
        let t = Tuple::empty();
        assert!(t.is_empty());
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn single_tuple_has_one_member() {
        let t = Tuple::single(mr("Time", "FY2026"));
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn tuple_of_preserves_insertion_order() {
        let t = Tuple::of([mr("Time", "FY2026"), mr("Scenario", "Actual")]).expect("distinct dims");
        assert_eq!(t.members()[0].dim, n("Time"));
        assert_eq!(t.members()[1].dim, n("Scenario"));
    }

    #[test]
    fn tuple_rejects_duplicate_dims_on_construction() {
        let err =
            Tuple::of([mr("Time", "FY2026"), mr("Time", "FY2025")]).expect_err("duplicate dims");
        assert!(matches!(err, Error::DuplicateDim(_)));
    }

    #[test]
    fn tuple_rejects_three_members_two_share_dim() {
        let err = Tuple::of([
            mr("Time", "FY2026"),
            mr("Scenario", "Actual"),
            mr("Time", "FY2025"),
        ])
        .expect_err("duplicate dims");
        assert!(matches!(err, Error::DuplicateDim(_)));
    }

    #[test]
    fn tuple_roundtrips_as_flat_json_array() {
        let t = Tuple::of([mr("Time", "FY2026"), mr("Scenario", "Actual")]).expect("distinct");
        let json = serde_json::to_string(&t).expect("serialize");
        assert!(json.starts_with('['));
        assert!(json.ends_with(']'));
        let back: Tuple = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(t, back);
    }

    #[test]
    fn tuple_rejects_duplicate_dims_on_deserialize() {
        // Hand-constructed JSON with two members on the same dim.
        let json = r#"[
            {"dim":"Time","hierarchy":"Fiscal","path":["FY2026"]},
            {"dim":"Time","hierarchy":"Fiscal","path":["FY2025"]}
        ]"#;
        let err = serde_json::from_str::<Tuple>(json);
        assert!(err.is_err());
    }

    #[test]
    fn empty_tuple_serializes_as_empty_array() {
        let t = Tuple::empty();
        let json = serde_json::to_string(&t).expect("serialize");
        assert_eq!(json, "[]");
        let back: Tuple = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(t, back);
    }
}

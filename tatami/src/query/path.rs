//! Non-empty [`Path`] — an ordered sequence of hierarchy level keys.
//!
//! The struct shape encodes non-emptiness: a `head` segment is always
//! present. Total construction via [`Path::of`] and [`Path::with`];
//! boundary via [`Path::parse`].
//!
//! Serde shape is a flat JSON array `[Name, Name, …]` — not the derived
//! `{"head": …, "tail": [...]}` object. The hand-rolled serde below
//! enforces non-emptiness on deserialisation.

use std::fmt;

use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::schema::Name;

/// Non-empty path of hierarchy segment keys, root-to-leaf.
///
/// Invariant: at least one segment. Construct via [`Path::of`] (total),
/// [`Path::with`] (total), or [`Path::parse`] (boundary).
///
/// [`Display`] joins segments with `/`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Path {
    head: Name,
    tail: Vec<Name>,
}

impl Path {
    /// Total constructor: a path of exactly one segment.
    #[must_use]
    pub fn of(head: Name) -> Self {
        Self {
            head,
            tail: Vec::new(),
        }
    }

    /// Total constructor: head plus a possibly-empty tail.
    pub fn with<I>(head: Name, tail: I) -> Self
    where
        I: IntoIterator<Item = Name>,
    {
        Self {
            head,
            tail: tail.into_iter().collect(),
        }
    }

    /// Boundary constructor from a `Vec<Name>`. Rejects an empty vector.
    pub fn parse(segments: Vec<Name>) -> Result<Self, Error> {
        let mut it = segments.into_iter();
        let head = it.next().ok_or(Error::Empty)?;
        let tail: Vec<Name> = it.collect();
        Ok(Self { head, tail })
    }

    /// Fluent: append a segment.
    #[must_use]
    pub fn push(mut self, segment: Name) -> Self {
        self.tail.push(segment);
        self
    }

    /// The first (root) segment.
    #[must_use]
    pub fn head(&self) -> &Name {
        &self.head
    }

    /// Tail segments after [`Path::head`].
    #[must_use]
    pub fn tail(&self) -> &[Name] {
        &self.tail
    }

    /// Iterate over every segment, head first.
    pub fn segments(&self) -> impl Iterator<Item = &Name> {
        std::iter::once(&self.head).chain(self.tail.iter())
    }

    /// Number of segments (always ≥ 1).
    #[must_use]
    pub fn len(&self) -> usize {
        1 + self.tail.len()
    }

    /// Paths are never empty by construction — provided for clippy parity.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        false
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.head.as_str())?;
        for seg in &self.tail {
            f.write_str("/")?;
            f.write_str(seg.as_str())?;
        }
        Ok(())
    }
}

impl Serialize for Path {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        seq.serialize_element(&self.head)?;
        for seg in &self.tail {
            seq.serialize_element(seg)?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for Path {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let segments: Vec<Name> = Vec::deserialize(deserializer)?;
        Self::parse(segments).map_err(serde::de::Error::custom)
    }
}

/// Errors produced by [`Path::parse`] (and the hand-rolled deserialiser).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The segment list was empty.
    #[error("path must contain at least one segment")]
    Empty,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(s: &str) -> Name {
        Name::parse(s).expect("valid name")
    }

    #[test]
    fn path_of_builds_single_segment() {
        let p = Path::of(n("FY2026"));
        assert_eq!(p.len(), 1);
        assert_eq!(p.head(), &n("FY2026"));
        assert!(p.tail().is_empty());
    }

    #[test]
    fn path_with_accepts_tail() {
        let p = Path::with(n("FY2026"), vec![n("Q1"), n("Jan")]);
        assert_eq!(p.len(), 3);
        let collected: Vec<_> = p.segments().cloned().collect();
        assert_eq!(collected, vec![n("FY2026"), n("Q1"), n("Jan")]);
    }

    #[test]
    fn path_parse_rejects_empty_segment_list() {
        assert!(matches!(Path::parse(vec![]), Err(Error::Empty)));
    }

    #[test]
    fn path_displays_segments_slash_joined() {
        let p = Path::with(n("FY2026"), vec![n("Q1")]);
        assert_eq!(format!("{p}"), "FY2026/Q1");
    }

    #[test]
    fn path_roundtrips_as_flat_json_array() {
        let p = Path::with(n("FY2026"), vec![n("Q1"), n("Jan")]);
        let json = serde_json::to_string(&p).expect("serialize");
        assert_eq!(json, r#"["FY2026","Q1","Jan"]"#);
        let back: Path = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(p, back);
    }

    #[test]
    fn path_deserialize_rejects_empty_array() {
        let err = serde_json::from_str::<Path>("[]");
        assert!(err.is_err());
    }
}

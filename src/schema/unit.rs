//! Opaque [`Unit`] — a validated unit-of-measure string.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Unit of measurement — `"USD"`, `"units"`, `"heads"`. Validated as a
/// non-empty, trimmed string today; will become a richer sum type
/// (`enum Unit { Currency(Iso), Count, Ratio, … }`) post-v0.1.
///
/// Serde-transparent: a JSON `Unit` is a plain string.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Unit(String);

impl Unit {
    /// Parse a unit at the boundary. Rejects empty and whitespace-only
    /// inputs; requires the input to be already trimmed (no leading /
    /// trailing whitespace).
    pub fn parse(s: &str) -> Result<Self, Error> {
        if s.is_empty() {
            return Err(Error::Empty);
        }
        if s.trim().is_empty() {
            return Err(Error::WhitespaceOnly);
        }
        if s.len() != s.trim().len() {
            return Err(Error::LeadingOrTrailingWhitespace);
        }
        Ok(Self(s.to_owned()))
    }

    /// Borrow the underlying string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Unit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for Unit {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Unit {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = <&str>::deserialize(deserializer)?;
        Self::parse(raw).map_err(serde::de::Error::custom)
    }
}

/// Errors produced by [`Unit::parse`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The input was the empty string.
    #[error("unit must not be empty")]
    Empty,
    /// The input was whitespace-only.
    #[error("unit must not be whitespace-only")]
    WhitespaceOnly,
    /// The input had leading or trailing whitespace.
    #[error("unit must not have leading or trailing whitespace")]
    LeadingOrTrailingWhitespace,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn units_accept_common_currencies() {
        assert_eq!(Unit::parse("USD").expect("valid").as_str(), "USD");
        assert_eq!(Unit::parse("heads").expect("valid").as_str(), "heads");
    }

    #[test]
    fn units_reject_empty() {
        assert!(matches!(Unit::parse(""), Err(Error::Empty)));
    }

    #[test]
    fn units_reject_whitespace_only() {
        assert!(matches!(Unit::parse("  "), Err(Error::WhitespaceOnly)));
    }

    #[test]
    fn units_reject_leading_whitespace() {
        assert!(matches!(
            Unit::parse(" USD"),
            Err(Error::LeadingOrTrailingWhitespace)
        ));
    }

    #[test]
    fn units_roundtrip_as_plain_json_strings() {
        let unit = Unit::parse("USD").expect("valid");
        let json = serde_json::to_string(&unit).expect("serialize");
        assert_eq!(json, "\"USD\"");
        let back: Unit = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(unit, back);
    }
}

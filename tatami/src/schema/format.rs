//! Opaque [`Format`] — a validated formatter-string.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Formatter string — `"0.0%"`, `"$#,##0.00"`. Phase 1 validates only
/// non-emptiness; format-spec parsing is out of scope until a concrete
/// renderer needs it.
///
/// Serde-transparent: a JSON `Format` is a plain string.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Format(String);

impl Format {
    /// Parse a format at the boundary. Rejects empty / whitespace-only
    /// inputs.
    pub fn parse(s: &str) -> Result<Self, Error> {
        if s.is_empty() {
            return Err(Error::Empty);
        }
        if s.trim().is_empty() {
            return Err(Error::WhitespaceOnly);
        }
        Ok(Self(s.to_owned()))
    }

    /// Borrow the underlying string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Format {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for Format {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Format {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = <&str>::deserialize(deserializer)?;
        Self::parse(raw).map_err(serde::de::Error::custom)
    }
}

/// Errors produced by [`Format::parse`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The input was the empty string.
    #[error("format must not be empty")]
    Empty,
    /// The input contained only whitespace.
    #[error("format must not be whitespace-only")]
    WhitespaceOnly,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_accept_common_patterns() {
        assert_eq!(Format::parse("0.0%").expect("valid").as_str(), "0.0%");
        assert_eq!(
            Format::parse("$#,##0.00").expect("valid").as_str(),
            "$#,##0.00"
        );
    }

    #[test]
    fn formats_reject_empty() {
        assert!(matches!(Format::parse(""), Err(Error::Empty)));
    }

    #[test]
    fn formats_reject_whitespace_only() {
        assert!(matches!(Format::parse("   "), Err(Error::WhitespaceOnly)));
    }

    #[test]
    fn formats_roundtrip_as_plain_json_strings() {
        let format = Format::parse("0.0%").expect("valid");
        let json = serde_json::to_string(&format).expect("serialize");
        assert_eq!(json, "\"0.0%\"");
        let back: Format = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(format, back);
    }
}

//! Opaque [`Name`] — a validated identifier.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Validated identifier — non-empty, no leading or trailing whitespace, no
/// interior control characters. Case-preserved.
///
/// Construct via [`Name::parse`]. Internal fields are private; the library
/// treats every `Name` value as already validated.
///
/// Serde-transparent: a JSON `Name` is a plain string.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Name(String);

impl Name {
    /// Parse an identifier at the boundary.
    ///
    /// Rejects empty strings, strings that contain only whitespace, strings
    /// with leading or trailing whitespace, and strings containing control
    /// characters (including `\n`, `\r`, `\t`).
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
        if let Some(c) = s.chars().find(|c| c.is_control()) {
            return Err(Error::ControlCharacter(c));
        }
        Ok(Self(s.to_owned()))
    }

    /// Borrow the underlying string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for Name {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Name {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = <&str>::deserialize(deserializer)?;
        Self::parse(raw).map_err(serde::de::Error::custom)
    }
}

/// Errors produced by [`Name::parse`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The input was the empty string.
    #[error("name must not be empty")]
    Empty,
    /// The input contained only whitespace.
    #[error("name must not be whitespace-only")]
    WhitespaceOnly,
    /// The input had leading or trailing whitespace.
    #[error("name must not have leading or trailing whitespace")]
    LeadingOrTrailingWhitespace,
    /// The input contained a control character.
    #[error("name must not contain control character {0:?}")]
    ControlCharacter(char),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_identifier() {
        let name = Name::parse("Revenue").expect("valid");
        assert_eq!(name.as_str(), "Revenue");
        assert_eq!(format!("{name}"), "Revenue");
    }

    #[test]
    fn preserves_case_and_internal_spaces() {
        let name = Name::parse("Line Item").expect("valid");
        assert_eq!(name.as_str(), "Line Item");
    }

    #[test]
    fn names_reject_empty_strings() {
        assert!(matches!(Name::parse(""), Err(Error::Empty)));
    }

    #[test]
    fn names_reject_whitespace_only() {
        assert!(matches!(Name::parse("   "), Err(Error::WhitespaceOnly)));
    }

    #[test]
    fn names_reject_leading_whitespace() {
        assert!(matches!(
            Name::parse(" leading"),
            Err(Error::LeadingOrTrailingWhitespace)
        ));
    }

    #[test]
    fn names_reject_trailing_whitespace() {
        assert!(matches!(
            Name::parse("trailing "),
            Err(Error::LeadingOrTrailingWhitespace)
        ));
    }

    #[test]
    fn names_reject_interior_control_characters() {
        assert!(matches!(
            Name::parse("bad\nname"),
            Err(Error::ControlCharacter('\n'))
        ));
    }

    #[test]
    fn names_roundtrip_as_plain_json_strings() {
        let name = Name::parse("Revenue").expect("valid");
        let json = serde_json::to_string(&name).expect("serialize");
        assert_eq!(json, "\"Revenue\"");
        let back: Name = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(name, back);
    }

    #[test]
    fn names_deserialize_rejects_invalid_json_string() {
        let err = serde_json::from_str::<Name>("\"\"");
        assert!(err.is_err());
    }
}

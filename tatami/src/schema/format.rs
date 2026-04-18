//! Opaque [`Format`] — a formatter string (`"0.0%"`, `"$#,##0.00"`, etc.).
//!
//! v0.1 wraps any string; format-spec parsing lands when a renderer needs
//! to interpret these. Construction is total, so `From<&str>` and
//! `From<String>` are both infallible.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Formatter string. Serde-transparent — a JSON `Format` is a plain string.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Format(String);

impl Format {
    /// Wrap any string as a `Format`. Total — no validation in v0.1;
    /// format-spec parsing happens at the renderer.
    #[must_use]
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Borrow the underlying string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Format {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for Format {
    fn from(s: String) -> Self {
        Self::new(s)
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
        let raw = String::deserialize(deserializer)?;
        Ok(Self(raw))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_roundtrips_as_plain_json_string() {
        let format = Format::new("0.0%");
        let json = serde_json::to_string(&format).expect("serialize");
        assert_eq!(json, "\"0.0%\"");
        let back: Format = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(format, back);
    }

    #[test]
    fn format_accepts_arbitrary_strings() {
        // v0.1 validates nothing; the renderer interprets the spec at use.
        assert_eq!(Format::new("").as_str(), "");
        assert_eq!(Format::new("0.0%").as_str(), "0.0%");
        assert_eq!(Format::from("$#,##0.00").as_str(), "$#,##0.00");
    }
}

//! [`NamedSet`] — a named, schema-level reusable [`crate::query::Set`].
//!
//! Named sets live alongside measures and metrics in the schema's reference
//! namespace — their names must not collide with measures, metrics, or
//! other named sets.

use serde::{Deserialize, Serialize};

use crate::query::Set;
use crate::schema::Name;

/// A named set declared at schema construction time, referenced from a
/// query via [`crate::query::Set::Named`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NamedSet {
    /// The set's name — unique within the schema and distinct from measure /
    /// metric names.
    pub name: Name,
    /// The set expression.
    pub set: Set,
}

impl NamedSet {
    /// Total constructor.
    #[must_use]
    pub fn new(name: Name, set: Set) -> Self {
        Self { name, set }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{MemberRef, Path};

    fn n(s: &str) -> Name {
        Name::parse(s).expect("valid")
    }

    #[test]
    fn named_set_roundtrips() {
        let ns = NamedSet::new(
            n("TopRegions"),
            Set::Children {
                of: MemberRef::new(n("Geography"), n("Default"), Path::of(n("World"))),
            },
        );
        let json = serde_json::to_string(&ns).expect("serialize");
        let back: NamedSet = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(ns, back);
    }
}

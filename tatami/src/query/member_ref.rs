//! [`MemberRef`] — a fully-qualified hierarchy member coordinate.

use serde::{Deserialize, Serialize};

use crate::query::Path;
use crate::schema::Name;

/// Fully-qualified reference to a single member in a hierarchy —
/// `(dim, hierarchy, path)`.
///
/// Construct via [`MemberRef::new`] (total). A number of convenience
/// constructors mirror the Phase 2 example queries — they assume the
/// `"Default"` hierarchy when no hierarchy is given.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MemberRef {
    /// The dimension this member lives in.
    pub dim: Name,
    /// The hierarchy within that dimension.
    pub hierarchy: Name,
    /// The path of segment keys, root-to-leaf.
    pub path: Path,
}

impl MemberRef {
    /// Total constructor.
    #[must_use]
    pub fn new(dim: Name, hierarchy: Name, path: Path) -> Self {
        Self {
            dim,
            hierarchy,
            path,
        }
    }

    /// Convenience: a single-segment member in the `"Scenario"` dimension /
    /// `"Default"` hierarchy.
    ///
    /// Assumes a conventional `"Scenario"` dim with a `"Default"` hierarchy;
    /// resolve-time validation (Phase 5) catches a mismatch.
    #[must_use]
    pub fn scenario(name: Name) -> Self {
        Self::new(
            default_name("Scenario"),
            default_name("Default"),
            Path::of(name),
        )
    }

    /// Convenience: a single-segment member in the `"Time"` dimension /
    /// `"Fiscal"` hierarchy.
    ///
    /// Assumes a conventional `"Time"` dim with a `"Fiscal"` hierarchy.
    #[must_use]
    pub fn time(name: Name) -> Self {
        Self::new(default_name("Time"), default_name("Fiscal"), Path::of(name))
    }

    /// Convenience: the root `"World"` member in the `"Geography"` dimension /
    /// `"Default"` hierarchy.
    #[must_use]
    pub fn world() -> Self {
        Self::new(
            default_name("Geography"),
            default_name("Default"),
            Path::of(default_name("World")),
        )
    }

    /// Convenience: build a `(from, to)` pair of member references for use as
    /// endpoints of a [`crate::query::Set::Range`] — both members live in the
    /// same `dim` and `hierarchy`.
    #[must_use]
    pub fn range(dim: Name, hierarchy: Name, from: Name, to: Name) -> (Self, Self) {
        (
            Self::new(dim.clone(), hierarchy.clone(), Path::of(from)),
            Self::new(dim, hierarchy, Path::of(to)),
        )
    }
}

/// Build a [`Name`] from a string literal known to be valid. Used by the
/// convenience constructors above where the string is a compile-time
/// constant from this module.
fn default_name(s: &str) -> Name {
    Name::parse(s).expect("static name is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(s: &str) -> Name {
        Name::parse(s).expect("valid")
    }

    #[test]
    fn member_ref_new_roundtrips() {
        let m = MemberRef::new(n("Time"), n("Fiscal"), Path::of(n("FY2026")));
        let json = serde_json::to_string(&m).expect("serialize");
        let back: MemberRef = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(m, back);
    }

    #[test]
    fn member_ref_scenario_convenience() {
        let m = MemberRef::scenario(n("Actual"));
        assert_eq!(m.dim, n("Scenario"));
        assert_eq!(m.hierarchy, n("Default"));
        assert_eq!(m.path.head(), &n("Actual"));
    }

    #[test]
    fn member_ref_world_convenience() {
        let m = MemberRef::world();
        assert_eq!(m.dim, n("Geography"));
        assert_eq!(m.path.head(), &n("World"));
    }

    #[test]
    fn member_ref_range_shares_dim_and_hierarchy() {
        let (lo, hi) = MemberRef::range(n("Time"), n("Fiscal"), n("FY2025"), n("FY2030"));
        assert_eq!(lo.dim, hi.dim);
        assert_eq!(lo.hierarchy, hi.hierarchy);
        assert_eq!(lo.path.head(), &n("FY2025"));
        assert_eq!(hi.path.head(), &n("FY2030"));
    }
}

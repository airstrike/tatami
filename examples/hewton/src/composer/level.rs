//! Level-picker option type.

use std::fmt;

/// A level option in a `pick_list` — indexed pair `(hierarchy, level)`
/// within an already-chosen dimension.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Choice {
    pub hierarchy: usize,
    pub level: usize,
    pub label: String,
}

impl fmt::Display for Choice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.label)
    }
}

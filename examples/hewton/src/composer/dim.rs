//! Dim-picker options and schema-lookup helpers.

use std::fmt;

use tatami::schema::{Name, Schema};

/// A dimension option in a `pick_list`. Stores the index into
/// `schema.dimensions` and a display label cloned from the dim's name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Choice {
    pub index: usize,
    pub label: String,
}

impl fmt::Display for Choice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.label)
    }
}

/// Build the `Choice` list for every dim. The list is small so
/// rebuilding per `view` call is cheap.
pub fn options(schema: &Schema) -> Vec<Choice> {
    schema
        .dimensions
        .iter()
        .enumerate()
        .map(|(i, d)| Choice {
            index: i,
            label: d.name.as_str().to_owned(),
        })
        .collect()
}

/// Index of `name` within `schema.dimensions`. Axis and slicer state
/// key everything by this index; this translates a pivot-click's dim
/// name back into that index space.
pub fn index_for(name: &Name, schema: &Schema) -> Option<usize> {
    schema.dimensions.iter().position(|d| d.name == *name)
}

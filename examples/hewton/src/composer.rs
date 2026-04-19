//! Composer — the left-hand sidebar of pickers that assemble the
//! `Query` fired against the cube.
//!
//! One leaf module per sidebar section — [`axis`] (reused for Rows and
//! Columns), [`metric`], [`top_n`], [`filter`], [`slicer`] — each
//! owning its own `State` / `Message` / `update` / `view`. This file
//! holds the shared picker-option types and helpers consumed by every
//! leaf and by `App` in `main.rs`.

use std::fmt;

use tatami::query::MemberRef;
use tatami::schema::{Name, Schema};

pub mod axis;
pub mod filter;
pub mod metric;
pub mod slicer;
pub mod top_n;

/// A dimension option in a `pick_list`. Stores the index into
/// `schema.dimensions` and a display label cloned from the dim's name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DimChoice {
    pub index: usize,
    pub label: String,
}

impl fmt::Display for DimChoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.label)
    }
}

/// A level option in a `pick_list` — indexed pair `(hierarchy, level)`
/// within an already-chosen dimension.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LevelChoice {
    pub hierarchy: usize,
    pub level: usize,
    pub label: String,
}

impl fmt::Display for LevelChoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.label)
    }
}

/// A metric choice — either an index into `schema.measures` or an index
/// into `schema.metrics`. No names cross this boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum MetricPick {
    /// Index into `schema.measures`.
    Measure(usize),
    /// Index into `schema.metrics`.
    Metric(usize),
}

/// A metric option in a `pick_list` — indexes into measures or metrics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricChoice {
    pub pick: MetricPick,
    pub label: String,
}

impl fmt::Display for MetricChoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.label)
    }
}

/// Build the `DimChoice` list for every dim in the schema. The list is
/// small so rebuilding per `view` call is cheap.
pub fn dim_options(schema: &Schema) -> Vec<DimChoice> {
    schema
        .dimensions
        .iter()
        .enumerate()
        .map(|(i, d)| DimChoice {
            index: i,
            label: d.name.as_str().to_owned(),
        })
        .collect()
}

/// Build the `MetricChoice` list spanning `schema.measures` ++
/// `schema.metrics`; the `MetricPick` variant encodes which array the
/// index targets.
pub fn metric_options(schema: &Schema) -> Vec<MetricChoice> {
    schema
        .measures
        .iter()
        .enumerate()
        .map(|(i, m)| MetricChoice {
            pick: MetricPick::Measure(i),
            label: m.name.as_str().to_owned(),
        })
        .chain(
            schema
                .metrics
                .iter()
                .enumerate()
                .map(|(i, m)| MetricChoice {
                    pick: MetricPick::Metric(i),
                    label: m.name.as_str().to_owned(),
                }),
        )
        .collect()
}

/// Look up the [`Name`] backing a [`MetricPick`]. Returns `None` when
/// the index is out of range — the caller treats that as "metric picker
/// is stale; skip this metric".
pub fn metric_name(schema: &Schema, pick: MetricPick) -> Option<Name> {
    match pick {
        MetricPick::Measure(i) => schema.measures.get(i).map(|m| m.name.clone()),
        MetricPick::Metric(i) => schema.metrics.get(i).map(|m| m.name.clone()),
    }
}

/// Index of `dim_name` within `schema.dimensions`. Axis and slicer
/// state key everything by this index; this is the translation from a
/// pivot-click's dim name back into that index space.
pub fn dim_index_for(dim_name: &Name, schema: &Schema) -> Option<usize> {
    schema.dimensions.iter().position(|d| d.name == *dim_name)
}

pub fn member_dim_index(member: &MemberRef, schema: &Schema) -> Option<usize> {
    dim_index_for(&member.dim, schema)
}

//! Composer — the left-hand sidebar of pickers that assemble the
//! `Query` fired against the cube.
//!
//! One leaf module per sidebar section — [`axis`] (reused for Rows and
//! Columns), [`metric`], [`top_n`], [`filter`], [`slicer`] — each
//! owning its own `State` / `Message` / `update` / `view`. The
//! [`dim`] and [`level`] modules carry the shared picker-option types
//! consumed across leaves; [`metric`] additionally exposes its shared
//! `Pick` / `Choice` / `options` / `name` alongside its leaf.

pub mod axis;
pub mod dim;
pub mod filter;
pub mod level;
pub mod metric;
pub mod slicer;
pub mod top_n;

//! `tatami` — backend-agnostic multidimensional cube trait.
//!
//! This crate provides the type vocabulary (schema, query, results — Phase 1
//! ships the schema types) that concrete OLAP backends implement against.
//! See `.claude/map/v0-1/MAP_PLAN.md` for the design rationale.
//!
//! # Phase 1 scope
//!
//! - Opaque scalar types: [`schema::Name`], [`schema::Unit`],
//!   [`schema::Format`], [`schema::MonthDay`].
//! - Schema product types: [`Schema`], [`schema::Dimension`],
//!   [`schema::Measure`], [`schema::Metric`], and their components.
//! - A typestate [`Schema::builder`] that makes partial schemas fail to
//!   compile.
//!
//! Phase 2 will wire up `query::*` (including `Tuple` and `Set`) and replace
//! the [`schema::AtPlaceholder`] stub inside `MetricExpr::At`. Phase 2 will
//! also re-introduce `NamedSet` (which depends on `Set`).

#![warn(missing_docs)]

pub mod schema;

pub use schema::Schema;

//! `tatami` — backend-agnostic multidimensional cube trait.
//!
//! This crate provides the type vocabulary (schema, query, results — Phase 2
//! ships schema + query) that concrete OLAP backends implement against.
//! See `.claude/map/v0-1/MAP_PLAN.md` for the design rationale.
//!
//! # Phase 2 scope
//!
//! - Opaque scalar types: [`schema::Name`], [`schema::Unit`],
//!   [`schema::Format`], [`schema::MonthDay`].
//! - Schema product types: [`Schema`], [`schema::Dimension`],
//!   [`schema::Measure`], [`schema::Metric`], [`schema::NamedSet`] and their
//!   components.
//! - Query types: [`Query`], [`Axes`], [`Tuple`], [`Path`], [`Set`],
//!   [`MemberRef`], [`Predicate`], [`QueryOptions`].
//! - A typestate [`Schema::builder`] that makes partial schemas fail to
//!   compile.
//!
//! Phase 3 will add the Results + Cube trait surface.

#![warn(missing_docs)]

pub mod query;
pub mod schema;

pub use query::{
    Axes, Direction, MemberRef, OrderBy, Path, Predicate, Query, QueryOptions, Set, Tuple,
};
pub use schema::Schema;

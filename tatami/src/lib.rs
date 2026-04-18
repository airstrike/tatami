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
//!   [`MemberRef`], [`Predicate`], [`query::Options`].
//! - A typestate [`Schema::builder`] that makes partial schemas fail to
//!   compile.
//!
//! Phase 3 adds the Results + Cube trait surface.
//!
//! # Phase 3 additions
//!
//! - Result shapes under [`results`]: [`scalar::Result`],
//!   [`series::Result`], [`pivot::Result`], [`rollup::Tree`].
//! - [`Cell`] (Valid / Missing / Error) and [`missing::Reason`].
//! - The closed [`Results`] sum over the four shapes.
//! - The [`Cube`] trait with native `async fn` + [`MemberRelation`].

#![warn(missing_docs)]

pub mod cube;
pub mod query;
pub mod results;
pub mod schema;

pub use query::{Axes, Direction, MemberRef, OrderBy, Path, Predicate, Query, Set, Tuple};
pub use schema::Schema;

// `Expr` at crate root — it's the formula-tree constructor, used often in
// metric declarations, and doesn't clash with anything else in the crate.
// `dimension`, `metric`, `query` stay module-qualified at call sites —
// their narrower types (Kind, BinOp, Options, etc.) carry module context.
pub use schema::metric::Expr;

pub use cube::{Cube, MemberRelation};
pub use results::cell::missing;
pub use results::{Cell, Results, cell, pivot, rollup, scalar, series};

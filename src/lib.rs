//! Backend-agnostic multidimensional cube trait — schema, query algebra,
//! typed result shapes, and the [`Cube`] trait every OLAP backend
//! implements against. The reference implementation lives in
//! [`tatami-inmem`](https://docs.rs/tatami-inmem) (Polars-backed);
//! `examples/hewton/` is the frozen target API.
//!
//! # Mental model
//!
//! A cube is a function `f: D₁ × D₂ × … × Dₙ → Measures`. Dimensions
//! define a space; a [`Query`] picks a subspace (the [`Query::slicer`])
//! and specifies an axis projection (rows / columns / pages) as sets of
//! tuples. The result is a typed grid, not a bag of rows.
//!
//! Three transformation stages — separate types, one per stage — carry a
//! query from wire form to evaluated result:
//!
//! 1. [`Query`] — shape-valid, serde-roundtrippable; refs are raw [`schema::Name`] values.
//! 2. `ResolvedQuery` (crate-internal to each backend) — refs bound to
//!    schema handles; structural checks (cross-join disjointness,
//!    hierarchy existence) done.
//! 3. [`Results`] — a closed sum over the four result shapes.
//!
//! `Query → ResolvedQuery → Results` is three types, not one type with
//! a status field. The library's invariants are structural: if you can
//! construct a value, it is already valid.
//!
//! # Types at a glance
//!
//! **Schema layer** — [`Schema`], built through a typestate
//! [`Schema::builder`] that makes partial schemas a compile error:
//! [`schema::Dimension`] (regular / time / scenario), [`schema::Measure`]
//! with [`schema::Aggregation`] (sum, avg, semi-additive, …),
//! [`schema::Metric`] carrying an [`Expr`] formula tree,
//! [`schema::NamedSet`]. Opaque scalar types [`schema::Name`],
//! [`schema::Unit`], [`schema::Format`], [`schema::MonthDay`] validate at
//! the boundary.
//!
//! **Query layer** — [`Query`] composes [`Axes`] (the four projection
//! shapes Scalar / Series / Pivot / Pages), a [`Tuple`] slicer,
//! [`MemberRef`] coordinates, [`Path`] segment lists, and the [`Set`]
//! algebra (Members / Children / Descendants / Range / CrossJoin /
//! Union / Filter / TopN / Named / Explicit). Tidy-style combinator
//! methods (`set.descendants_to(level)`, `set.filter(pred)`) sit next
//! to the variant constructors.
//!
//! **Result layer** — the closed [`Results`] sum over [`scalar::Result`]
//! (KPI tile), [`series::Result`] (line / bar chart),
//! [`pivot::Result`] (2-D grid), and [`rollup::Tree`] (hierarchical).
//! Each cell is a [`Cell`] — `Valid` / `Missing` (with a typed
//! [`missing::Reason`]) / `Error`.
//!
//! **Backend surface** — the [`Cube`] trait with three async methods
//! ([`Cube::schema`], [`Cube::query`], [`Cube::members`]), plus
//! [`MemberRelation`] as the navigation verb fed to `Cube::members`.
//!
//! # Further reading
//!
//! - `examples/hewton/` — a worked hotel-sales cube that exercises every
//!   public type against ~2,300 rows of synthetic data.
//! - The crate's `README.md` has a short example and pointers to the
//!   OLAP prior art this design draws on.

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

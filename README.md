<div align="center">

# tatami

A backend-agnostic multidimensional cube trait for Rust

</div>

## Overview

`tatami` is the foundation layer for Pigment / Anaplan-style financial
dashboards in Rust — a closed algebra of dimensions, hierarchies, measures,
and metrics joined under a single `Cube` trait. Day-one consumers: the
[`hyozu`](https://github.com/airstrike/hyozu) charting library and an
[`iced`](https://github.com/iced-rs/iced)-based planning app.

> **Prime directive.** `tatami` is not "a better GROUP BY." The cube model
> is structurally different from relational: dimensions define a _space_;
> queries _project_ onto axes (rows / columns / pages); measures roll up
> according to schema-declared additivity. A `SUM` over time for a stock
> measure silently gives wrong answers — `tatami` makes that
> unrepresentable.

> **Status.** v0.1 in active development. Not yet published to crates.io;
> depends on `iced` from git while 0.15 is in flight.

## Crates

- **`tatami`** — core. Schema types, query algebra, `Results` shapes, the
  `Cube` trait. Pure types + serde; no backend deps.
- **`tatami-inmem`** — reference implementation over `polars::DataFrame`.
  Validates the fact frame, builds a member catalogue, resolves queries,
  evaluates the full set + metric algebra (including semi-additive rollup,
  `Lag`, `PeriodsToDate`, and `At`-pinned coordinates).
- **`examples/hewton`** — a worked hotel-sales cube. An iced application
  that demonstrates all four `Results` shapes (Scalar, Series, Pivot,
  Rollup) against a CSV-backed Hewton dataset.

Future siblings (out of scope for v0.1): `tatami-polars-lazy`,
`tatami-duckdb`, `tatami-datafusion`, `tatami-http` (Cube.js-shaped).

## Design principles

1. **Invalid states unrepresentable.** Structural sum types, refinement
   via the inner type, head-tail decomposition, typestate builders.
   `Result` is the boundary, not the default.
2. **Types encode transformations.** `Query → ResolvedQuery → Evaluated`
   is three types, not one with a status field.
3. **Tidyverse ergonomics.** Verbs-as-methods-on-data.
   `Set::range(…).descendants_to(Quarter)` reads flat, and the struct
   variants stay public so pattern matches remain exhaustive over the
   algebra.
4. **Example-first.** The Hewton example is the acceptance spec — any
   API change that breaks it needs explicit sign-off.

## The algebra

`tatami`'s query surface is a two-level algebra, joined by tuple-context
evaluation:

- **Set algebra over cube members** (`Set`, 10 constructors, closed under
  combination). Atoms: `Members`, `Range`, `Named`, `Explicit`. Unary
  combinators: `Children`, `Descendants`, `Filter`, `TopN`. Binary:
  `CrossJoin`, `Union`. The combinator methods on `Set` pipe — the
  shape reads top-down as
  `world.descendants_to(Country).filter(revenue_gt_threshold)` rather
  than as nested struct literals.
- **Expression algebra over cube cells** (`Expr`, 6 constructors).
  Terminals: `Ref`, `Const`. Binary operators: `Binary`. Coordinate
  transforms: `Lag`, `PeriodsToDate`, `At`. Composes to YoY, QTD,
  variance, what-if — all as tree nodes over a tuple context.

`Cube::query` is a homomorphism from the query algebra into the
`Results` algebra: every backend must be _observationally equivalent_
on the algebra against the reference `InMemoryCube`. Eighteen
algebraic laws (Union commutativity, CrossJoin associativity, Filter
push-through, TopN collapse, Descendants-of-Union, …) are verified as
proptests in `tatami-inmem/tests/laws.rs` — empirical in v0.1,
candidates for the public contract in v0.2 once a second backend
stress-tests them.

## Example

```rust
use tatami::schema::{Aggregation, Dimension, Measure, Metric, Name};
use tatami::{Axes, Expr, MemberRef, Path, Query, Results, Schema, Tuple};
use tatami_inmem::InMemoryCube;

// Construct a validated schema via the typestate builder.
let schema = Schema::builder()
    .dimension(Dimension::scenario(Name::parse("Scenario")?))
    .dimension(Dimension::time(Name::parse("Time")?, vec![]))
    .measure(Measure::new(Name::parse("amount")?, Aggregation::sum()))
    .metric(Metric::new(
        Name::parse("Revenue")?,
        Expr::Ref { name: Name::parse("amount")? },
    ))
    .build()?;

// Wrap a Polars DataFrame — columns are checked against the schema.
let cube = InMemoryCube::new(fact_frame, schema)?;

// Evaluate a single-cell KPI query.
let query = Query {
    axes: Axes::Scalar,
    slicer: Tuple::of([
        MemberRef::new(Name::parse("Time")?, Name::parse("Fiscal")?,
                       Path::of(Name::parse("FY2026")?)),
        MemberRef::scenario(Name::parse("Actual")?),
    ])?,
    metrics: vec![Name::parse("Revenue")?],
    options: Default::default(),
};

let Results::Scalar(result) = cube.query(&query).await? else { unreachable!() };
println!("FY2026 Revenue = {:?}", result.values());
```

## Running the example

```bash
cargo run -p hewton
```

Opens an iced window with four cards — a Scalar KPI, two Pivot variants
(one promoted to Rollup because the rows axis is `Descendants`), and a
Series — backed by ~2,300 rows of synthetic hotel-sales data loaded from
`examples/hewton/assets/hewton.csv`.

## Implementing a backend

The `Cube` trait is three async methods:

```rust
pub trait Cube: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn schema (&self) -> Result<Schema, Self::Error>;
    async fn query  (&self, q: &Query) -> Result<Results, Self::Error>;
    async fn members(
        &self,
        dim:       &Name,
        hierarchy: &Name,
        at:        &MemberRef,
        relation:  MemberRelation,
    ) -> Result<Vec<MemberRef>, Self::Error>;
}
```

The canonical internal pipeline is `Query → ResolvedQuery → Results`
(see `tatami-inmem/src/resolve.rs` for the reference shape):

1. **Resolve.** Lift the public `Query` into a backend-internal
   `ResolvedQuery` that carries schema-binding proofs — every
   `Expr::Ref { name }` bound to a measure/metric handle, every
   `CrossJoin` verified disjoint, every `Lag { dim, .. }` confirmed as
   a `Time` dim. **This is the only place in the pipeline where
   ref-existence `Result`s appear.** Evaluation sees only
   `ResolvedQuery` and cannot fail for those reasons.
2. **Evaluate.** Walk `ResolvedAxes` → concrete tuples, walk `Expr` →
   cells, assemble the `Results` shape per the `Axes` variant (Scalar,
   Series, Pivot, or Rollup).

**Correctness target: observational equivalence with `tatami-inmem`.**
Copy `tatami-inmem/tests/laws.rs` into your backend's `tests/` and
run it; every law should pass.

## Prior art

The design is an idiomatic Rust translation of ideas from:

- [Microsoft MDX](https://learn.microsoft.com/en-us/analysis-services/multidimensional-models/mdx/mdx-query-fundamentals)
  — tuple / set algebra, `SELECT ON ROWS / COLUMNS / FROM cube / WHERE
  slicer`.
- [Pigment](https://www.pigment.com/) — transactions → dimensions →
  metrics → tables; boards as multidimensional views.
- [Anaplan](https://help.anaplan.com/) — modules, lists, line items;
  canonical semi-additive semantics.
- [Cube.js](https://cube.dev/docs/product/apis-integrations/rest-api/query-format)
  — serializable JSON query format.
- [LookML](https://cloud.google.com/looker/docs/reference/param-lookml)
  and [Malloy](https://www.malloydata.dev/) — declarative, schema-first
  modeling.

## License

[MIT](LICENSE).

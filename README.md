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

`tatami` is dual-licensed under [MIT](LICENSE-MIT) or
[Apache-2.0](LICENSE-APACHE), at your option.

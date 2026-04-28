//! Cube construction for the meridianre dataset.
//!
//! Loads `monthly_close.csv`, derives a `region` column from
//! `country_code`, builds the [`tatami::Schema`] documented in
//! `MAP_PLAN.md` §3.1, and wraps the pair in a
//! [`tatami_inmem::InMemoryCube`].

use std::path::Path;

use anyhow::{Context, Result};
use polars_core::prelude::{DataFrame, IntoColumn, NamedFrom, Series};
use polars_io::prelude::{CsvReadOptions, SerReader};

use tatami::Expr;
use tatami::schema::{
    Aggregation, BinOp, Calendar, Dimension, Hierarchy, Level, Measure, Metric, Name, Schema,
};
use tatami_inmem::InMemoryCube;

/// Load the meridianre fact frame, derive the region column, build the
/// schema, and return a constructed [`InMemoryCube`].
pub fn build(csv_path: &Path) -> Result<InMemoryCube> {
    let df = load_dataframe(csv_path)?;
    let df = with_region(df)?;
    let schema = build_schema()?;
    let cube = InMemoryCube::new(df, schema)
        .context("InMemoryCube::new failed (check schema vs DataFrame columns)")?;
    Ok(cube)
}

fn load_dataframe(path: &Path) -> Result<DataFrame> {
    CsvReadOptions::default()
        .with_has_header(true)
        .with_infer_schema_length(Some(1024))
        .try_into_reader_with_file_path(Some(path.to_path_buf()))
        .with_context(|| format!("opening CSV at {}", path.display()))?
        .finish()
        .with_context(|| format!("parsing CSV at {}", path.display()))
}

/// Insert a `region` column derived from `country_code`. Region map is
/// MAP_PLAN.md §3.2 verbatim.
fn with_region(mut df: DataFrame) -> Result<DataFrame> {
    let country = df
        .column("country_code")
        .context("country_code column missing from monthly_close.csv")?
        .str()
        .context("country_code column is not utf8")?;

    let region: Vec<Option<&'static str>> = country.into_iter().map(|c| c.map(region_of)).collect();
    let series = Series::new("region".into(), region);
    df.with_column(series.into_column())
        .context("inserting derived region column")?;
    Ok(df)
}

/// Country → region lookup. Codes outside the meridianre/SPEC.md set fall
/// to `"Other"` so the cube survives unexpected input rather than
/// rejecting the load.
fn region_of(country: &str) -> &'static str {
    match country {
        "US" | "CA" => "NAM",
        "BR" | "MX" | "CB" | "CO" | "CL" | "AR" | "PR" => "LAC",
        "UK" | "DE" | "FR" | "CH" | "ES" | "IT" => "EUR",
        "JP" | "AU" | "SG" => "APJ",
        _ => "Other",
    }
}

/// Build the meridianre cube schema — 4 dimensions, 8 measures, 6 metrics.
/// Matches MAP_PLAN.md §3.1 exactly. The Time hierarchy is FY → Month
/// (no Quarter level — host-side period selectors compose Q-rollups via
/// `Month IN [...]` slicers per MAP_PHASE_L3.md "Quarter handling").
fn build_schema() -> Result<Schema> {
    Schema::builder()
        .dimension(time_dim())
        .dimension(geography_dim())
        .dimension(product_dim())
        .dimension(scenario_dim())
        .measure(measure("npw"))
        .measure(measure("nep"))
        .measure(measure("net_losses"))
        .measure(measure("acquisition_exp"))
        .measure(measure("goe_exp"))
        .measure(measure("total_expenses"))
        .measure(measure("uw_result"))
        .measure(measure("treaty_count"))
        .metric(loss_ratio())
        .metric(expense_ratio())
        .metric(combined_ratio())
        .metric(revenue_yoy())
        .metric(revenue_mom())
        .metric(uw_margin())
        .build()
        .context("Schema::build failed")
}

fn time_dim() -> Dimension {
    Dimension::time(n("Time"), vec![Calendar::gregorian(n("Gregorian"))]).hierarchy(
        Hierarchy::new(n("Calendar"))
            .level(Level::new(n("FY"), n("year")))
            .level(Level::new(n("Month"), n("month"))),
    )
}

fn geography_dim() -> Dimension {
    Dimension::regular(n("Geography")).hierarchy(
        Hierarchy::new(n("World"))
            .level(Level::new(n("Region"), n("region")))
            .level(Level::new(n("Country"), n("country_code"))),
    )
}

fn product_dim() -> Dimension {
    Dimension::regular(n("Product")).hierarchy(
        Hierarchy::new(n("LineOfBusiness")).level(Level::new(n("ProductLine"), n("product_code"))),
    )
}

fn scenario_dim() -> Dimension {
    Dimension::scenario(n("Scenario"))
        .hierarchy(Hierarchy::new(n("Plan")).level(Level::new(n("Scenario"), n("scenario"))))
}

/// Sum-aggregated measure whose name doubles as the source column. The
/// inmem reference cube keys measures off the column of the same name.
fn measure(col: &str) -> Measure {
    Measure::new(n(col), Aggregation::sum())
}

fn loss_ratio() -> Metric {
    Metric::new(n("LossRatio"), div(ref_("net_losses"), ref_("nep"))).with_format("0.0%".into())
}

fn expense_ratio() -> Metric {
    Metric::new(n("ExpenseRatio"), div(ref_("total_expenses"), ref_("nep")))
        .with_format("0.0%".into())
}

fn combined_ratio() -> Metric {
    Metric::new(
        n("CombinedRatio"),
        add(ref_("LossRatio"), ref_("ExpenseRatio")),
    )
    .with_format("0.0%".into())
}

/// YoY = NPW / Lag(NPW, 12, Time) − 1. Lag's `n` is in periods of the
/// dimension's leaf level (Month here), so 12 months = one fiscal year.
fn revenue_yoy() -> Metric {
    let prev = lag(ref_("npw"), "Time", 12);
    Metric::new(
        n("RevenueYoY"),
        sub(div(ref_("npw"), prev), Expr::Const { value: 1.0 }),
    )
    .with_format("0.0%".into())
}

/// MoM = NPW / Lag(NPW, 1, Time) − 1.
fn revenue_mom() -> Metric {
    let prev = lag(ref_("npw"), "Time", 1);
    Metric::new(
        n("RevenueMoM"),
        sub(div(ref_("npw"), prev), Expr::Const { value: 1.0 }),
    )
    .with_format("0.0%".into())
}

fn uw_margin() -> Metric {
    Metric::new(n("UwMargin"), div(ref_("uw_result"), ref_("npw"))).with_format("0.0%".into())
}

fn ref_(name: &str) -> Expr {
    Expr::Ref { name: n(name) }
}

fn div(l: Expr, r: Expr) -> Expr {
    Expr::Binary {
        bin_op: BinOp::Div,
        l: Box::new(l),
        r: Box::new(r),
    }
}

fn sub(l: Expr, r: Expr) -> Expr {
    Expr::Binary {
        bin_op: BinOp::Sub,
        l: Box::new(l),
        r: Box::new(r),
    }
}

fn add(l: Expr, r: Expr) -> Expr {
    Expr::Binary {
        bin_op: BinOp::Add,
        l: Box::new(l),
        r: Box::new(r),
    }
}

fn lag(of: Expr, dim: &str, n_periods: i32) -> Expr {
    Expr::Lag {
        of: Box::new(of),
        dim: n(dim),
        n: n_periods,
    }
}

/// Schema-side identifiers are static here, so a parse failure is a build
/// bug, not user input — `expect` is appropriate per RUST_STYLE.md.
fn n(s: &str) -> Name {
    Name::parse(s).expect("static meridianre schema identifier is valid")
}

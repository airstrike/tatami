//! Hewton schema — six dimensions, three stored measures, six metrics.
//!
//! Serves as the realistic example of `Schema::builder()` usage. Every
//! helper call here is part of the tatami public API contract.

use tatami::schema::{
    self, Aggregation, BinOp, Calendar, Dimension, Hierarchy, Level, Measure, Metric, MetricExpr,
    Name, Schema, Unit,
};

pub fn hewton_schema() -> Result<Schema, schema::Error> {
    Schema::builder()
        .dimension(geography())
        .dimension(brand_tier())
        .dimension(channel())
        .dimension(segment())
        .dimension(time())
        .dimension(scenario())
        .measure(amount())
        .measure(room_nights_sold())
        .measure(rooms_available())
        .metric(revenue())
        .metric(adr())
        .metric(occupancy())
        .metric(revpar())
        .metric(revenue_yoy())
        .metric(revenue_mom())
        .build()
}

// ── Dimensions ─────────────────────────────────────────────────────────────

fn geography() -> Dimension {
    Dimension::regular(n("Geography")).hierarchy(
        Hierarchy::new(n("Default"))
            .level(Level::new(n("World"), n("world")))
            .level(Level::new(n("Region"), n("region")))
            .level(Level::new(n("Country"), n("country")))
            .level(Level::new(n("State"), n("state"))),
    )
}

fn brand_tier() -> Dimension {
    Dimension::regular(n("BrandTier"))
        .hierarchy(Hierarchy::new(n("Default")).level(Level::new(n("Tier"), n("tier"))))
}

fn channel() -> Dimension {
    Dimension::regular(n("Channel"))
        .hierarchy(Hierarchy::new(n("Default")).level(Level::new(n("Channel"), n("channel"))))
}

fn segment() -> Dimension {
    Dimension::regular(n("Segment"))
        .hierarchy(Hierarchy::new(n("Default")).level(Level::new(n("Segment"), n("segment"))))
}

fn time() -> Dimension {
    Dimension::time(n("Time"), vec![Calendar::gregorian(n("Gregorian"))]).hierarchy(
        Hierarchy::new(n("Fiscal"))
            .level(Level::new(n("FiscalYear"), n("fy")))
            .level(Level::new(n("Quarter"), n("quarter")))
            .level(Level::new(n("Month"), n("month"))),
    )
}

fn scenario() -> Dimension {
    Dimension::scenario(n("Scenario"))
        .hierarchy(Hierarchy::new(n("Default")).level(Level::new(n("Scenario"), n("scenario"))))
}

// ── Measures ───────────────────────────────────────────────────────────────

fn amount() -> Measure {
    Measure::new(n("amount"), Aggregation::sum()).with_unit(Unit::parse("USD").expect("usd"))
}

fn room_nights_sold() -> Measure {
    Measure::new(n("room_nights_sold"), Aggregation::sum())
}

fn rooms_available() -> Measure {
    Measure::new(n("rooms_available"), Aggregation::sum())
}

// ── Metrics ────────────────────────────────────────────────────────────────

fn revenue() -> Metric {
    Metric::new(n("Revenue"), MetricExpr::Ref { name: n("amount") })
        .with_unit(Unit::parse("USD").expect("usd"))
}

/// ADR = Revenue / room_nights_sold.
fn adr() -> Metric {
    Metric::new(n("ADR"), div(ref_("Revenue"), ref_("room_nights_sold")))
        .with_unit(Unit::parse("USD").expect("usd"))
}

/// Occupancy = room_nights_sold / rooms_available.
fn occupancy() -> Metric {
    Metric::new(
        n("Occupancy"),
        div(ref_("room_nights_sold"), ref_("rooms_available")),
    )
    .with_format("0.0%".into())
}

/// RevPAR = Revenue / rooms_available.
fn revpar() -> Metric {
    Metric::new(n("RevPAR"), div(ref_("Revenue"), ref_("rooms_available")))
        .with_unit(Unit::parse("USD").expect("usd"))
}

/// Year-over-year Revenue growth = (Revenue − Revenue.Lag(Time, 12)) / Revenue.Lag(Time, 12).
fn revenue_yoy() -> Metric {
    let prev = lag(ref_("Revenue"), "Time", 12);
    Metric::new(
        n("RevenueYoY"),
        div(sub(ref_("Revenue"), prev.clone()), prev),
    )
    .with_format("0.0%".into())
}

/// Month-over-month Revenue growth.
fn revenue_mom() -> Metric {
    let prev = lag(ref_("Revenue"), "Time", 1);
    Metric::new(
        n("RevenueMoM"),
        div(sub(ref_("Revenue"), prev.clone()), prev),
    )
    .with_format("0.0%".into())
}

// ── Local helpers — keep call sites readable ───────────────────────────────

fn ref_(name: &str) -> MetricExpr {
    MetricExpr::Ref { name: n(name) }
}

fn div(l: MetricExpr, r: MetricExpr) -> MetricExpr {
    MetricExpr::Binary {
        bin_op: BinOp::Div,
        l: Box::new(l),
        r: Box::new(r),
    }
}

fn sub(l: MetricExpr, r: MetricExpr) -> MetricExpr {
    MetricExpr::Binary {
        bin_op: BinOp::Sub,
        l: Box::new(l),
        r: Box::new(r),
    }
}

fn lag(of: MetricExpr, dim: &str, n_periods: i32) -> MetricExpr {
    MetricExpr::Lag {
        of: Box::new(of),
        dim: n(dim),
        n: n_periods,
    }
}

fn n(s: &str) -> Name {
    Name::parse(s).expect("hewton identifiers are valid")
}

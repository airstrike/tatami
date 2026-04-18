//! Synthetic Hewton fact DataFrame.
//!
//! Shape: one row per (state, brand_tier, channel, segment, month, scenario).
//! Scope of the sample is deliberately small (~4 states × 2 tiers × 2
//! channels × 2 segments × 12 months × 3 scenarios = 1,152 rows) so the
//! example runs in milliseconds; it's the same schema at any scale.
//!
//! Columns must match the schema's dimension + measure column names (see
//! `schema.rs` levels and measure names). `InMemoryCube::new` validates
//! this at construction — a mismatch here surfaces as a schema error, not
//! silent NaN.

use polars_core::df;
use polars_core::prelude::*;

pub fn hewton_facts() -> DataFrame {
    let states = ["CA", "NY", "TX", "FL"];
    let tiers = ["Luxury", "Premium"];
    let channels = ["Direct", "OTA"];
    let segments = ["Leisure", "Business"];
    let scenarios = ["Actual", "Plan", "WhatIf_A"];
    let months = (1..=12).collect::<Vec<i32>>();

    let mut world = Vec::new();
    let mut region = Vec::new();
    let mut country = Vec::new();
    let mut state = Vec::new();
    let mut tier = Vec::new();
    let mut channel = Vec::new();
    let mut segment = Vec::new();
    let mut scenario = Vec::new();
    let mut fy = Vec::new();
    let mut quarter = Vec::new();
    let mut month = Vec::new();

    let mut amount = Vec::new();
    let mut room_nights_sold = Vec::new();
    let mut rooms_available = Vec::new();

    let mut rng: u64 = 0xC0FFEE;
    let mut rand = || {
        rng = rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (rng >> 33) as f64 / (1u64 << 31) as f64
    };

    for &st in &states {
        for &tr in &tiers {
            for &ch in &channels {
                for &sg in &segments {
                    for &sc in &scenarios {
                        for &m in &months {
                            world.push("World");
                            region.push(region_for(st));
                            country.push("US");
                            state.push(st);
                            tier.push(tr);
                            channel.push(ch);
                            segment.push(sg);
                            scenario.push(sc);
                            fy.push("FY2026");
                            quarter.push(match m {
                                1..=3 => "Q1",
                                4..=6 => "Q2",
                                7..=9 => "Q3",
                                _ => "Q4",
                            });
                            month.push(format!("{:04}-{:02}", 2026, m));

                            let base = 120_000.0 * (1.0 + 0.4 * rand());
                            let mult = if sc == "Plan" {
                                0.95
                            } else if sc == "WhatIf_A" {
                                1.10
                            } else {
                                1.0
                            };
                            amount.push(base * mult);
                            room_nights_sold.push((600.0 + 200.0 * rand()) as i64);
                            rooms_available.push(900_i64);
                        }
                    }
                }
            }
        }
    }

    df! {
        "world"              => world,
        "region"             => region,
        "country"            => country,
        "state"              => state,
        "tier"               => tier,
        "channel"            => channel,
        "segment"            => segment,
        "scenario"           => scenario,
        "fy"                 => fy,
        "quarter"            => quarter,
        "month"              => month,
        "amount"             => amount,
        "room_nights_sold"   => room_nights_sold,
        "rooms_available"    => rooms_available,
    }
    .expect("hewton fact schema is consistent")
}

fn region_for(state: &str) -> &'static str {
    match state {
        "CA" => "West",
        "TX" => "South",
        "FL" => "Southeast",
        _ => "Northeast",
    }
}

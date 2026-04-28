//! `meridianre-board` — drill-down GUI demo over the meridianre cube.
//!
//! Boots `tatami-serve` in-process on `127.0.0.1:0`, points a
//! [`tatami_http::Remote`] at the resulting URL, and renders one of six
//! pre-defined dashboard tiles. Clicking a member name in any row drills
//! the cube down by pinning that member onto a slicer trail; switching
//! tiles preserves the trail (drill-across); clicking the × on a chip
//! pops that index (drill-up).
//!
//! Run via `cargo run -p meridianre-board`. The CSV path follows the
//! same `MERIDIANRE_DATA_DIR` env-var convention as the
//! `meridianre-serve` binary; default
//! `~/inboard-ai/crates/meridianre/sample/monthly_close.csv`.

#![warn(missing_docs)]

use iced::{Font, Theme};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod board;
mod infolet;
mod server;

fn main() -> iced::Result {
    init_tracing();

    iced::application(board::App::new, board::App::update, board::App::view)
        .theme(Theme::Oxocarbon)
        .default_font(Font::new("Inter"))
        .window_size((1200.0, 800.0))
        .title("meridianre-board — drill-down dashboard demo")
        .run()
}

/// Tracing init mirroring `meridianre-serve`'s defaults so local-dev
/// logs share the same surface.
fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "info,hyper=warn".into());
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .init();
}

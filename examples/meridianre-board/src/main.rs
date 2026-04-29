//! `meridianre-board` — schema-blind drill-down GUI demo over a cube
//! served by `tatami-serve`.
//!
//! Boots `tatami-serve` in-process on `127.0.0.1:0`, points a
//! [`tatami_http::Remote`] at the resulting URL, and runs a composer
//! UI that drives every picker — measure, dimension, hierarchy — off
//! the introspected `Schema` alone. The binary holds **zero**
//! knowledge of any specific schema name; pointed at a different cube
//! the pickers repopulate themselves.
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
mod composer;
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

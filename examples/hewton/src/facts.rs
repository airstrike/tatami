//! Load the Hewton fact table asynchronously from disk.
//!
//! CSV lives at `examples/hewton/assets/hewton.csv` — ~2,300 rows spanning
//! US states × brand tier × channel × segment × scenario × six fiscal
//! years at quarterly granularity. Path is anchored at
//! `CARGO_MANIFEST_DIR` so the load succeeds regardless of the binary's
//! working directory.
//!
//! Parsing is synchronous polars-io on a blocking thread pool via
//! `tokio::task::spawn_blocking`; the returned future is what the iced
//! `Task` awaits. The dashboard stays interactive while the CSV parses.
//!
//! Columns must match every `Measure.name` and every `Level.key` declared
//! in `schema::hewton_schema` — `InMemoryCube::new` enforces this at
//! construction.

use std::path::PathBuf;

use polars_core::prelude::DataFrame;
use polars_io::prelude::{CsvReadOptions, SerReader};

/// Asynchronously read `assets/hewton.csv` and return the parsed
/// DataFrame. Errors are stringified — iced Messages are `Clone`, so
/// carrying a `polars_io::Error` would require a wrapper.
pub async fn load() -> Result<DataFrame, String> {
    let path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "assets", "hewton.csv"]
        .iter()
        .collect();

    tokio::task::spawn_blocking(move || {
        CsvReadOptions::default()
            .with_has_header(true)
            .try_into_reader_with_file_path(Some(path))
            .map_err(|e| e.to_string())?
            .finish()
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| format!("csv task panicked: {e}"))?
}

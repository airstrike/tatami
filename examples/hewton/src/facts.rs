//! Load the Hewton fact table from the bundled CSV.
//!
//! Data lives in `examples/hewton/assets/hewton.csv` — roughly 2,300 rows
//! spanning US states × brand tier × channel × segment × scenario × six
//! fiscal years at quarterly granularity. The CSV is bundled with
//! `include_str!` so the binary is self-contained and doesn't depend on
//! the working directory at run time.
//!
//! Regeneration: the CSV was produced by a one-off script matching the
//! column schema below. Bump it any time the shape needs to cover a new
//! query. Columns must match every `Measure.name` and every `Level.key`
//! declared in `schema::hewton_schema` — `InMemoryCube::new` enforces this
//! at construction.

use std::io::Cursor;

use polars_core::prelude::DataFrame;
use polars_io::prelude::{CsvReadOptions, SerReader};

const HEWTON_CSV: &str = include_str!("../assets/hewton.csv");

pub fn hewton_facts() -> DataFrame {
    let cursor = Cursor::new(HEWTON_CSV.as_bytes());
    CsvReadOptions::default()
        .with_has_header(true)
        .into_reader_with_file_handle(cursor)
        .finish()
        .expect("bundled hewton.csv is well-formed")
}

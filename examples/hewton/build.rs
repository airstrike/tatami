//! Hewton build script.
//!
//! Runs `iced_lucide::build` to generate `src/icon.rs` — a type-safe
//! module with one function per Lucide icon name listed in
//! `fonts/icons.toml`. The generated file is gitignored; Cargo re-runs
//! this script whenever `fonts/icons.toml` changes.

fn main() {
    println!("cargo::rerun-if-changed=fonts/icons.toml");
    iced_lucide::build("fonts/icons.toml").expect("build icon module");
}

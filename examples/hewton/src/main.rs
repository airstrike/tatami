//! # Hewton — the TARGET API for tatami v0.1.
//!
//! **This file is frozen.** Every call it makes is a promise the tatami
//! library must keep. If a call here feels awkward, the fix is to change
//! the *library*, not hewton.
//!
//! Hewton is a hotel-sales cube — US states × brand tier × channel × segment
//! × month, with `amount`, `room_nights_sold`, and `rooms_available`
//! measures and derived metrics (Revenue, ADR, Occupancy, RevPAR, YoY, MoM).
//! Four example queries exercise the `Results` shapes against one schema:
//!
//! - **Scalar**   — FY2026 Revenue + MoM delta.
//! - **Pivot**    — Quarterly Revenue by Region, FY2025–FY2030.
//! - **Pivot**    — AOP Plan vs What-If by line item.
//! - **Series**   — Sales volume by territory, World → Region → Country.
//!
//! ## North star
//!
//! TEA (Elm architecture) via `iced::application(new, update, view)`.
//! Facts load asynchronously from `assets/hewton.csv`; once the CSV parse
//! completes, the cube is constructed and every `ExampleQuery` fires
//! as its own `Task`. Each result lands in a `HashMap` keyed by the
//! enum, and `view` is a pure function of that state.

use std::collections::HashMap;
use std::sync::Arc;

use iced::widget::{center, column, pick_list, row, scrollable, text};
use iced::{Alignment, Element, Font, Task, Theme, font};

use polars_core::prelude::DataFrame;
use tatami::schema::Schema;
use tatami::{Cube, Results};
use tatami_inmem::InMemoryCube;

mod facts;
mod queries;
mod scenario;
mod schema;
mod theme;
mod widgets;

use queries::ExampleQuery;
use scenario::Scenario;

/// Primary UI typeface — Inter. Loaded from Google Fonts at startup via
/// `fount`; until the network call resolves, iced falls back to its
/// platform default sans-serif.
pub const INTER: Font = Font {
    family: font::Family::Name("Inter"),
    weight: font::Weight::Normal,
    stretch: font::Stretch::Normal,
    style: font::Style::Normal,
    optical_size: font::OpticalSize::None,
};

fn main() -> iced::Result {
    iced::application(App::new, App::update, App::view)
        .theme(|_: &App| Theme::Light)
        .default_font(INTER)
        .window_size((1200.0, 800.0))
        .title("Hewton — tatami v0.1 worked example")
        .run()
}

// ── Model ──────────────────────────────────────────────────────────────────

/// Application state. The schema is built synchronously at startup; the
/// cube arrives asynchronously once `assets/hewton.csv` parses. Results
/// land in the `ExampleQuery`-keyed map as each query finishes.
struct App {
    schema: Schema,
    cube: Option<Arc<InMemoryCube>>,
    scenario: Scenario,
    results: HashMap<ExampleQuery, QueryState>,
    load_error: Option<String>,
}

enum QueryState {
    Running,
    Ok(Results),
    Err(String),
}

// ── Messages ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum Message {
    /// `assets/hewton.csv` parsed — cube can now be constructed.
    FactsLoaded(Result<DataFrame, String>),
    /// A named query completed.
    QueryDone(ExampleQuery, Result<Results, String>),
    /// A Google Fonts family finished downloading + registering with iced.
    FontLoaded(&'static str, Result<(), String>),
    /// The scenario picker changed — re-issue every query against the new
    /// scenario. `PlanVsWhatIf` re-runs too (it just ignores the value).
    ScenarioChanged(Scenario),
}

// ── new / update / view ────────────────────────────────────────────────────

impl App {
    fn new() -> (Self, Task<Message>) {
        let schema = schema::hewton_schema().expect("hewton schema is valid");

        // Fire the initial async tasks: the CSV load and the font load.
        // Query tasks only launch once FactsLoaded arrives in update.
        let init = Task::batch([
            load_family("Inter"),
            Task::future(facts::load()).map(Message::FactsLoaded),
        ]);

        let app = Self {
            schema,
            cube: None,
            scenario: Scenario::Actual,
            results: HashMap::new(),
            load_error: None,
        };

        (app, init)
    }

    fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::FactsLoaded(Ok(df)) => {
                // Build the cube. InMemoryCube validates every measure/level
                // column at construction; after this point every query is
                // against a known-good cube.
                match InMemoryCube::new(df, self.schema.clone()) {
                    Ok(cube) => {
                        let cube = Arc::new(cube);
                        let scenario = self.scenario;
                        let tasks: Vec<Task<Message>> = ExampleQuery::ALL
                            .into_iter()
                            .map(|eq| {
                                self.results.insert(eq, QueryState::Running);
                                spawn(cube.clone(), eq, scenario)
                            })
                            .collect();
                        self.cube = Some(cube);
                        Task::batch(tasks)
                    }
                    Err(error) => {
                        self.load_error = Some(format!("cube construction: {error}"));
                        Task::none()
                    }
                }
            }
            Message::FactsLoaded(Err(error)) => {
                self.load_error = Some(format!("facts load: {error}"));
                Task::none()
            }
            Message::QueryDone(eq, Ok(results)) => {
                self.results.insert(eq, QueryState::Ok(results));
                Task::none()
            }
            Message::QueryDone(eq, Err(error)) => {
                self.results.insert(eq, QueryState::Err(error));
                Task::none()
            }
            Message::FontLoaded(_name, Ok(())) => Task::none(),
            Message::FontLoaded(name, Err(error)) => {
                eprintln!("font load failed: {name} — {error}");
                Task::none()
            }
            Message::ScenarioChanged(scenario) => {
                self.scenario = scenario;
                if let Some(cube) = self.cube.clone() {
                    let tasks: Vec<Task<Message>> = ExampleQuery::ALL
                        .into_iter()
                        .map(|eq| {
                            self.results.insert(eq, QueryState::Running);
                            spawn(cube.clone(), eq, scenario)
                        })
                        .collect();
                    Task::batch(tasks)
                } else {
                    Task::none()
                }
            }
        }
    }

    fn view(&self) -> Element<'_, Message> {
        // Loading / error splash until the cube is ready.
        if let Some(error) = &self.load_error {
            return center(text(format!("Error: {error}")).size(14)).into();
        }
        if self.cube.is_none() {
            return center(text("Loading hewton facts\u{2026}").size(14)).into();
        }

        // Scenario picker — drives the slicer for `FyRevenue`,
        // `QuarterlyByRegion`, `WorldToCountry`. `PlanVsWhatIf` re-runs
        // too but ignores the value (columns = [Plan, WhatIf_A]).
        let picker: Element<'_, Message> = row![
            text("Scenario:").size(13),
            pick_list(Some(self.scenario), Scenario::ALL, Scenario::to_string,)
                .on_select(Message::ScenarioChanged),
        ]
        .spacing(12)
        .align_y(Alignment::Center)
        .into();

        let cards = ExampleQuery::ALL
            .into_iter()
            .map(|eq| widgets::card(eq.heading(), eq.subtitle(), self.results.get(&eq)));

        scrollable(
            column(std::iter::once(picker).chain(cards))
                .spacing(16)
                .padding(24)
                .width(iced::Length::Fill),
        )
        .into()
    }
}

// ── Query plumbing ─────────────────────────────────────────────────────────

/// Fire one example query as an iced `Task` against the selected scenario.
/// The returned `Task` resolves to `Message::QueryDone(eq, Ok/Err)` when
/// `cube.query(&q)` finishes.
fn spawn(cube: Arc<InMemoryCube>, eq: ExampleQuery, scenario: Scenario) -> Task<Message> {
    let query = eq.query(scenario);
    Task::future(async move {
        let outcome = cube.query(&query).await.map_err(|e| e.to_string());
        Message::QueryDone(eq, outcome)
    })
}

// ── Font loading ───────────────────────────────────────────────────────────

/// Fetch a Google Fonts family via `fount`, then register every variant's
/// bytes with iced. Folds every outcome into a single `FontLoaded` Message
/// — the first failure wins, success is an empty Ok.
fn load_family(name: &'static str) -> Task<Message> {
    Task::future(async move { fount::google::load(name, None).await }).then(move |result| {
        match result {
            Ok(variants) => {
                let register = variants.into_iter().map(|bytes| {
                    iced::font::load(bytes).map(move |r: Result<(), iced::font::Error>| {
                        r.map_err(|e| format!("{e:?}"))
                    })
                });
                Task::batch(register)
                    .collect()
                    .map(move |results: Vec<Result<(), String>>| {
                        let combined = results.into_iter().find(Result::is_err).unwrap_or(Ok(()));
                        Message::FontLoaded(name, combined)
                    })
            }
            Err(error) => Task::done(Message::FontLoaded(name, Err(format!("{error:?}")))),
        }
    })
}

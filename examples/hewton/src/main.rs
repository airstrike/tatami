//! # Hewton — the TARGET API for tatami v0.1.
//!
//! **This file is frozen.** Every call it makes is a promise the tatami
//! library must keep. If a call here feels awkward, the fix is to change
//! the *library*, not hewton.
//!
//! Hewton is a hotel-sales cube — US states × brand tier × channel × segment
//! × month, with `amount`, `room_nights_sold`, and `rooms_available`
//! measures and derived metrics (Revenue, ADR, Occupancy, RevPAR, YoY, MoM).
//! It demonstrates all four `Results` shapes against one schema:
//!
//! - **Scalar**   — FY2026 Revenue + MoM delta.
//! - **Pivot**    — Quarterly Revenue by Region, FY2025–FY2030.
//! - **Pivot**    — AOP Plan vs What-If by line item.
//! - **Rollup**   — Sales volume by territory, World → Region → Country.
//!
//! ## Compilation status
//!
//! May not compile until the following land:
//! - Phase 3  — `tatami::Cube` trait + `tatami::Results` enum.
//! - Phase 4  — `tatami_inmem::InMemoryCube` scaffold (workspace member +
//!   `InMemoryCube::new(df, schema)`).
//! - Phase 5  — real `cube.query(&q).await` evaluation.
//!
//! The compile errors against this file are the phase checklist.
//!
//! ## North star
//!
//! TEA (Elm architecture) via `iced::application(new, update, view)`.
//! Queries flow as `Message` values; `update` dispatches them to
//! `Arc<InMemoryCube>`; view is a pure function of `Results`.

use std::collections::HashMap;
use std::sync::Arc;

use iced::widget::{column, scrollable};
use iced::{Element, Font, Task, Theme, font};

use tatami::{Cube, Results};
use tatami_inmem::InMemoryCube;

mod facts;
mod queries;
mod schema;
mod theme;
mod widgets;

use queries::ExampleQuery;

/// Primary UI typeface — Inter. Loaded from Google Fonts at startup via
/// `fount`; until the network call resolves, iced falls back to its
/// platform default sans-serif.
pub const INTER: Font = Font {
    family: font::Family::Name("Inter"),
    weight: font::Weight::Normal,
    stretch: font::Stretch::Normal,
    style: font::Style::Normal,
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

/// Application state. Cube is shared via `Arc` so each query task gets a
/// cheap handle; results land in an `ExampleQuery`-keyed map as they
/// complete — the enum is both the identity and the UI metadata source.
struct App {
    // Retained so the cube lives as long as the app; Phase 5 will re-issue
    // queries on interaction and this field becomes read-every-frame.
    #[allow(dead_code)]
    cube: Arc<InMemoryCube>,
    results: HashMap<ExampleQuery, QueryState>,
}

enum QueryState {
    Running,
    Ok(Results),
    Err(String),
}

// ── Messages ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum Message {
    /// A named query completed — success or failure.
    QueryDone(ExampleQuery, Result<Results, String>),
    /// A Google Fonts family finished downloading + registering with iced.
    /// The second payload is `Err(reason)` on download/parse failure; iced
    /// falls back to the platform default silently, so this is logged but
    /// not surfaced to the view.
    FontLoaded(&'static str, Result<(), String>),
}

// ── new / update / view ────────────────────────────────────────────────────

impl App {
    fn new() -> (Self, Task<Message>) {
        // Build the schema using the tidy-style fluent API — the single
        // non-compile-error path from Name::parse to a validated Schema.
        let schema = schema::hewton_schema().expect("hewton schema is valid");

        // Build the in-memory fact source (Polars DataFrame).
        let facts = facts::hewton_facts();

        // Wrap into a Cube. InMemoryCube validates measure/dim column
        // existence at construction; after this point every query is
        // against a known-good cube.
        let cube = Arc::new(InMemoryCube::new(facts, schema).expect("fact source matches schema"));

        // Kick off each example query concurrently. Each returns a Task
        // that resolves to a `QueryDone` message keyed by `ExampleQuery`.
        let mut results = HashMap::with_capacity(ExampleQuery::ALL.len());
        let query_tasks: Vec<Task<Message>> = ExampleQuery::ALL
            .into_iter()
            .map(|eq| {
                results.insert(eq, QueryState::Running);
                spawn(cube.clone(), eq)
            })
            .collect();

        // Load Inter from Google Fonts. Until it resolves, text renders in
        // the platform default. No blocking on this — font arrival triggers
        // a natural re-layout.
        let init = Task::batch(
            std::iter::once(load_family("Inter"))
                .chain(query_tasks)
                .collect::<Vec<_>>(),
        );

        (Self { cube, results }, init)
    }

    fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::QueryDone(eq, Ok(results)) => {
                self.results.insert(eq, QueryState::Ok(results));
            }
            Message::QueryDone(eq, Err(error)) => {
                self.results.insert(eq, QueryState::Err(error));
            }
            Message::FontLoaded(_name, Ok(())) => {
                // iced re-lays out on next frame; nothing to do here.
            }
            Message::FontLoaded(name, Err(error)) => {
                // Silent fall-through to the platform default is fine —
                // log for local debugging and keep going.
                eprintln!("font load failed: {name} — {error}");
            }
        }
        Task::none()
    }

    fn view(&self) -> Element<'_, Message> {
        let cards = ExampleQuery::ALL
            .into_iter()
            .map(|eq| widgets::card(eq.heading(), eq.subtitle(), self.results.get(&eq)));

        scrollable(
            column(cards)
                .spacing(16)
                .padding(24)
                .width(iced::Length::Fill),
        )
        .into()
    }
}

// ── Query plumbing ─────────────────────────────────────────────────────────

/// Fire one example query as an iced `Task`. The returned `Task` resolves
/// to `Message::QueryDone(eq, Ok/Err)` when `cube.query(&q)` finishes.
///
/// This is the entire "how does the app talk to tatami" story: query is
/// `async fn`, we wrap it in `Task::future`, we map the result into a
/// Message. Nothing else.
fn spawn(cube: Arc<InMemoryCube>, eq: ExampleQuery) -> Task<Message> {
    let query = eq.query();
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

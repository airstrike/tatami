//! # Hewton — the TARGET API for tatami v0.1.
//!
//! **This file is frozen.** Every call it makes is a promise the tatami
//! library must keep. If a call here feels awkward, the fix is to change
//! the *library*, not hewton.
//!
//! Hewton is a hotel-sales cube (see `schema.rs` and `facts.rs` — these
//! still define the specific Hewton shape). The composer UI itself is
//! **schema-blind**: it picks rows / columns / metric by *index* into the
//! introspected `Schema`, never by hard-coded field names. Point the
//! same binary at a different cube and the pickers repopulate themselves.
//!
//! ## North star
//!
//! TEA (Elm architecture) via `iced::application(new, update, view)`.
//! Facts load asynchronously from `assets/hewton.csv`; once the CSV parse
//! completes, the cube is constructed and `cube.schema().await` drives
//! every picker. A pick changes the `Query` assembled from schema
//! indices, which is fired against the cube and rendered via the
//! generic `widgets::render_*` adapters.

use std::fmt;
use std::sync::Arc;

use iced::widget::{center, column, pick_list, row, scrollable, text};
use iced::{Alignment, Element, Font, Length, Task, Theme, font};

use polars_core::prelude::DataFrame;
use tatami::query::{self, Set, Tuple};
use tatami::schema::{Name, Schema};
use tatami::{Axes, Cube, Query, Results};
use tatami_inmem::InMemoryCube;

mod facts;
mod schema;
mod theme;
mod widgets;

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

/// Application state. The cube and its [`Schema`] arrive asynchronously
/// once `assets/hewton.csv` parses; all picker options are indices into
/// that schema, so nothing here names a specific dim / measure / metric.
struct App {
    cube: Option<Arc<InMemoryCube>>,
    schema_ref: Option<Schema>,
    load_error: Option<String>,

    rows: AxisPick,
    columns: AxisPick,
    metric: Option<MetricPick>,

    result: QueryState,
}

/// An axis choice, sourced entirely from schema indices. `None` means the
/// axis is absent from the query shape (the rows+columns absence gives
/// [`Axes::Scalar`], rows-only gives [`Axes::Series`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
enum AxisPick {
    /// Axis absent — contributes nothing to [`Axes`].
    None,
    /// Axis present at the given `(dimension, hierarchy, level)` position
    /// within the schema's `dimensions` vector.
    Pick {
        /// Index into `schema.dimensions`.
        dim: usize,
        /// Index into `schema.dimensions[dim].hierarchies`.
        hierarchy: usize,
        /// Index into `schema.dimensions[dim].hierarchies[hierarchy].levels`.
        level: usize,
    },
}

/// A metric choice — either an index into `schema.measures` or an index
/// into `schema.metrics`. No names cross this boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
enum MetricPick {
    /// Index into `schema.measures`.
    Measure(usize),
    /// Index into `schema.metrics`.
    Metric(usize),
}

/// The most recent query outcome. Picker changes eagerly kick the state
/// back to `Running` until the new task resolves.
#[non_exhaustive]
enum QueryState {
    /// No query has been assembled yet (missing a metric, etc.).
    Idle,
    /// A task is in flight.
    Running,
    /// Last query succeeded.
    Ok(Results),
    /// Last query failed — the string is the backend error.
    Err(String),
}

// ── Messages ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum Message {
    /// `assets/hewton.csv` parsed — cube can now be constructed.
    FactsLoaded(Result<DataFrame, String>),
    /// The cube's schema is ready; we cache it locally so `view` can
    /// build pickers synchronously.
    SchemaReady(Schema),
    /// A query completed.
    QueryDone(Result<Results, String>),
    /// A Google Fonts family finished downloading + registering with iced.
    FontLoaded(&'static str, Result<(), String>),

    /// Rows dim picker changed. `None` means "no rows axis".
    RowsDimPicked(Option<DimChoice>),
    /// Rows level picker changed.
    RowsLevelPicked(Option<LevelChoice>),
    /// Columns dim picker changed. `None` means "no columns axis".
    ColumnsDimPicked(Option<DimChoice>),
    /// Columns level picker changed.
    ColumnsLevelPicked(Option<LevelChoice>),
    /// Metric picker changed.
    MetricPicked(Option<MetricChoice>),
}

/// A dimension option in a pick_list. Stores the index into
/// `schema.dimensions` and a display label cloned from the dim's name.
#[derive(Clone, Debug, PartialEq, Eq)]
struct DimChoice {
    index: usize,
    label: String,
}

impl fmt::Display for DimChoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.label)
    }
}

/// A level option in a pick_list — indexed pair `(hierarchy, level)`
/// within an already-chosen dimension.
#[derive(Clone, Debug, PartialEq, Eq)]
struct LevelChoice {
    hierarchy: usize,
    level: usize,
    label: String,
}

impl fmt::Display for LevelChoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.label)
    }
}

/// A metric option in a pick_list — indexes into measures or metrics.
#[derive(Clone, Debug, PartialEq, Eq)]
struct MetricChoice {
    pick: MetricPick,
    label: String,
}

impl fmt::Display for MetricChoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.label)
    }
}

// ── new / update / view ────────────────────────────────────────────────────

impl App {
    fn new() -> (Self, Task<Message>) {
        let init = Task::batch([
            load_family("Inter"),
            Task::future(facts::load()).map(Message::FactsLoaded),
        ]);

        let app = Self {
            cube: None,
            schema_ref: None,
            load_error: None,
            rows: AxisPick::None,
            columns: AxisPick::None,
            metric: None,
            result: QueryState::Idle,
        };

        (app, init)
    }

    fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::FactsLoaded(Ok(df)) => {
                // Schema is built synchronously from the same helper that
                // defines the Hewton-specific shape. The UI below never
                // touches this helper — everything downstream indexes the
                // `Schema` returned by `cube.schema().await`.
                let schema = match schema::hewton_schema() {
                    Ok(s) => s,
                    Err(error) => {
                        self.load_error = Some(format!("schema: {error}"));
                        return Task::none();
                    }
                };
                match InMemoryCube::new(df, schema) {
                    Ok(cube) => {
                        let cube = Arc::new(cube);
                        self.cube = Some(cube.clone());
                        // Fetch the schema via the Cube trait — this is
                        // the introspection surface every picker reads.
                        Task::future(async move {
                            match cube.schema().await {
                                Ok(s) => Message::SchemaReady(s),
                                Err(e) => Message::FactsLoaded(Err(e.to_string())),
                            }
                        })
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
            Message::SchemaReady(schema) => {
                self.schema_ref = Some(schema);
                self.run_query()
            }
            Message::QueryDone(Ok(results)) => {
                self.result = QueryState::Ok(results);
                Task::none()
            }
            Message::QueryDone(Err(error)) => {
                self.result = QueryState::Err(error);
                Task::none()
            }
            Message::FontLoaded(_name, Ok(())) => Task::none(),
            Message::FontLoaded(name, Err(error)) => {
                eprintln!("font load failed: {name} — {error}");
                Task::none()
            }
            Message::RowsDimPicked(choice) => {
                self.rows = axis_for(self.schema_ref.as_ref(), choice);
                self.run_query()
            }
            Message::RowsLevelPicked(choice) => {
                self.rows = level_for(&self.rows, choice);
                self.run_query()
            }
            Message::ColumnsDimPicked(choice) => {
                self.columns = axis_for(self.schema_ref.as_ref(), choice);
                self.run_query()
            }
            Message::ColumnsLevelPicked(choice) => {
                self.columns = level_for(&self.columns, choice);
                self.run_query()
            }
            Message::MetricPicked(choice) => {
                self.metric = choice.map(|c| c.pick);
                self.run_query()
            }
        }
    }

    fn view(&self) -> Element<'_, Message> {
        if let Some(error) = &self.load_error {
            return center(text(format!("Error: {error}")).size(14)).into();
        }
        let Some(schema) = self.schema_ref.as_ref() else {
            return center(text("Loading hewton facts\u{2026}").size(14)).into();
        };

        scrollable(
            row![
                sidebar(schema, &self.rows, &self.columns, self.metric),
                widgets::result_panel(&self.result),
            ]
            .spacing(24)
            .padding(24),
        )
        .into()
    }

    /// Rebuild the current `Query` from picker state and spawn it. Leaves
    /// `result` as [`QueryState::Idle`] if no valid query can be assembled
    /// (e.g. columns picked without rows, or no metric selected).
    fn run_query(&mut self) -> Task<Message> {
        let Some(schema) = self.schema_ref.as_ref() else {
            return Task::none();
        };
        let Some(cube) = self.cube.clone() else {
            return Task::none();
        };
        let Some(query) = build_query(schema, &self.rows, &self.columns, self.metric) else {
            self.result = QueryState::Idle;
            return Task::none();
        };
        self.result = QueryState::Running;
        Task::future(async move {
            let outcome = cube.query(&query).await.map_err(|e| e.to_string());
            Message::QueryDone(outcome)
        })
    }
}

// ── Sidebar pickers ────────────────────────────────────────────────────────

/// Build the left-hand sidebar of pickers. Every option is derived from
/// the introspected [`Schema`] — there are no string literals referring to
/// specific dims, levels, or metrics in this function.
fn sidebar<'a>(
    schema: &'a Schema,
    rows: &AxisPick,
    columns: &AxisPick,
    metric: Option<MetricPick>,
) -> Element<'a, Message> {
    let dim_options: Vec<DimChoice> = schema
        .dimensions
        .iter()
        .enumerate()
        .map(|(i, d)| DimChoice {
            index: i,
            label: d.name.as_str().to_owned(),
        })
        .collect();

    let metric_options: Vec<MetricChoice> = schema
        .measures
        .iter()
        .enumerate()
        .map(|(i, m)| MetricChoice {
            pick: MetricPick::Measure(i),
            label: m.name.as_str().to_owned(),
        })
        .chain(
            schema
                .metrics
                .iter()
                .enumerate()
                .map(|(i, m)| MetricChoice {
                    pick: MetricPick::Metric(i),
                    label: m.name.as_str().to_owned(),
                }),
        )
        .collect();

    let rows_dim_selected = current_dim_choice(&dim_options, rows);
    let columns_dim_selected = current_dim_choice(&dim_options, columns);
    let metric_selected =
        metric.and_then(|pick| metric_options.iter().find(|c| c.pick == pick).cloned());

    let rows_block = axis_picker(
        "Rows",
        dim_options.clone(),
        rows_dim_selected,
        schema,
        rows,
        Message::RowsDimPicked,
        Message::RowsLevelPicked,
    );

    let columns_block = axis_picker(
        "Columns",
        dim_options,
        columns_dim_selected,
        schema,
        columns,
        Message::ColumnsDimPicked,
        Message::ColumnsLevelPicked,
    );

    let metric_block = column![
        text("Metric").size(13),
        pick_list(metric_selected, metric_options, |c: &MetricChoice| {
            c.label.clone()
        })
        .on_select(|c: MetricChoice| Message::MetricPicked(Some(c)))
        .placeholder("(pick a metric)")
        .width(Length::Fill),
    ]
    .spacing(6);

    column![rows_block, columns_block, metric_block]
        .spacing(16)
        .width(Length::Fixed(260.0))
        .into()
}

#[allow(clippy::too_many_arguments)]
fn axis_picker<'a>(
    heading: &'static str,
    dim_options: Vec<DimChoice>,
    selected_dim: Option<DimChoice>,
    schema: &'a Schema,
    pick: &AxisPick,
    on_dim: fn(Option<DimChoice>) -> Message,
    on_level: fn(Option<LevelChoice>) -> Message,
) -> Element<'a, Message> {
    let dim_list = pick_list(selected_dim, dim_options, |c: &DimChoice| c.label.clone())
        .on_select(move |c: DimChoice| on_dim(Some(c)))
        .placeholder("(none)")
        .width(Length::Fill);

    // Level picker is populated only when a dim is chosen. The option
    // list walks every hierarchy × level in that dim, so the same control
    // works for regular, time, and scenario dims.
    let level_element: Element<'a, Message> = match *pick {
        AxisPick::Pick {
            dim,
            hierarchy,
            level,
        } => {
            let options: Vec<LevelChoice> = schema.dimensions[dim]
                .hierarchies
                .iter()
                .enumerate()
                .flat_map(|(h_idx, h)| {
                    h.levels
                        .iter()
                        .enumerate()
                        .map(move |(l_idx, l)| LevelChoice {
                            hierarchy: h_idx,
                            level: l_idx,
                            label: if schema.dimensions[dim].hierarchies.len() > 1 {
                                format!("{} / {}", h.name, l.name)
                            } else {
                                l.name.as_str().to_owned()
                            },
                        })
                })
                .collect();
            let selected = options
                .iter()
                .find(|c| c.hierarchy == hierarchy && c.level == level)
                .cloned();
            pick_list(selected, options, |c: &LevelChoice| c.label.clone())
                .on_select(move |c: LevelChoice| on_level(Some(c)))
                .placeholder("(level)")
                .width(Length::Fill)
                .into()
        }
        AxisPick::None => text("").into(),
    };

    column![
        text(heading).size(13),
        row![dim_list].align_y(Alignment::Center),
        level_element,
    ]
    .spacing(6)
    .into()
}

fn current_dim_choice(options: &[DimChoice], pick: &AxisPick) -> Option<DimChoice> {
    match *pick {
        AxisPick::Pick { dim, .. } => options.iter().find(|c| c.index == dim).cloned(),
        AxisPick::None => None,
    }
}

// ── Picker → state transitions ─────────────────────────────────────────────

/// Translate a dim-picker message into an [`AxisPick`]. When the user
/// picks a dim, we seed the axis to that dim's first hierarchy / first
/// level so the query is immediately runnable without a second click.
fn axis_for(schema: Option<&Schema>, choice: Option<DimChoice>) -> AxisPick {
    let Some(choice) = choice else {
        return AxisPick::None;
    };
    let Some(schema) = schema else {
        return AxisPick::None;
    };
    let dim = &schema.dimensions[choice.index];
    // A dim with zero hierarchies / levels can't participate as an axis.
    // Leave the axis as None so `build_query` produces nothing.
    if dim.hierarchies.is_empty() || dim.hierarchies[0].levels.is_empty() {
        return AxisPick::None;
    }
    AxisPick::Pick {
        dim: choice.index,
        hierarchy: 0,
        level: 0,
    }
}

/// Translate a level-picker message into an [`AxisPick`]. Only meaningful
/// once a dim is already selected; otherwise identity.
fn level_for(current: &AxisPick, choice: Option<LevelChoice>) -> AxisPick {
    match (*current, choice) {
        (AxisPick::Pick { dim, .. }, Some(c)) => AxisPick::Pick {
            dim,
            hierarchy: c.hierarchy,
            level: c.level,
        },
        (AxisPick::Pick { .. }, None) => AxisPick::None,
        (AxisPick::None, _) => AxisPick::None,
    }
}

// ── Query assembly ─────────────────────────────────────────────────────────

/// Infer an [`Axes`] shape from the two axis picks:
///
/// - `(None,   None)` → [`Axes::Scalar`].
/// - `(Pick,   None)` → [`Axes::Series`] — rows only.
/// - `(Pick,   Pick)` → [`Axes::Pivot`] — rows × columns.
/// - `(None,   Pick)` → invalid (columns without rows); `None` is returned.
///
/// `build_set` lifts each [`AxisPick::Pick`] into a `Set::members(...)`
/// with every `Name` cloned from the schema's already-validated values.
fn build_axes(schema: &Schema, rows: &AxisPick, columns: &AxisPick) -> Option<Axes> {
    match (rows, columns) {
        (AxisPick::None, AxisPick::None) => Some(Axes::Scalar),
        (AxisPick::Pick { .. }, AxisPick::None) => Some(Axes::Series {
            rows: build_set(schema, rows)?,
        }),
        (AxisPick::Pick { .. }, AxisPick::Pick { .. }) => Some(Axes::Pivot {
            rows: build_set(schema, rows)?,
            columns: build_set(schema, columns)?,
        }),
        (AxisPick::None, AxisPick::Pick { .. }) => None,
    }
}

fn build_set(schema: &Schema, pick: &AxisPick) -> Option<Set> {
    let AxisPick::Pick {
        dim,
        hierarchy,
        level,
    } = *pick
    else {
        return None;
    };
    let d = schema.dimensions.get(dim)?;
    let h = d.hierarchies.get(hierarchy)?;
    let l = h.levels.get(level)?;
    Some(Set::members(d.name.clone(), h.name.clone(), l.name.clone()))
}

fn metric_name(schema: &Schema, pick: MetricPick) -> Option<Name> {
    match pick {
        MetricPick::Measure(i) => schema.measures.get(i).map(|m| m.name.clone()),
        MetricPick::Metric(i) => schema.metrics.get(i).map(|m| m.name.clone()),
    }
}

fn build_query(
    schema: &Schema,
    rows: &AxisPick,
    columns: &AxisPick,
    metric: Option<MetricPick>,
) -> Option<Query> {
    let axes = build_axes(schema, rows, columns)?;
    let metric = metric.and_then(|p| metric_name(schema, p))?;
    Some(Query {
        axes,
        slicer: Tuple::empty(),
        metrics: vec![metric],
        options: query::Options::default(),
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

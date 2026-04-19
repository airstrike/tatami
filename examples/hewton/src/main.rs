//! # Hewton — the TARGET API for tatami v0.1.
//!
//! **This file is frozen.** Every call it makes is a promise the tatami
//! library must keep. If a call here feels awkward, the fix is to change
//! the *library*, not hewton.
//!
//! Hewton is a hotel-sales cube (see `schema.rs` and `facts.rs`). The
//! composer UI itself is **schema-blind**: it picks rows / columns /
//! metric by *index* into the introspected `Schema`, never by hard-coded
//! field names. Point the same binary at a different cube and the
//! pickers repopulate themselves.
//!
//! ## Composer layout
//!
//! Sidebar leaves under [`composer`] — [`axis`](composer::axis) (two
//! instances, Rows and Columns), [`metric`](composer::metric),
//! [`top_n`](composer::top_n), [`filter`](composer::filter),
//! [`slicer`](composer::slicer) — each own a `State` / `Message` /
//! `update` / `view`. This file routes `Message::Rows(axis::Message)` /
//! etc. through `.map(Message::Leaf)`.

use std::num::NonZeroUsize;
use std::sync::Arc;

use iced::widget::{button, center, column, row, scrollable, text};
use iced::{Element, Font, Length, Task, Theme};

use polars_core::prelude::DataFrame;
use tatami::query::{self, MemberRef, Set, Tuple};
use tatami::schema::{Name, Schema};
use tatami::{Axes, Cube, Query, Results};
use tatami_inmem::InMemoryCube;

mod action;
mod composer;
mod facts;
mod icon;
mod schema;
mod theme;
mod widgets;

use composer::{axis, dim, filter, metric, slicer, top_n};
use theme::constants::{CONTROL_HEIGHT, ICON_BUTTON_PADDING, ICON_SIZE, TEXT_SIZE};

fn main() -> iced::Result {
    iced::application(App::new, App::update, App::view)
        .theme(Theme::Oxocarbon)
        .default_font(Font::new("Inter"))
        .font(icon::FONT)
        .window_size((1200.0, 800.0))
        .title("Hewton — tatami v0.1 worked example")
        .run()
}

struct App {
    cube: Option<Arc<InMemoryCube>>,
    schema_ref: Option<Schema>,
    load_error: Option<String>,

    rows: axis::State,
    columns: axis::State,
    metric: metric::State,
    top_n: top_n::State,
    filter: filter::State,
    slicer: slicer::State,

    /// Pre-drill snapshots. `DrillInto` pushes, `Back` pops-and-restores.
    trail: Vec<ComposerSnapshot>,

    result: QueryState,
}

#[derive(Clone)]
struct ComposerSnapshot {
    rows: axis::State,
    columns: axis::State,
    metric: metric::State,
    top_n: top_n::State,
    filter: filter::State,
    slicer: slicer::State,
}

#[non_exhaustive]
pub(crate) enum QueryState {
    Idle,
    Running,
    Ok(Results),
    Err(String),
}

#[derive(Debug, Clone)]
pub(crate) enum Message {
    FactsLoaded(Result<DataFrame, String>),
    SchemaReady(Schema),
    QueryDone(Result<Results, String>),
    FontLoaded(&'static str, Result<(), String>),

    Rows(axis::Message),
    Columns(axis::Message),
    Metric(metric::Message),
    TopN(top_n::Message),
    Filter(filter::Message),
    Slicer(slicer::Message),

    /// Pivot row header clicked — pin the member in the slicer and
    /// drill the rows axis one level deeper.
    DrillInto(MemberRef),
    Back,
}

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
            rows: axis::State::default(),
            columns: axis::State::default(),
            metric: metric::State::default(),
            top_n: top_n::State::default(),
            filter: filter::State::default(),
            slicer: slicer::State::default(),
            trail: Vec::new(),
            result: QueryState::Idle,
        };

        (app, init)
    }

    fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::FactsLoaded(Ok(df)) => {
                // `schema::hewton_schema` is the only Hewton-specific
                // touchpoint in the app path; the UI downstream reads
                // only the `Schema` returned by `cube.schema().await`.
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
                let slicer_tasks: Vec<Task<Message>> = match self.cube.as_ref() {
                    Some(cube) => self
                        .slicer
                        .load_options(cube, &schema)
                        .into_iter()
                        .map(|t| t.map(Message::Slicer))
                        .collect(),
                    None => Vec::new(),
                };
                self.schema_ref = Some(schema);
                self.metric.seed_if_empty();
                let query_task = self.run_query();
                Task::batch(std::iter::once(query_task).chain(slicer_tasks))
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
            Message::Rows(msg) => {
                let Some(schema) = self.schema_ref.as_ref() else {
                    return Task::none();
                };
                let prev = self.rows.pick;
                self.rows.update(msg, schema);
                // A dim is on an axis or in the slicer, never both.
                if self.rows.pick != prev {
                    self.slicer.prune(&self.rows.pick, &self.columns.pick);
                }
                self.run_query()
            }
            Message::Columns(msg) => {
                let Some(schema) = self.schema_ref.as_ref() else {
                    return Task::none();
                };
                let prev = self.columns.pick;
                self.columns.update(msg, schema);
                if self.columns.pick != prev {
                    self.slicer.prune(&self.rows.pick, &self.columns.pick);
                }
                self.run_query()
            }
            Message::Metric(msg) => {
                self.metric.update(msg);
                self.run_query()
            }
            Message::TopN(msg) => {
                self.top_n.update(msg);
                self.run_query()
            }
            Message::Filter(msg) => {
                self.filter.update(msg);
                self.run_query()
            }
            Message::Slicer(msg) => {
                self.slicer.update(msg);
                self.run_query()
            }
            Message::DrillInto(member) => self.drill(member),
            Message::Back => {
                if let Some(snap) = self.trail.pop() {
                    self.restore(snap);
                    return self.run_query();
                }
                Task::none()
            }
        }
    }

    fn view(&self) -> Element<'_, Message> {
        if let Some(error) = &self.load_error {
            return center(text(format!("Error: {error}")).size(TEXT_SIZE)).into();
        }
        let Some(schema) = self.schema_ref.as_ref() else {
            return center(text("Loading hewton facts\u{2026}").size(TEXT_SIZE)).into();
        };

        let metric_options = metric::options(schema);

        let rows_view = axis::view(&self.rows, schema, "Rows").map(Message::Rows);
        let columns_view = axis::view(&self.columns, schema, "Columns").map(Message::Columns);
        let metric_view = metric::view(&self.metric, &metric_options).map(Message::Metric);
        let top_n_view =
            top_n::view(&self.top_n, &self.rows.pick, &metric_options).map(Message::TopN);
        let filter_view = filter::view(&self.filter, &metric_options).map(Message::Filter);
        let slicer_view = slicer::view(&self.slicer, schema).map(Message::Slicer);

        let sidebar_body: Element<'_, Message> = column![
            rows_view,
            columns_view,
            metric_view,
            top_n_view,
            filter_view,
            slicer_view
        ]
        .spacing(10)
        .width(Length::Fixed(240.0))
        .into();

        let sidebar_column: Element<'_, Message> = if self.trail.is_empty() {
            sidebar_body
        } else {
            column![back_button(), sidebar_body].spacing(10).into()
        };

        row![
            scrollable(sidebar_column)
                .width(Length::Fixed(280.0))
                .height(Length::Fill),
            widgets::result_panel(&self.result),
        ]
        .spacing(16)
        .padding(16)
        .into()
    }

    /// Rebuild the current `Query` and spawn it. Falls back to
    /// [`QueryState::Idle`] when no valid query can be assembled.
    fn run_query(&mut self) -> Task<Message> {
        let Some(schema) = self.schema_ref.as_ref() else {
            return Task::none();
        };
        let Some(cube) = self.cube.clone() else {
            return Task::none();
        };
        let Some(query) = build_query(
            schema,
            &self.rows.pick,
            &self.columns.pick,
            &self.metric.slots,
            &self.slicer.pins,
            self.top_n.by,
            self.filter.pick.as_ref(),
        ) else {
            self.result = QueryState::Idle;
            return Task::none();
        };
        self.result = QueryState::Running;
        Task::perform(
            async move { cube.query(&query).await.map_err(|e| e.to_string()) },
            Message::QueryDone,
        )
    }

    fn snapshot(&self) -> ComposerSnapshot {
        ComposerSnapshot {
            rows: self.rows.clone(),
            columns: self.columns.clone(),
            metric: self.metric.clone(),
            top_n: self.top_n,
            filter: self.filter.clone(),
            slicer: self.slicer.clone(),
        }
    }

    fn restore(&mut self, snap: ComposerSnapshot) {
        self.rows = snap.rows;
        self.columns = snap.columns;
        self.metric = snap.metric;
        self.top_n = snap.top_n;
        self.filter = snap.filter;
        self.slicer = snap.slicer;
    }

    /// Push snapshot, pin the clicked member in the slicer, advance
    /// the rows axis one level. Pinning deliberately overlaps with
    /// the rows dim — the slicer + advanced rows level together
    /// express "children of this member".
    ///
    /// No-op when the axis can't advance; in that case no snapshot
    /// is pushed, so Back wouldn't restore to the current state.
    fn drill(&mut self, member: MemberRef) -> Task<Message> {
        let Some(schema) = self.schema_ref.as_ref() else {
            return Task::none();
        };
        let Some(dim_index) = dim::index_for(&member.dim, schema) else {
            return Task::none();
        };
        let snap = self.snapshot();
        if !self.rows.drill(&member, schema) {
            return Task::none();
        }
        self.trail.push(snap);
        self.slicer.pin(dim_index, member);
        self.run_query()
    }
}

fn back_button<'a>() -> Element<'a, Message> {
    button(icon::chevron_left().size(ICON_SIZE).line_height(1.0))
        .padding(ICON_BUTTON_PADDING)
        .height(Length::Fixed(CONTROL_HEIGHT))
        .on_press(Message::Back)
        .into()
}

/// Infer an [`Axes`] shape from the two axis picks. `(None, Set)` is
/// invalid — columns without rows returns `None`.
fn build_axes(schema: &Schema, rows: &axis::Pick, columns: &axis::Pick) -> Option<Axes> {
    match (rows, columns) {
        (axis::Pick::None, axis::Pick::None) => Some(Axes::Scalar),
        (axis::Pick::Set { .. }, axis::Pick::None) => Some(Axes::Series {
            rows: build_set(schema, rows)?,
        }),
        (axis::Pick::Set { .. }, axis::Pick::Set { .. }) => Some(Axes::Pivot {
            rows: build_set(schema, rows)?,
            columns: build_set(schema, columns)?,
        }),
        (axis::Pick::None, axis::Pick::Set { .. }) => None,
    }
}

fn build_set(schema: &Schema, pick: &axis::Pick) -> Option<Set> {
    let axis::Pick::Set {
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

fn build_query(
    schema: &Schema,
    rows: &axis::Pick,
    columns: &axis::Pick,
    metrics: &[Option<metric::Pick>],
    slicer_pins: &std::collections::HashMap<usize, MemberRef>,
    top_n_by: Option<metric::Pick>,
    filter_pick: Option<&filter::Pick>,
) -> Option<Query> {
    if metrics.is_empty() || metrics.iter().any(Option::is_none) {
        return None;
    }
    let metric_names: Vec<Name> = metrics
        .iter()
        .flatten()
        .map(|pick| metric::name(schema, *pick))
        .collect::<Option<Vec<_>>>()?;

    let mut axes = build_axes(schema, rows, columns)?;

    // Filter before Top-N so Top-N ranks the surviving rows — the
    // natural reading of "top 10 of the rows where revenue > 1M".
    if let Some(f) = filter_pick {
        let pred = filter::build_predicate(schema, f)?;
        axes = match axes {
            Axes::Series { rows } => Axes::Series {
                rows: rows.filter(pred),
            },
            Axes::Pivot { rows, columns } => Axes::Pivot {
                rows: rows.filter(pred),
                columns,
            },
            other => other,
        };
    }

    // Top-N is inert without a rows axis; Scalar / Pages pass through.
    if let Some(pick) = top_n_by {
        let by_name = metric::name(schema, pick)?;
        let n = NonZeroUsize::new(10).expect("10 is non-zero");
        axes = match axes {
            Axes::Series { rows } => Axes::Series {
                rows: rows.top(n, by_name),
            },
            Axes::Pivot { rows, columns } => Axes::Pivot {
                rows: rows.top(n, by_name),
                columns,
            },
            other => other,
        };
    }

    // Pins are keyed by dim index (one entry per dim), so the
    // duplicate-dim check inside `Tuple::of` never fires.
    let slicer_tuple = Tuple::of(slicer_pins.values().cloned()).ok()?;
    Some(Query {
        axes,
        slicer: slicer_tuple,
        metrics: metric_names,
        options: query::Options::default(),
    })
}

/// Fetch a Google Fonts family via `fount`, then register every
/// variant's bytes with iced. Folds every outcome into a single
/// `FontLoaded` Message — the first failure wins, success is an empty Ok.
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

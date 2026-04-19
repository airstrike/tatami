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

use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

use iced::widget::{Column, button, center, column, pick_list, row, scrollable, text, text_input};
use iced::{Alignment, Element, Font, Length, Task, Theme, font};

use polars_core::prelude::DataFrame;
use tatami::query::{self, MemberRef, Predicate, Set, Tuple};
use tatami::schema::{Name, Schema};
use tatami::{Axes, Cube, Query, Results};
use tatami_inmem::InMemoryCube;

mod facts;
mod icon;
mod schema;
mod theme;
mod widgets;

use theme::{HEADING_SIZE, ICON_BUTTON_PADDING, ICON_SIZE, PICKER_PADDING, PICKER_SIZE, TEXT_SIZE};

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
        .font(icon::FONT)
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

    /// Zero or more metric slots. Each slot is independently `None`
    /// (visible empty picker) or `Some(pick)`. The query fires only once
    /// every slot is filled; empty slots block it. We start with zero
    /// slots at boot and push one on [`Message::SchemaReady`] so the user
    /// lands on a visible picker without hunting for "+ Add metric".
    metrics: Vec<Option<MetricPick>>,

    /// When `Some`, [`build_query`] wraps the rows axis in
    /// `Set::top(rows, 10, by)`. N is hard-coded at 10 for v1 — a number
    /// input is a later iteration. The picker is inert whenever the rows
    /// axis is absent; the user will see the rows picker light up first.
    top_n_by: Option<MetricPick>,

    /// When all three filter fields are valid, [`build_query`] wraps the
    /// rows axis in `Set::filter(pred)` using [`build_predicate`]. Unlike
    /// Top-N, Filter makes sense on any axis shape, including Scalar —
    /// though this implementation only wraps the rows set when rows are
    /// present. `Predicate::In` / `NotIn` are deferred; only numeric
    /// Eq / Gt / Lt.
    filter: Option<FilterPick>,

    /// Scratch "kind" picker state. `None` means the filter is Off.
    /// Once all three scratch fields (`filter_kind`, `filter_by`, and a
    /// parseable `filter_value_text`) are populated, [`App::recompute_filter`]
    /// composes them into `self.filter` and the query wraps the rows axis.
    filter_kind: Option<FilterKind>,

    /// Scratch "by-metric" picker state — which metric the comparator
    /// reads. `None` means no metric is picked; the filter won't apply.
    filter_by: Option<MetricPick>,

    /// Scratch state for the filter-value `text_input` — the string the
    /// user is typing, before it parses to `f64`. Holding it out-of-band
    /// means a partial input like "12." mid-typing doesn't blow up the
    /// query: [`App::recompute_filter`] only materializes `self.filter`
    /// when `parse::<f64>()` succeeds, and the widget keeps showing
    /// whatever the user typed regardless.
    filter_value_text: String,

    /// Pinned members for dims *not currently on an axis*. Keyed by dim
    /// index into `schema.dimensions`. Entries are pruned when the user
    /// moves the same dim onto an axis — a dim can be "on rows" or "in the
    /// slicer", never both.
    slicer: HashMap<usize, MemberRef>,

    /// Cached per-dim top-level members. Populated after schema arrival
    /// by one [`InMemoryCube::level_members`] call per dim. Read-only
    /// after initial load; picker options come from this map.
    slicer_options: HashMap<usize, Vec<MemberRef>>,

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

/// Three-way numeric predicate picker. `Predicate::In` / `NotIn` are
/// deferred to a later iteration — picking a `Path` prefix is its own
/// UI project (a Path picker with hierarchy-aware drill-down).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
enum FilterKind {
    /// `metric == value`.
    #[default]
    Eq,
    /// `metric > value`.
    Gt,
    /// `metric < value`.
    Lt,
}

impl fmt::Display for FilterKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            FilterKind::Eq => "=",
            FilterKind::Gt => ">",
            FilterKind::Lt => "<",
        })
    }
}

/// A numeric filter on the rows axis. When present in [`App::filter`],
/// `build_query` wraps the rows set in `Set::filter(pred)` using
/// [`build_predicate`] to translate into a [`Predicate`].
#[derive(Clone, Copy, Debug, PartialEq)]
struct FilterPick {
    /// Which comparator to apply.
    kind: FilterKind,
    /// Metric whose value the comparator reads.
    by: MetricPick,
    /// Right-hand side of the comparator.
    value: f64,
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
    /// A metric slot picker changed. `slot` is the slot's index into
    /// `App.metrics`; `pick` is `None` when the user clears the slot.
    MetricSlotPicked {
        slot: usize,
        pick: Option<MetricChoice>,
    },
    /// "+ Add metric" clicked — appends an empty slot to `App.metrics`.
    MetricSlotAdded,
    /// "×" clicked on a metric slot — removes the slot at this index.
    MetricSlotRemoved(usize),
    /// Top-N by-metric picker changed. `None` turns Top-N off.
    TopNByPicked(Option<MetricChoice>),
    /// Filter kind picker changed. `None` turns the filter off — the
    /// other filter controls get disabled / cleared to match.
    FilterKindPicked(Option<FilterKind>),
    /// Filter by-metric picker changed. `None` clears the by-metric —
    /// the filter stays kind-picked but won't apply until a metric is
    /// picked again.
    FilterByPicked(Option<MetricChoice>),
    /// Raw keystroke from the filter-value `text_input`. Stored in
    /// `filter_value_text`; parsed to `f64` on every change to update
    /// `filter.value`. Invalid / partial input leaves the last valid
    /// value in place so the query doesn't thrash while typing.
    FilterValueChanged(String),
    /// Per-dim top-level members loaded via
    /// [`InMemoryCube::level_members`]. `usize` is the dim's index into
    /// `schema.dimensions`; on error the string is the backend message.
    SlicerMembersLoaded(usize, Result<Vec<MemberRef>, String>),
    /// User picked (or cleared) the slicer pin for a dim. `None` means
    /// "unpin this dim".
    SlicerPicked(usize, Option<SlicerChoice>),
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

/// A filter-kind option in a pick_list — wraps [`FilterKind`] with an
/// explicit `Off` entry so "turn the filter off" is a visible option in
/// the list, not a "clear" button on the side.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FilterKindChoice {
    Off,
    On(FilterKind),
}

impl fmt::Display for FilterKindChoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterKindChoice::Off => f.write_str("(off)"),
            FilterKindChoice::On(k) => write!(f, "{k}"),
        }
    }
}

/// A slicer option in a pick_list — a single top-level member of a dim
/// *not currently on an axis*. Pinning seeds `Query.slicer` with this
/// member; unpinning removes the dim from the slicer tuple.
#[derive(Clone, Debug, PartialEq, Eq)]
struct SlicerChoice {
    member: MemberRef,
    label: String,
}

impl fmt::Display for SlicerChoice {
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
            // No slots at boot — the schema isn't loaded yet, so a picker
            // would have nothing to show. One empty slot is pushed on
            // `SchemaReady`.
            metrics: Vec::new(),
            top_n_by: None,
            filter: None,
            filter_kind: None,
            filter_by: None,
            filter_value_text: String::new(),
            slicer: HashMap::new(),
            slicer_options: HashMap::new(),
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
                // Kick off one `level_members` call per dim — populates
                // `slicer_options` so the slicer pickers can render
                // synchronously from cached data.
                let tasks = load_slicer_options(self.cube.as_ref(), &schema);
                self.schema_ref = Some(schema);
                // Seed one empty metric slot so the user sees a picker
                // without hunting for "+ Add metric".
                if self.metrics.is_empty() {
                    self.metrics.push(None);
                }
                let query_task = self.run_query();
                Task::batch(std::iter::once(query_task).chain(tasks))
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
                self.prune_slicer();
                self.run_query()
            }
            Message::RowsLevelPicked(choice) => {
                self.rows = level_for(&self.rows, choice);
                self.run_query()
            }
            Message::ColumnsDimPicked(choice) => {
                self.columns = axis_for(self.schema_ref.as_ref(), choice);
                self.prune_slicer();
                self.run_query()
            }
            Message::ColumnsLevelPicked(choice) => {
                self.columns = level_for(&self.columns, choice);
                self.run_query()
            }
            Message::MetricSlotPicked { slot, pick } => {
                if let Some(entry) = self.metrics.get_mut(slot) {
                    *entry = pick.map(|c| c.pick);
                }
                self.run_query()
            }
            Message::MetricSlotAdded => {
                self.metrics.push(None);
                // Empty slot blocks the query — `run_query` goes Idle.
                self.run_query()
            }
            Message::MetricSlotRemoved(slot) => {
                if slot < self.metrics.len() {
                    self.metrics.remove(slot);
                }
                self.run_query()
            }
            Message::TopNByPicked(choice) => {
                self.top_n_by = choice.map(|c| c.pick);
                self.run_query()
            }
            Message::FilterKindPicked(kind) => {
                self.filter_kind = kind;
                if kind.is_none() {
                    // Off: also clear the by/value scratch so re-enabling
                    // the filter starts fresh rather than snapping to a
                    // surprise state.
                    self.filter_by = None;
                    self.filter_value_text.clear();
                }
                self.recompute_filter();
                self.run_query()
            }
            Message::FilterByPicked(choice) => {
                self.filter_by = choice.map(|c| c.pick);
                self.recompute_filter();
                self.run_query()
            }
            Message::FilterValueChanged(raw) => {
                self.filter_value_text = raw;
                self.recompute_filter();
                self.run_query()
            }
            Message::SlicerMembersLoaded(dim_index, Ok(members)) => {
                self.slicer_options.insert(dim_index, members);
                Task::none()
            }
            Message::SlicerMembersLoaded(_dim_index, Err(error)) => {
                // Failing to load one dim's members is non-fatal — the
                // picker for that dim stays in its loading state and the
                // rest of the UI works.
                eprintln!("slicer members load failed: {error}");
                Task::none()
            }
            Message::SlicerPicked(dim_index, choice) => {
                match choice {
                    Some(c) => {
                        self.slicer.insert(dim_index, c.member);
                    }
                    None => {
                        self.slicer.remove(&dim_index);
                    }
                }
                self.run_query()
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

        // Each half owns its own scroll — the sidebar can scroll when it
        // overflows vertically without also pushing the result panel up
        // or down. The result panel fills the remaining space so it
        // doesn't collapse to the height of a "Running…" label.
        row![
            scrollable(sidebar(
                schema,
                &self.rows,
                &self.columns,
                &self.metrics,
                self.top_n_by,
                self.filter_kind,
                self.filter_by,
                &self.filter_value_text,
                &self.slicer,
                &self.slicer_options,
            ))
            .width(Length::Fixed(280.0))
            .height(Length::Fill),
            widgets::result_panel(&self.result),
        ]
        .spacing(16)
        .padding(16)
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
        let Some(query) = build_query(
            schema,
            &self.rows,
            &self.columns,
            &self.metrics,
            &self.slicer,
            self.top_n_by,
            self.filter.as_ref(),
        ) else {
            self.result = QueryState::Idle;
            return Task::none();
        };
        self.result = QueryState::Running;
        Task::future(async move {
            let outcome = cube.query(&query).await.map_err(|e| e.to_string());
            Message::QueryDone(outcome)
        })
    }

    /// Drop slicer entries for dims that are now on rows or columns — a
    /// dim can be "on an axis" or "in the slicer", never both.
    fn prune_slicer(&mut self) {
        let on_axis = axis_dim_set(&self.rows, &self.columns);
        self.slicer
            .retain(|dim_index, _| !on_axis.contains(dim_index));
    }

    /// Materialize `self.filter` from the three scratch fields. Only
    /// produces `Some` when kind is On, by-metric is picked, and the
    /// value text parses to `f64`. Otherwise leaves `self.filter = None`
    /// so `build_query` skips the filter wrap.
    fn recompute_filter(&mut self) {
        let value = match self.filter_value_text.parse::<f64>() {
            Ok(v) => v,
            Err(_) => {
                self.filter = None;
                return;
            }
        };
        self.filter = match (self.filter_kind, self.filter_by) {
            (Some(kind), Some(by)) => Some(FilterPick { kind, by, value }),
            _ => None,
        };
    }
}

// ── Sidebar pickers ────────────────────────────────────────────────────────

/// Build the left-hand sidebar of pickers. Every option is derived from
/// the introspected [`Schema`] — there are no string literals referring to
/// specific dims, levels, or metrics in this function.
#[allow(clippy::too_many_arguments)]
fn sidebar<'a>(
    schema: &'a Schema,
    rows: &AxisPick,
    columns: &AxisPick,
    metrics: &[Option<MetricPick>],
    top_n_by: Option<MetricPick>,
    filter_kind: Option<FilterKind>,
    filter_by: Option<MetricPick>,
    filter_value_text: &str,
    slicer: &HashMap<usize, MemberRef>,
    slicer_options: &HashMap<usize, Vec<MemberRef>>,
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

    let metric_block = metric_panel(metrics, &metric_options);
    let top_n_block = top_n_panel(rows, top_n_by, &metric_options);
    let filter_block = filter_panel(filter_kind, filter_by, filter_value_text, &metric_options);

    let slicer_block = slicer_panel(schema, rows, columns, slicer, slicer_options);

    column![
        rows_block,
        columns_block,
        metric_block,
        top_n_block,
        filter_block,
        slicer_block
    ]
    .spacing(10)
    .width(Length::Fixed(240.0))
    .into()
}

// ── Styling helpers ────────────────────────────────────────────────────────

/// Section heading — small-caps, muted, one notch above body size. Used
/// at the top of every sidebar section to separate pickers visually
/// without adding chrome.
fn heading<'a, M: 'a>(label: &str) -> Element<'a, M> {
    text(label.to_uppercase())
        .size(HEADING_SIZE)
        .style(theme::muted)
        .into()
}

/// Muted hint — the low-contrast one-liner that explains why a section
/// is inert (filter off, no rows axis, slicer empty).
fn hint<'a, M: 'a>(label: &'a str) -> Element<'a, M> {
    text(label).size(TEXT_SIZE).style(theme::muted).into()
}

/// Inline label — fixed-width muted text, meant to sit to the left of a
/// picker so the label and its control share a single row.
const INLINE_LABEL_WIDTH: f32 = 64.0;

fn inline_label<'a, M: 'a>(label: impl Into<String>) -> Element<'a, M> {
    text(label.into())
        .size(TEXT_SIZE)
        .style(theme::muted)
        .width(Length::Fixed(INLINE_LABEL_WIDTH))
        .into()
}

/// Build the stacked Metric pickers — one pick_list per slot, a remove
/// icon button (lucide `x`) beside each, and an add icon button
/// (lucide `plus`) at the bottom.
fn metric_panel<'a>(
    metrics: &[Option<MetricPick>],
    metric_options: &[MetricChoice],
) -> Element<'a, Message> {
    let mut children: Vec<Element<'a, Message>> = vec![heading("Metric")];

    for (slot, entry) in metrics.iter().enumerate() {
        let selected =
            entry.and_then(|pick| metric_options.iter().find(|c| c.pick == pick).cloned());
        let options = metric_options.to_vec();
        let picker = pick_list(selected, options, |c: &MetricChoice| c.label.clone())
            .on_select(move |c: MetricChoice| Message::MetricSlotPicked {
                slot,
                pick: Some(c),
            })
            .placeholder("(pick a metric)")
            .text_size(PICKER_SIZE)
            .padding(PICKER_PADDING)
            .width(Length::Fill);
        let remove = button(icon::close().size(ICON_SIZE))
            .padding(ICON_BUTTON_PADDING)
            .on_press(Message::MetricSlotRemoved(slot));
        children.push(
            row![picker, remove]
                .spacing(4)
                .align_y(Alignment::Center)
                .into(),
        );
    }

    children.push(
        button(icon::plus().size(ICON_SIZE))
            .padding(ICON_BUTTON_PADDING)
            .on_press(Message::MetricSlotAdded)
            .into(),
    );

    Column::with_children(children).spacing(4).into()
}

/// Build the Top-N picker. Disabled when the rows axis is absent — Top-N
/// ranks the rows tuples, so there's nothing to filter without a rows
/// axis. N is hard-coded at 10 for v1; a number input is a later iteration.
fn top_n_panel<'a>(
    rows: &AxisPick,
    top_n_by: Option<MetricPick>,
    metric_options: &[MetricChoice],
) -> Element<'a, Message> {
    let rows_present = matches!(rows, AxisPick::Pick { .. });

    if !rows_present {
        return column![heading("Top-N"), hint("(pick a rows axis first)")]
            .spacing(4)
            .into();
    }

    let options = metric_options.to_vec();
    let selected =
        top_n_by.and_then(|pick| metric_options.iter().find(|c| c.pick == pick).cloned());

    let picker = pick_list(selected, options, |c: &MetricChoice| c.label.clone())
        .on_select(|c: MetricChoice| Message::TopNByPicked(Some(c)))
        .placeholder("(off)")
        .text_size(PICKER_SIZE)
        .padding(PICKER_PADDING)
        .width(Length::Fill);

    // Clear-pin control — visible only when Top-N is on, so the common
    // "off" case has one less widget on screen.
    let clear: Element<'_, Message> = if top_n_by.is_some() {
        button(icon::close().size(ICON_SIZE))
            .padding(ICON_BUTTON_PADDING)
            .on_press(Message::TopNByPicked(None))
            .into()
    } else {
        text("").into()
    };

    column![
        heading("Top-N"),
        hint("Top 10 by\u{2026}"),
        row![picker, clear].spacing(4).align_y(Alignment::Center),
    ]
    .spacing(4)
    .into()
}

/// Build the Filter panel. Three stacked controls: a kind picker
/// (Off / = / > / <), a by-metric picker, and a `text_input` for the
/// numeric threshold. When kind is Off, the by / value controls collapse
/// to a hint line so the common "filter not in use" case stays quiet.
/// `Predicate::In` / `NotIn` are deferred until a Path picker lands.
fn filter_panel<'a>(
    filter_kind: Option<FilterKind>,
    filter_by: Option<MetricPick>,
    filter_value_text: &str,
    metric_options: &[MetricChoice],
) -> Element<'a, Message> {
    let kind_options = vec![
        FilterKindChoice::Off,
        FilterKindChoice::On(FilterKind::Eq),
        FilterKindChoice::On(FilterKind::Gt),
        FilterKindChoice::On(FilterKind::Lt),
    ];
    let selected_kind = match filter_kind {
        None => Some(FilterKindChoice::Off),
        Some(k) => Some(FilterKindChoice::On(k)),
    };
    let kind_picker = pick_list(selected_kind, kind_options, |c: &FilterKindChoice| {
        c.to_string()
    })
    .on_select(|c: FilterKindChoice| {
        Message::FilterKindPicked(match c {
            FilterKindChoice::Off => None,
            FilterKindChoice::On(k) => Some(k),
        })
    })
    .text_size(PICKER_SIZE)
    .padding(PICKER_PADDING)
    .width(Length::Fill);

    if filter_kind.is_none() {
        return column![heading("Filter"), kind_picker, hint("(filter off)")]
            .spacing(4)
            .into();
    }

    let by_options = metric_options.to_vec();
    let selected_by =
        filter_by.and_then(|pick| metric_options.iter().find(|c| c.pick == pick).cloned());
    let by_picker = pick_list(selected_by, by_options, |c: &MetricChoice| c.label.clone())
        .on_select(|c: MetricChoice| Message::FilterByPicked(Some(c)))
        .placeholder("(pick a metric)")
        .text_size(PICKER_SIZE)
        .padding(PICKER_PADDING)
        .width(Length::Fill);

    let value_input = text_input("(value)", filter_value_text)
        .on_input(Message::FilterValueChanged)
        .size(PICKER_SIZE)
        .padding(PICKER_PADDING)
        .width(Length::Fill);

    // Hint line when the three inputs aren't all valid yet — makes it
    // obvious to the user why the query hasn't re-fired after typing.
    let ready = filter_by.is_some() && filter_value_text.parse::<f64>().is_ok();
    let status: Element<'_, Message> = if ready {
        text("").into()
    } else {
        hint("(pick metric + numeric value)")
    };

    column![
        heading("Filter"),
        kind_picker,
        by_picker,
        value_input,
        status
    ]
    .spacing(4)
    .into()
}

/// Build the slicer shelf — one picker per dim *not* currently on rows or
/// columns. Each picker lists that dim's top-level members (as cached in
/// `slicer_options`), with an "Unbound" entry to clear the pin.
fn slicer_panel<'a>(
    schema: &'a Schema,
    rows: &AxisPick,
    columns: &AxisPick,
    slicer: &HashMap<usize, MemberRef>,
    slicer_options: &HashMap<usize, Vec<MemberRef>>,
) -> Element<'a, Message> {
    let on_axis = axis_dim_set(rows, columns);

    let mut children: Vec<Element<'a, Message>> = vec![heading("Slicer")];
    let mut shown_any = false;
    for (dim_index, dim) in schema.dimensions.iter().enumerate() {
        if on_axis.contains(&dim_index) {
            continue;
        }
        shown_any = true;
        let picker = slicer_picker(dim_index, dim, slicer, slicer_options);
        children.push(picker);
    }
    if !shown_any {
        children.push(hint("(all dims on axes)"));
    }
    Column::with_children(children).spacing(6).into()
}

fn slicer_picker<'a>(
    dim_index: usize,
    dim: &'a tatami::schema::Dimension,
    slicer: &HashMap<usize, MemberRef>,
    slicer_options: &HashMap<usize, Vec<MemberRef>>,
) -> Element<'a, Message> {
    let label = dim.name.as_str().to_owned();

    let Some(members) = slicer_options.get(&dim_index) else {
        // Cached data not ready yet — inline hint alongside the label.
        return row![inline_label(label), hint("(loading\u{2026})")]
            .align_y(Alignment::Center)
            .spacing(6)
            .into();
    };

    let options: Vec<SlicerChoice> = members
        .iter()
        .map(|m| SlicerChoice {
            member: m.clone(),
            label: m.path.to_string(),
        })
        .collect();

    let selected = slicer
        .get(&dim_index)
        .and_then(|pinned| options.iter().find(|c| c.member == *pinned).cloned());

    let picker = pick_list(selected, options, |c: &SlicerChoice| c.label.clone())
        .on_select(move |c: SlicerChoice| Message::SlicerPicked(dim_index, Some(c)))
        .placeholder("(unbound)")
        .text_size(PICKER_SIZE)
        .padding(PICKER_PADDING)
        .width(Length::Fill);

    // Clear-pin control — visible only when something is pinned so the
    // common case of "no pin" has one less widget on screen.
    let clear: Element<'a, Message> = if slicer.contains_key(&dim_index) {
        button(icon::close().size(ICON_SIZE))
            .padding(ICON_BUTTON_PADDING)
            .on_press(Message::SlicerPicked(dim_index, None))
            .into()
    } else {
        text("").into()
    };

    row![inline_label(label), picker, clear]
        .align_y(Alignment::Center)
        .spacing(4)
        .into()
}

/// Set of dim indices currently occupied by an axis pick.
fn axis_dim_set(rows: &AxisPick, columns: &AxisPick) -> std::collections::HashSet<usize> {
    let mut set = std::collections::HashSet::new();
    if let AxisPick::Pick { dim, .. } = *rows {
        set.insert(dim);
    }
    if let AxisPick::Pick { dim, .. } = *columns {
        set.insert(dim);
    }
    set
}

#[allow(clippy::too_many_arguments)]
fn axis_picker<'a>(
    label: &'static str,
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
        .text_size(PICKER_SIZE)
        .padding(PICKER_PADDING)
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
                .text_size(PICKER_SIZE)
                .padding(PICKER_PADDING)
                .width(Length::Fill)
                .into()
        }
        AxisPick::None => text("").into(),
    };

    // Label + dim picker (+ level picker when a dim is chosen) all on one
    // row. When no dim is picked, the level slot is simply absent; when
    // one is, dim and level split the remaining width evenly.
    let body: Element<'a, Message> = match *pick {
        AxisPick::Pick { .. } => row![dim_list, level_element].spacing(4).into(),
        AxisPick::None => dim_list.into(),
    };

    row![inline_label(label), body]
        .align_y(Alignment::Center)
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

/// Translate a [`FilterPick`] into a [`Predicate`]. Only the numeric
/// comparators (Eq / Gt / Lt) are in scope for v1 — the `Predicate::In` /
/// `NotIn` variants need a `Path` picker UI which is its own project.
fn build_predicate(schema: &Schema, filter: &FilterPick) -> Option<Predicate> {
    let metric = metric_name(schema, filter.by)?;
    Some(match filter.kind {
        FilterKind::Eq => Predicate::Eq {
            metric,
            value: filter.value,
        },
        FilterKind::Gt => Predicate::Gt {
            metric,
            value: filter.value,
        },
        FilterKind::Lt => Predicate::Lt {
            metric,
            value: filter.value,
        },
    })
}

fn build_query(
    schema: &Schema,
    rows: &AxisPick,
    columns: &AxisPick,
    metrics: &[Option<MetricPick>],
    slicer: &HashMap<usize, MemberRef>,
    top_n_by: Option<MetricPick>,
    filter: Option<&FilterPick>,
) -> Option<Query> {
    // Any empty slot blocks the query — including the single seeded slot
    // at boot, so the panel shows the Idle placeholder until the user
    // picks something.
    if metrics.is_empty() || metrics.iter().any(Option::is_none) {
        return None;
    }
    let metric_names: Vec<Name> = metrics
        .iter()
        .flatten()
        .map(|pick| metric_name(schema, *pick))
        .collect::<Option<Vec<_>>>()?;

    let mut axes = build_axes(schema, rows, columns)?;

    // Wrap the rows axis in `Set::filter` *before* Top-N so the natural
    // reading order holds: "of rows where revenue > 1M, take the top 10
    // by revenue". Applying filter first also keeps the rank stable
    // under filter changes — Top-N ranks what's left. Filter is
    // Predicate::Eq/Gt/Lt only; Predicate::In / NotIn are deferred until
    // a Path picker lands.
    if let Some(f) = filter {
        let pred = build_predicate(schema, f)?;
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

    // Wrap the rows axis in `Set::top` when the user has enabled Top-N
    // and a rows axis exists. Scalar / Pages have no rows to top; the
    // Top-N panel is disabled when rows is None, so this arm is
    // defensive only. N is hard-coded at 10 for v1 — a number input is a
    // later iteration.
    if let Some(pick) = top_n_by {
        let by_name = metric_name(schema, pick)?;
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

    // `Tuple::of` rejects duplicate-dim inputs; we never add two members
    // for the same dim (the key is the dim's index into `schema.dimensions`,
    // one entry per dim), so `.ok()` is sound here.
    let slicer_tuple = Tuple::of(slicer.values().cloned()).ok()?;
    Some(Query {
        axes,
        slicer: slicer_tuple,
        metrics: metric_names,
        options: query::Options::default(),
    })
}

/// Spawn one [`InMemoryCube::level_members`] call per dim — the top-level
/// members of each `(dim, hierarchies[0], levels[0])` triple are what the
/// slicer pickers show. Returns one [`Task`] per dim with a schema +
/// cube; empty if the cube isn't constructed yet or the schema has no
/// dims.
fn load_slicer_options(cube: Option<&Arc<InMemoryCube>>, schema: &Schema) -> Vec<Task<Message>> {
    let Some(cube) = cube else {
        return Vec::new();
    };
    let mut tasks = Vec::new();
    for (dim_index, dim) in schema.dimensions.iter().enumerate() {
        let Some(hierarchy) = dim.hierarchies.first() else {
            continue;
        };
        let Some(level) = hierarchy.levels.first() else {
            continue;
        };
        let dim_name = dim.name.clone();
        let hierarchy_name = hierarchy.name.clone();
        let level_name = level.name.clone();
        let cube = cube.clone();
        tasks.push(Task::future(async move {
            let outcome = cube
                .level_members(&dim_name, &hierarchy_name, &level_name)
                .map_err(|e| e.to_string());
            Message::SlicerMembersLoaded(dim_index, outcome)
        }));
    }
    tasks
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

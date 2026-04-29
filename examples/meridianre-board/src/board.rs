//! Board orchestrator — `State`, `Message`, `update`, `view`.
//!
//! The board boots an in-process `tatami-serve` instance ([`crate::server::Boot`]),
//! talks to it through a [`tatami_http::Remote`], and exposes a
//! schema-blind composer ([`crate::composer`]) for picking
//! `(measure, dim, hierarchy)` and drilling down via row clicks.
//!
//! ## Phases
//!
//! `App::new` returns synchronously with a `Phase::Loading` placeholder —
//! the cube boot is async and we can't construct a [`tatami_http::Remote`]
//! before runway has bound a port. A single boot task fires on startup
//! and resolves into `Phase::Ready` carrying the schema and the result
//! of an initial query built from `composer::defaults`.
//!
//! ## Drill model
//!
//! - **focus stack**: clicking a member name pushes it onto
//!   [`Ready::focus`] and re-fires the query with the rows axis
//!   replaced by `Set::children(Set::from_member(member))`. An empty
//!   focus draws the top level of the chosen hierarchy.
//! - **slicer trail**: previous-version drill chips persist as a list
//!   of `(dim, member)` pins. The composer doesn't add to the trail in
//!   this revision — the filter-add UI is deferred (see commit body).
//! - **breadcrumb**: a `Top` button resets the focus to empty; a `× Up`
//!   button pops the topmost focus entry. Per-chip truncation is
//!   deferred.

use std::sync::{Arc, Mutex};

use iced::widget::{column, container, scrollable, text};
use iced::{Element, Length, Task};

use tatami::Cube;
use tatami::query::{MemberRef, Set, Tuple};
use tatami::schema::{self, Schema};
use tatami::{Axes, Query, Results, query};

use crate::composer;
use crate::server::Boot;

/// One-shot carrier for non-Clone boot artefacts. iced messages must be
/// `Clone`; [`Boot`] holds a `tokio::task::JoinHandle` and isn't. The
/// `Mutex<Option<_>>` lets the update handler take the bundle out
/// exactly once when the boot message arrives.
type BootCarrier = Arc<Mutex<Option<BootBundle>>>;

/// Top-level board phase — loading until the server is up, ready once
/// the schema and initial query have come back.
///
/// The `Ready` payload is large (it owns a runway server handle, an
/// HTTP client, the schema, and the latest results), so it lives behind
/// a `Box` to keep the discriminant cheap on the iced Task path.
#[non_exhaustive]
pub enum Phase {
    /// Waiting for `tatami-serve` to bind, the schema to come back, and
    /// the initial query to evaluate.
    Loading {
        /// Human-readable error if any boot step has failed; rendered
        /// instead of the spinner.
        error: Option<String>,
    },
    /// Boot complete. The server, cube remote, and active query state
    /// are all live.
    Ready(Box<Ready>),
}

/// Live runtime state — only constructed once the server is up.
pub struct Ready {
    /// In-process tatami-serve handle. Held so the runway server stays
    /// alive as long as `Ready` does — dropping the [`Boot`] closes the
    /// shutdown channel and stops the accept loop.
    #[allow(dead_code, reason = "kept alive for the embedded server's lifetime")]
    pub boot: Boot,
    /// HTTP-side cube client pointing at `boot.base_url`.
    pub remote: Arc<tatami_http::Remote>,
    /// The schema fetched at startup. The composer drives every picker
    /// off this single source of truth — no string literals
    /// originating in this binary cross the `Name` boundary.
    pub schema: Arc<Schema>,

    /// Currently-selected measure / metric. The composer's first
    /// option (first `schema.measures`, falling through to first
    /// `schema.metrics`) is the default.
    pub measure: Option<schema::Name>,
    /// Currently-selected dimension. Schema-first dim is default.
    pub dim: Option<schema::Name>,
    /// Currently-selected hierarchy under [`Self::dim`]. The dim's
    /// first hierarchy is default; resets on a dim change.
    pub hierarchy: Option<schema::Name>,
    /// Drill-into stack — empty means "rows axis is the top level of
    /// the chosen hierarchy". Each entry is a member the user clicked.
    pub focus: Vec<MemberRef>,
    /// Slicer trail — one entry per dim. Newer drills on the same dim
    /// replace the older entry rather than appending. Kept for
    /// continuity with the v1 board; the filter-add UI that would
    /// populate this is deferred.
    pub slicer: Vec<(schema::Name, MemberRef)>,
    /// Latest query result (or its error / loading state).
    pub results: ResultsState,
}

/// Result lifecycle for the active composition. `Idle` is the
/// degenerate-schema fallback (zero measures, zero dims, or zero
/// hierarchies on the chosen dim) — the composition can't be assembled
/// and no query gets fired.
#[non_exhaustive]
pub enum ResultsState {
    /// The composition is incomplete — `Idle` is the rest state when
    /// the schema lacks something we need.
    Idle,
    /// A query is in flight.
    Loading,
    /// The latest query returned `Ok` and we have a `Results` to render.
    Ready(Results),
    /// The latest query failed; rendered as an inline error line.
    Error(String),
    /// The drill landed on a leaf — children of the focused member
    /// resolved to nothing. Rendered as a placeholder with no table.
    Leaf,
}

/// Boot bundle delivered as a single message when the in-process
/// server, schema, and initial query have all come back.
///
/// Carries the [`Boot`] handle by value (it owns a non-`Clone`
/// `JoinHandle`); see [`BootCarrier`] for how it crosses the iced
/// message boundary.
#[derive(Debug)]
pub struct BootBundle {
    /// In-process server handle.
    pub boot: Boot,
    /// Cube client bound to the boot's URL.
    pub remote: Arc<tatami_http::Remote>,
    /// Schema fetched on first call.
    pub schema: Arc<Schema>,
    /// Initial query result derived from `composer::defaults` — `None`
    /// when the schema is too sparse for a default composition.
    pub initial: Option<Results>,
}

/// All UI events the board responds to.
///
/// `Clone + Debug` are required by iced. The `Booted` variant carries
/// non-`Clone` artefacts via [`BootCarrier`] so the surrounding enum
/// stays cloneable.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Message {
    /// Boot completed (or failed). On success the carrier holds the
    /// live bundle exactly once; the update handler takes it out.
    Booted(BootCarrier, Option<String>),
    /// User picked a measure / metric.
    PickMeasure(schema::Name),
    /// User picked a dimension. Hierarchy resets to that dim's first.
    PickDim(schema::Name),
    /// User picked a hierarchy under the current dim.
    PickHierarchy(schema::Name),
    /// User clicked a member name — push onto the focus stack and
    /// re-fire.
    FocusInto(MemberRef),
    /// User clicked the `× Up` breadcrumb — pop one focus level.
    FocusUp,
    /// User clicked the `Top` breadcrumb — empty the focus stack.
    FocusReset,
    /// User clicked the × on a slicer chip; the index points into
    /// `slicer`.
    PopSlicer(usize),
    /// A query fired by `update` came back.
    QueryReady(Result<Results, String>),
}

/// The top-level iced application.
pub struct App {
    phase: Phase,
}

impl App {
    /// Returns the initial state and the boot task. iced 0.15-dev
    /// expects `(State, Task<Message>)`.
    pub fn new() -> (Self, Task<Message>) {
        let app = Self {
            phase: Phase::Loading { error: None },
        };
        let task = Task::perform(boot_and_initial_query(), |result| match result {
            Ok(bundle) => Message::Booted(Arc::new(Mutex::new(Some(bundle))), None),
            Err(err) => Message::Booted(Arc::new(Mutex::new(None)), Some(err)),
        });
        (app, task)
    }

    /// iced update entry point.
    pub fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::Booted(carrier, None) => {
                let Ok(mut guard) = carrier.lock() else {
                    self.phase = Phase::Loading {
                        error: Some("internal: boot mutex poisoned".into()),
                    };
                    return Task::none();
                };
                let Some(bundle) = guard.take() else {
                    return Task::none();
                };
                let (measure, dim, hierarchy) = composer::defaults(&bundle.schema);
                let results = match bundle.initial {
                    Some(r) => ResultsState::Ready(r),
                    None => ResultsState::Idle,
                };
                let ready = Ready {
                    boot: bundle.boot,
                    remote: bundle.remote,
                    schema: bundle.schema,
                    measure,
                    dim,
                    hierarchy,
                    focus: Vec::new(),
                    slicer: Vec::new(),
                    results,
                };
                self.phase = Phase::Ready(Box::new(ready));
                Task::none()
            }
            Message::Booted(_carrier, Some(err)) => {
                self.phase = Phase::Loading { error: Some(err) };
                Task::none()
            }
            Message::PickMeasure(name) => {
                let Phase::Ready(ready) = &mut self.phase else {
                    return Task::none();
                };
                ready.measure = Some(name);
                fire_active_query(ready)
            }
            Message::PickDim(name) => {
                let Phase::Ready(ready) = &mut self.phase else {
                    return Task::none();
                };
                ready.dim = Some(name);
                ready.hierarchy = ready.dim.as_ref().and_then(|d| {
                    composer::hierarchy_options(&ready.schema, Some(d))
                        .into_iter()
                        .next()
                });
                ready.focus.clear();
                fire_active_query(ready)
            }
            Message::PickHierarchy(name) => {
                let Phase::Ready(ready) = &mut self.phase else {
                    return Task::none();
                };
                ready.hierarchy = Some(name);
                ready.focus.clear();
                fire_active_query(ready)
            }
            Message::FocusInto(member) => {
                let Phase::Ready(ready) = &mut self.phase else {
                    return Task::none();
                };
                ready.focus.push(member);
                fire_active_query(ready)
            }
            Message::FocusUp => {
                let Phase::Ready(ready) = &mut self.phase else {
                    return Task::none();
                };
                ready.focus.pop();
                fire_active_query(ready)
            }
            Message::FocusReset => {
                let Phase::Ready(ready) = &mut self.phase else {
                    return Task::none();
                };
                ready.focus.clear();
                fire_active_query(ready)
            }
            Message::PopSlicer(idx) => {
                let Phase::Ready(ready) = &mut self.phase else {
                    return Task::none();
                };
                if idx < ready.slicer.len() {
                    ready.slicer.remove(idx);
                }
                fire_active_query(ready)
            }
            Message::QueryReady(Ok(results)) => {
                if let Phase::Ready(ready) = &mut self.phase {
                    ready.results = if is_empty_results(&results) && !ready.focus.is_empty() {
                        ResultsState::Leaf
                    } else {
                        ResultsState::Ready(results)
                    };
                }
                Task::none()
            }
            Message::QueryReady(Err(err)) => {
                if let Phase::Ready(ready) = &mut self.phase {
                    ready.results = ResultsState::Error(err);
                }
                Task::none()
            }
        }
    }

    /// iced view entry point.
    pub fn view(&self) -> Element<'_, Message> {
        match &self.phase {
            Phase::Loading { error: Some(err) } => {
                container(text(format!("Boot failed: {err}")).size(16))
                    .padding(24)
                    .center_x(Length::Fill)
                    .center_y(Length::Fill)
                    .into()
            }
            Phase::Loading { error: None } => container(text("Booting cube\u{2026}").size(16))
                .padding(24)
                .center_x(Length::Fill)
                .center_y(Length::Fill)
                .into(),
            Phase::Ready(ready) => view_ready(ready),
        }
    }
}

fn view_ready(ready: &Ready) -> Element<'_, Message> {
    let pickers = composer::pickers(
        &ready.schema,
        ready.measure.as_ref(),
        ready.dim.as_ref(),
        ready.hierarchy.as_ref(),
    );
    let breadcrumb = composer::breadcrumb(ready.hierarchy.as_ref(), &ready.focus);
    let slicer_view = composer::slicer_trail(&ready.slicer);

    let main_panel: Element<'_, Message> = match &ready.results {
        ResultsState::Idle => text("Pick a measure, dimension, and hierarchy to query.")
            .size(14)
            .into(),
        ResultsState::Loading => text("Querying\u{2026}").size(16).into(),
        ResultsState::Error(err) => text(format!("Error: {err}")).size(14).into(),
        ResultsState::Ready(r) => composer::render(r, Message::FocusInto),
        ResultsState::Leaf => composer::leaf_placeholder(),
    };

    let body = container(
        scrollable(main_panel)
            .width(Length::Fill)
            .height(Length::Fill),
    )
    .padding(16)
    .width(Length::Fill)
    .height(Length::Fill);

    column![pickers, breadcrumb, slicer_view, body]
        .spacing(8)
        .padding(8)
        .into()
}

/// Build the active query from `(measure, dim, hierarchy, focus, slicer)`,
/// then evaluate it. A degenerate schema (any of the three picks
/// missing, or the chosen `(dim, hierarchy)` having no levels) lands
/// the result panel on `Idle`.
fn fire_active_query(ready: &mut Ready) -> Task<Message> {
    let Some(query) = build_query(
        &ready.schema,
        ready.measure.as_ref(),
        ready.dim.as_ref(),
        ready.hierarchy.as_ref(),
        &ready.focus,
        &ready.slicer,
    ) else {
        ready.results = ResultsState::Idle;
        return Task::none();
    };
    let remote = ready.remote.clone();
    ready.results = ResultsState::Loading;
    Task::perform(
        async move { remote.query(&query).await.map_err(|e| e.to_string()) },
        Message::QueryReady,
    )
}

/// Compose the rows axis from `focus`: empty → top level of
/// `(dim, hierarchy)`; otherwise → children of the topmost focused
/// member. The query is only well-formed when all three picks resolve
/// against the schema.
fn build_query(
    schema: &Schema,
    measure: Option<&schema::Name>,
    dim: Option<&schema::Name>,
    hierarchy: Option<&schema::Name>,
    focus: &[MemberRef],
    slicer: &[(schema::Name, MemberRef)],
) -> Option<Query> {
    let measure = measure?.clone();
    let dim = dim?.clone();
    let hierarchy = hierarchy?.clone();
    let top = composer::top_level(schema, &dim, &hierarchy)?;

    let rows = match focus.last() {
        None => Set::members(dim, hierarchy, top),
        Some(parent) => Set::from_member(parent.clone()).children(),
    };
    let axes = Axes::Series { rows };

    // By construction each dim appears at most once in the trail, so
    // `Tuple::of`'s uniqueness check always succeeds.
    let slicer_tuple = Tuple::of(slicer.iter().map(|(_, m)| m.clone())).ok()?;

    Some(Query {
        axes,
        slicer: slicer_tuple,
        metrics: vec![measure],
        options: query::Options::default(),
    })
}

/// Heuristic: a Series result with zero x-members is what `Set::children`
/// returns for a leaf member. Distinct from "schema is empty" because
/// the focus stack is non-empty in the leaf case.
fn is_empty_results(results: &Results) -> bool {
    matches!(results, Results::Series(r) if r.x().is_empty())
}

/// Boot the in-process server, connect a cube client, fetch the schema,
/// and run the default composition's query. Any failure fans out into
/// a single string for the `Booted(Err)` message.
async fn boot_and_initial_query() -> Result<BootBundle, String> {
    let csv_path = resolve_data_path().map_err(|e| e.to_string())?;
    let cube = meridianre_serve::cube::build(&csv_path).map_err(|e| e.to_string())?;

    let boot = Boot::run(cube).await.map_err(|e| e.to_string())?;
    let base_url = boot.base_url.clone();

    let remote = tatami_http::connect(base_url)
        .await
        .map_err(|e| e.to_string())?;
    let remote = Arc::new(remote);

    let schema = remote.schema().await.map_err(|e| e.to_string())?;
    let schema = Arc::new(schema);

    let (measure, dim, hierarchy) = composer::defaults(&schema);
    let initial = match build_query(
        &schema,
        measure.as_ref(),
        dim.as_ref(),
        hierarchy.as_ref(),
        &[],
        &[],
    ) {
        Some(q) => Some(remote.query(&q).await.map_err(|e| e.to_string())?),
        None => None,
    };

    Ok(BootBundle {
        boot,
        remote,
        schema,
        initial,
    })
}

/// Resolve the path to `monthly_close.csv`. Honours `MERIDIANRE_DATA_DIR`
/// and falls back to the same default the `meridianre-serve` binary uses.
fn resolve_data_path() -> anyhow::Result<std::path::PathBuf> {
    let dir = std::env::var("MERIDIANRE_DATA_DIR")
        .unwrap_or_else(|_| "~/inboard-ai/crates/meridianre/sample".to_owned());
    let dir = expand_tilde(&dir);
    Ok(dir.join("monthly_close.csv"))
}

/// Expand a leading `~/` against `$HOME`. Same pattern as `meridianre-serve`.
fn expand_tilde(s: &str) -> std::path::PathBuf {
    if let Some(stripped) = s.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME")
    {
        return std::path::PathBuf::from(home).join(stripped);
    }
    std::path::PathBuf::from(s)
}

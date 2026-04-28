//! Board orchestrator — `State`, `Message`, `update`, `view`.
//!
//! The board boots an in-process `tatami-serve` instance ([`crate::server::Boot`]),
//! talks to it through a [`tatami_http::Remote`], and routes drill events
//! to the active [`crate::infolet::Infolet`].
//!
//! ## Phases
//!
//! `App::new` returns synchronously with a `Phase::Loading` placeholder —
//! the cube boot is async and we can't construct a [`tatami_http::Remote`]
//! before runway has bound a port. A single boot task fires on startup
//! and resolves into `Phase::Ready`.
//!
//! ## Drill model
//!
//! - **down**: clicking a member name in a result row pushes
//!   `(dim, member)` onto [`State::slicer`] and re-fires the active query.
//!   Re-clicking on a member of a dim already in the trail replaces the
//!   prior pin (one slicer per dim).
//! - **across**: changing the active infolet keeps the slicer trail —
//!   same context, different shape.
//! - **up**: clicking the × on a breadcrumb chip pops that index from
//!   the trail.

use std::sync::{Arc, Mutex};

use iced::widget::{Column, button, column, container, pick_list, row, scrollable, text};
use iced::{Element, Length, Task};

use tatami::Cube;
use tatami::query::{MemberRef, Tuple};
use tatami::schema::Name;
use tatami::{Results, Schema};

use crate::infolet::{self, Infolet};
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
/// a `Box` to keep the discriminant cheap to pass on the iced Task path.
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
    /// shutdown channel and stops the accept loop. Read only via Drop.
    #[allow(dead_code, reason = "kept alive for the embedded server's lifetime")]
    pub boot: Boot,
    /// HTTP-side cube client pointing at `boot.base_url`.
    pub remote: Arc<tatami_http::Remote>,
    /// The schema fetched at startup. Reserved for upcoming dim/level
    /// introspection in the breadcrumb chips; not read in v1.
    #[allow(dead_code, reason = "reserved for richer breadcrumb labelling")]
    pub schema: Schema,
    /// The currently-selected dashboard tile.
    pub active: Infolet,
    /// Drill trail — one entry per dim. Newer drills on the same dim
    /// replace the older entry rather than appending.
    pub slicer: Vec<(Name, MemberRef)>,
    /// Current query result, keyed by `(active, slicer)`.
    pub results: ResultsState,
}

/// The three states a tile's results can be in.
#[non_exhaustive]
pub enum ResultsState {
    /// A query is in flight.
    Loading,
    /// The latest query returned `Ok` and we have a `Results` to render.
    Ready(Results),
    /// The latest query failed; rendered as an inline error line.
    Error(String),
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
    pub schema: Schema,
    /// Initial query result for the default infolet.
    pub initial: Results,
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
    /// User picked a different tile from the pick-list.
    PickInfolet(Infolet),
    /// User clicked a member name in a result row — drill down by
    /// pinning that member in the slicer trail.
    DrillInto(MemberRef),
    /// User clicked the × on a breadcrumb chip; the index points into
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
                // The carrier ought to hold the bundle on the success
                // path; drop the message if a duplicate sneaks through.
                let Ok(mut guard) = carrier.lock() else {
                    self.phase = Phase::Loading {
                        error: Some("internal: boot mutex poisoned".into()),
                    };
                    return Task::none();
                };
                let Some(bundle) = guard.take() else {
                    return Task::none();
                };
                let ready = Ready {
                    boot: bundle.boot,
                    remote: bundle.remote,
                    schema: bundle.schema,
                    active: default_infolet(),
                    slicer: Vec::new(),
                    results: ResultsState::Ready(bundle.initial),
                };
                self.phase = Phase::Ready(Box::new(ready));
                Task::none()
            }
            Message::Booted(_carrier, Some(err)) => {
                self.phase = Phase::Loading { error: Some(err) };
                Task::none()
            }
            Message::PickInfolet(next) => {
                let Phase::Ready(ready) = &mut self.phase else {
                    return Task::none();
                };
                ready.active = next;
                fire_active_query(ready)
            }
            Message::DrillInto(member) => {
                let Phase::Ready(ready) = &mut self.phase else {
                    return Task::none();
                };
                push_slicer(&mut ready.slicer, member);
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
                    ready.results = ResultsState::Ready(results);
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
    let options = infolet::infolets();
    let picker = pick_list(Some(ready.active), options, |i: &Infolet| {
        i.label().to_owned()
    })
    .on_select(Message::PickInfolet)
    .placeholder("Pick a tile")
    .padding(8);

    let breadcrumbs = breadcrumb_chips(&ready.slicer);

    let top_bar = row![picker, breadcrumbs].spacing(16).padding(8);

    let main_panel: Element<'_, Message> = match &ready.results {
        ResultsState::Loading => text("Querying\u{2026}").size(16).into(),
        ResultsState::Error(err) => text(format!("Error: {err}")).size(14).into(),
        ResultsState::Ready(r) => infolet::render(r),
    };

    let body = container(
        scrollable(main_panel)
            .width(Length::Fill)
            .height(Length::Fill),
    )
    .padding(16)
    .width(Length::Fill)
    .height(Length::Fill);

    column![top_bar, body].spacing(8).padding(8).into()
}

/// Render the slicer trail as a row of `dim=member ×` chips. `×` pops
/// the chip at that index.
fn breadcrumb_chips(slicer: &[(Name, MemberRef)]) -> Element<'_, Message> {
    let chips = slicer.iter().enumerate().map(|(i, (dim, member))| {
        let label = format!("{} = {}  \u{00d7}", dim.as_str(), member.path);
        button(text(label).size(12))
            .padding(4)
            .on_press(Message::PopSlicer(i))
            .style(button::secondary)
            .into()
    });
    let chips: Vec<Element<'_, Message>> = chips.collect();
    if chips.is_empty() {
        text("(no slicer — full cube)").size(12).into()
    } else {
        Column::with_children(vec![
            iced::widget::Row::with_children(chips).spacing(6).into(),
        ])
        .into()
    }
}

/// Append `(member.dim, member)` to the slicer, replacing any prior
/// entry on the same dim. Keeps `Tuple::of`'s uniqueness check satisfied
/// when we fold the trail into a [`Tuple`].
fn push_slicer(slicer: &mut Vec<(Name, MemberRef)>, member: MemberRef) {
    let dim = member.dim.clone();
    if let Some(slot) = slicer.iter_mut().find(|(d, _)| d == &dim) {
        *slot = (dim, member);
    } else {
        slicer.push((dim, member));
    }
}

/// Build a [`Tuple`] from the trail, then evaluate the active infolet's
/// query against the remote cube.
fn fire_active_query(ready: &mut Ready) -> Task<Message> {
    let Some(slicer_tuple) = build_slicer_tuple(&ready.slicer) else {
        ready.results = ResultsState::Error("internal: slicer trail is malformed".into());
        return Task::none();
    };
    let query = ready.active.query(slicer_tuple);
    let remote = ready.remote.clone();
    ready.results = ResultsState::Loading;
    Task::perform(
        async move { remote.query(&query).await.map_err(|e| e.to_string()) },
        Message::QueryReady,
    )
}

/// Fold the trail into a `Tuple`. By construction each dim appears at
/// most once (see [`push_slicer`]), so `Tuple::of`'s uniqueness check
/// never trips — but we surface the error rather than `unwrap`.
fn build_slicer_tuple(slicer: &[(Name, MemberRef)]) -> Option<Tuple> {
    Tuple::of(slicer.iter().map(|(_, m)| m.clone())).ok()
}

/// The default tile shown on first paint.
fn default_infolet() -> Infolet {
    Infolet::NpwScalar
}

/// Boot the in-process server, connect a cube client, fetch the schema,
/// run the default infolet's empty-slicer query. Any failure fans out
/// into a single string for the `Booted(Err)` message.
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

    let initial_query = default_infolet().query(Tuple::empty());
    let initial = remote
        .query(&initial_query)
        .await
        .map_err(|e| e.to_string())?;

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

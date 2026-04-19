//! Slicer leaf — per-dim pins plus a cache of each dim's top-level
//! members. Async: [`State::load_options`] fires one
//! `cube.level_members` call per dim; pickers render synchronously
//! from the resulting cache.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use iced::widget::{Column, button, pick_list, row, text};
use iced::{Alignment, Element, Length, Task};

use tatami::query::MemberRef;
use tatami::schema::{Dimension, Schema};
use tatami_inmem::InMemoryCube;

use crate::composer::axis;
use crate::icon;
use crate::theme::constants::{
    CONTROL_HEIGHT, HEADING_SIZE, ICON_BUTTON_PADDING, ICON_SIZE, INLINE_LABEL_WIDTH,
    PICKER_PADDING, PICKER_SIZE, TEXT_SIZE,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Choice {
    pub member: MemberRef,
    pub label: String,
}

impl std::fmt::Display for Choice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.label)
    }
}

#[derive(Clone, Default)]
pub struct State {
    /// Pinned members keyed by dim index into `schema.dimensions`.
    pub pins: HashMap<usize, MemberRef>,
    /// Cached per-dim top-level members. Populated after schema
    /// arrival by one [`InMemoryCube::level_members`] call per dim.
    pub options: HashMap<usize, Vec<MemberRef>>,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Message {
    MembersLoaded(usize, Result<Vec<MemberRef>, String>),
    Picked(usize, Option<Choice>),
}

impl State {
    pub fn update(&mut self, msg: Message) {
        match msg {
            Message::MembersLoaded(dim_index, Ok(members)) => {
                self.options.insert(dim_index, members);
            }
            Message::MembersLoaded(_dim_index, Err(error)) => {
                // One dim's load failing is non-fatal; its picker
                // stays in the loading state, the rest of the UI runs.
                eprintln!("slicer members load failed: {error}");
            }
            Message::Picked(dim_index, choice) => match choice {
                Some(c) => {
                    self.pins.insert(dim_index, c.member);
                }
                None => {
                    self.pins.remove(&dim_index);
                }
            },
        }
    }

    /// Spawn one [`InMemoryCube::level_members`] call per dim.
    pub fn load_options(&self, cube: &Arc<InMemoryCube>, schema: &Schema) -> Vec<Task<Message>> {
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
                Message::MembersLoaded(dim_index, outcome)
            }));
        }
        tasks
    }

    /// Pin a member. Paired with an axis drill one level deeper on
    /// the same dim, this expresses "children of this member".
    pub fn pin(&mut self, dim_index: usize, member: MemberRef) {
        self.pins.insert(dim_index, member);
    }

    /// Drop pins for any dim now on an axis — a dim is on an axis or
    /// in the slicer, never both. Only called from dim-picker paths;
    /// drill deliberately violates this invariant (see [`State::pin`]).
    pub fn prune(&mut self, rows: &axis::Pick, columns: &axis::Pick) {
        let on_axis = axis_dim_set(rows, columns);
        self.pins
            .retain(|dim_index, _| !on_axis.contains(dim_index));
    }
}

pub fn view<'a>(state: &'a State, schema: &'a Schema) -> Element<'a, Message> {
    let mut children: Vec<Element<'a, Message>> = vec![heading("Slicer")];
    for (dim_index, dim) in schema.dimensions.iter().enumerate() {
        children.push(picker_row(dim_index, dim, state));
    }
    if schema.dimensions.is_empty() {
        children.push(hint("(schema has no dims)"));
    }
    Column::with_children(children).spacing(6).into()
}

fn picker_row<'a>(dim_index: usize, dim: &'a Dimension, state: &'a State) -> Element<'a, Message> {
    let label = dim.name.as_str().to_owned();

    let Some(members) = state.options.get(&dim_index) else {
        return row![inline_label(label), hint("(loading\u{2026})")]
            .align_y(Alignment::Center)
            .spacing(6)
            .into();
    };

    let options: Vec<Choice> = members
        .iter()
        .map(|m| Choice {
            member: m.clone(),
            label: m.path.to_string(),
        })
        .collect();

    let selected = state
        .pins
        .get(&dim_index)
        .and_then(|pinned| options.iter().find(|c| c.member == *pinned).cloned());

    let picker = pick_list(selected, options, |c: &Choice| c.label.clone())
        .on_select(move |c: Choice| Message::Picked(dim_index, Some(c)))
        .placeholder("(unbound)")
        .text_size(PICKER_SIZE)
        .padding(PICKER_PADDING)
        .width(Length::Fill);

    let clear: Element<'a, Message> = if state.pins.contains_key(&dim_index) {
        button(icon::close().size(ICON_SIZE).line_height(1.0))
            .padding(ICON_BUTTON_PADDING)
            .height(Length::Fixed(CONTROL_HEIGHT))
            .on_press(Message::Picked(dim_index, None))
            .into()
    } else {
        text("").into()
    };

    row![inline_label(label), picker, clear]
        .align_y(Alignment::Center)
        .spacing(4)
        .into()
}

fn axis_dim_set(rows: &axis::Pick, columns: &axis::Pick) -> HashSet<usize> {
    let mut set = HashSet::new();
    if let axis::Pick::Set { dim, .. } = *rows {
        set.insert(dim);
    }
    if let axis::Pick::Set { dim, .. } = *columns {
        set.insert(dim);
    }
    set
}

fn heading<'a>(label: &str) -> Element<'a, Message> {
    text(label.to_uppercase())
        .size(HEADING_SIZE)
        .style(crate::theme::text::muted)
        .into()
}

fn hint<'a>(label: &'a str) -> Element<'a, Message> {
    text(label)
        .size(TEXT_SIZE)
        .style(crate::theme::text::muted)
        .into()
}

fn inline_label<'a>(label: impl Into<String>) -> Element<'a, Message> {
    text(label.into())
        .size(TEXT_SIZE)
        .style(crate::theme::text::muted)
        .width(Length::Fixed(INLINE_LABEL_WIDTH))
        .into()
}

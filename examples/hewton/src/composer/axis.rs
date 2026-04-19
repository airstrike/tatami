//! Axis leaf — dim + level picker, reused as two instances for Rows
//! and Columns. Clearing either picker collapses the axis to
//! `Pick::None`; a partially-picked axis is "off" until a level is
//! chosen.

use iced::widget::{pick_list, row, text};
use iced::{Alignment, Element, Length};

use tatami::query::MemberRef;
use tatami::schema::Schema;

use crate::composer::{dim, level};
use crate::theme::constants::{INLINE_LABEL_WIDTH, PICKER_PADDING, PICKER_SIZE, TEXT_SIZE};

/// An axis choice, sourced entirely from schema indices. `None` means
/// the axis is absent from the query shape (rows+columns both absent
/// → `Axes::Scalar`, rows-only → `Axes::Series`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Pick {
    /// Axis absent — contributes nothing to `Axes`.
    #[default]
    None,
    /// Axis present at the given `(dimension, hierarchy, level)`
    /// position within the schema's `dimensions` vector.
    Set {
        /// Index into `schema.dimensions`.
        dim: usize,
        /// Index into `schema.dimensions[dim].hierarchies`.
        hierarchy: usize,
        /// Index into
        /// `schema.dimensions[dim].hierarchies[hierarchy].levels`.
        level: usize,
    },
}

#[derive(Clone, Default)]
pub struct State {
    pub pick: Pick,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Message {
    /// Dim picker changed. `None` clears the axis entirely.
    DimPicked(Option<dim::Choice>),
    /// Level picker changed. `None` clears the axis back to `Pick::None`.
    LevelPicked(Option<level::Choice>),
}

impl State {
    pub fn update(&mut self, msg: Message, schema: &Schema) {
        match msg {
            Message::DimPicked(choice) => {
                self.pick = pick_from_dim(schema, choice);
            }
            Message::LevelPicked(choice) => {
                self.pick = pick_from_level(self.pick, choice);
            }
        }
    }

    /// Advance one level deeper on the current dim — the drill mutator.
    /// No-op when the axis is off, when the clicked member isn't on
    /// this axis's dim, or when the axis is already at its deepest
    /// level for the current hierarchy. Returns `true` when the pick
    /// actually moved so the parent knows a drill is worth recording.
    pub fn drill(&mut self, member: &MemberRef, schema: &Schema) -> bool {
        let Pick::Set {
            dim,
            hierarchy,
            level,
        } = self.pick
        else {
            return false;
        };
        let Some(target) = dim::index_for(&member.dim, schema) else {
            return false;
        };
        if target != dim {
            return false;
        }
        let Some(hier) = schema
            .dimensions
            .get(dim)
            .and_then(|d| d.hierarchies.get(hierarchy))
        else {
            return false;
        };
        if level + 1 >= hier.levels.len() {
            return false;
        }
        self.pick = Pick::Set {
            dim,
            hierarchy,
            level: level + 1,
        };
        true
    }
}

/// Build the dim + optional-level picker row for this axis. `label` is
/// the static "Rows"/"Columns" tag shown to the left of the pickers.
pub fn view<'a>(state: &'a State, schema: &'a Schema, label: &'static str) -> Element<'a, Message> {
    let options = dim::options(schema);
    let selected = current_dim_choice(&options, &state.pick);

    let dim_list = pick_list(selected, options, |c: &dim::Choice| c.label.clone())
        .on_select(|c: dim::Choice| Message::DimPicked(Some(c)))
        .placeholder("(none)")
        .text_size(PICKER_SIZE)
        .padding(PICKER_PADDING)
        .width(Length::Fill);

    // Options walk every hierarchy × level in the chosen dim, so the
    // same control works uniformly across regular / time / scenario dims.
    let level_element: Element<'a, Message> = match state.pick {
        Pick::Set {
            dim,
            hierarchy,
            level,
        } => {
            let options: Vec<level::Choice> = schema.dimensions[dim]
                .hierarchies
                .iter()
                .enumerate()
                .flat_map(|(h_idx, h)| {
                    h.levels
                        .iter()
                        .enumerate()
                        .map(move |(l_idx, l)| level::Choice {
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
            pick_list(selected, options, |c: &level::Choice| c.label.clone())
                .on_select(|c: level::Choice| Message::LevelPicked(Some(c)))
                .placeholder("(level)")
                .text_size(PICKER_SIZE)
                .padding(PICKER_PADDING)
                .width(Length::Fill)
                .into()
        }
        Pick::None => text("").into(),
    };

    let body: Element<'a, Message> = match state.pick {
        Pick::Set { .. } => row![dim_list, level_element].spacing(4).into(),
        Pick::None => dim_list.into(),
    };

    let label_cell: Element<'a, Message> = text(label)
        .size(TEXT_SIZE)
        .style(crate::theme::text::muted)
        .width(Length::Fixed(INLINE_LABEL_WIDTH))
        .into();

    row![label_cell, body]
        .align_y(Alignment::Center)
        .spacing(6)
        .into()
}

/// Translate a dim-picker selection into a `Pick`. A new dim seeds the
/// axis to that dim's first hierarchy / first level so the query is
/// immediately runnable without a second click.
fn pick_from_dim(schema: &Schema, choice: Option<dim::Choice>) -> Pick {
    let Some(choice) = choice else {
        return Pick::None;
    };
    let Some(dim) = schema.dimensions.get(choice.index) else {
        return Pick::None;
    };
    if dim.hierarchies.is_empty() || dim.hierarchies[0].levels.is_empty() {
        return Pick::None;
    }
    Pick::Set {
        dim: choice.index,
        hierarchy: 0,
        level: 0,
    }
}

/// Translate a level-picker selection into a `Pick`. Only meaningful
/// once a dim is already selected.
fn pick_from_level(current: Pick, choice: Option<level::Choice>) -> Pick {
    match (current, choice) {
        (Pick::Set { dim, .. }, Some(c)) => Pick::Set {
            dim,
            hierarchy: c.hierarchy,
            level: c.level,
        },
        (Pick::Set { .. }, None) => Pick::None,
        (Pick::None, _) => Pick::None,
    }
}

fn current_dim_choice(options: &[dim::Choice], pick: &Pick) -> Option<dim::Choice> {
    match *pick {
        Pick::Set { dim, .. } => options.iter().find(|c| c.index == dim).cloned(),
        Pick::None => None,
    }
}

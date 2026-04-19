//! Metric leaf — a stack of independently-picked metric slots. An
//! empty slot blocks the query and leaves the result panel idle until
//! the user fills it.
//!
//! Also hosts the shared metric-picker types ([`Pick`], [`Choice`])
//! and the [`options`] / [`name`] helpers consumed by sibling leaves
//! (`top_n`, `filter`) and the parent `App`.

use std::fmt;

use iced::widget::{Column, button, pick_list, row, text};
use iced::{Alignment, Element, Fill};

use tatami::schema::{Name, Schema};

use crate::icon;
use crate::theme::constants::*;

/// A metric choice — either an index into `schema.measures` or an
/// index into `schema.metrics`. No names cross this boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Pick {
    /// Index into `schema.measures`.
    Measure(usize),
    /// Index into `schema.metrics`.
    Metric(usize),
}

/// A metric option in a `pick_list` — indexes into measures or metrics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Choice {
    pub pick: Pick,
    pub label: String,
}

impl fmt::Display for Choice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.label)
    }
}

/// Build the `Choice` list spanning `schema.measures` ++
/// `schema.metrics`; the [`Pick`] variant encodes which array the
/// index targets.
pub fn options(schema: &Schema) -> Vec<Choice> {
    schema
        .measures
        .iter()
        .enumerate()
        .map(|(i, m)| Choice {
            pick: Pick::Measure(i),
            label: m.name.as_str().to_owned(),
        })
        .chain(schema.metrics.iter().enumerate().map(|(i, m)| Choice {
            pick: Pick::Metric(i),
            label: m.name.as_str().to_owned(),
        }))
        .collect()
}

/// Look up the [`Name`] backing a [`Pick`]. Returns `None` when the
/// index is out of range — the caller treats that as "metric picker
/// is stale; skip this metric".
pub fn name(schema: &Schema, pick: Pick) -> Option<Name> {
    match pick {
        Pick::Measure(i) => schema.measures.get(i).map(|m| m.name.clone()),
        Pick::Metric(i) => schema.metrics.get(i).map(|m| m.name.clone()),
    }
}

#[derive(Clone, Default)]
pub struct State {
    /// One entry per visible picker. A `None` entry is a rendered-but-
    /// empty picker and blocks the query.
    pub slots: Vec<Option<Pick>>,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Message {
    Picked { slot: usize, pick: Option<Choice> },
    Added,
    Removed(usize),
}

impl State {
    pub fn update(&mut self, msg: Message) {
        match msg {
            Message::Picked { slot, pick } => {
                if let Some(entry) = self.slots.get_mut(slot) {
                    *entry = pick.map(|c| c.pick);
                }
            }
            Message::Added => {
                self.slots.push(None);
            }
            Message::Removed(slot) => {
                if slot < self.slots.len() {
                    self.slots.remove(slot);
                }
            }
        }
    }

    /// Seed one empty slot so the user lands on a visible picker on
    /// first render.
    pub fn seed_if_empty(&mut self) {
        if self.slots.is_empty() {
            self.slots.push(None);
        }
    }
}

pub fn view<'a>(state: &'a State, options: &[Choice]) -> Element<'a, Message> {
    let mut children: Vec<Element<'a, Message>> = vec![heading("Metric")];

    for (slot, entry) in state.slots.iter().enumerate() {
        let selected = entry.and_then(|pick| options.iter().find(|c| c.pick == pick).cloned());
        let picker_options = options.to_vec();
        let picker = pick_list(selected, picker_options, |c: &Choice| c.label.clone())
            .on_select(move |c: Choice| Message::Picked {
                slot,
                pick: Some(c),
            })
            .placeholder("(pick a metric)")
            .text_size(PICKER_SIZE)
            .padding(PICKER_PADDING)
            .width(Fill);
        let remove = button(icon::close().size(ICON_SIZE).line_height(1.0))
            .padding(ICON_BUTTON_PADDING)
            .height(CONTROL_HEIGHT)
            .on_press(Message::Removed(slot));
        children.push(
            row![picker, remove]
                .spacing(4)
                .align_y(Alignment::Center)
                .into(),
        );
    }

    children.push(
        button(icon::plus().size(ICON_SIZE).line_height(1.0))
            .padding(ICON_BUTTON_PADDING)
            .height(CONTROL_HEIGHT)
            .on_press(Message::Added)
            .into(),
    );

    Column::with_children(children).spacing(4).into()
}

fn heading<'a>(label: &str) -> Element<'a, Message> {
    text(label.to_uppercase())
        .size(HEADING_SIZE)
        .style(crate::theme::text::muted)
        .into()
}

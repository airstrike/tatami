//! Filter leaf — numeric predicate on the rows axis. Three controls
//! (kind, by-metric, value); when all three are valid, [`State::pick`]
//! is `Some` and the query wraps the rows set in `Set::filter`.

use std::fmt;

use iced::widget::{column, pick_list, text, text_input};
use iced::{Element, Length};

use tatami::query::Predicate;
use tatami::schema::Schema;

use crate::composer::{self, MetricChoice, MetricPick};
use crate::theme::constants::{HEADING_SIZE, PICKER_PADDING, PICKER_SIZE, TEXT_SIZE};

/// Numeric predicate kind. `Predicate::In` / `NotIn` aren't modelled
/// here — they need a `Path` picker.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Kind {
    #[default]
    Eq,
    Gt,
    Lt,
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Kind::Eq => "=",
            Kind::Gt => ">",
            Kind::Lt => "<",
        })
    }
}

/// `Kind` plus an explicit `Off` variant so "turn the filter off" is
/// a visible entry in the picker rather than a separate clear button.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KindChoice {
    Off,
    On(Kind),
}

impl fmt::Display for KindChoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KindChoice::Off => f.write_str("(off)"),
            KindChoice::On(k) => write!(f, "{k}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Pick {
    pub kind: Kind,
    pub by: MetricPick,
    pub value: f64,
}

#[derive(Clone, Default)]
pub struct State {
    pub kind: Option<Kind>,
    pub by: Option<MetricPick>,
    /// Raw text buffer behind the value input. Held separately from
    /// [`Pick::value`] so a mid-typing string like "12." doesn't
    /// flap the query.
    pub value_text: String,
    /// `Some` iff all three scratch fields are valid together.
    pub pick: Option<Pick>,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Message {
    KindPicked(Option<Kind>),
    ByPicked(Option<MetricChoice>),
    ValueChanged(String),
}

impl State {
    pub fn update(&mut self, msg: Message) {
        match msg {
            Message::KindPicked(kind) => {
                self.kind = kind;
                if kind.is_none() {
                    // Clear by/value so re-enabling starts fresh
                    // rather than snapping to a stale combination.
                    self.by = None;
                    self.value_text.clear();
                }
            }
            Message::ByPicked(choice) => {
                self.by = choice.map(|c| c.pick);
            }
            Message::ValueChanged(raw) => {
                self.value_text = raw;
            }
        }
        self.recompute();
    }

    fn recompute(&mut self) {
        let value = match self.value_text.parse::<f64>() {
            Ok(v) => v,
            Err(_) => {
                self.pick = None;
                return;
            }
        };
        self.pick = match (self.kind, self.by) {
            (Some(kind), Some(by)) => Some(Pick { kind, by, value }),
            _ => None,
        };
    }
}

pub fn build_predicate(schema: &Schema, filter: &Pick) -> Option<Predicate> {
    let metric = composer::metric_name(schema, filter.by)?;
    Some(match filter.kind {
        Kind::Eq => Predicate::Eq {
            metric,
            value: filter.value,
        },
        Kind::Gt => Predicate::Gt {
            metric,
            value: filter.value,
        },
        Kind::Lt => Predicate::Lt {
            metric,
            value: filter.value,
        },
    })
}

pub fn view<'a>(state: &'a State, metric_options: &[MetricChoice]) -> Element<'a, Message> {
    let kind_options = vec![
        KindChoice::Off,
        KindChoice::On(Kind::Eq),
        KindChoice::On(Kind::Gt),
        KindChoice::On(Kind::Lt),
    ];
    let selected_kind = match state.kind {
        None => Some(KindChoice::Off),
        Some(k) => Some(KindChoice::On(k)),
    };
    let kind_picker = pick_list(selected_kind, kind_options, |c: &KindChoice| c.to_string())
        .on_select(|c: KindChoice| {
            Message::KindPicked(match c {
                KindChoice::Off => None,
                KindChoice::On(k) => Some(k),
            })
        })
        .text_size(PICKER_SIZE)
        .padding(PICKER_PADDING)
        .width(Length::Fill);

    if state.kind.is_none() {
        return column![heading("Filter"), kind_picker, hint("(filter off)")]
            .spacing(4)
            .into();
    }

    let by_options = metric_options.to_vec();
    let selected_by = state
        .by
        .and_then(|pick| metric_options.iter().find(|c| c.pick == pick).cloned());
    let by_picker = pick_list(selected_by, by_options, |c: &MetricChoice| c.label.clone())
        .on_select(|c: MetricChoice| Message::ByPicked(Some(c)))
        .placeholder("(pick a metric)")
        .text_size(PICKER_SIZE)
        .padding(PICKER_PADDING)
        .width(Length::Fill);

    let value_input = text_input("(value)", &state.value_text)
        .on_input(Message::ValueChanged)
        .size(PICKER_SIZE)
        .padding(PICKER_PADDING)
        .width(Length::Fill);

    let ready = state.by.is_some() && state.value_text.parse::<f64>().is_ok();
    let status: Element<'a, Message> = if ready {
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

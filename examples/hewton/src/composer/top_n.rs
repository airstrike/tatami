//! Top-N leaf — a by-metric picker that, when set together with a
//! rows axis, wraps the rows set in `Set::top(rows, 10, by)`. N is
//! hard-coded at 10.

use iced::widget::{button, column, pick_list, row, text};
use iced::{Alignment, Element, Length};

use crate::composer::axis;
use crate::composer::{MetricChoice, MetricPick};
use crate::icon;
use crate::theme::constants::{
    CONTROL_HEIGHT, HEADING_SIZE, ICON_BUTTON_PADDING, ICON_SIZE, PICKER_PADDING, PICKER_SIZE,
    TEXT_SIZE,
};

#[derive(Clone, Copy, Default)]
pub struct State {
    pub by: Option<MetricPick>,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Message {
    ByPicked(Option<MetricChoice>),
}

impl State {
    pub fn update(&mut self, msg: Message) {
        match msg {
            Message::ByPicked(choice) => {
                self.by = choice.map(|c| c.pick);
            }
        }
    }
}

pub fn view<'a>(
    state: &'a State,
    rows: &axis::Pick,
    options: &[MetricChoice],
) -> Element<'a, Message> {
    let rows_present = matches!(rows, axis::Pick::Set { .. });

    if !rows_present {
        return column![heading("Top-N"), hint("(pick a rows axis first)")]
            .spacing(4)
            .into();
    }

    let picker_options = options.to_vec();
    let selected = state
        .by
        .and_then(|pick| options.iter().find(|c| c.pick == pick).cloned());

    let picker = pick_list(selected, picker_options, |c: &MetricChoice| c.label.clone())
        .on_select(|c: MetricChoice| Message::ByPicked(Some(c)))
        .placeholder("(off)")
        .text_size(PICKER_SIZE)
        .padding(PICKER_PADDING)
        .width(Length::Fill);

    let clear: Element<'a, Message> = if state.by.is_some() {
        button(icon::close().size(ICON_SIZE))
            .padding(ICON_BUTTON_PADDING)
            .height(Length::Fixed(CONTROL_HEIGHT))
            .on_press(Message::ByPicked(None))
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

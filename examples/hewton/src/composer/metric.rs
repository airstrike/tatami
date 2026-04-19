//! Metric leaf — a stack of independently-picked metric slots. An
//! empty slot blocks the query and leaves the result panel idle until
//! the user fills it.

use iced::widget::{Column, button, pick_list, row, text};
use iced::{Alignment, Element, Length};

use crate::composer::{MetricChoice, MetricPick};
use crate::icon;
use crate::theme::constants::{
    CONTROL_HEIGHT, HEADING_SIZE, ICON_BUTTON_PADDING, ICON_SIZE, PICKER_PADDING, PICKER_SIZE,
};

#[derive(Clone, Default)]
pub struct State {
    /// One entry per visible picker. A `None` entry is a rendered-but-
    /// empty picker and blocks the query.
    pub slots: Vec<Option<MetricPick>>,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Message {
    Picked {
        slot: usize,
        pick: Option<MetricChoice>,
    },
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

pub fn view<'a>(state: &'a State, options: &[MetricChoice]) -> Element<'a, Message> {
    let mut children: Vec<Element<'a, Message>> = vec![heading("Metric")];

    for (slot, entry) in state.slots.iter().enumerate() {
        let selected = entry.and_then(|pick| options.iter().find(|c| c.pick == pick).cloned());
        let picker_options = options.to_vec();
        let picker = pick_list(selected, picker_options, |c: &MetricChoice| c.label.clone())
            .on_select(move |c: MetricChoice| Message::Picked {
                slot,
                pick: Some(c),
            })
            .placeholder("(pick a metric)")
            .text_size(PICKER_SIZE)
            .padding(PICKER_PADDING)
            .width(Length::Fill);
        let remove = button(icon::close().size(ICON_SIZE))
            .padding(ICON_BUTTON_PADDING)
            .height(Length::Fixed(CONTROL_HEIGHT))
            .on_press(Message::Removed(slot));
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
            .height(Length::Fixed(CONTROL_HEIGHT))
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

//! Container styles — card surfaces, panels, etc.

use iced::widget::container;
use iced::{Background, Border, Color, Shadow, Theme, Vector, color};

/// Card container style — background, border, soft drop shadow. Used
/// for the main result panel and any future panelled surface.
pub fn card(theme: &Theme) -> container::Style {
    let palette = theme.palette();
    container::Style {
        background: Some(Background::Color(palette.background.base.color)),
        border: Border {
            color: color!(0xdde1e6),
            width: 1.0,
            radius: 6.0.into(),
        },
        shadow: Shadow {
            color: Color::BLACK.scale_alpha(0.06),
            offset: Vector::new(0.0, 2.0),
            blur_radius: 8.0,
        },
        text_color: Some(palette.background.base.text),
        ..Default::default()
    }
}

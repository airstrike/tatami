//! Minimal styling — enough to distinguish cards and mute captions. Kept
//! intentionally thin; production styling is out of scope for the
//! example's point.

use iced::widget::{container, text};
use iced::{Background, Border, Color, Shadow, Theme, Vector, color};

// ── Shared sizing constants ───────────────────────────────────────────────
//
// Sidebar aesthetic: tight, small-caps headings over compact pickers.
// Everything picker-sized (text_size + padding) is pulled from these so a
// single edit retunes density across all six sidebar sections.

/// Base text size for labels and inline controls inside the sidebar.
/// Stored as `f32` because `iced::Pixels` only accepts `f32` / `u32`.
pub const TEXT_SIZE: f32 = 12.0;

/// Section-heading size — one notch up from body. Rendered uppercase
/// with the [`muted`] style for a small-caps feel.
pub const HEADING_SIZE: f32 = 11.0;

/// `pick_list` / `text_input` text size.
pub const PICKER_SIZE: f32 = 12.0;

/// `pick_list` / `text_input` padding — `[vertical, horizontal]`.
pub const PICKER_PADDING: [u16; 2] = [4, 8];

/// Icon button glyph size (for `plus` / `close` etc.).
pub const ICON_SIZE: f32 = 14.0;

/// Icon button padding — `[vertical, horizontal]`. Pairs with
/// [`ICON_SIZE`] to land near a 24 px square.
pub const ICON_BUTTON_PADDING: [u16; 2] = [3, 6];

/// Fixed visual height for every pick_list / text_input / icon button in
/// the sidebar. Pinning both sides prevents iced's intrinsic-size
/// computations from drifting the button 1px off the picker and visually
/// misaligning rows.
pub const CONTROL_HEIGHT: f32 = 26.0;

// ── Container / text styles ───────────────────────────────────────────────

/// Card container style — background, border, soft drop shadow.
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

/// Muted text style — secondary foreground with lower contrast. Used
/// for section headings and hint lines.
pub fn muted(theme: &Theme) -> text::Style {
    let palette = theme.palette();
    text::Style {
        color: Some(palette.background.base.text.scale_alpha(0.55)),
    }
}

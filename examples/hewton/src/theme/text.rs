//! Text styles — low-contrast captions, headings, hints.

use iced::Theme;
use iced::widget::text;

/// Muted text — secondary foreground with lower contrast. Used for
/// section headings, hint lines, and inline labels.
pub fn muted(theme: &Theme) -> text::Style {
    let palette = theme.palette();
    text::Style {
        color: Some(palette.background.base.text.scale_alpha(0.55)),
    }
}

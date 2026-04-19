//! Shared sizing constants — tune the sidebar density from one place.
//!
//! Stored as `f32` because `iced::Pixels` only accepts `f32` / `u32`
//! (no `u16` conversion in this iced version).

/// Base text size for labels and inline controls inside the sidebar.
pub const TEXT_SIZE: f32 = 12.0;

/// Section-heading size — one notch up from body. Rendered uppercase
/// with the muted text style for a small-caps feel.
pub const HEADING_SIZE: f32 = 11.0;

/// `pick_list` / `text_input` text size.
pub const PICKER_SIZE: f32 = 12.0;

/// `pick_list` / `text_input` padding — `[vertical, horizontal]`.
pub const PICKER_PADDING: [u16; 2] = [4, 8];

/// Icon button glyph size (for `plus` / `close` etc.).
pub const ICON_SIZE: f32 = 14.0;

/// Icon button padding — `[vertical, horizontal]`, symmetric so the
/// button is square. `(ICON_SIZE + 2×pad) == CONTROL_HEIGHT`.
pub const ICON_BUTTON_PADDING: [u16; 2] = [4, 4];

/// Fixed visual height for icon buttons — pinned to match iced's
/// intrinsic pick_list height (PICKER_SIZE 12 + PICKER_PADDING vertical
/// × 2 + internal border). Tune this one number if the pick_list drifts.
pub const CONTROL_HEIGHT: f32 = 22.0;

/// Fixed-width gutter for inline labels that sit to the left of a
/// picker on a single row. Keeps the labels tidy-aligned across
/// sidebar sections.
pub const INLINE_LABEL_WIDTH: f32 = 64.0;

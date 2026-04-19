//! Styling for the hewton composer.
//!
//! Submodules split styles by the widget kind they target:
//! - [`container`] — card / panel surfaces.
//! - [`text`] — muted, heading, hint styles.
//! - [`button`] — icon buttons and other tap targets.
//! - [`constants`] — shared sizing constants (text sizes, padding,
//!   fixed control heights).
//!
//! Call sites import the submodule directly, e.g.
//! `.style(theme::container::card)` or
//! `text(…).style(theme::text::muted)`.

pub mod button;
pub mod constants;
pub mod container;
pub mod text;

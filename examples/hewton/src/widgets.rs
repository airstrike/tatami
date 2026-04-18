//! Rendering per `Results` variant. Kept deliberately minimal — the widgets
//! are not the point; the `Results → widget` mapping is.
//!
//! For real dashboards, this module becomes `hyozu` adapters: `Scalar → KPI
//! card`, `Series → Mark::Line`, `Pivot → iced::widget::table`, `Rollup →
//! Mark::Choropleth / Mark::BubbleMap`.

use iced::widget::{Column, column, container, row, rule, text};
use iced::{Element, Font, Length, Padding, font};

use tatami::{Cell, Results, Tuple, pivot, rollup, scalar, series};

use crate::theme;
use crate::{Message, QueryState};

const BOLD: Font = Font {
    weight: font::Weight::Bold,
    ..Font::DEFAULT
};

/// One rendered card — heading, subtitle, and a body keyed off the query's
/// current `QueryState` (running / ok / error).
pub fn card<'a>(
    heading: &'static str,
    subtitle: &'static str,
    state: Option<&'a QueryState>,
) -> Element<'a, Message> {
    let body: Element<'a, Message> = match state {
        None => text("(no task)").into(),
        Some(QueryState::Running) => text("Running…").into(),
        Some(QueryState::Err(message)) => text(format!("Error: {message}")).size(14).into(),
        Some(QueryState::Ok(results)) => render(results),
    };

    container(
        column![
            text(heading).font(BOLD).size(18),
            text(subtitle).size(12).style(theme::muted),
            rule::horizontal(8),
            body,
        ]
        .spacing(8),
    )
    .padding(Padding::from(16))
    .style(theme::card)
    .into()
}

/// The exhaustive `Results` match — this is the payoff of §3.3's closed
/// shape. A backend returning a shape-appropriate variant is enforced by
/// the `Axes → Results` table in §3.3.
fn render(results: &Results) -> Element<'_, Message> {
    // `Results` is `#[non_exhaustive]` so downstream matches need a wildcard;
    // the four variants below cover v0.1's full surface.
    match results {
        Results::Scalar(r) => render_scalar(r),
        Results::Series(r) => render_series(r),
        Results::Pivot(r) => render_pivot(r),
        Results::Rollup(r) => render_rollup(r, 0),
        _ => text("(unknown Results variant)").into(),
    }
}

fn render_scalar(r: &scalar::Result) -> Element<'_, Message> {
    column(r.values().iter().map(render_cell_line))
        .spacing(4)
        .into()
}

fn render_series(r: &series::Result) -> Element<'_, Message> {
    // Minimal: two-column text grid (x-axis members on the left, each row's
    // values on the right). Real dashboards swap in a hyozu line chart.
    let header = row![
        text("").width(Length::FillPortion(2)),
        text("value").font(BOLD).width(Length::FillPortion(3)),
    ];
    let body = r.x().iter().zip(r.rows().iter()).map(|(x, series_row)| {
        row![
            text(format!("{}", x.path)).width(Length::FillPortion(2)),
            text(format_cells(&series_row.values)).width(Length::FillPortion(3)),
        ]
        .into()
    });
    column![header, Column::with_children(body).spacing(2)]
        .spacing(6)
        .into()
}

fn render_pivot(r: &pivot::Result) -> Element<'_, Message> {
    // Header row — col_headers across the top.
    let header_cells = std::iter::once(text("").width(Length::FillPortion(2)).into()).chain(
        r.col_headers().iter().map(|col| {
            text(format_tuple(col))
                .font(BOLD)
                .width(Length::FillPortion(3))
                .into()
        }),
    );
    let header: Element<'_, Message> = row(header_cells).into();

    // Body rows — one per row_header.
    let body = r
        .row_headers()
        .iter()
        .zip(r.cells().iter())
        .map(|(row_header, cells)| {
            let cells_iter = std::iter::once(
                text(format_tuple(row_header))
                    .font(BOLD)
                    .width(Length::FillPortion(2))
                    .into(),
            )
            .chain(
                cells
                    .iter()
                    .map(|cell| text(format_cell(cell)).width(Length::FillPortion(3)).into()),
            );
            row(cells_iter).into()
        });

    column![header, Column::with_children(body).spacing(2)]
        .spacing(6)
        .into()
}

fn render_rollup(tree: &rollup::Tree, depth: u16) -> Element<'_, Message> {
    let indent = " ".repeat(depth as usize * 2);
    let head = row![
        text(format!("{indent}{}", tree.root.path)).width(Length::FillPortion(3)),
        text(format_cell(&tree.value)).width(Length::FillPortion(2)),
    ];
    let children = tree
        .children
        .iter()
        .map(|child| render_rollup(child, depth + 1));
    column![head, Column::with_children(children).spacing(2)]
        .spacing(2)
        .into()
}

// ── Cell formatting ────────────────────────────────────────────────────────

fn render_cell_line(cell: &Cell) -> Element<'_, Message> {
    text(format_cell(cell)).into()
}

fn format_cells(cells: &[Cell]) -> String {
    cells.iter().map(format_cell).collect::<Vec<_>>().join("  ")
}

fn format_cell(cell: &Cell) -> String {
    match cell {
        Cell::Valid {
            value,
            unit,
            format,
        } => {
            // Simple: respect `format` for percent, fall back to unit-suffixed number.
            // A real renderer threads ICU or a format-spec parser.
            if format.as_ref().is_some_and(|f| f.as_str().contains('%')) {
                format!("{:.1}%", value * 100.0)
            } else if let Some(unit) = unit {
                format!("{} {}", human_number(*value), unit.as_str())
            } else {
                human_number(*value)
            }
        }
        Cell::Missing { .. } => "—".into(),
        Cell::Error { message } => format!("⚠ {message}"),
        // `Cell` is `#[non_exhaustive]` so the match needs a wildcard.
        _ => "?".into(),
    }
}

fn format_tuple(t: &Tuple) -> String {
    // Tuples are small; join members as `Dim=head`. Real dashboards swap in
    // richer header-cell rendering (leading icon, drill affordance, etc.).
    t.members()
        .iter()
        .map(|m| format!("{}={}", m.dim, m.path))
        .collect::<Vec<_>>()
        .join(", ")
}

fn human_number(v: f64) -> String {
    if v.abs() >= 1_000_000.0 {
        format!("{:.2}M", v / 1_000_000.0)
    } else if v.abs() >= 1_000.0 {
        format!("{:.1}k", v / 1_000.0)
    } else {
        format!("{v:.2}")
    }
}

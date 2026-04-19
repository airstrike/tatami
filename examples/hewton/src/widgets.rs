//! Rendering per `Results` variant. Kept deliberately minimal — the widgets
//! are not the point; the `Results → widget` mapping is.
//!
//! For real dashboards, this module becomes `hyozu` adapters: `Scalar → KPI
//! card`, `Series → Mark::Line`, `Pivot → sweeten::widget::table`, `Rollup →
//! Mark::Choropleth / Mark::BubbleMap`.

use iced::widget::{Column, button, column, container, row, scrollable, text};
use iced::{Alignment, Element, Font, Length, Padding, font};
use sweeten::widget::table;

use tatami::query::MemberRef;
use tatami::{Cell, Results, Tuple, pivot, rollup, scalar, series};

use crate::theme;
use crate::{Message, QueryState};

// Inter at bold weight — matches the application's default_font family so
// headings stay in the same typeface as body text, one step heavier.
const BOLD: Font = Font {
    family: font::Family::Name("Inter"),
    weight: font::Weight::Bold,
    stretch: font::Stretch::Normal,
    style: font::Style::Normal,
    optical_size: font::OpticalSize::None,
};

/// A panel wrapping the current query's outcome — loading, error, or the
/// rendered `Results`. Schema-blind: the panel content is entirely derived
/// from the runtime `QueryState`.
///
/// The container always fills the available space and scrolls internally so
/// transient states (`Idle` / `Running` / `Err`) don't collapse the panel
/// height and jitter the surrounding layout when the result arrives.
pub fn result_panel(state: &QueryState) -> Element<'_, Message> {
    let body: Element<'_, Message> = match state {
        QueryState::Idle => text("Pick row/column/metric to run a query.").into(),
        QueryState::Running => text("Running\u{2026}").into(),
        QueryState::Err(message) => text(format!("Error: {message}")).size(14).into(),
        QueryState::Ok(results) => render(results),
    };

    container(scrollable(body).width(Length::Fill).height(Length::Fill))
        .padding(Padding::from(16))
        .width(Length::Fill)
        .height(Length::Fill)
        .style(theme::container::card)
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

/// A pre-formatted pivot row. Cells are pre-stringified so the column
/// closures are pure indexed lookups. `Clone` is required by
/// `sweeten::widget::table` — cheap since all fields are `String`.
///
/// `header_member` carries the *schema-bound* [`MemberRef`] behind the
/// stringified `header`, so the drill-down button can emit
/// [`Message::DrillInto`] without the widget layer ever parsing names.
/// `None` only when a pivot row has zero members in its row tuple, which
/// the backend does not currently produce — but we stay defensive and
/// render a plain label in that case.
#[derive(Clone)]
struct PivotRow {
    header: String,
    header_member: Option<MemberRef>,
    cells: Vec<String>,
}

fn render_pivot(r: &pivot::Result) -> Element<'_, Message> {
    let rows: Vec<PivotRow> = r
        .row_headers()
        .iter()
        .zip(r.cells().iter())
        .map(|(h, cs)| PivotRow {
            header: format_tuple(h),
            // Typically a single member per row tuple (the rows-axis
            // member); deeper composite axes would still drill on the
            // first member, which is the one the rows axis pick names.
            header_member: h.members().first().cloned(),
            cells: cs.iter().map(format_cell).collect(),
        })
        .collect();

    // Column 0 — the row header. Wrapped in a text-styled button so a
    // click emits `Message::DrillInto(member)`. The `button::text` style
    // drops the border and background so it still reads as a header
    // cell; alignment stays left-driven by `sweeten::widget::table`'s
    // default `Alignment::Start` on this column.
    //
    // NOTE: Clicking a rollup-tree node is the obvious next iteration —
    // same pattern (push snapshot, pin member, drill).
    let header_column = table::column(
        Some(Element::from(text("").font(BOLD))),
        |row: PivotRow| -> Element<'_, Message> {
            let label = text(row.header).font(BOLD);
            match row.header_member {
                Some(member) => button(label)
                    .on_press(Message::DrillInto(member))
                    .padding(0)
                    .style(button::text)
                    .into(),
                None => label.into(),
            }
        },
    );

    // One data column per col_header. Each closure captures `i` by copy
    // and indexes into the pre-formatted `cells` vec.
    let mut columns = vec![header_column];
    for (i, col_header) in r.col_headers().iter().enumerate() {
        let label = format_tuple(col_header);
        columns.push(
            table::column(
                Some(Element::from(text(label).font(BOLD))),
                move |row: PivotRow| -> Element<'_, Message> { text(row.cells[i].clone()).into() },
            )
            .align_x(Alignment::End),
        );
    }

    scrollable(
        table(columns, rows)
            .padding_x(8.0)
            .padding_y(4.0)
            .separator_x(0.0)
            .separator_y(1.0),
    )
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
    // Drop the `Dim=` prefix — the column/row header's position in the
    // table already tells you which dim it is. Real dashboards swap in
    // richer header rendering (leading icon, drill affordance, etc.).
    t.members()
        .iter()
        .map(|m| m.path.to_string())
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

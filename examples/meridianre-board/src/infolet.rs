//! The dashboard's [`Infolet`] enum — six pre-defined cube queries plus the
//! `Query` builder and `Results` renderer for each.
//!
//! An infolet is a function `(slicer) -> Query` paired with a one-shot
//! result renderer. The slicer parameter carries the cumulative drill
//! trail (zero or more `(dim, member)` pairs); each infolet folds it into
//! its base query at construction time.

use std::fmt;

use iced::widget::{Column, button, column, row, scrollable, text};
use iced::{Element, Length};

use tatami::query::{self, MemberRef, Set, Tuple};
use tatami::schema::Name;
use tatami::{Axes, Cell, Query, Results, pivot, rollup, scalar, series};

use crate::board::Message;

/// One named tile in the dashboard.
///
/// Each variant wires a fixed `(Axes, metrics)` shape against the
/// meridianre cube. The slicer the user has accumulated through drill
/// gets folded into the query at build time — see [`Infolet::query`].
///
/// `#[non_exhaustive]` lets us add tiles later without breaking demo
/// callers; today the GUI iterates [`infolets`] verbatim.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Infolet {
    /// FY-aggregated NPW as a single KPI value.
    NpwScalar,
    /// NPW summed by month — series, x = Month.
    NpwByMonth,
    /// NPW summed by country — series, x = Country.
    NpwByCountry,
    /// NPW summed by product line — series, x = ProductLine.
    NpwByProduct,
    /// NPW pivoted: rows = Region, columns = Month.
    FinancialPivot,
    /// Year-over-year NPW change — single KPI value via the `RevenueYoY`
    /// metric defined in the meridianre schema.
    RevenueYoyScalar,
}

/// Every infolet the dashboard offers, in pick-list order.
pub fn infolets() -> Vec<Infolet> {
    vec![
        Infolet::NpwScalar,
        Infolet::NpwByMonth,
        Infolet::NpwByCountry,
        Infolet::NpwByProduct,
        Infolet::FinancialPivot,
        Infolet::RevenueYoyScalar,
    ]
}

impl Infolet {
    /// The label shown in the [`iced::widget::pick_list`].
    pub fn label(self) -> &'static str {
        match self {
            Self::NpwScalar => "NPW (FY total)",
            Self::NpwByMonth => "NPW by Month",
            Self::NpwByCountry => "NPW by Country",
            Self::NpwByProduct => "NPW by Product",
            Self::FinancialPivot => "Region x Month NPW",
            Self::RevenueYoyScalar => "Revenue YoY",
        }
    }

    /// Build the cube query for this tile, narrowing on the cumulative
    /// drill trail (`slicer`).
    ///
    /// `slicer` is already a [`Tuple`] — the orchestrator collapses
    /// duplicate dims so `Tuple::of`'s uniqueness check is satisfied
    /// upstream. We hand it through verbatim.
    pub fn query(self, slicer: Tuple) -> Query {
        let (axes, metrics) = match self {
            Self::NpwScalar => (Axes::Scalar, vec![n("npw")]),
            Self::NpwByMonth => (
                Axes::Series {
                    rows: Set::members(n("Time"), n("Calendar"), n("Month")),
                },
                vec![n("npw")],
            ),
            Self::NpwByCountry => (
                Axes::Series {
                    rows: Set::members(n("Geography"), n("World"), n("Country")),
                },
                vec![n("npw")],
            ),
            Self::NpwByProduct => (
                Axes::Series {
                    rows: Set::members(n("Product"), n("LineOfBusiness"), n("ProductLine")),
                },
                vec![n("npw")],
            ),
            Self::FinancialPivot => (
                Axes::Pivot {
                    rows: Set::members(n("Geography"), n("World"), n("Region")),
                    columns: Set::members(n("Time"), n("Calendar"), n("Month")),
                },
                vec![n("npw")],
            ),
            Self::RevenueYoyScalar => (Axes::Scalar, vec![n("RevenueYoY")]),
        };

        Query {
            axes,
            slicer,
            metrics,
            options: query::Options::default(),
        }
    }
}

impl fmt::Display for Infolet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// Render a tile's `Results` to an iced [`Element`]. Tabular variants
/// produce buttons on each row header so the user can drill in by
/// clicking a member name.
///
/// `Results` is `#[non_exhaustive]`, so the match carries a wildcard arm
/// surfacing unknown variants as a placeholder line — the renderer
/// deliberately doesn't panic on a future variant.
pub fn render(results: &Results) -> Element<'_, Message> {
    match results {
        Results::Scalar(r) => render_scalar(r),
        Results::Series(r) => render_series(r),
        Results::Pivot(r) => render_pivot(r),
        Results::Rollup(t) => render_rollup(t, 0),
        _ => text("(unknown Results variant)").into(),
    }
}

fn render_scalar(r: &scalar::Result) -> Element<'_, Message> {
    column(
        r.values()
            .iter()
            .map(|c| text(format_cell(c)).size(28).into()),
    )
    .spacing(4)
    .into()
}

/// Series rendering: one row of `(member, value)` per x-axis member,
/// for each metric row. The member header is a text-styled button so
/// the user can drill into that member.
fn render_series(r: &series::Result) -> Element<'_, Message> {
    // For each x-axis member, emit one row per metric; the leading
    // column is a drill button on the member, the trailing columns
    // are the metric values for that x position.
    let header = row![
        text("Member").width(Length::FillPortion(2)),
        text("Value").width(Length::FillPortion(3)),
    ];

    let body_rows = r.x().iter().enumerate().map(|(i, x)| {
        let label = format!("{}", x.path);
        let mut value_text = String::new();
        for series_row in r.rows() {
            if !value_text.is_empty() {
                value_text.push_str("  ");
            }
            if let Some(cell) = series_row.values.get(i) {
                value_text.push_str(&format_cell(cell));
            }
        }
        row![
            drill_button(label, x.clone()).width(Length::FillPortion(2)),
            text(value_text).width(Length::FillPortion(3)),
        ]
        .into()
    });

    column![header, Column::with_children(body_rows).spacing(2)]
        .spacing(6)
        .into()
}

/// Pivot rendering: one row per row-header tuple, one column per
/// col-header tuple. Row headers drill on their first member.
fn render_pivot(r: &pivot::Result) -> Element<'_, Message> {
    // Header row: a leading blank cell, then one cell per column header.
    let mut header_children: Vec<Element<'_, Message>> =
        vec![text("").width(Length::FillPortion(2)).into()];
    for col in r.col_headers() {
        header_children.push(text(format_tuple(col)).width(Length::FillPortion(3)).into());
    }
    let header = iced::widget::Row::with_children(header_children).spacing(8);

    let body_rows = r
        .row_headers()
        .iter()
        .zip(r.cells().iter())
        .map(|(h, cells)| {
            let label = format_tuple(h);
            let mut children: Vec<Element<'_, Message>> = match h.members().first().cloned() {
                Some(member) => vec![
                    drill_button(label, member)
                        .width(Length::FillPortion(2))
                        .into(),
                ],
                None => vec![text(label).width(Length::FillPortion(2)).into()],
            };
            for cell in cells {
                children.push(text(format_cell(cell)).width(Length::FillPortion(3)).into());
            }
            iced::widget::Row::with_children(children).spacing(8).into()
        });

    let body = Column::with_children(body_rows).spacing(2);
    scrollable(column![header, body].spacing(6))
        .width(Length::Fill)
        .into()
}

fn render_rollup(tree: &rollup::Tree, depth: u16) -> Element<'_, Message> {
    let indent = " ".repeat(depth as usize * 2);
    let label = format!("{indent}{}", tree.root.path);
    let head = row![
        drill_button(label, tree.root.clone()).width(Length::FillPortion(3)),
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

/// A text-styled button that fires `Message::DrillInto` on the given
/// member when pressed.
fn drill_button(label: String, member: MemberRef) -> iced::widget::Button<'static, Message> {
    button(text(label))
        .on_press(Message::DrillInto(member))
        .padding(0)
        .style(button::text)
}

/// Format a tuple as a comma-separated list of leaf paths. Drops the
/// `Dim=` qualifier — the row/column header position already names the
/// dim.
fn format_tuple(t: &Tuple) -> String {
    t.members()
        .iter()
        .map(|m| m.path.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Cell formatter — same shape hewton uses, minus the icon font.
fn format_cell(cell: &Cell) -> String {
    match cell {
        Cell::Valid {
            value,
            unit,
            format,
        } => {
            if format.as_ref().is_some_and(|f| f.as_str().contains('%')) {
                format!("{:.1}%", value * 100.0)
            } else if let Some(unit) = unit {
                format!("{} {}", human_number(*value), unit.as_str())
            } else {
                human_number(*value)
            }
        }
        Cell::Missing { .. } => "—".into(),
        Cell::Error { message } => format!("! {message}"),
        // `Cell` is `#[non_exhaustive]` — handle future states gracefully.
        _ => "?".into(),
    }
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

/// Schema-side identifiers are static, so a parse failure is a build
/// bug rather than user input — `expect` is appropriate.
fn n(s: &str) -> Name {
    Name::parse(s).expect("static meridianre identifier is valid")
}

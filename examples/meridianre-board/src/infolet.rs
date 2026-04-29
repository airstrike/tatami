//! The dashboard's [`Infolet`] enum — six pre-defined cube queries plus the
//! `Query` builder and `Results` renderer for each.
//!
//! An infolet is a function `(slicer) -> Query` paired with a one-shot
//! result renderer. The slicer parameter carries the cumulative drill
//! trail (zero or more `(dim, member)` pairs); each infolet folds it into
//! its base query at construction time.

use std::fmt;
use std::sync::Arc;

use iced::Element;
use iced::widget::{column, scrollable, text};

use sweeten::widget::gt;

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
/// produce a `sweeten::gt::Table` whose body cells fire
/// `Message::DrillInto` for the row's anchor member when clicked.
///
/// `Results` is `#[non_exhaustive]`, so the match carries a wildcard arm
/// surfacing unknown variants as a placeholder line — the renderer
/// deliberately doesn't panic on a future variant.
pub fn render(results: &Results) -> Element<'_, Message> {
    match results {
        Results::Scalar(r) => render_scalar(r),
        Results::Series(r) => render_series(r),
        Results::Pivot(r) => render_pivot(r),
        Results::Rollup(t) => render_rollup(t),
        _ => text("(unknown Results variant)").into(),
    }
}

/// Scalar rendering stays plain iced — gt is overkill for one number.
fn render_scalar(r: &scalar::Result) -> Element<'_, Message> {
    column(
        r.values()
            .iter()
            .map(|c| text(format_cell(c)).size(28).into()),
    )
    .spacing(4)
    .into()
}

/// Series rendering: stub column of x-axis members + one numeric column
/// per metric row. Body clicks drill on the row's x-axis member.
fn render_series(r: &series::Result) -> Element<'_, Message> {
    let x_members: Vec<MemberRef> = r.x().to_vec();

    let mut columns = vec![gt::Column::text("member", "Member")];
    for (i, row) in r.rows().iter().enumerate() {
        // Metric labels aren't guaranteed unique across rows; suffix the
        // index to keep `Column::id` stable for selector targeting.
        columns.push(gt::Column::numeric(format!("m{i}"), row.label.clone()));
    }

    let rows: Vec<Vec<gt::Cell>> = x_members
        .iter()
        .enumerate()
        .map(|(i, x)| {
            let mut cells = Vec::with_capacity(r.rows().len() + 1);
            cells.push(gt::Cell::text(x.path.to_string()));
            for series_row in r.rows() {
                cells.push(
                    series_row
                        .values
                        .get(i)
                        .map(to_gt_cell)
                        .unwrap_or(gt::Cell::Empty),
                );
            }
            cells
        })
        .collect();

    let drill_targets = Arc::new(x_members);
    let table = gt::Table::new(columns, rows)
        .stub_column("member")
        .on_press(gt::cells::body(), {
            let targets = Arc::clone(&drill_targets);
            move |click: gt::Click<'_>| Message::DrillInto(targets[click.coord.row].clone())
        })
        .fmt(gt::cells::body(), gt::decimal(0));

    scrollable(table).into()
}

/// Rollup rendering: DFS-flatten the tree into a two-column gt table
/// (member, value). Indent is rendered in the member text. Body clicks
/// drill on the entry's member.
fn render_rollup(tree: &rollup::Tree) -> Element<'_, Message> {
    let mut flat: Vec<FlatRollup> = Vec::new();
    flatten_rollup(tree, 0, &mut flat);

    let columns = vec![
        gt::Column::text("member", "Member"),
        gt::Column::numeric("value", "Value"),
    ];

    let rows: Vec<Vec<gt::Cell>> = flat
        .iter()
        .map(|entry| {
            let label = format!("{}{}", "  ".repeat(entry.depth), entry.member.path);
            vec![gt::Cell::text(label), to_gt_cell(&entry.value)]
        })
        .collect();

    let drill_targets: Arc<Vec<MemberRef>> = Arc::new(flat.into_iter().map(|e| e.member).collect());

    let table = gt::Table::new(columns, rows)
        .stub_column("member")
        .on_press(gt::cells::body(), {
            let targets = Arc::clone(&drill_targets);
            move |click: gt::Click<'_>| Message::DrillInto(targets[click.coord.row].clone())
        })
        .fmt(gt::cells::body(), gt::decimal(0));

    scrollable(table).into()
}

/// Pivot rendering: stub column of row-header labels + one numeric
/// column per col-header. Body clicks drill on the row tuple's first
/// member, when present; rows with empty tuples are non-clickable.
fn render_pivot(r: &pivot::Result) -> Element<'_, Message> {
    let mut columns = vec![gt::Column::text("row_header", "")];
    for (i, col) in r.col_headers().iter().enumerate() {
        // Use a positional id so two col-headers that format identically
        // (unlikely but possible) still get distinct selector keys.
        columns.push(gt::Column::numeric(format!("c{i}"), format_tuple(col)));
    }

    let rows: Vec<Vec<gt::Cell>> = r
        .row_headers()
        .iter()
        .zip(r.cells().iter())
        .map(|(h, row_cells)| {
            let mut cells = Vec::with_capacity(r.col_headers().len() + 1);
            cells.push(gt::Cell::text(format_tuple(h)));
            for cell in row_cells {
                cells.push(to_gt_cell(cell));
            }
            cells
        })
        .collect();

    // Drill anchor per row: `Some(member)` if the row tuple has a first
    // member, `None` otherwise. The on_press selector filters out the
    // `None` rows so the closure only ever sees indices it can resolve.
    let drill_targets: Arc<Vec<Option<MemberRef>>> = Arc::new(
        r.row_headers()
            .iter()
            .map(|h| h.members().first().cloned())
            .collect(),
    );

    let row_predicate = {
        let targets = Arc::clone(&drill_targets);
        move |row: usize| targets.get(row).is_some_and(Option::is_some)
    };

    let table = gt::Table::new(columns, rows)
        .stub_column("row_header")
        .on_press(gt::cells::body().rows(row_predicate), {
            let targets = Arc::clone(&drill_targets);
            move |click: gt::Click<'_>| {
                let member = targets[click.coord.row]
                    .clone()
                    .expect("row_predicate guarantees Some at this row");
                Message::DrillInto(member)
            }
        })
        .fmt(gt::cells::body(), gt::decimal(0));

    scrollable(table).into()
}

/// One entry in a DFS flattening of a [`rollup::Tree`], used to project
/// the recursive shape onto the flat `Vec<Vec<gt::Cell>>` gt expects.
struct FlatRollup {
    member: MemberRef,
    value: Cell,
    depth: usize,
}

fn flatten_rollup(tree: &rollup::Tree, depth: usize, out: &mut Vec<FlatRollup>) {
    out.push(FlatRollup {
        member: tree.root.clone(),
        value: tree.value.clone(),
        depth,
    });
    for child in &tree.children {
        flatten_rollup(child, depth + 1, out);
    }
}

/// Convert a tatami [`Cell`] to a [`gt::Cell`]. `Missing` collapses to
/// `gt::Cell::Empty` (numeric formatters render it as the empty glyph).
/// `Error` carries its message in-band as text so failures are visible
/// without a separate error channel.
fn to_gt_cell(cell: &Cell) -> gt::Cell {
    match cell {
        Cell::Valid {
            value,
            unit: _,
            format,
        } => {
            // Percent-formatted values still belong to a numeric column;
            // emit the human form as text so `gt::decimal` doesn't
            // re-format the raw fraction.
            if format.as_ref().is_some_and(|f| f.as_str().contains('%')) {
                gt::Cell::text(format!("{:.1}%", value * 100.0))
            } else {
                gt::Cell::Number(*value)
            }
        }
        Cell::Missing { .. } => gt::Cell::Empty,
        Cell::Error { message } => gt::Cell::text(format!("! {message}")),
        // `tatami::Cell` is `#[non_exhaustive]` — render unknowns as Empty.
        _ => gt::Cell::Empty,
    }
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

/// Cell formatter for the scalar variant — same shape hewton uses,
/// minus the icon font.
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

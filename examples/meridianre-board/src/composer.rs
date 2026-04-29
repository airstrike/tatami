//! Schema-blind composer — drives the board's measure / dimension /
//! hierarchy pickers and the focus-stack drill state from a
//! [`tatami::Schema`] alone. The composer holds **zero** knowledge of
//! the cube's specific schema names; point the same binary at any
//! cube and the pickers repopulate themselves.
//!
//! Two responsibilities live here:
//!
//! 1. **Top-bar pickers** — `measure`, `dim`, `hierarchy`, with
//!    cascading defaults so the first paint already fires a sane
//!    query (first measure × first dim × first hierarchy).
//! 2. **Result rendering** — a `Results → Element` projection
//!    parameterised by a `Fn(MemberRef) -> Message` click callback.
//!    Renderers carry no schema-specific knowledge.
//!
//! The slicer-trail breadcrumb chips and the drill-into-member
//! semantics that drive `focus` live in the parent [`crate::board`];
//! this module contributes only the picker view + the formatter shop.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use iced::Element;
use iced::widget::{column, pick_list, row, scrollable, text};

use sweeten::widget::gt;

use tatami::query::{MemberRef, Tuple};
use tatami::schema::{Format, Name, Schema};
use tatami::{Cell, Results, pivot, rollup, scalar, series};

use crate::board::Message;

/// Picker shape for the rows-axis projection.
///
/// Each variant maps onto a single composed [`tatami::Axes`] shape; the
/// inmem evaluator then chooses the [`Results`] variant. In particular,
/// [`AxisMode::Rollup`] composes an `Axes::Pivot { rows: Descendants(..) }`
/// with a single top-level ancestor — the inmem pivot path
/// short-circuits that into [`Results::Rollup`]
/// (`inmem/eval/query.rs:144-163`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AxisMode {
    /// One axis (rows). Renders as [`Results::Series`].
    Series,
    /// Two axes (rows × columns). Renders as [`Results::Pivot`].
    Pivot,
    /// Single hierarchy descended top-to-leaf with a single root.
    /// Renders as [`Results::Rollup`] when the rows hierarchy has
    /// exactly one top-level member; otherwise the rollup short-circuit
    /// in `inmem/eval/query.rs` falls through to a flat
    /// [`Results::Pivot`].
    Rollup,
}

impl fmt::Display for AxisMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Series => f.write_str("Series"),
            Self::Pivot => f.write_str("Pivot"),
            Self::Rollup => f.write_str("Rollup"),
        }
    }
}

/// All axis modes in pick-list order.
#[must_use]
pub fn modes() -> Vec<AxisMode> {
    vec![AxisMode::Series, AxisMode::Pivot, AxisMode::Rollup]
}

/// Top-bar pickers: `(Measure | Metric)`, `Dimension`, `Hierarchy`,
/// and the axis mode (Series / Pivot / Rollup).
///
/// Every option list is built from `Schema` only — no string literals
/// originating in this binary cross the `Name` boundary.
pub fn pickers<'a>(
    schema: &'a Schema,
    measure: Option<&'a Name>,
    dim: Option<&'a Name>,
    hierarchy: Option<&'a Name>,
    axis_mode: AxisMode,
) -> Element<'a, Message> {
    let measure_options = measure_options(schema);
    let dim_options = dim_options(schema);
    let hierarchy_options = hierarchy_options(schema, dim);

    let measure_picker = pick_list(measure.cloned(), measure_options, |n: &Name| {
        n.as_str().to_owned()
    })
    .on_select(Message::PickMeasure)
    .placeholder("(measure)")
    .padding(8);

    let dim_picker = pick_list(dim.cloned(), dim_options, |n: &Name| n.as_str().to_owned())
        .on_select(Message::PickDim)
        .placeholder("(dimension)")
        .padding(8);

    let hierarchy_picker = pick_list(hierarchy.cloned(), hierarchy_options, |n: &Name| {
        n.as_str().to_owned()
    })
    .on_select(Message::PickHierarchy)
    .placeholder("(hierarchy)")
    .padding(8);

    let axis_mode_picker = pick_list(Some(axis_mode), modes(), |m: &AxisMode| m.to_string())
        .on_select(Message::PickAxisMode)
        .placeholder("(axes)")
        .padding(8);

    row![
        measure_picker,
        dim_picker,
        hierarchy_picker,
        axis_mode_picker
    ]
    .spacing(8)
    .into()
}

/// Column-axis pickers — only meaningful when the active axis mode is
/// [`AxisMode::Pivot`]. Renders a labelled `Columns:` row carrying a
/// `(col_dim, col_hierarchy)` pair, drawn from the same `Schema` source
/// as the rows-axis pickers above.
pub fn column_pickers<'a>(
    schema: &'a Schema,
    col_dim: Option<&'a Name>,
    col_hierarchy: Option<&'a Name>,
) -> Element<'a, Message> {
    let dim_options = dim_options(schema);
    let hierarchy_options = hierarchy_options(schema, col_dim);

    let dim_picker = pick_list(col_dim.cloned(), dim_options, |n: &Name| {
        n.as_str().to_owned()
    })
    .on_select(Message::PickColDim)
    .placeholder("(column dimension)")
    .padding(8);

    let hierarchy_picker = pick_list(col_hierarchy.cloned(), hierarchy_options, |n: &Name| {
        n.as_str().to_owned()
    })
    .on_select(Message::PickColHierarchy)
    .placeholder("(column hierarchy)")
    .padding(8);

    row![text("Columns:").size(12), dim_picker, hierarchy_picker]
        .spacing(8)
        .align_y(iced::Alignment::Center)
        .into()
}

/// Names available for the measure picker — `schema.measures` first,
/// then `schema.metrics`. Order is the schema's own iteration order so
/// the first measure (or, for measure-less schemas, the first metric)
/// is the canonical default.
pub fn measure_options(schema: &Schema) -> Vec<Name> {
    schema
        .measures
        .iter()
        .map(|m| m.name.clone())
        .chain(schema.metrics.iter().map(|m| m.name.clone()))
        .collect()
}

/// Names of every dim in the schema, in declaration order.
pub fn dim_options(schema: &Schema) -> Vec<Name> {
    schema.dimensions.iter().map(|d| d.name.clone()).collect()
}

/// Names of every hierarchy on the chosen dim, or empty if no dim is
/// picked / the name doesn't resolve.
pub fn hierarchy_options(schema: &Schema, dim: Option<&Name>) -> Vec<Name> {
    let Some(dim_name) = dim else {
        return Vec::new();
    };
    schema
        .dimensions
        .iter()
        .find(|d| &d.name == dim_name)
        .map(|d| d.hierarchies.iter().map(|h| h.name.clone()).collect())
        .unwrap_or_default()
}

/// First level under `(dim, hierarchy)`. The composer's row axis
/// terminates here when `focus` is empty.
pub fn top_level(schema: &Schema, dim: &Name, hierarchy: &Name) -> Option<Name> {
    schema
        .dimensions
        .iter()
        .find(|d| &d.name == dim)?
        .hierarchies
        .iter()
        .find(|h| &h.name == hierarchy)?
        .levels
        .first()
        .map(|l| l.name.clone())
}

/// Last level under `(dim, hierarchy)`. The Rollup-axis composition
/// descends to here, so the inmem `Descendants` evaluator walks the
/// hierarchy from the top member down through every intermediate level
/// to its leaves.
pub fn leaf_level(schema: &Schema, dim: &Name, hierarchy: &Name) -> Option<Name> {
    schema
        .dimensions
        .iter()
        .find(|d| &d.name == dim)?
        .hierarchies
        .iter()
        .find(|h| &h.name == hierarchy)?
        .levels
        .last()
        .map(|l| l.name.clone())
}

/// First-of-each defaults. Returns `(measure, dim, hierarchy)` —
/// any of the three may be `None` for a degenerate schema, in which
/// case the board lands in the idle results state.
pub fn defaults(schema: &Schema) -> (Option<Name>, Option<Name>, Option<Name>) {
    let measure = measure_options(schema).into_iter().next();
    let dim = schema.dimensions.first().map(|d| d.name.clone());
    let hierarchy = dim
        .as_ref()
        .and_then(|d| hierarchy_options(schema, Some(d)).into_iter().next());
    (measure, dim, hierarchy)
}

/// Column-axis defaults — used by [`AxisMode::Pivot`]. Picks the first
/// dim that is *not* `rows_dim` (so the cross-product spans two
/// distinct dimensions); falls back to `rows_dim` itself when the
/// schema has only one dim, in which case the inmem evaluator will
/// produce a degenerate single-dim pivot rather than crashing. The
/// hierarchy default is the chosen dim's first hierarchy.
pub fn default_columns(schema: &Schema, rows_dim: Option<&Name>) -> (Option<Name>, Option<Name>) {
    let col_dim = schema
        .dimensions
        .iter()
        .find(|d| Some(&d.name) != rows_dim)
        .or_else(|| schema.dimensions.first())
        .map(|d| d.name.clone());
    let col_hierarchy = col_dim
        .as_ref()
        .and_then(|d| hierarchy_options(schema, Some(d)).into_iter().next());
    (col_dim, col_hierarchy)
}

/// Render the focus breadcrumb. The leftmost button resets to the
/// hierarchy's top level; trailing chips show each pinned member; the
/// rightmost button pops one focus level.
///
/// Chips are decorative — the only message-producing controls are
/// `Reset` (left) and `Up` (right), keeping the surface tight per
/// brief §3. Per-chip truncation is deferred.
pub fn breadcrumb<'a>(
    hierarchy_label: Option<&'a Name>,
    focus: &'a [MemberRef],
) -> Element<'a, Message> {
    let root_label = match hierarchy_label {
        Some(name) => format!("Top — {}", name.as_str()),
        None => "Top".to_owned(),
    };
    let root = iced::widget::button(text(root_label).size(12))
        .padding(4)
        .on_press(Message::FocusReset)
        .style(iced::widget::button::secondary);

    let chips = focus.iter().map(|m| {
        let label = format!("/ {}", m.path);
        iced::widget::container(text(label).size(12))
            .padding(4)
            .into()
    });

    let mut children: Vec<Element<'a, Message>> = vec![root.into()];
    children.extend(chips);

    if !focus.is_empty() {
        let up = iced::widget::button(text("\u{00d7} Up").size(12))
            .padding(4)
            .on_press(Message::FocusUp)
            .style(iced::widget::button::secondary);
        children.push(up.into());
    }

    iced::widget::Row::with_children(children).spacing(6).into()
}

/// Render the slicer trail as drill-up chips, identical in semantics
/// to the v1 board: each chip shows `dim = path` and pops itself when
/// clicked.
pub fn slicer_trail<'a>(slicer: &'a [(Name, MemberRef)]) -> Element<'a, Message> {
    if slicer.is_empty() {
        return text("(no slicer — full cube)").size(12).into();
    }
    let chips: Vec<Element<'a, Message>> = slicer
        .iter()
        .enumerate()
        .map(|(i, (dim, member))| {
            let label = format!("{} = {}  \u{00d7}", dim.as_str(), member.path);
            iced::widget::button(text(label).size(12))
                .padding(4)
                .on_press(Message::PopSlicer(i))
                .style(iced::widget::button::secondary)
                .into()
        })
        .collect();
    iced::widget::Row::with_children(chips).spacing(6).into()
}

/// Render `Results` for a tile, wiring body-cell clicks through
/// `on_drill`. The renderer is schema-blind: it sees only the
/// `Results` and the click callback.
///
/// `Results` is `#[non_exhaustive]`; unknown variants render as a
/// single placeholder line rather than panicking.
pub fn render<'a>(
    results: &'a Results,
    on_drill: impl Fn(MemberRef) -> Message + Clone + 'static,
) -> Element<'a, Message> {
    match results {
        Results::Scalar(r) => render_scalar(r),
        Results::Series(r) => render_series(r, on_drill),
        Results::Pivot(r) => render_pivot(r, on_drill),
        Results::Rollup(t) => render_rollup(t, on_drill),
        _ => text("(unknown Results variant)").into(),
    }
}

/// Stand-in for the "leaf — no further levels" result rendered when a
/// drill click lands on a member with no children. Lives in the
/// composer because it needs to sit where `render` would.
pub fn leaf_placeholder<'a>() -> Element<'a, Message> {
    column![text("Leaf — no further levels.").size(14)]
        .spacing(8)
        .padding(16)
        .into()
}

// --- renderers ---------------------------------------------------------------

fn render_scalar<'a>(r: &'a scalar::Result) -> Element<'a, Message> {
    column(
        r.values()
            .iter()
            .map(|c| text(format_cell(c)).size(28).into()),
    )
    .spacing(4)
    .into()
}

fn render_series<'a>(
    r: &'a series::Result,
    on_drill: impl Fn(MemberRef) -> Message + Clone + 'static,
) -> Element<'a, Message> {
    let x_members: Vec<MemberRef> = r.x().to_vec();

    let mut columns = vec![gt::Column::text("member", "Member")];
    for (i, sr) in r.rows().iter().enumerate() {
        // Series rows aren't guaranteed unique by label; suffix with
        // the index to keep `Column::id` stable for selector targeting.
        columns.push(gt::Column::numeric(format!("m{i}"), sr.label.clone()));
    }

    let rows: Vec<Vec<gt::Cell>> = x_members
        .iter()
        .enumerate()
        .map(|(i, x)| {
            let mut cells = Vec::with_capacity(r.rows().len() + 1);
            cells.push(gt::Cell::text(x.path.to_string()));
            for sr in r.rows() {
                cells.push(sr.values.get(i).map(to_gt_cell).unwrap_or(gt::Cell::Empty));
            }
            cells
        })
        .collect();

    let drill_targets = Arc::new(x_members);
    // Register the global default first; per-metric formatters land
    // afterwards on a more specific selector. gt's resolver walks the
    // formatter list in registration order and the LAST match wins, so
    // per-column overrides must come after the broad fallback.
    let mut table = gt::Table::new(columns, rows)
        .stub_column("member")
        .on_press(gt::cells::body(), {
            let targets = Arc::clone(&drill_targets);
            let drill = on_drill.clone();
            move |click: gt::Click<'_>| drill(targets[click.coord.row].clone())
        })
        .fmt(gt::cells::body(), gt::decimal(0));
    for (i, sr) in r.rows().iter().enumerate() {
        let hint = sr.values.iter().find_map(|c| match c {
            Cell::Valid { format, .. } => format.as_ref(),
            _ => None,
        });
        table = table.fmt(
            gt::cells::body().columns([format!("m{i}")]),
            formatter_for(hint),
        );
    }

    scrollable(table).into()
}

fn render_rollup<'a>(
    tree: &'a rollup::Tree,
    on_drill: impl Fn(MemberRef) -> Message + Clone + 'static,
) -> Element<'a, Message> {
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

    // Sample the first valid cell to settle on a value-column formatter.
    // Cloned because `flat` gets consumed below to build the click
    // anchors.
    let value_hint: Option<Format> = flat.iter().find_map(|e| match &e.value {
        Cell::Valid { format, .. } => format.clone(),
        _ => None,
    });

    let drill_targets: Arc<Vec<MemberRef>> = Arc::new(flat.into_iter().map(|e| e.member).collect());

    let table = gt::Table::new(columns, rows)
        .stub_column("member")
        .on_press(gt::cells::body(), {
            let targets = Arc::clone(&drill_targets);
            let drill = on_drill.clone();
            move |click: gt::Click<'_>| drill(targets[click.coord.row].clone())
        })
        .fmt(gt::cells::body(), gt::decimal(0))
        .fmt(
            gt::cells::body().columns(["value"]),
            formatter_for(value_hint.as_ref()),
        );

    scrollable(table).into()
}

fn render_pivot<'a>(
    r: &'a pivot::Result,
    on_drill: impl Fn(MemberRef) -> Message + Clone + 'static,
) -> Element<'a, Message> {
    let n_cols = r.col_headers().len();

    let mut columns = vec![gt::Column::text("row_header", "")];
    for (i, col) in r.col_headers().iter().enumerate() {
        // Positional id keeps the selector key distinct even when two
        // headers format identically.
        columns.push(gt::Column::numeric(format!("c{i}"), format_tuple(col)));
    }

    // Bucket original rows by the leading-segment key on each row's
    // first member (its parent in the hierarchy). A `None` key
    // captures rows we couldn't summarise (empty tuple, single-segment
    // path) so data is never silently dropped — the renderer only
    // emits a region-label drill target when the key is `Some`.
    let mut buckets: BTreeMap<Option<Name>, Vec<usize>> = BTreeMap::new();
    for (i, header) in r.row_headers().iter().enumerate() {
        let parent: Option<Name> = header.members().first().map(|m| m.path.head().clone());
        buckets.entry(parent).or_default().push(i);
    }

    let mut body_rows: Vec<Vec<gt::Cell>> = Vec::with_capacity(r.row_headers().len());
    let mut drill_targets: Vec<Option<MemberRef>> = Vec::with_capacity(r.row_headers().len());
    let mut row_groups: Vec<gt::RowGroup> = Vec::with_capacity(buckets.len());
    let mut summary_rows: Vec<gt::SummaryRow> = Vec::with_capacity(buckets.len());
    let mut group_targets: HashMap<String, MemberRef> = HashMap::with_capacity(buckets.len());

    for (parent, original_indices) in &buckets {
        let group_id = match parent {
            Some(name) => name.as_str().to_owned(),
            None => "ungrouped".to_owned(),
        };
        let group_label = group_id.clone();

        let mut new_indices: Vec<usize> = Vec::with_capacity(original_indices.len());
        // Per-column running sums, with a parallel `valid` counter so
        // an all-missing column renders as Empty rather than 0.0.
        let mut col_sums: Vec<f64> = vec![0.0; n_cols];
        let mut col_valid: Vec<usize> = vec![0; n_cols];

        for &orig in original_indices {
            let new_idx = body_rows.len();
            new_indices.push(new_idx);

            let header = &r.row_headers()[orig];
            let row_cells = &r.cells()[orig];

            let mut cells = Vec::with_capacity(n_cols + 1);
            cells.push(gt::Cell::text(format_tuple(header)));
            for (col_idx, cell) in row_cells.iter().enumerate() {
                cells.push(to_gt_cell(cell));
                if let Cell::Valid { value, .. } = cell
                    && col_idx < n_cols
                {
                    col_sums[col_idx] += *value;
                    col_valid[col_idx] += 1;
                }
            }
            body_rows.push(cells);

            drill_targets.push(header.members().first().cloned());
        }

        let mut summary_cells: Vec<gt::Cell> = Vec::with_capacity(n_cols + 1);
        summary_cells.push(gt::Cell::Empty);
        for col_idx in 0..n_cols {
            if col_valid[col_idx] == 0 {
                summary_cells.push(gt::Cell::Empty);
            } else {
                summary_cells.push(gt::Cell::Number(col_sums[col_idx]));
            }
        }

        row_groups
            .push(gt::RowGroup::new(group_id.clone(), new_indices).label(group_label.clone()));
        summary_rows.push(gt::SummaryRow::group(
            group_id.clone(),
            format!("{group_label} total"),
            summary_cells,
        ));

        // Synthesise a parent-level MemberRef for region-label drill,
        // borrowing dim/hierarchy from any row in the bucket. Schemas
        // where `original_indices` is empty don't reach this branch
        // (an empty bucket isn't inserted in the loop above).
        if let Some(name) = parent
            && let Some(&first_orig) = original_indices.first()
            && let Some(member) = r.row_headers()[first_orig].members().first()
        {
            let parent_member = MemberRef::new(
                member.dim.clone(),
                member.hierarchy.clone(),
                tatami::Path::of(name.clone()),
            );
            group_targets.insert(group_id, parent_member);
        }
    }

    let drill_targets: Arc<Vec<Option<MemberRef>>> = Arc::new(drill_targets);
    let group_targets: Arc<HashMap<String, MemberRef>> = Arc::new(group_targets);

    let row_predicate = {
        let targets = Arc::clone(&drill_targets);
        move |row: usize| targets.get(row).is_some_and(Option::is_some)
    };

    // Group-label clicks register before the body fallback so a click
    // on the group header resolves to the synthesised parent member
    // rather than dropping into the body handler. Groups without a
    // synthesised target (empty-tuple rows) fall through to the body
    // handler — `row_predicate` filters those out.
    let body_anchor: Option<MemberRef> = drill_targets.iter().find_map(|m| m.clone());
    let table = gt::Table::new(columns, body_rows)
        .stub_column("row_header")
        .row_groups(row_groups)
        .summary_rows(summary_rows)
        .on_press(gt::cells::row_group_labels(), {
            let region_targets = Arc::clone(&group_targets);
            let drill = on_drill.clone();
            let body_anchor = body_anchor.clone();
            move |click: gt::Click<'_>| {
                let member = click
                    .coord
                    .group
                    .and_then(|gid| region_targets.get(gid).cloned())
                    .or_else(|| body_anchor.clone());
                match member {
                    Some(m) => drill(m),
                    None => Message::FocusUp,
                }
            }
        })
        .on_press(gt::cells::body().rows(row_predicate), {
            let targets = Arc::clone(&drill_targets);
            let drill = on_drill.clone();
            move |click: gt::Click<'_>| {
                let member = targets[click.coord.row]
                    .clone()
                    .expect("row_predicate guarantees Some at this row");
                drill(member)
            }
        })
        .fmt(
            gt::cells::body().union(gt::cells::summary()),
            gt::decimal(0),
        );

    // Pivot today carries one metric across every `c{i}` column, so a
    // single shared formatter covers the data columns and the per-group
    // summary row. Sample any valid cell for the cube's format hint.
    let pivot_hint = r.cells().iter().flatten().find_map(|c| match c {
        Cell::Valid { format, .. } => format.as_ref(),
        _ => None,
    });
    let data_col_ids: Vec<String> = (0..n_cols).map(|i| format!("c{i}")).collect();
    let table = table.fmt(
        gt::cells::body()
            .columns(data_col_ids.clone())
            .union(gt::cells::summary().columns(data_col_ids)),
        formatter_for(pivot_hint),
    );

    scrollable(table).into()
}

// --- shared helpers ----------------------------------------------------------

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

fn to_gt_cell(cell: &Cell) -> gt::Cell {
    match cell {
        Cell::Valid { value, .. } => gt::Cell::Number(*value),
        Cell::Missing { .. } => gt::Cell::Empty,
        Cell::Error { message } => gt::Cell::text(format!("! {message}")),
        // `tatami::Cell` is `#[non_exhaustive]` — render unknowns as Empty.
        _ => gt::Cell::Empty,
    }
}

/// Map a `Format` hint to a `gt::Formatter`. See the deleted `infolet`
/// module's notes for the reasoning — `"0.0%"` → percent(1), `"0"` →
/// decimal(0), no hint → decimal(0). Currency / scaled formats remain
/// deferred until the cube publishes a richer contract.
fn formatter_for(hint: Option<&Format>) -> gt::Formatter {
    let Some(format) = hint else {
        return gt::decimal(0);
    };
    let s = format.as_str().trim();
    if let Some(rest) = s.strip_suffix('%') {
        return gt::percent(decimals_in(rest));
    }
    if s.starts_with('0') {
        return gt::decimal(decimals_in(s));
    }
    gt::decimal(0)
}

fn decimals_in(pattern: &str) -> u8 {
    pattern
        .split_once('.')
        .map(|(_, frac)| {
            frac.chars()
                .take_while(|c| *c == '0')
                .count()
                .min(u8::MAX as usize) as u8
        })
        .unwrap_or(0)
}

fn format_tuple(t: &Tuple) -> String {
    t.members()
        .iter()
        .map(|m| m.path.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

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

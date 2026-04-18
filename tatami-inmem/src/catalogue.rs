//! Member catalogue — the in-memory, per-hierarchy tree of distinct members
//! discovered at [`super::InMemoryCube::new`] time.
//!
//! Phase 5b of MAP_PLAN.md §5. The catalogue powers [`super::InMemoryCube`]'s
//! implementation of [`tatami::Cube::members`] without revisiting the fact
//! frame at query time: build once, then every navigation query is O(depth ×
//! log branching).
//!
//! One [`Node`] tree per `(dim, hierarchy)` pair; the root is virtual and its
//! [`Node::children`] are the top-level members.

use std::collections::{BTreeMap, HashMap};

use polars_core::prelude::{AnyValue, DataFrame};
use tatami::query::Path;
use tatami::schema::{Dimension, Name, Schema};
use tatami::{MemberRef, MemberRelation};

use crate::Error;

/// Per-hierarchy tree of distinct members.
///
/// Outer key is the `(dim, hierarchy)` pair because the same dimension can
/// host multiple hierarchies (e.g., Time's `"Fiscal"` vs `"Gregorian"`).
#[derive(Debug, Default)]
pub(crate) struct Catalogue {
    by_hierarchy: HashMap<(Name, Name), Node>,
}

/// A node in a per-hierarchy member tree.
///
/// Children are keyed by the member's own level-value (a [`Name`]) and stored
/// in a [`BTreeMap`] so iteration order is deterministic — snapshot tests and
/// later [`tatami::query::Set::Members`] enumeration depend on stable
/// ordering.
#[derive(Debug, Default)]
pub(crate) struct Node {
    children: BTreeMap<Name, Node>,
}

impl Catalogue {
    /// Build a catalogue by scanning `df` once per dimension-hierarchy pair.
    ///
    /// Assumes [`super::validate`] has already checked that every level-key
    /// column exists with a discrete dtype — this function reads cells
    /// without re-validating the schema.
    ///
    /// Rows with a null cell at some level skip that hierarchy only; they
    /// cannot be placed on a partial path without fabricating a fake child.
    /// Rows with a non-null cell whose value fails [`Name::parse`] (e.g.,
    /// empty string after a cast) surface as [`Error::MalformedMemberValue`].
    pub(crate) fn build(df: &DataFrame, schema: &Schema) -> Result<Self, Error> {
        let mut by_hierarchy: HashMap<(Name, Name), Node> = HashMap::new();
        for dim in &schema.dimensions {
            for hierarchy in &dim.hierarchies {
                let root = build_tree(df, dim, hierarchy.levels.as_slice())?;
                by_hierarchy.insert((dim.name.clone(), hierarchy.name.clone()), root);
            }
        }
        Ok(Self { by_hierarchy })
    }

    /// Implement [`tatami::Cube::members`] against this catalogue.
    pub(crate) fn members(
        &self,
        dim: &Name,
        hierarchy: &Name,
        at: &MemberRef,
        relation: MemberRelation,
    ) -> Result<Vec<MemberRef>, Error> {
        // Check hierarchy existence before checking ref alignment — a bad
        // `(dim, hierarchy)` arg is the more fundamental user error.
        let key = (dim.clone(), hierarchy.clone());
        let root = self
            .by_hierarchy
            .get(&key)
            .ok_or_else(|| Error::UnknownHierarchy {
                dim: dim.clone(),
                hierarchy: hierarchy.clone(),
            })?;

        if at.dim != *dim || at.hierarchy != *hierarchy {
            return Err(Error::MemberRefHierarchyMismatch {
                expected_dim: dim.clone(),
                expected_hierarchy: hierarchy.clone(),
                actual_dim: at.dim.clone(),
                actual_hierarchy: at.hierarchy.clone(),
            });
        }

        let segments: Vec<Name> = at.path.segments().cloned().collect();
        let node = walk(root, &segments).ok_or_else(|| Error::UnknownMember {
            dim: dim.clone(),
            hierarchy: hierarchy.clone(),
            path: at.path.clone(),
        })?;

        match relation {
            MemberRelation::Children => Ok(children(node, dim, hierarchy, &segments)),
            MemberRelation::Descendants(depth) => {
                let mut out = Vec::new();
                collect_descendants(node, dim, hierarchy, &segments, depth, &mut out);
                Ok(out)
            }
            MemberRelation::Siblings => {
                // `segments` is non-empty because `Path` is non-empty.
                let (last, parent_segments) = split_last(&segments);
                let parent_node = walk(root, parent_segments).expect(
                    "parent path must exist — we just walked to its child via the same tree",
                );
                let mut out = children(parent_node, dim, hierarchy, parent_segments);
                out.retain(|m| m.path.segments().last() != Some(last));
                Ok(out)
            }
            MemberRelation::Parent => {
                if segments.len() <= 1 {
                    return Ok(Vec::new());
                }
                let parent_segments = &segments[..segments.len() - 1];
                Ok(vec![member_ref(dim, hierarchy, parent_segments)])
            }
            MemberRelation::Leaves => {
                let mut out = Vec::new();
                collect_leaves(node, dim, hierarchy, &segments, &mut out);
                Ok(out)
            }
            // `MemberRelation` is `#[non_exhaustive]`; surface the unknown
            // variant cleanly instead of panicking.
            other => Err(Error::UnsupportedRelation(other)),
        }
    }

    /// Whether the given `(dim, hierarchy, path)` locates a real node in
    /// this catalogue.
    ///
    /// Used by Phase 5c's `resolve::resolve_member_ref` to verify that
    /// [`MemberRef`] instances carried by queries point at members the
    /// backing fact source has actually observed. `None` if the `(dim,
    /// hierarchy)` pair isn't catalogued at all — the caller distinguishes
    /// that from "hierarchy exists but path doesn't".
    pub(crate) fn path_exists(&self, dim: &Name, hierarchy: &Name, path: &Path) -> Option<bool> {
        let key = (dim.clone(), hierarchy.clone());
        let root = self.by_hierarchy.get(&key)?;
        let segments: Vec<&Name> = path.segments().collect();
        let mut cursor = root;
        for seg in segments {
            match cursor.children.get(seg) {
                Some(child) => cursor = child,
                None => return Some(false),
            }
        }
        Some(true)
    }

    /// Every member at `level_index` (0-based from the root) of a given
    /// `(dim, hierarchy)`. Returns `None` if the pair is not catalogued.
    ///
    /// `level_index = 0` means the root-level members (direct children of the
    /// virtual root); `level_index = 1` means their children; and so on.
    ///
    /// Pre-order DFS so ancestors precede descendants — this is the order
    /// Phase 5d's `Set::Members` evaluation will want. `#[allow(dead_code)]`
    /// keeps the build warning-free until 5d wires this in.
    #[allow(dead_code)]
    pub(crate) fn members_at(
        &self,
        dim: &Name,
        hierarchy: &Name,
        level_index: usize,
    ) -> Option<Vec<MemberRef>> {
        let key = (dim.clone(), hierarchy.clone());
        let root = self.by_hierarchy.get(&key)?;
        let mut out = Vec::new();
        collect_at_depth(root, dim, hierarchy, &[], level_index, &mut out);
        Some(out)
    }
}

// ── Construction ───────────────────────────────────────────────────────────

/// Walk every row of `df` and insert the corresponding path into a fresh
/// tree for the given `levels` (in order, root-to-leaf).
fn build_tree(
    df: &DataFrame,
    dim: &Dimension,
    levels: &[tatami::schema::Level],
) -> Result<Node, Error> {
    let mut root = Node::default();
    if levels.is_empty() {
        return Ok(root);
    }

    // Resolve the column per level once. Phase 5a's `validate` guarantees
    // every column exists with a discrete dtype, so `column` succeeds.
    let columns: Vec<_> = levels
        .iter()
        .map(|level| {
            df.column(level.key.as_str())
                .expect("phase 5a validation guarantees every level column exists")
        })
        .collect();

    let height = df.height();
    for row in 0..height {
        let mut cursor = &mut root;
        let mut abort_row = false;
        for (level, column) in levels.iter().zip(columns.iter()) {
            let any = column
                .get(row)
                .expect("row index is within column height; validated above");
            let raw = match cell_to_string(&any) {
                CellValue::Null => {
                    // Null at this level — the row can't produce a full path
                    // beyond here for this hierarchy. Skip it.
                    abort_row = true;
                    break;
                }
                CellValue::Text(s) => s,
            };
            let name = Name::parse(&raw).map_err(|_| Error::MalformedMemberValue {
                dim: dim.name.clone(),
                level: level.name.clone(),
                value: raw,
            })?;
            cursor = cursor.children.entry(name).or_default();
        }
        // `abort_row` just stops walking this row; the next row gets a fresh
        // cursor from `root`.
        let _ = abort_row;
    }
    Ok(root)
}

/// Extract a discrete cell value as a plain string, preserving string values
/// without the quotes that `AnyValue`'s `Display` impl would add.
///
/// Only the dtypes accepted by Phase 5a's `Class::is_discrete` produce a
/// `Text` result; others fall through to `Display`, which is a safety net —
/// validation should have rejected them upstream.
fn cell_to_string(av: &AnyValue<'_>) -> CellValue {
    match av {
        AnyValue::Null => CellValue::Null,
        AnyValue::String(s) => CellValue::Text((*s).to_owned()),
        AnyValue::StringOwned(s) => CellValue::Text(s.to_string()),
        AnyValue::UInt8(v) => CellValue::Text(v.to_string()),
        AnyValue::UInt16(v) => CellValue::Text(v.to_string()),
        AnyValue::UInt32(v) => CellValue::Text(v.to_string()),
        AnyValue::UInt64(v) => CellValue::Text(v.to_string()),
        AnyValue::Int8(v) => CellValue::Text(v.to_string()),
        AnyValue::Int16(v) => CellValue::Text(v.to_string()),
        AnyValue::Int32(v) => CellValue::Text(v.to_string()),
        AnyValue::Int64(v) => CellValue::Text(v.to_string()),
        // Fallback — defensive only; Phase 5a rejects non-discrete dtypes as
        // level keys so reaching here implies a future dtype we haven't
        // classified yet.
        other => CellValue::Text(other.to_string()),
    }
}

enum CellValue {
    Null,
    Text(String),
}

// ── Navigation helpers ─────────────────────────────────────────────────────

/// Descend `root` along `segments`, returning the reached node or `None` if
/// any segment is missing.
fn walk<'a>(root: &'a Node, segments: &[Name]) -> Option<&'a Node> {
    let mut cursor = root;
    for segment in segments {
        cursor = cursor.children.get(segment)?;
    }
    Some(cursor)
}

/// Assemble a [`MemberRef`] for `(dim, hierarchy, segments)`. `segments` must
/// be non-empty — paths in `MemberRef` always have at least one segment.
fn member_ref(dim: &Name, hierarchy: &Name, segments: &[Name]) -> MemberRef {
    let path = Path::parse(segments.to_vec()).expect("caller passes a non-empty segment slice");
    MemberRef::new(dim.clone(), hierarchy.clone(), path)
}

/// Direct children of `node`, in BTreeMap order.
fn children(node: &Node, dim: &Name, hierarchy: &Name, at: &[Name]) -> Vec<MemberRef> {
    node.children
        .keys()
        .map(|child| {
            let mut segs = at.to_vec();
            segs.push(child.clone());
            member_ref(dim, hierarchy, &segs)
        })
        .collect()
}

/// Pre-order DFS down to `remaining` levels below `node`. `remaining == 0`
/// emits nothing (the `at` member itself is not returned by the public API).
fn collect_descendants(
    node: &Node,
    dim: &Name,
    hierarchy: &Name,
    at: &[Name],
    remaining: u8,
    out: &mut Vec<MemberRef>,
) {
    if remaining == 0 {
        return;
    }
    for (child_name, child_node) in &node.children {
        let mut segs = at.to_vec();
        segs.push(child_name.clone());
        out.push(member_ref(dim, hierarchy, &segs));
        collect_descendants(child_node, dim, hierarchy, &segs, remaining - 1, out);
    }
}

/// Pre-order DFS collecting every leaf below `node`. A leaf is a node with
/// no children. If `node` itself has no children, nothing is emitted — the
/// `at` member is never returned by the public API.
fn collect_leaves(
    node: &Node,
    dim: &Name,
    hierarchy: &Name,
    at: &[Name],
    out: &mut Vec<MemberRef>,
) {
    for (child_name, child_node) in &node.children {
        let mut segs = at.to_vec();
        segs.push(child_name.clone());
        if child_node.children.is_empty() {
            out.push(member_ref(dim, hierarchy, &segs));
        } else {
            collect_leaves(child_node, dim, hierarchy, &segs, out);
        }
    }
}

/// Pre-order DFS collecting every member at exactly `depth` levels below
/// `node`. `depth == 0` emits each direct child of `node` keyed under its
/// own name; the caller controls `at` to supply the path prefix.
#[allow(dead_code)]
fn collect_at_depth(
    node: &Node,
    dim: &Name,
    hierarchy: &Name,
    at: &[Name],
    depth: usize,
    out: &mut Vec<MemberRef>,
) {
    if depth == 0 {
        for child_name in node.children.keys() {
            let mut segs = at.to_vec();
            segs.push(child_name.clone());
            out.push(member_ref(dim, hierarchy, &segs));
        }
        return;
    }
    for (child_name, child_node) in &node.children {
        let mut segs = at.to_vec();
        segs.push(child_name.clone());
        collect_at_depth(child_node, dim, hierarchy, &segs, depth - 1, out);
    }
}

/// Split the last segment off a non-empty slice.
fn split_last(segments: &[Name]) -> (&Name, &[Name]) {
    let (last, rest) = segments
        .split_last()
        .expect("caller guarantees a non-empty slice — Path is non-empty by shape");
    (last, rest)
}

//! Rollup-shape [`Tree`] — a recursive tree of `(MemberRef, Cell, children)`.
//!
//! Public fields: unlike the other result shapes this type has no
//! non-structural invariant to enforce — it's genuinely recursive data.
//! Lives at `tatami::rollup::Tree` via the crate-root re-export (no
//! composite `RollupTree` name per `RUST_STYLE.md`).

use serde::{Deserialize, Serialize};

use crate::Cell;
use crate::query::MemberRef;

/// Rollup-shape result: a tree keyed by `MemberRef`, each node carrying
/// the aggregated [`Cell`] at that member and the child subtrees.
///
/// Shape mirrors the `Set::Descendants` hierarchy that produced it — the
/// root is the descendants' starting member and leaves live at the
/// configured depth.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Tree {
    /// The member this node represents.
    pub root: MemberRef,
    /// The aggregated cell value at `root`.
    pub value: Cell,
    /// Child subtrees in hierarchy order.
    pub children: Vec<Tree>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::Path;
    use crate::schema::Name;

    fn mr(head: &str) -> MemberRef {
        MemberRef::new(
            Name::parse("Geography").expect("valid"),
            Name::parse("Default").expect("valid"),
            Path::of(Name::parse(head).expect("valid")),
        )
    }

    fn leaf(head: &str, value: f64) -> Tree {
        Tree {
            root: mr(head),
            value: Cell::Valid {
                value,
                unit: None,
                format: None,
            },
            children: Vec::new(),
        }
    }

    #[test]
    fn rollup_tree_holds_nested_children() {
        let t = Tree {
            root: mr("World"),
            value: Cell::Valid {
                value: 3.0,
                unit: None,
                format: None,
            },
            children: vec![leaf("Americas", 2.0), leaf("EMEA", 1.0)],
        };
        assert_eq!(t.children.len(), 2);
    }

    #[test]
    fn rollup_tree_roundtrips_via_serde() {
        let t = Tree {
            root: mr("World"),
            value: Cell::Valid {
                value: 3.0,
                unit: None,
                format: None,
            },
            children: vec![leaf("Americas", 2.0)],
        };
        let s = serde_json::to_string(&t).expect("serialize");
        let back: Tree = serde_json::from_str(&s).expect("deserialize");
        assert_eq!(t, back);
    }
}

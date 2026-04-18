//! [`Options`] (query-level tuning knobs), [`OrderBy`], [`Direction`].

use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

use crate::schema::Name;

/// Optional query-level tuning knobs. Defaults: no ordering, no limit, empty
/// tuples kept.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Options {
    /// Orderings applied to the result rows, in priority order.
    pub order: Vec<OrderBy>,
    /// Cap on the number of rows returned. `None` means unlimited.
    pub limit: Option<NonZeroUsize>,
    /// MDX `NON EMPTY` — drop tuples whose cells are all missing.
    pub non_empty: bool,
}

/// A single ordering directive over a named metric.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderBy {
    /// The metric by which to order.
    pub metric: Name,
    /// The direction to sort.
    pub direction: Direction,
}

/// Sort direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Direction {
    /// Ascending — smallest first.
    Asc,
    /// Descending — largest first.
    Desc,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(s: &str) -> Name {
        Name::parse(s).expect("valid")
    }

    #[test]
    fn default_options_roundtrip() {
        let o = Options::default();
        let json = serde_json::to_string(&o).expect("serialize");
        let back: Options = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(o, back);
    }

    #[test]
    fn options_with_order_and_limit_roundtrip() {
        let o = Options {
            order: vec![OrderBy {
                metric: n("Revenue"),
                direction: Direction::Desc,
            }],
            limit: Some(NonZeroUsize::new(10).expect("nonzero")),
            non_empty: true,
        };
        let json = serde_json::to_string(&o).expect("serialize");
        let back: Options = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(o, back);
    }

    #[test]
    fn direction_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&Direction::Asc).expect("serialize"),
            r#""asc""#
        );
        assert_eq!(
            serde_json::to_string(&Direction::Desc).expect("serialize"),
            r#""desc""#
        );
    }
}

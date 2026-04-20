//! Pivot-shape [`Result`] — two axis headers plus a 2-D cell grid.
//!
//! Opaque fields; the only constructor is [`Result::new`]. v0.1 scaffold is
//! total — Phase 5 tightens the invariant
//! `cells.len() == row_headers.len()` and
//! `cells[i].len() == col_headers.len()` once the counts are plumbed
//! through.

use serde::{Deserialize, Serialize};

use crate::Cell;
use crate::query::Tuple;

/// Pivot-shape result: row-axis tuples, column-axis tuples, and a 2-D grid
/// of cells `cells[row][col]`.
///
/// Lives at `tatami::pivot::Result` via the crate-root re-export.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Result {
    row_headers: Vec<Tuple>,
    col_headers: Vec<Tuple>,
    cells: Vec<Vec<Cell>>,
}

impl Result {
    /// Construct a pivot result.
    ///
    /// v0.1 scaffold: total. Phase 5 enforces
    /// `cells.len() == row_headers.len()` and
    /// `cells[i].len() == col_headers.len()` for every row.
    #[must_use]
    pub fn new(row_headers: Vec<Tuple>, col_headers: Vec<Tuple>, cells: Vec<Vec<Cell>>) -> Self {
        Self {
            row_headers,
            col_headers,
            cells,
        }
    }

    /// Read-only view of the row-axis tuples.
    #[must_use]
    pub fn row_headers(&self) -> &[Tuple] {
        &self.row_headers
    }

    /// Read-only view of the column-axis tuples.
    #[must_use]
    pub fn col_headers(&self) -> &[Tuple] {
        &self.col_headers
    }

    /// Read-only view of the 2-D cell grid — outer is rows, inner is cols.
    #[must_use]
    pub fn cells(&self) -> &[Vec<Cell>] {
        &self.cells
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pivot_result_preserves_headers_and_cells() {
        let v = Result::new(
            vec![Tuple::empty()],
            vec![Tuple::empty(), Tuple::empty()],
            vec![vec![
                Cell::Valid {
                    value: 1.0,
                    unit: None,
                    format: None,
                },
                Cell::Valid {
                    value: 2.0,
                    unit: None,
                    format: None,
                },
            ]],
        );
        assert_eq!(v.row_headers().len(), 1);
        assert_eq!(v.col_headers().len(), 2);
        assert_eq!(v.cells().len(), 1);
        assert_eq!(v.cells()[0].len(), 2);
    }

    #[test]
    fn pivot_result_roundtrips_via_serde() {
        let v = Result::new(
            vec![Tuple::empty()],
            vec![Tuple::empty()],
            vec![vec![Cell::Valid {
                value: 1.0,
                unit: None,
                format: None,
            }]],
        );
        let s = serde_json::to_string(&v).expect("serialize");
        let back: Result = serde_json::from_str(&s).expect("deserialize");
        assert_eq!(v, back);
    }
}

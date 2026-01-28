// Approach C: Two-Phase Index-then-Extract Parser
//
// Phase 1: Build an index of row/field boundaries (fast scan)
// Phase 2: Extract data using the index
//
// Benefits: Better cache utilization, can skip rows, predictable memory usage

use crate::core::{extract_field, extract_field_cow, extract_field_cow_with_escape, is_separator};
use std::borrow::Cow;

/// Represents a field's position within a row
#[derive(Debug, Clone, Copy)]
pub struct FieldBound {
    pub start: usize,
    pub end: usize,
}

/// Index of all row and field boundaries in a CSV
#[derive(Debug)]
pub struct CsvIndex {
    /// (row_start, row_end) positions in the input
    #[allow(dead_code)]
    pub row_bounds: Vec<(usize, usize)>,
    /// For each row, the field boundaries relative to input start
    pub field_bounds: Vec<Vec<FieldBound>>,
}

impl CsvIndex {
    #[allow(dead_code)]
    pub fn row_count(&self) -> usize {
        self.row_bounds.len()
    }

    #[allow(dead_code)]
    pub fn field_count(&self, row: usize) -> usize {
        self.field_bounds.get(row).map(|f| f.len()).unwrap_or(0)
    }
}

/// Parse a single row and return field bounds plus next position
#[allow(dead_code)]
fn parse_row_index(input: &[u8], start: usize) -> (Vec<FieldBound>, usize) {
    parse_row_index_with_config(input, start, b',', b'"')
}

/// Parse a single row with configurable separator and escape
fn parse_row_index_with_config(
    input: &[u8],
    start: usize,
    separator: u8,
    escape: u8,
) -> (Vec<FieldBound>, usize) {
    let mut fields = Vec::new();
    let mut pos = start;
    let mut field_start = start;
    let mut in_quotes = false;

    while pos < input.len() {
        let byte = input[pos];

        if in_quotes {
            if byte == escape {
                if pos + 1 < input.len() && input[pos + 1] == escape {
                    pos += 2;
                    continue;
                }
                in_quotes = false;
            }
            pos += 1;
        } else if byte == escape {
            in_quotes = true;
            pos += 1;
        } else if byte == separator {
            fields.push(FieldBound {
                start: field_start,
                end: pos,
            });
            pos += 1;
            field_start = pos;
        } else if byte == b'\n' {
            fields.push(FieldBound {
                start: field_start,
                end: pos,
            });
            pos += 1;
            return (fields, pos);
        } else if byte == b'\r' {
            fields.push(FieldBound {
                start: field_start,
                end: pos,
            });
            pos += 1;
            if pos < input.len() && input[pos] == b'\n' {
                pos += 1;
            }
            return (fields, pos);
        } else {
            pos += 1;
        }
    }

    // End of input - add final field if any content
    if field_start < input.len() || !fields.is_empty() {
        fields.push(FieldBound {
            start: field_start,
            end: input.len(),
        });
    }

    (fields, pos)
}

/// Phase 1: Build an index of row and field boundaries
#[allow(dead_code)]
pub fn build_index(input: &[u8]) -> CsvIndex {
    build_index_with_config(input, b',', b'"')
}

/// Phase 1: Build an index with configurable separator and escape
pub fn build_index_with_config(input: &[u8], separator: u8, escape: u8) -> CsvIndex {
    let mut row_bounds = Vec::new();
    let mut field_bounds = Vec::new();
    let mut pos = 0;

    while pos < input.len() {
        let row_start = pos;
        let (fields, next_pos) = parse_row_index_with_config(input, pos, separator, escape);

        if !fields.is_empty() {
            // Calculate row end (before newline)
            let row_end = if !fields.is_empty() {
                fields.last().unwrap().end
            } else {
                pos
            };
            row_bounds.push((row_start, row_end));
            field_bounds.push(fields);
        }

        pos = next_pos;
    }

    CsvIndex {
        row_bounds,
        field_bounds,
    }
}

/// Phase 2: Extract all fields using the index (zero-copy when possible)
#[allow(dead_code)]
pub fn extract_all<'a>(input: &'a [u8], index: &CsvIndex) -> Vec<Vec<Cow<'a, [u8]>>> {
    extract_all_with_escape(input, index, b'"')
}

/// Phase 2: Extract all fields with configurable escape character
pub fn extract_all_with_escape<'a>(
    input: &'a [u8],
    index: &CsvIndex,
    escape: u8,
) -> Vec<Vec<Cow<'a, [u8]>>> {
    index
        .field_bounds
        .iter()
        .map(|row_fields| {
            row_fields
                .iter()
                .map(|bound| extract_field_cow_with_escape(input, bound.start, bound.end, escape))
                .collect()
        })
        .collect()
}

/// Phase 2: Extract all fields using the index (borrowed, no quote unescaping)
#[allow(dead_code)]
pub fn extract_all_borrowed<'a>(input: &'a [u8], index: &CsvIndex) -> Vec<Vec<&'a [u8]>> {
    index
        .field_bounds
        .iter()
        .map(|row_fields| {
            row_fields
                .iter()
                .map(|bound| extract_field(input, bound.start, bound.end))
                .collect()
        })
        .collect()
}

/// Extract a range of rows (for pagination/streaming use cases)
#[allow(dead_code)]
pub fn extract_rows<'a>(
    input: &'a [u8],
    index: &CsvIndex,
    start_row: usize,
    count: usize,
) -> Vec<Vec<Cow<'a, [u8]>>> {
    let end_row = (start_row + count).min(index.row_count());

    index.field_bounds[start_row..end_row]
        .iter()
        .map(|row_fields| {
            row_fields
                .iter()
                .map(|bound| extract_field_cow(input, bound.start, bound.end))
                .collect()
        })
        .collect()
}

/// Combined parse using two-phase approach
pub fn parse_csv_indexed(input: &[u8]) -> Vec<Vec<Cow<'_, [u8]>>> {
    parse_csv_indexed_with_config(input, b',', b'"')
}

/// Combined parse with configurable separator and escape
pub fn parse_csv_indexed_with_config(
    input: &[u8],
    separator: u8,
    escape: u8,
) -> Vec<Vec<Cow<'_, [u8]>>> {
    let index = build_index_with_config(input, separator, escape);
    extract_all_with_escape(input, &index, escape)
}

/// Parse a single row with multiple separator support and return field bounds
fn parse_row_index_multi_sep(
    input: &[u8],
    start: usize,
    separators: &[u8],
    escape: u8,
) -> (Vec<FieldBound>, usize) {
    let mut fields = Vec::new();
    let mut pos = start;
    let mut field_start = start;
    let mut in_quotes = false;

    while pos < input.len() {
        let byte = input[pos];

        if in_quotes {
            if byte == escape {
                if pos + 1 < input.len() && input[pos + 1] == escape {
                    pos += 2;
                    continue;
                }
                in_quotes = false;
            }
            pos += 1;
        } else if byte == escape {
            in_quotes = true;
            pos += 1;
        } else if is_separator(byte, separators) {
            fields.push(FieldBound {
                start: field_start,
                end: pos,
            });
            pos += 1;
            field_start = pos;
        } else if byte == b'\n' {
            fields.push(FieldBound {
                start: field_start,
                end: pos,
            });
            pos += 1;
            return (fields, pos);
        } else if byte == b'\r' {
            fields.push(FieldBound {
                start: field_start,
                end: pos,
            });
            pos += 1;
            if pos < input.len() && input[pos] == b'\n' {
                pos += 1;
            }
            return (fields, pos);
        } else {
            pos += 1;
        }
    }

    // End of input - add final field if any content
    if field_start < input.len() || !fields.is_empty() {
        fields.push(FieldBound {
            start: field_start,
            end: input.len(),
        });
    }

    (fields, pos)
}

/// Build an index with multiple separator support
pub fn build_index_multi_sep(input: &[u8], separators: &[u8], escape: u8) -> CsvIndex {
    // Optimize for single separator case
    if separators.len() == 1 {
        return build_index_with_config(input, separators[0], escape);
    }

    let mut row_bounds = Vec::new();
    let mut field_bounds = Vec::new();
    let mut pos = 0;

    while pos < input.len() {
        let row_start = pos;
        let (fields, next_pos) = parse_row_index_multi_sep(input, pos, separators, escape);

        if !fields.is_empty() {
            let row_end = if !fields.is_empty() {
                fields.last().unwrap().end
            } else {
                pos
            };
            row_bounds.push((row_start, row_end));
            field_bounds.push(fields);
        }

        pos = next_pos;
    }

    CsvIndex {
        row_bounds,
        field_bounds,
    }
}

/// Combined parse with multiple separator support
pub fn parse_csv_indexed_multi_sep<'a>(
    input: &'a [u8],
    separators: &[u8],
    escape: u8,
) -> Vec<Vec<Cow<'a, [u8]>>> {
    let index = build_index_multi_sep(input, separators, escape);
    extract_all_with_escape(input, &index, escape)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn to_strings(rows: Vec<Vec<Cow<'_, [u8]>>>) -> Vec<Vec<String>> {
        rows.into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|f| String::from_utf8_lossy(&f).to_string())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn test_build_index() {
        let input = b"a,b,c\n1,2,3\n";
        let index = build_index(input);
        assert_eq!(index.row_count(), 2);
        assert_eq!(index.field_count(0), 3);
        assert_eq!(index.field_count(1), 3);
    }

    #[test]
    fn test_extract_all() {
        let input = b"a,b,c\n1,2,3\n";
        let index = build_index(input);
        let rows = to_strings(extract_all(input, &index));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec!["a", "b", "c"]);
        assert_eq!(rows[1], vec!["1", "2", "3"]);
    }

    #[test]
    fn test_quoted_fields() {
        let input = b"a,\"b,c\",d\n";
        let index = build_index(input);
        let rows = to_strings(extract_all(input, &index));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec!["a", "b,c", "d"]);
    }

    #[test]
    fn test_escaped_quotes() {
        let input = b"a,\"say \"\"hi\"\"\",c\n";
        let index = build_index(input);
        let rows = to_strings(extract_all(input, &index));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec!["a", "say \"hi\"", "c"]);
    }

    #[test]
    fn test_no_trailing_newline() {
        let input = b"a,b\nc,d";
        let index = build_index(input);
        let rows = to_strings(extract_all(input, &index));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec!["a", "b"]);
        assert_eq!(rows[1], vec!["c", "d"]);
    }

    #[test]
    fn test_extract_rows_range() {
        let input = b"a,b\nc,d\ne,f\n";
        let index = build_index(input);
        let rows = to_strings(extract_rows(input, &index, 1, 1));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec!["c", "d"]);
    }

    #[test]
    fn test_parse_csv_indexed() {
        let input = b"a,b,c\n1,2,3\n";
        let rows = to_strings(parse_csv_indexed(input));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec!["a", "b", "c"]);
    }
}

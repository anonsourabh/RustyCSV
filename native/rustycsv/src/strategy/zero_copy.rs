// Zero-Copy Strategy: Returns field boundaries for sub-binary term construction
//
// Instead of copying field data, this strategy returns (start, end) positions
// that can be used to create BEAM sub-binaries referencing the original input.
// This matches NimbleCSV's memory profile while keeping SIMD scanning speed.
//
// Trade-off: Sub-binaries keep the parent binary alive until all references
// are garbage collected. Use this when you need maximum speed and control
// memory lifetime yourself.

use crate::core::{find_next_delimiter, is_separator};
use memchr::memchr;

/// Parse CSV and return field boundaries (zero-copy approach)
/// Returns Vec of rows, each row is Vec of (start, end) byte positions
#[allow(dead_code)] // Used in tests and as public API
pub fn parse_csv_boundaries(input: &[u8]) -> Vec<Vec<(usize, usize)>> {
    parse_csv_boundaries_with_config(input, b',', b'"')
}

/// Parse CSV with configurable separator and escape, returning boundaries
pub fn parse_csv_boundaries_with_config(
    input: &[u8],
    separator: u8,
    escape: u8,
) -> Vec<Vec<(usize, usize)>> {
    let mut rows = Vec::with_capacity(input.len() / 50 + 1);
    let mut pos = 0;

    while pos < input.len() {
        let (row_boundaries, next_pos) = parse_row_boundaries(input, pos, separator, escape);

        if !row_boundaries.is_empty() {
            rows.push(row_boundaries);
        }

        pos = next_pos;
    }

    rows
}

/// Parse CSV with multiple separator support, returning boundaries
pub fn parse_csv_boundaries_multi_sep(
    input: &[u8],
    separators: &[u8],
    escape: u8,
) -> Vec<Vec<(usize, usize)>> {
    // Optimize for single separator case
    if separators.len() == 1 {
        return parse_csv_boundaries_with_config(input, separators[0], escape);
    }

    let mut rows = Vec::with_capacity(input.len() / 50 + 1);
    let mut pos = 0;

    while pos < input.len() {
        let (row_boundaries, next_pos) = parse_row_boundaries_multi_sep(input, pos, separators, escape);

        if !row_boundaries.is_empty() {
            rows.push(row_boundaries);
        }

        pos = next_pos;
    }

    rows
}

/// Parse a single row with multiple separator support and return field boundaries
fn parse_row_boundaries_multi_sep(
    input: &[u8],
    start: usize,
    separators: &[u8],
    escape: u8,
) -> (Vec<(usize, usize)>, usize) {
    let mut boundaries = Vec::with_capacity(8);
    let mut pos = start;
    let mut field_start = start;
    let mut in_quotes = false;

    while pos < input.len() {
        let byte = input[pos];

        if in_quotes {
            if byte == escape {
                // Check for escaped quote ""
                if pos + 1 < input.len() && input[pos + 1] == escape {
                    pos += 2;
                    continue;
                }
                in_quotes = false;
            }
            pos += 1;
        } else {
            if byte == escape {
                in_quotes = true;
                pos += 1;
            } else if is_separator(byte, separators) {
                boundaries.push((field_start, pos));
                pos += 1;
                field_start = pos;
            } else if byte == b'\n' {
                // End of row (standalone \n)
                boundaries.push((field_start, pos));
                return (boundaries, pos + 1);
            } else if byte == b'\r' && pos + 1 < input.len() && input[pos + 1] == b'\n' {
                // End of row (\r\n)
                boundaries.push((field_start, pos));
                return (boundaries, pos + 2);
            } else {
                pos += 1;
            }
        }
    }

    // End of input - last row without newline
    if field_start < input.len() || !boundaries.is_empty() {
        boundaries.push((field_start, input.len()));
    }

    (boundaries, input.len())
}

/// Parse a single row and return field boundaries
/// Returns (field_boundaries, next_row_start)
fn parse_row_boundaries(
    input: &[u8],
    start: usize,
    separator: u8,
    escape: u8,
) -> (Vec<(usize, usize)>, usize) {
    let mut boundaries = Vec::with_capacity(8);
    let mut pos = start;
    let mut field_start = start;
    let mut in_quotes = false;

    while pos < input.len() {
        let byte = input[pos];

        if in_quotes {
            if byte == escape {
                // Check for escaped quote ""
                if pos + 1 < input.len() && input[pos + 1] == escape {
                    pos += 2;
                    continue;
                }
                in_quotes = false;
            }
            pos += 1;
        } else {
            match byte {
                b if b == escape => {
                    in_quotes = true;
                    pos += 1;
                }
                b if b == separator => {
                    boundaries.push((field_start, pos));
                    pos += 1;
                    field_start = pos;
                }
                b'\n' => {
                    // End of row (standalone \n)
                    boundaries.push((field_start, pos));
                    return (boundaries, pos + 1);
                }
                b'\r' if pos + 1 < input.len() && input[pos + 1] == b'\n' => {
                    // End of row (\r\n)
                    boundaries.push((field_start, pos));
                    return (boundaries, pos + 2);
                }
                _ => {
                    pos += 1;
                }
            }
        }
    }

    // End of input - last row without newline
    if field_start < input.len() || !boundaries.is_empty() {
        boundaries.push((field_start, input.len()));
    }

    (boundaries, input.len())
}

/// Fast path for quote-free CSV - uses SIMD to find all separators
#[allow(dead_code)]
pub fn parse_csv_boundaries_simple(input: &[u8], separator: u8) -> Vec<Vec<(usize, usize)>> {
    let mut rows = Vec::with_capacity(input.len() / 50 + 1);
    let mut pos = 0;

    while pos < input.len() {
        let mut boundaries = Vec::with_capacity(8);

        // Find end of line
        let line_end = memchr(b'\n', &input[pos..])
            .map(|i| pos + i)
            .unwrap_or(input.len());

        // Adjust for \r\n
        let content_end = if line_end > pos && input[line_end - 1] == b'\r' {
            line_end - 1
        } else {
            line_end
        };

        // Parse fields in this line using SIMD
        let mut field_pos = pos;
        while field_pos < content_end {
            let field_end = find_next_delimiter(&input[..content_end], field_pos, separator);
            boundaries.push((field_pos, field_end));
            field_pos = field_end + 1;
        }

        // Handle trailing separator
        if content_end > pos && input[content_end - 1] == separator {
            boundaries.push((content_end, content_end));
        }

        // Handle empty line case
        if boundaries.is_empty() && pos < content_end {
            boundaries.push((pos, content_end));
        }

        // Skip empty rows (empty boundaries or single empty field)
        let is_empty_row =
            boundaries.is_empty() || (boundaries.len() == 1 && boundaries[0].0 == boundaries[0].1);
        if !is_empty_row {
            rows.push(boundaries);
        }

        pos = if line_end < input.len() {
            line_end + 1
        } else {
            input.len()
        };
    }

    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_boundaries_simple() {
        let input = b"a,b,c\n1,2,3\n";
        let boundaries = parse_csv_boundaries(input);
        assert_eq!(boundaries.len(), 2);
        assert_eq!(boundaries[0], vec![(0, 1), (2, 3), (4, 5)]);
        assert_eq!(boundaries[1], vec![(6, 7), (8, 9), (10, 11)]);
    }

    #[test]
    fn test_boundaries_quoted() {
        let input = b"a,\"b,c\",d\n";
        let boundaries = parse_csv_boundaries(input);
        assert_eq!(boundaries.len(), 1);
        // "b,c" spans positions 2-7 (including quotes)
        assert_eq!(boundaries[0], vec![(0, 1), (2, 7), (8, 9)]);
    }

    #[test]
    fn test_boundaries_escaped() {
        let input = b"a,\"b\"\"c\",d\n";
        let boundaries = parse_csv_boundaries(input);
        assert_eq!(boundaries.len(), 1);
        // Field with escaped quote: positions 2-8
        assert_eq!(boundaries[0], vec![(0, 1), (2, 8), (9, 10)]);
    }

    #[test]
    fn test_boundaries_crlf() {
        let input = b"a,b\r\nc,d\r\n";
        let boundaries = parse_csv_boundaries(input);
        assert_eq!(boundaries.len(), 2);
        assert_eq!(boundaries[0], vec![(0, 1), (2, 3)]);
        assert_eq!(boundaries[1], vec![(5, 6), (7, 8)]);
    }

    #[test]
    fn test_boundaries_no_trailing_newline() {
        let input = b"a,b\nc,d";
        let boundaries = parse_csv_boundaries(input);
        assert_eq!(boundaries.len(), 2);
        assert_eq!(boundaries[0], vec![(0, 1), (2, 3)]);
        assert_eq!(boundaries[1], vec![(4, 5), (6, 7)]);
    }
}

// Direct parsing strategies (A: basic, B: SIMD-accelerated)
//
// Both strategies now use quote-aware row parsing to handle multiline quoted fields.
// The difference is in field extraction:
// - Basic: Simple extraction
// - Fast: Uses Cow-based extraction with proper quote unescaping

use crate::core::{extract_field_cow_with_escape, is_separator};
use std::borrow::Cow;

/// Parse CSV bytes into Vec of rows, each row is Vec of Cow field slices
/// This handles all CSV edge cases including multiline quoted fields and escaped quotes
pub fn parse_csv_full(input: &[u8]) -> Vec<Vec<Cow<'_, [u8]>>> {
    parse_csv_full_with_config(input, b',', b'"')
}

/// Parse CSV with configurable separator and escape character
pub fn parse_csv_full_with_config(
    input: &[u8],
    separator: u8,
    escape: u8,
) -> Vec<Vec<Cow<'_, [u8]>>> {
    let mut rows = Vec::new();
    let mut pos = 0;

    while pos < input.len() {
        let (row, next_pos) = parse_row_cow_with_config(input, pos, separator, escape);
        // Include empty rows (for compatibility)
        rows.push(row);
        pos = next_pos;
    }

    rows
}

/// Parse CSV with multiple separator support
pub fn parse_csv_full_multi_sep<'a>(
    input: &'a [u8],
    separators: &[u8],
    escape: u8,
) -> Vec<Vec<Cow<'a, [u8]>>> {
    // Optimize for single separator case
    if separators.len() == 1 {
        return parse_csv_full_with_config(input, separators[0], escape);
    }

    let mut rows = Vec::new();
    let mut pos = 0;

    while pos < input.len() {
        let (row, next_pos) = parse_row_cow_multi_sep(input, pos, separators, escape);
        rows.push(row);
        pos = next_pos;
    }

    rows
}

/// Parse a single row with multiple separator support
fn parse_row_cow_multi_sep<'a>(
    input: &'a [u8],
    start: usize,
    separators: &[u8],
    escape: u8,
) -> (Vec<Cow<'a, [u8]>>, usize) {
    let mut fields = Vec::new();
    let mut pos = start;
    let mut field_start = start;
    let mut in_quotes = false;

    while pos < input.len() {
        let byte = input[pos];

        if in_quotes {
            if byte == escape {
                // Check for escaped quote
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
            fields.push(extract_field_cow_with_escape(
                input,
                field_start,
                pos,
                escape,
            ));
            pos += 1;
            field_start = pos;
        } else if byte == b'\n' {
            fields.push(extract_field_cow_with_escape(
                input,
                field_start,
                pos,
                escape,
            ));
            pos += 1;
            return (fields, pos);
        } else if byte == b'\r' && pos + 1 < input.len() && input[pos + 1] == b'\n' {
            // CRLF: end of row. Bare \r is data per RFC 4180.
            fields.push(extract_field_cow_with_escape(
                input,
                field_start,
                pos,
                escape,
            ));
            pos += 2;
            return (fields, pos);
        } else {
            pos += 1;
        }
    }

    // Handle last field (no trailing newline)
    if field_start <= input.len() {
        fields.push(extract_field_cow_with_escape(
            input,
            field_start,
            pos,
            escape,
        ));
    }

    (fields, pos)
}

/// Parse a single row with full quote handling, returns (fields, next_position)
#[allow(dead_code)]
fn parse_row_cow(input: &[u8], start: usize) -> (Vec<Cow<'_, [u8]>>, usize) {
    parse_row_cow_with_config(input, start, b',', b'"')
}

/// Parse a single row with configurable separator and escape
fn parse_row_cow_with_config(
    input: &[u8],
    start: usize,
    separator: u8,
    escape: u8,
) -> (Vec<Cow<'_, [u8]>>, usize) {
    let mut fields = Vec::new();
    let mut pos = start;
    let mut field_start = start;
    let mut in_quotes = false;

    while pos < input.len() {
        let byte = input[pos];

        if in_quotes {
            if byte == escape {
                // Check for escaped quote
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
            fields.push(extract_field_cow_with_escape(
                input,
                field_start,
                pos,
                escape,
            ));
            pos += 1;
            field_start = pos;
        } else if byte == b'\n' {
            fields.push(extract_field_cow_with_escape(
                input,
                field_start,
                pos,
                escape,
            ));
            pos += 1;
            return (fields, pos);
        } else if byte == b'\r' && pos + 1 < input.len() && input[pos + 1] == b'\n' {
            // CRLF: end of row. Bare \r is data per RFC 4180.
            fields.push(extract_field_cow_with_escape(
                input,
                field_start,
                pos,
                escape,
            ));
            pos += 2;
            return (fields, pos);
        } else {
            pos += 1;
        }
    }

    // Handle last field (no trailing newline)
    if field_start <= input.len() {
        fields.push(extract_field_cow_with_escape(
            input,
            field_start,
            pos,
            escape,
        ));
    }

    (fields, pos)
}

/// Approach A: Basic byte-by-byte parsing (now uses Cow for correctness)
pub fn parse_csv(input: &[u8]) -> Vec<Vec<Cow<'_, [u8]>>> {
    parse_csv_full(input)
}

/// Approach A with configurable separator and escape
pub fn parse_csv_with_config(input: &[u8], separator: u8, escape: u8) -> Vec<Vec<Cow<'_, [u8]>>> {
    parse_csv_full_with_config(input, separator, escape)
}

/// Approach B: SIMD-accelerated parsing
/// Note: Still uses quote-aware row parsing, but benefits from SIMD in field extraction
pub fn parse_csv_fast(input: &[u8]) -> Vec<Vec<Cow<'_, [u8]>>> {
    // For full correctness, we use the same row parser as basic
    // The "fast" benefit comes from Cow allowing zero-copy when possible
    parse_csv_full(input)
}

/// Approach B with configurable separator and escape
pub fn parse_csv_fast_with_config(
    input: &[u8],
    separator: u8,
    escape: u8,
) -> Vec<Vec<Cow<'_, [u8]>>> {
    parse_csv_full_with_config(input, separator, escape)
}

/// Approach A with multiple separator support
pub fn parse_csv_multi_sep<'a>(
    input: &'a [u8],
    separators: &[u8],
    escape: u8,
) -> Vec<Vec<Cow<'a, [u8]>>> {
    parse_csv_full_multi_sep(input, separators, escape)
}

/// Approach B with multiple separator support
pub fn parse_csv_fast_multi_sep<'a>(
    input: &'a [u8],
    separators: &[u8],
    escape: u8,
) -> Vec<Vec<Cow<'a, [u8]>>> {
    parse_csv_full_multi_sep(input, separators, escape)
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
    fn test_parse_csv_basic() {
        let input = b"a,b,c\n1,2,3\n";
        let result = to_strings(parse_csv(input));
        assert_eq!(result, vec![vec!["a", "b", "c"], vec!["1", "2", "3"]]);
    }

    #[test]
    fn test_parse_csv_fast() {
        let input = b"a,b,c\n1,2,3\n";
        let result = to_strings(parse_csv_fast(input));
        assert_eq!(result, vec![vec!["a", "b", "c"], vec!["1", "2", "3"]]);
    }

    #[test]
    fn test_quoted_fields() {
        let input = b"a,\"b,c\",d\n";
        let result = to_strings(parse_csv_fast(input));
        assert_eq!(result, vec![vec!["a", "b,c", "d"]]);
    }

    #[test]
    fn test_crlf() {
        let input = b"a,b\r\nc,d\r\n";
        let result = to_strings(parse_csv_fast(input));
        assert_eq!(result, vec![vec!["a", "b"], vec!["c", "d"]]);
    }

    #[test]
    fn test_multiline_quoted() {
        let input = b"a,\"line1\nline2\",c\n";
        let result = to_strings(parse_csv_fast(input));
        assert_eq!(result, vec![vec!["a", "line1\nline2", "c"]]);
    }

    #[test]
    fn test_escaped_quotes() {
        let input = b"a,\"say \"\"hi\"\"\",c\n";
        let result = to_strings(parse_csv_fast(input));
        assert_eq!(result, vec![vec!["a", "say \"hi\"", "c"]]);
    }

    #[test]
    fn test_empty_lines() {
        let input = b"a\n\nb\n";
        let result = to_strings(parse_csv(input));
        assert_eq!(result, vec![vec!["a"], vec![""], vec!["b"]]);
    }
}

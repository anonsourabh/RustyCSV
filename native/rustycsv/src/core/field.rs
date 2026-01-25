// Field extraction and quote handling

use super::scanner::{find_next_comma, find_next_delimiter, line_has_escape, line_has_quotes};
use std::borrow::Cow;

/// Extract a field from input, stripping surrounding quotes and unescaping doubled quotes.
/// Returns Cow::Borrowed when no unescaping needed, Cow::Owned when we had to allocate.
#[inline]
pub fn extract_field_cow(input: &[u8], start: usize, end: usize) -> Cow<'_, [u8]> {
    extract_field_cow_with_escape(input, start, end, b'"')
}

/// Extract a field with configurable escape character.
#[inline]
pub fn extract_field_cow_with_escape(
    input: &[u8],
    start: usize,
    end: usize,
    escape: u8,
) -> Cow<'_, [u8]> {
    if start >= end {
        return Cow::Borrowed(&[]);
    }

    let field = &input[start..end];

    // Not quoted - return as-is
    if field.len() < 2 || field[0] != escape || field[field.len() - 1] != escape {
        return Cow::Borrowed(field);
    }

    // Quoted - strip quotes and check for escaped quotes
    let inner = &field[1..field.len() - 1];

    // Fast path: no escaped quotes inside
    if !inner.contains(&escape) {
        return Cow::Borrowed(inner);
    }

    // Slow path: unescape doubled escape chars
    let mut result = Vec::with_capacity(inner.len());
    let mut i = 0;
    while i < inner.len() {
        if inner[i] == escape && i + 1 < inner.len() && inner[i + 1] == escape {
            result.push(escape);
            i += 2;
        } else {
            result.push(inner[i]);
            i += 1;
        }
    }
    Cow::Owned(result)
}

/// Extract a field from input, stripping surrounding quotes if present.
/// NOTE: This does NOT unescape doubled quotes. Use extract_field_cow for full compliance.
#[inline]
pub fn extract_field(input: &[u8], start: usize, end: usize) -> &[u8] {
    extract_field_with_escape(input, start, end, b'"')
}

/// Extract a field with configurable escape character (no unescaping).
#[inline]
pub fn extract_field_with_escape(input: &[u8], start: usize, end: usize, escape: u8) -> &[u8] {
    if start >= end {
        return &[];
    }

    let field = &input[start..end];

    // Strip surrounding escape chars if present
    if field.len() >= 2 && field[0] == escape && field[field.len() - 1] == escape {
        &field[1..field.len() - 1]
    } else {
        field
    }
}

/// Extract a field and unescape quotes, returning owned data
/// Used when we need owned data (streaming, parallel)
#[allow(dead_code)]
pub fn extract_field_owned(input: &[u8], start: usize, end: usize) -> Vec<u8> {
    extract_field_owned_with_escape(input, start, end, b'"')
}

/// Extract a field with configurable escape character, returning owned data
pub fn extract_field_owned_with_escape(
    input: &[u8],
    start: usize,
    end: usize,
    escape: u8,
) -> Vec<u8> {
    if start >= end {
        return Vec::new();
    }

    let field = &input[start..end];

    // Not quoted
    if field.len() < 2 || field[0] != escape || field[field.len() - 1] != escape {
        return field.to_vec();
    }

    // Quoted - need to unescape doubled escape chars
    let inner = &field[1..field.len() - 1];

    // Fast path: no escaped chars
    if !inner.contains(&escape) {
        return inner.to_vec();
    }

    // Slow path: unescape doubled escape chars
    let mut result = Vec::with_capacity(inner.len());
    let mut i = 0;
    while i < inner.len() {
        if inner[i] == escape && i + 1 < inner.len() && inner[i + 1] == escape {
            result.push(escape);
            i += 2;
        } else {
            result.push(inner[i]);
            i += 1;
        }
    }
    result
}

/// Parse a line into fields using Cow (handles unescaping correctly)
#[allow(dead_code)]
pub fn parse_line_fields_cow(line: &[u8]) -> Vec<Cow<'_, [u8]>> {
    // Fast path: no quotes in line
    if !line_has_quotes(line) {
        return parse_line_simple_cow(line);
    }

    // Slow path: handle quotes
    parse_line_quoted_cow(line)
}

#[allow(dead_code)]
fn parse_line_simple_cow(line: &[u8]) -> Vec<Cow<'_, [u8]>> {
    let mut fields = Vec::with_capacity(8);
    let mut pos = 0;

    while pos < line.len() {
        let field_end = find_next_comma(line, pos);
        fields.push(Cow::Borrowed(&line[pos..field_end]));
        pos = field_end + 1;
    }

    // Handle trailing comma
    if !line.is_empty() && line[line.len() - 1] == b',' {
        fields.push(Cow::Borrowed(&line[line.len()..line.len()]));
    }

    fields
}

#[allow(dead_code)]
fn parse_line_quoted_cow(line: &[u8]) -> Vec<Cow<'_, [u8]>> {
    let mut fields = Vec::with_capacity(8);
    let mut pos = 0;
    let mut field_start = 0;
    let mut in_quotes = false;

    while pos < line.len() {
        let byte = line[pos];

        if in_quotes {
            if byte == b'"' {
                if pos + 1 < line.len() && line[pos + 1] == b'"' {
                    pos += 2;
                    continue;
                }
                in_quotes = false;
            }
            pos += 1;
        } else {
            match byte {
                b'"' => {
                    in_quotes = true;
                    pos += 1;
                }
                b',' => {
                    fields.push(extract_field_cow(line, field_start, pos));
                    pos += 1;
                    field_start = pos;
                }
                _ => {
                    pos += 1;
                }
            }
        }
    }

    // Last field
    fields.push(extract_field_cow(line, field_start, pos));

    fields
}

/// Parse a line into fields, returning slices (zero-copy)
/// NOTE: Does not unescape doubled quotes. Use parse_line_fields_cow for full compliance.
#[allow(dead_code)]
pub fn parse_line_fields(line: &[u8]) -> Vec<&[u8]> {
    // Fast path: no quotes in line
    if !line_has_quotes(line) {
        return parse_line_simple(line);
    }

    // Slow path: handle quotes
    parse_line_quoted(line)
}

/// Parse a simple line (no quotes) into fields
#[allow(dead_code)]
fn parse_line_simple(line: &[u8]) -> Vec<&[u8]> {
    let mut fields = Vec::with_capacity(8);
    let mut pos = 0;

    while pos < line.len() {
        let field_end = find_next_comma(line, pos);
        fields.push(&line[pos..field_end]);
        pos = field_end + 1;
    }

    // Handle trailing comma
    if !line.is_empty() && line[line.len() - 1] == b',' {
        fields.push(&line[line.len()..line.len()]);
    }

    fields
}

/// Parse a line with quotes
#[allow(dead_code)]
fn parse_line_quoted(line: &[u8]) -> Vec<&[u8]> {
    let mut fields = Vec::with_capacity(8);
    let mut pos = 0;
    let mut field_start = 0;
    let mut in_quotes = false;

    while pos < line.len() {
        let byte = line[pos];

        if in_quotes {
            if byte == b'"' {
                if pos + 1 < line.len() && line[pos + 1] == b'"' {
                    pos += 2;
                    continue;
                }
                in_quotes = false;
            }
            pos += 1;
        } else {
            match byte {
                b'"' => {
                    in_quotes = true;
                    pos += 1;
                }
                b',' => {
                    fields.push(extract_field(line, field_start, pos));
                    pos += 1;
                    field_start = pos;
                }
                _ => {
                    pos += 1;
                }
            }
        }
    }

    // Last field
    fields.push(extract_field(line, field_start, pos));

    fields
}

/// Parse a line into owned fields (for streaming/parallel)
#[allow(dead_code)]
pub fn parse_line_fields_owned(line: &[u8]) -> Vec<Vec<u8>> {
    parse_line_fields_owned_with_config(line, b',', b'"')
}

/// Parse a line into owned fields with configurable separator and escape
pub fn parse_line_fields_owned_with_config(line: &[u8], separator: u8, escape: u8) -> Vec<Vec<u8>> {
    // Fast path: no escape chars
    if !line_has_escape(line, escape) {
        return parse_line_simple_owned_with_sep(line, separator);
    }

    // Slow path: handle quoted fields
    parse_line_quoted_owned_with_config(line, separator, escape)
}

#[allow(dead_code)]
fn parse_line_simple_owned(line: &[u8]) -> Vec<Vec<u8>> {
    parse_line_simple_owned_with_sep(line, b',')
}

fn parse_line_simple_owned_with_sep(line: &[u8], separator: u8) -> Vec<Vec<u8>> {
    // Pre-allocate with estimate of ~8 fields per row
    let mut fields = Vec::with_capacity(8);
    let mut pos = 0;

    while pos < line.len() {
        let field_end = find_next_delimiter(line, pos, separator);
        fields.push(line[pos..field_end].to_vec());
        pos = field_end + 1;
    }

    // Handle trailing separator
    if !line.is_empty() && line[line.len() - 1] == separator {
        fields.push(Vec::new());
    }

    fields
}

#[allow(dead_code)]
fn parse_line_quoted_owned(line: &[u8]) -> Vec<Vec<u8>> {
    parse_line_quoted_owned_with_config(line, b',', b'"')
}

fn parse_line_quoted_owned_with_config(line: &[u8], separator: u8, escape: u8) -> Vec<Vec<u8>> {
    // Pre-allocate with estimate of ~8 fields per row
    let mut fields = Vec::with_capacity(8);
    let mut pos = 0;
    let mut field_start = 0;
    let mut in_quotes = false;

    while pos < line.len() {
        let byte = line[pos];

        if in_quotes {
            if byte == escape {
                if pos + 1 < line.len() && line[pos + 1] == escape {
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
            fields.push(extract_field_owned_with_escape(
                line,
                field_start,
                pos,
                escape,
            ));
            pos += 1;
            field_start = pos;
        } else {
            pos += 1;
        }
    }

    // Last field
    fields.push(extract_field_owned_with_escape(
        line,
        field_start,
        pos,
        escape,
    ));

    fields
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_field_simple() {
        assert_eq!(extract_field(b"hello", 0, 5), b"hello");
        assert_eq!(extract_field(b"", 0, 0), b"");
    }

    #[test]
    fn test_extract_field_quoted() {
        assert_eq!(extract_field(b"\"hello\"", 0, 7), b"hello");
    }

    #[test]
    fn test_extract_field_cow_escaped() {
        let result = extract_field_cow(b"\"hello \"\"world\"\"\"", 0, 17);
        assert_eq!(result.as_ref(), b"hello \"world\"");
    }

    #[test]
    fn test_parse_line_simple() {
        let fields = parse_line_fields(b"a,b,c");
        assert_eq!(
            fields,
            vec![b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]
        );
    }

    #[test]
    fn test_parse_line_quoted() {
        let fields = parse_line_fields(b"a,\"b,c\",d");
        assert_eq!(
            fields,
            vec![b"a".as_slice(), b"b,c".as_slice(), b"d".as_slice()]
        );
    }

    #[test]
    fn test_extract_field_owned_escaped() {
        let result = extract_field_owned(b"\"hello \"\"world\"\"\"", 0, 17);
        assert_eq!(result, b"hello \"world\"");
    }

    #[test]
    fn test_parse_line_cow_escaped() {
        let fields = parse_line_fields_cow(b"a,\"say \"\"hi\"\"\"");
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].as_ref(), b"a");
        assert_eq!(fields[1].as_ref(), b"say \"hi\"");
    }
}

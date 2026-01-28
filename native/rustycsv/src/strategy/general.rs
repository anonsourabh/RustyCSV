// General multi-byte separator and escape strategy
//
// This module handles arbitrary-length separators and escape sequences.
// It is only used when at least one separator or the escape is multi-byte.
// For single-byte cases, the existing optimized strategies are used instead.
//
// No SIMD — clean byte-by-byte with starts_with checks. This is acceptable
// since multi-byte delimiters are uncommon.

use std::borrow::Cow;

// ============================================================================
// Helpers
// ============================================================================

/// Check if any separator matches at position. Returns separator length if matched.
#[inline]
fn matches_separator(data: &[u8], pos: usize, separators: &[Vec<u8>]) -> Option<usize> {
    for sep in separators {
        if data[pos..].starts_with(sep) {
            return Some(sep.len());
        }
    }
    None
}

/// Check if escape sequence starts at position.
#[inline]
fn starts_with_escape(data: &[u8], pos: usize, escape: &[u8]) -> bool {
    pos + escape.len() <= data.len() && data[pos..pos + escape.len()] == *escape
}

/// Unescape doubled multi-byte escape sequences in a field's inner content.
/// E.g., for escape `$$`: `val$$$$ue` → `val$$ue`
pub fn unescape_field_general(inner: &[u8], escape: &[u8]) -> Vec<u8> {
    let esc_len = escape.len();
    let mut result = Vec::with_capacity(inner.len());
    let mut i = 0;
    while i < inner.len() {
        if i + 2 * esc_len <= inner.len()
            && inner[i..i + esc_len] == *escape
            && inner[i + esc_len..i + 2 * esc_len] == *escape
        {
            result.extend_from_slice(escape);
            i += 2 * esc_len;
        } else {
            result.push(inner[i]);
            i += 1;
        }
    }
    result
}

/// Check if inner content contains the escape sequence
pub fn contains_escape(inner: &[u8], escape: &[u8]) -> bool {
    if escape.len() == 1 {
        return inner.contains(&escape[0]);
    }
    inner.windows(escape.len()).any(|w| w == escape)
}

/// Extract a field with multi-byte escape support (Cow version)
pub fn extract_field_cow_general<'a>(
    input: &'a [u8],
    start: usize,
    end: usize,
    escape: &[u8],
) -> Cow<'a, [u8]> {
    if start >= end {
        return Cow::Borrowed(&[]);
    }

    let field = &input[start..end];
    let esc_len = escape.len();

    // Check if quoted (starts and ends with escape)
    if field.len() >= 2 * esc_len
        && field[..esc_len] == *escape
        && field[field.len() - esc_len..] == *escape
    {
        let inner = &field[esc_len..field.len() - esc_len];

        if contains_escape(inner, escape) {
            // Must unescape doubled escapes
            Cow::Owned(unescape_field_general(inner, escape))
        } else {
            // Quoted but no escapes inside
            Cow::Borrowed(inner)
        }
    } else {
        // Unquoted
        Cow::Borrowed(field)
    }
}

/// Extract a field with multi-byte escape support (owned version)
pub fn extract_field_owned_general(
    input: &[u8],
    start: usize,
    end: usize,
    escape: &[u8],
) -> Vec<u8> {
    extract_field_cow_general(input, start, end, escape).into_owned()
}

// ============================================================================
// Strategy A/B: General direct parsing (Cow)
// ============================================================================

/// General parser: handles any separator/escape lengths.
pub fn parse_csv_general<'a>(
    input: &'a [u8],
    separators: &[Vec<u8>],
    escape: &[u8],
) -> Vec<Vec<Cow<'a, [u8]>>> {
    let mut rows = Vec::new();
    let mut pos = 0;

    while pos < input.len() {
        let (row, next_pos) = parse_row_general(input, pos, separators, escape);
        rows.push(row);
        pos = next_pos;
    }

    rows
}

fn parse_row_general<'a>(
    input: &'a [u8],
    start: usize,
    separators: &[Vec<u8>],
    escape: &[u8],
) -> (Vec<Cow<'a, [u8]>>, usize) {
    let mut fields = Vec::new();
    let mut pos = start;
    let mut field_start = start;
    let mut in_quotes = false;
    let esc_len = escape.len();

    while pos < input.len() {
        if in_quotes {
            if starts_with_escape(input, pos, escape) {
                // Check for doubled escape (escaped escape)
                if starts_with_escape(input, pos + esc_len, escape) {
                    pos += 2 * esc_len;
                    continue;
                }
                in_quotes = false;
                pos += esc_len;
            } else {
                pos += 1;
            }
        } else if starts_with_escape(input, pos, escape) {
            in_quotes = true;
            pos += esc_len;
        } else if let Some(sep_len) = matches_separator(input, pos, separators) {
            fields.push(extract_field_cow_general(input, field_start, pos, escape));
            pos += sep_len;
            field_start = pos;
        } else if input[pos] == b'\n' {
            fields.push(extract_field_cow_general(input, field_start, pos, escape));
            pos += 1;
            return (fields, pos);
        } else if input[pos] == b'\r' {
            fields.push(extract_field_cow_general(input, field_start, pos, escape));
            pos += 1;
            if pos < input.len() && input[pos] == b'\n' {
                pos += 1;
            }
            return (fields, pos);
        } else {
            pos += 1;
        }
    }

    // Handle last field (no trailing newline)
    if field_start <= input.len() {
        fields.push(extract_field_cow_general(input, field_start, pos, escape));
    }

    (fields, pos)
}

// ============================================================================
// Strategy C: General two-phase index-then-extract
// ============================================================================

/// Field boundary for general parsing
#[derive(Debug, Clone, Copy)]
pub struct GeneralFieldBound {
    pub start: usize,
    pub end: usize,
}

/// Build an index of row/field boundaries with multi-byte separators/escape
pub fn build_index_general(
    input: &[u8],
    separators: &[Vec<u8>],
    escape: &[u8],
) -> Vec<Vec<GeneralFieldBound>> {
    let mut all_fields = Vec::new();
    let mut pos = 0;

    while pos < input.len() {
        let (fields, next_pos) = index_row_general(input, pos, separators, escape);
        if !fields.is_empty() {
            all_fields.push(fields);
        }
        pos = next_pos;
    }

    all_fields
}

fn index_row_general(
    input: &[u8],
    start: usize,
    separators: &[Vec<u8>],
    escape: &[u8],
) -> (Vec<GeneralFieldBound>, usize) {
    let mut fields = Vec::new();
    let mut pos = start;
    let mut field_start = start;
    let mut in_quotes = false;
    let esc_len = escape.len();

    while pos < input.len() {
        if in_quotes {
            if starts_with_escape(input, pos, escape) {
                if starts_with_escape(input, pos + esc_len, escape) {
                    pos += 2 * esc_len;
                    continue;
                }
                in_quotes = false;
                pos += esc_len;
            } else {
                pos += 1;
            }
        } else if starts_with_escape(input, pos, escape) {
            in_quotes = true;
            pos += esc_len;
        } else if let Some(sep_len) = matches_separator(input, pos, separators) {
            fields.push(GeneralFieldBound {
                start: field_start,
                end: pos,
            });
            pos += sep_len;
            field_start = pos;
        } else if input[pos] == b'\n' {
            fields.push(GeneralFieldBound {
                start: field_start,
                end: pos,
            });
            pos += 1;
            return (fields, pos);
        } else if input[pos] == b'\r' {
            fields.push(GeneralFieldBound {
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

    // End of input
    if field_start < input.len() || !fields.is_empty() {
        fields.push(GeneralFieldBound {
            start: field_start,
            end: input.len(),
        });
    }

    (fields, pos)
}

/// Combined parse using two-phase approach with multi-byte support
pub fn parse_csv_indexed_general<'a>(
    input: &'a [u8],
    separators: &[Vec<u8>],
    escape: &[u8],
) -> Vec<Vec<Cow<'a, [u8]>>> {
    let index = build_index_general(input, separators, escape);
    index
        .iter()
        .map(|row_fields| {
            row_fields
                .iter()
                .map(|bound| extract_field_cow_general(input, bound.start, bound.end, escape))
                .collect()
        })
        .collect()
}

// ============================================================================
// Strategy E: General parallel parsing
// ============================================================================

/// Find all row start positions with multi-byte escape
pub fn find_row_starts_general(input: &[u8], escape: &[u8]) -> Vec<usize> {
    let mut starts = Vec::with_capacity(input.len() / 50 + 1);
    starts.push(0);
    let mut pos = 0;
    let mut in_quotes = false;
    let esc_len = escape.len();

    while pos < input.len() {
        if in_quotes {
            if starts_with_escape(input, pos, escape) {
                if starts_with_escape(input, pos + esc_len, escape) {
                    pos += 2 * esc_len;
                    continue;
                }
                in_quotes = false;
                pos += esc_len;
            } else {
                pos += 1;
            }
        } else if starts_with_escape(input, pos, escape) {
            in_quotes = true;
            pos += esc_len;
        } else if input[pos] == b'\n' {
            pos += 1;
            if pos < input.len() {
                starts.push(pos);
            }
        } else if input[pos] == b'\r' {
            pos += 1;
            if pos < input.len() && input[pos] == b'\n' {
                pos += 1;
            }
            if pos < input.len() {
                starts.push(pos);
            }
        } else {
            pos += 1;
        }
    }

    starts
}

/// Parse a single line into owned fields with multi-byte separator/escape support
fn parse_line_fields_owned_general(
    line: &[u8],
    separators: &[Vec<u8>],
    escape: &[u8],
) -> Vec<Vec<u8>> {
    let mut fields = Vec::with_capacity(8);
    let mut pos = 0;
    let mut field_start = 0;
    let mut in_quotes = false;
    let esc_len = escape.len();

    while pos < line.len() {
        if in_quotes {
            if starts_with_escape(line, pos, escape) {
                if starts_with_escape(line, pos + esc_len, escape) {
                    pos += 2 * esc_len;
                    continue;
                }
                in_quotes = false;
                pos += esc_len;
            } else {
                pos += 1;
            }
        } else if starts_with_escape(line, pos, escape) {
            in_quotes = true;
            pos += esc_len;
        } else if let Some(sep_len) = matches_separator(line, pos, separators) {
            fields.push(extract_field_owned_general(line, field_start, pos, escape));
            pos += sep_len;
            field_start = pos;
        } else {
            pos += 1;
        }
    }

    // Last field
    fields.push(extract_field_owned_general(line, field_start, pos, escape));

    fields
}

/// Parse CSV in parallel with multi-byte separator/escape support
pub fn parse_csv_parallel_general(
    input: &[u8],
    separators: &[Vec<u8>],
    escape: &[u8],
) -> Vec<Vec<Vec<u8>>> {
    use rayon::prelude::*;

    let row_starts = find_row_starts_general(input, escape);

    if row_starts.is_empty() {
        return Vec::new();
    }

    let row_ranges: Vec<(usize, usize)> = row_starts
        .windows(2)
        .map(|w| (w[0], w[1]))
        .chain(std::iter::once((*row_starts.last().unwrap(), input.len())))
        .collect();

    let separators_vec: Vec<Vec<u8>> = separators.to_vec();
    let escape_vec: Vec<u8> = escape.to_vec();

    row_ranges
        .into_par_iter()
        .filter_map(|(start, end)| {
            let mut line_end = end;
            while line_end > start
                && (input[line_end - 1] == b'\n' || input[line_end - 1] == b'\r')
            {
                line_end -= 1;
            }

            if line_end <= start {
                return None;
            }

            let line = &input[start..line_end];
            let fields = parse_line_fields_owned_general(line, &separators_vec, &escape_vec);

            if fields.is_empty() || (fields.len() == 1 && fields[0].is_empty()) {
                None
            } else {
                Some(fields)
            }
        })
        .collect()
}

// ============================================================================
// Strategy F: General zero-copy boundaries
// ============================================================================

/// Parse CSV returning field boundaries with multi-byte separator/escape
pub fn parse_csv_boundaries_general(
    input: &[u8],
    separators: &[Vec<u8>],
    escape: &[u8],
) -> Vec<Vec<(usize, usize)>> {
    let mut rows = Vec::with_capacity(input.len() / 50 + 1);
    let mut pos = 0;

    while pos < input.len() {
        let (boundaries, next_pos) =
            parse_row_boundaries_general(input, pos, separators, escape);
        if !boundaries.is_empty() {
            rows.push(boundaries);
        }
        pos = next_pos;
    }

    rows
}

fn parse_row_boundaries_general(
    input: &[u8],
    start: usize,
    separators: &[Vec<u8>],
    escape: &[u8],
) -> (Vec<(usize, usize)>, usize) {
    let mut boundaries = Vec::with_capacity(8);
    let mut pos = start;
    let mut field_start = start;
    let mut in_quotes = false;
    let esc_len = escape.len();

    while pos < input.len() {
        if in_quotes {
            if starts_with_escape(input, pos, escape) {
                if starts_with_escape(input, pos + esc_len, escape) {
                    pos += 2 * esc_len;
                    continue;
                }
                in_quotes = false;
                pos += esc_len;
            } else {
                pos += 1;
            }
        } else if starts_with_escape(input, pos, escape) {
            in_quotes = true;
            pos += esc_len;
        } else if let Some(sep_len) = matches_separator(input, pos, separators) {
            boundaries.push((field_start, pos));
            pos += sep_len;
            field_start = pos;
        } else if input[pos] == b'\n' {
            let field_end = if pos > field_start && input[pos - 1] == b'\r' {
                pos - 1
            } else {
                pos
            };
            boundaries.push((field_start, field_end));
            return (boundaries, pos + 1);
        } else if input[pos] == b'\r' {
            boundaries.push((field_start, pos));
            pos += 1;
            if pos < input.len() && input[pos] == b'\n' {
                pos += 1;
            }
            return (boundaries, pos);
        } else {
            pos += 1;
        }
    }

    // End of input
    if field_start < input.len() || !boundaries.is_empty() {
        boundaries.push((field_start, input.len()));
    }

    (boundaries, input.len())
}

// ============================================================================
// Strategy D: General streaming parser
// ============================================================================

/// Streaming parser that handles multi-byte separators and escapes
pub struct GeneralStreamingParser {
    buffer: Vec<u8>,
    complete_rows: Vec<Vec<Vec<u8>>>,
    partial_row_start: usize,
    scan_pos: usize,
    in_quotes: bool,
    separators: Vec<Vec<u8>>,
    escape: Vec<u8>,
}

impl GeneralStreamingParser {
    pub fn new(separators: Vec<Vec<u8>>, escape: Vec<u8>) -> Self {
        GeneralStreamingParser {
            buffer: Vec::new(),
            complete_rows: Vec::new(),
            partial_row_start: 0,
            scan_pos: 0,
            in_quotes: false,
            separators,
            escape,
        }
    }

    pub fn feed(&mut self, chunk: &[u8]) {
        self.buffer.extend_from_slice(chunk);
        self.process_buffer();
    }

    fn process_buffer(&mut self) {
        let mut pos = self.scan_pos;
        let esc_len = self.escape.len();

        while pos < self.buffer.len() {
            if self.in_quotes {
                if starts_with_escape(&self.buffer, pos, &self.escape) {
                    if starts_with_escape(&self.buffer, pos + esc_len, &self.escape) {
                        pos += 2 * esc_len;
                        continue;
                    }
                    self.in_quotes = false;
                    pos += esc_len;
                } else {
                    pos += 1;
                }
            } else if starts_with_escape(&self.buffer, pos, &self.escape) {
                self.in_quotes = true;
                pos += esc_len;
            } else if self.buffer[pos] == b'\n' {
                let row_end = pos;
                let row = self.parse_row_owned(self.partial_row_start, row_end);
                if !row.is_empty() {
                    self.complete_rows.push(row);
                }
                pos += 1;
                self.partial_row_start = pos;
                self.in_quotes = false;
            } else if self.buffer[pos] == b'\r' {
                let row_end = pos;
                let row = self.parse_row_owned(self.partial_row_start, row_end);
                if !row.is_empty() {
                    self.complete_rows.push(row);
                }
                pos += 1;
                if pos < self.buffer.len() && self.buffer[pos] == b'\n' {
                    pos += 1;
                }
                self.partial_row_start = pos;
                self.in_quotes = false;
            } else {
                pos += 1;
            }
        }

        self.scan_pos = pos;

        if self.partial_row_start > 0 && self.partial_row_start >= self.buffer.len() / 2 {
            self.compact_buffer();
        }
    }

    fn parse_row_owned(&self, start: usize, end: usize) -> Vec<Vec<u8>> {
        if start >= end {
            return Vec::new();
        }

        let line = &self.buffer[start..end];
        parse_line_fields_owned_general(line, &self.separators, &self.escape)
    }

    fn compact_buffer(&mut self) {
        if self.partial_row_start > 0 {
            self.buffer.drain(0..self.partial_row_start);
            self.scan_pos -= self.partial_row_start;
            self.partial_row_start = 0;
        }
    }

    pub fn take_rows(&mut self, max: usize) -> Vec<Vec<Vec<u8>>> {
        let take_count = max.min(self.complete_rows.len());
        self.complete_rows.drain(0..take_count).collect()
    }

    pub fn available_rows(&self) -> usize {
        self.complete_rows.len()
    }

    pub fn has_partial(&self) -> bool {
        self.partial_row_start < self.buffer.len()
    }

    pub fn buffer_size(&self) -> usize {
        self.buffer.len()
    }

    pub fn finalize(&mut self) -> Vec<Vec<Vec<u8>>> {
        if self.partial_row_start < self.buffer.len() {
            let row = self.parse_row_owned(self.partial_row_start, self.buffer.len());
            if !row.is_empty() {
                self.complete_rows.push(row);
            }
            self.partial_row_start = self.buffer.len();
        }
        std::mem::take(&mut self.complete_rows)
    }
}

// ============================================================================
// Tests
// ============================================================================

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

    fn to_strings_owned(rows: Vec<Vec<Vec<u8>>>) -> Vec<Vec<String>> {
        rows.into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|f| String::from_utf8_lossy(&f).to_string())
                    .collect()
            })
            .collect()
    }

    // --- Multi-byte separator tests ---

    #[test]
    fn test_general_double_colon_separator() {
        let input = b"a::b::c\n1::2::3\n";
        let seps = vec![b"::".to_vec()];
        let esc = b"\"".to_vec();
        let result = to_strings(parse_csv_general(input, &seps, &esc));
        assert_eq!(result, vec![vec!["a", "b", "c"], vec!["1", "2", "3"]]);
    }

    #[test]
    fn test_general_mixed_separators() {
        let input = b"a,b::c\n1::2,3\n";
        let seps = vec![b",".to_vec(), b"::".to_vec()];
        let esc = b"\"".to_vec();
        let result = to_strings(parse_csv_general(input, &seps, &esc));
        assert_eq!(result, vec![vec!["a", "b", "c"], vec!["1", "2", "3"]]);
    }

    #[test]
    fn test_general_quoted_with_separator() {
        let input = b"\"a::b\"::c\n";
        let seps = vec![b"::".to_vec()];
        let esc = b"\"".to_vec();
        let result = to_strings(parse_csv_general(input, &seps, &esc));
        assert_eq!(result, vec![vec!["a::b", "c"]]);
    }

    // --- Multi-byte escape tests ---

    #[test]
    fn test_general_multi_byte_escape() {
        let input = b"$$hello$$::world\n";
        let seps = vec![b"::".to_vec()];
        let esc = b"$$".to_vec();
        let result = to_strings(parse_csv_general(input, &seps, &esc));
        assert_eq!(result, vec![vec!["hello", "world"]]);
    }

    #[test]
    fn test_general_multi_byte_escape_doubled() {
        // $$val$$$$ue$$ → val$$ue
        let input = b"$$val$$$$ue$$::other\n";
        let seps = vec![b"::".to_vec()];
        let esc = b"$$".to_vec();
        let result = to_strings(parse_csv_general(input, &seps, &esc));
        assert_eq!(result, vec![vec!["val$$ue", "other"]]);
    }

    // --- Indexed strategy tests ---

    #[test]
    fn test_indexed_general() {
        let input = b"a::b::c\n1::2::3\n";
        let seps = vec![b"::".to_vec()];
        let esc = b"\"".to_vec();
        let result = to_strings(parse_csv_indexed_general(input, &seps, &esc));
        assert_eq!(result, vec![vec!["a", "b", "c"], vec!["1", "2", "3"]]);
    }

    // --- Parallel strategy tests ---

    #[test]
    fn test_parallel_general() {
        let input = b"a::b::c\n1::2::3\n";
        let seps = vec![b"::".to_vec()];
        let esc = b"\"".to_vec();
        let result = to_strings_owned(parse_csv_parallel_general(input, &seps, &esc));
        assert_eq!(result, vec![vec!["a", "b", "c"], vec!["1", "2", "3"]]);
    }

    // --- Boundaries strategy tests ---

    #[test]
    fn test_boundaries_general() {
        let input = b"a::b::c\n1::2::3\n";
        let seps = vec![b"::".to_vec()];
        let esc = b"\"".to_vec();
        let boundaries = parse_csv_boundaries_general(input, &seps, &esc);
        assert_eq!(boundaries.len(), 2);
        // First row: "a" at 0..1, "b" at 3..4, "c" at 6..7
        assert_eq!(boundaries[0], vec![(0, 1), (3, 4), (6, 7)]);
    }

    // --- Streaming tests ---

    #[test]
    fn test_streaming_general() {
        let seps = vec![b"::".to_vec()];
        let esc = b"\"".to_vec();
        let mut parser = GeneralStreamingParser::new(seps, esc);
        parser.feed(b"a::b::c\n1::2::3\n");

        let rows = parser.take_rows(10);
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0],
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
    }

    #[test]
    fn test_streaming_general_chunked() {
        let seps = vec![b"::".to_vec()];
        let esc = b"\"".to_vec();
        let mut parser = GeneralStreamingParser::new(seps, esc);
        parser.feed(b"a::b::");
        assert_eq!(parser.available_rows(), 0);
        parser.feed(b"c\n1::2::3\n");
        assert_eq!(parser.available_rows(), 2);

        let rows = parser.take_rows(10);
        assert_eq!(
            rows[0],
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
    }

    #[test]
    fn test_streaming_general_finalize() {
        let seps = vec![b"::".to_vec()];
        let esc = b"\"".to_vec();
        let mut parser = GeneralStreamingParser::new(seps, esc);
        parser.feed(b"a::b\n1::2");
        let rows1 = parser.take_rows(10);
        assert_eq!(rows1.len(), 1);

        let rows2 = parser.finalize();
        assert_eq!(rows2.len(), 1);
        assert_eq!(rows2[0], vec![b"1".to_vec(), b"2".to_vec()]);
    }
}

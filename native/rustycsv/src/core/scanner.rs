// Row and field boundary detection using memchr (SIMD-accelerated)

use memchr::memchr;

/// Find the next newline position, handling \r\n
/// Returns (line_end_exclusive, next_line_start)
#[inline]
#[allow(dead_code)]
pub fn find_line_end(input: &[u8], start: usize) -> (usize, usize) {
    let line_end = memchr(b'\n', &input[start..])
        .map(|i| start + i)
        .unwrap_or(input.len());

    // Handle \r\n - actual content ends before \r
    let content_end = if line_end > start && input[line_end - 1] == b'\r' {
        line_end - 1
    } else {
        line_end
    };

    // Next line starts after \n
    let next_start = if line_end < input.len() {
        line_end + 1
    } else {
        input.len()
    };

    (content_end, next_start)
}

/// Find all row start positions in the input (for parallel parsing)
/// Returns positions where new rows begin (always includes 0)
#[allow(dead_code)]
pub fn find_row_starts(input: &[u8]) -> Vec<usize> {
    find_row_starts_with_escape(input, b'"')
}

/// Find all row start positions with configurable escape character
pub fn find_row_starts_with_escape(input: &[u8], escape: u8) -> Vec<usize> {
    let mut starts = vec![0];
    let mut pos = 0;
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
        } else {
            match byte {
                b if b == escape => {
                    in_quotes = true;
                    pos += 1;
                }
                b'\n' => {
                    pos += 1;
                    if pos < input.len() {
                        starts.push(pos);
                    }
                }
                b'\r' => {
                    pos += 1;
                    if pos < input.len() && input[pos] == b'\n' {
                        pos += 1;
                    }
                    if pos < input.len() {
                        starts.push(pos);
                    }
                }
                _ => {
                    pos += 1;
                }
            }
        }
    }

    starts
}

/// Fast check if a line contains any quotes
#[inline]
pub fn line_has_quotes(line: &[u8]) -> bool {
    memchr(b'"', line).is_some()
}

/// Fast check if a line contains any escape characters (configurable)
#[inline]
pub fn line_has_escape(line: &[u8], escape: u8) -> bool {
    memchr(escape, line).is_some()
}

/// Find next comma position using memchr
#[inline]
pub fn find_next_comma(line: &[u8], start: usize) -> usize {
    memchr(b',', &line[start..])
        .map(|i| start + i)
        .unwrap_or(line.len())
}

/// Find next delimiter position using memchr (configurable separator)
#[inline]
pub fn find_next_delimiter(line: &[u8], start: usize, separator: u8) -> usize {
    memchr(separator, &line[start..])
        .map(|i| start + i)
        .unwrap_or(line.len())
}

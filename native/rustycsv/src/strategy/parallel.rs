// Approach E: Parallel Parser using Rayon
//
// Strategy:
// 1. Single-threaded: Find all row boundaries (must handle quotes correctly)
// 2. Parallel: Parse each row independently using rayon
//
// Important: We can't build BEAM terms on worker threads, so we return
// owned Vec<Vec<Vec<u8>>> and convert to terms on the scheduler thread.

use crate::core::{find_row_starts_with_escape, parse_line_fields_owned_with_config, parse_line_fields_owned_multi_sep};
use rayon::prelude::*;

/// Parse CSV in parallel, returning owned rows
/// The caller must convert to BEAM terms on the main scheduler thread
pub fn parse_csv_parallel(input: &[u8]) -> Vec<Vec<Vec<u8>>> {
    parse_csv_parallel_with_config(input, b',', b'"')
}

/// Parse CSV in parallel with configurable separator and escape
pub fn parse_csv_parallel_with_config(
    input: &[u8],
    separator: u8,
    escape: u8,
) -> Vec<Vec<Vec<u8>>> {
    // Phase 1: Find row boundaries (single-threaded, quote-aware)
    let row_starts = find_row_starts_with_escape(input, escape);

    if row_starts.is_empty() {
        return Vec::new();
    }

    // Build (start, end) pairs for each row
    let row_ranges: Vec<(usize, usize)> = row_starts
        .windows(2)
        .map(|w| (w[0], w[1]))
        .chain(std::iter::once((*row_starts.last().unwrap(), input.len())))
        .collect();

    // Phase 2: Parse rows in parallel
    row_ranges
        .into_par_iter()
        .filter_map(|(start, end)| {
            // Strip trailing line ending (\n or \r\n). Bare \r is data per RFC 4180.
            let mut line_end = end;
            if line_end > start && input[line_end - 1] == b'\n' {
                line_end -= 1;
                if line_end > start && input[line_end - 1] == b'\r' {
                    line_end -= 1;
                }
            }

            if line_end <= start {
                return None;
            }

            let line = &input[start..line_end];
            let fields = parse_line_fields_owned_with_config(line, separator, escape);

            if fields.is_empty() || (fields.len() == 1 && fields[0].is_empty()) {
                None
            } else {
                Some(fields)
            }
        })
        .collect()
}

/// Parse CSV in parallel with multiple separator support
pub fn parse_csv_parallel_multi_sep(
    input: &[u8],
    separators: &[u8],
    escape: u8,
) -> Vec<Vec<Vec<u8>>> {
    // Optimize for single separator case
    if separators.len() == 1 {
        return parse_csv_parallel_with_config(input, separators[0], escape);
    }

    // Phase 1: Find row boundaries (single-threaded, quote-aware)
    let row_starts = find_row_starts_with_escape(input, escape);

    if row_starts.is_empty() {
        return Vec::new();
    }

    // Build (start, end) pairs for each row
    let row_ranges: Vec<(usize, usize)> = row_starts
        .windows(2)
        .map(|w| (w[0], w[1]))
        .chain(std::iter::once((*row_starts.last().unwrap(), input.len())))
        .collect();

    // Clone separators for thread safety
    let separators_vec: Vec<u8> = separators.to_vec();

    // Phase 2: Parse rows in parallel
    row_ranges
        .into_par_iter()
        .filter_map(|(start, end)| {
            // Strip trailing line ending (\n or \r\n). Bare \r is data per RFC 4180.
            let mut line_end = end;
            if line_end > start && input[line_end - 1] == b'\n' {
                line_end -= 1;
                if line_end > start && input[line_end - 1] == b'\r' {
                    line_end -= 1;
                }
            }

            if line_end <= start {
                return None;
            }

            let line = &input[start..line_end];
            let fields = parse_line_fields_owned_multi_sep(line, &separators_vec, escape);

            if fields.is_empty() || (fields.len() == 1 && fields[0].is_empty()) {
                None
            } else {
                Some(fields)
            }
        })
        .collect()
}

/// Configure rayon thread pool size based on system
#[allow(dead_code)]
pub fn recommended_threads() -> usize {
    // Use available parallelism, capped at 8 for NIF work
    std::thread::available_parallelism()
        .map(|p| p.get().min(8))
        .unwrap_or(4)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parallel_simple() {
        let input = b"a,b,c\n1,2,3\n";
        let rows = parse_csv_parallel(input);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        assert_eq!(rows[1], vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);
    }

    #[test]
    fn test_parallel_quoted() {
        let input = b"a,\"b,c\",d\n1,2,3\n";
        let rows = parse_csv_parallel(input);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![b"a".to_vec(), b"b,c".to_vec(), b"d".to_vec()]);
    }

    #[test]
    fn test_parallel_no_trailing_newline() {
        let input = b"a,b\nc,d";
        let rows = parse_csv_parallel(input);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_parallel_many_rows() {
        // Generate many rows to actually exercise parallelism
        let mut input = Vec::new();
        for i in 0..1000 {
            input.extend_from_slice(format!("{},{},{}\n", i, i + 1, i + 2).as_bytes());
        }

        let rows = parse_csv_parallel(&input);
        assert_eq!(rows.len(), 1000);
        assert_eq!(rows[0], vec![b"0".to_vec(), b"1".to_vec(), b"2".to_vec()]);
        assert_eq!(
            rows[999],
            vec![b"999".to_vec(), b"1000".to_vec(), b"1001".to_vec()]
        );
    }

    #[test]
    fn test_parallel_quoted_newline() {
        // Quoted field containing newline - must be handled correctly
        let input = b"a,\"line1\nline2\",c\nd,e,f\n";
        let rows = parse_csv_parallel(input);
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0],
            vec![b"a".to_vec(), b"line1\nline2".to_vec(), b"c".to_vec()]
        );
    }
}

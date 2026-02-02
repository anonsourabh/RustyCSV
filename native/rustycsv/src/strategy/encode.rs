// CSV encoding strategies — convert rows of fields back to CSV format
//
// Three strategies with increasing hardware acceleration:
//
// Scalar:  byte-by-byte scan for characters needing escaping. Baseline.
// SWAR:    SIMD Within A Register — Mycroft's trick on u64, 8 bytes/op. No SIMD HW needed.
// SIMD:    portable_simd 16/32-byte vectorized scanning. Fastest on supported targets.
//
// All strategies produce identical output. The hot path is the same:
// 1. Scan each field for "special" characters (separator, escape, newline).
// 2. If none found: copy field verbatim into output buffer.
// 3. If found: wrap in escape chars and double any escape chars inside.
//
// The strategies differ only in how step 1 (scanning) is performed.

use std::simd::prelude::*;

// ==========================================================================
// Shared: output buffer helpers
// ==========================================================================

/// Pre-compute the output size estimate (avoids frequent reallocation).
/// Estimate: sum of field lengths + separators + newlines + some quoting overhead.
#[inline]
fn estimate_output_size(rows: &[&[&[u8]]], sep_len: usize, newline_len: usize) -> usize {
    let mut size = 0;
    for row in rows {
        for field in *row {
            size += field.len() + 2; // field + potential quotes
        }
        size += (row.len().saturating_sub(1)) * sep_len + newline_len;
    }
    size
}

/// Write a field that needs quoting: escape_char + field_with_doubled_escapes + escape_char
#[inline]
fn write_quoted_field(out: &mut Vec<u8>, field: &[u8], escape: u8) {
    out.push(escape);
    let mut i = 0;
    while i < field.len() {
        let b = field[i];
        out.push(b);
        if b == escape {
            out.push(escape); // double the escape character
        }
        i += 1;
    }
    out.push(escape);
}

/// Write a field that needs quoting with multi-byte escape sequence
#[inline]
fn write_quoted_field_general(out: &mut Vec<u8>, field: &[u8], escape: &[u8]) {
    out.extend_from_slice(escape);
    let esc_len = escape.len();
    let mut i = 0;
    while i < field.len() {
        if i + esc_len <= field.len() && field[i..i + esc_len] == *escape {
            out.extend_from_slice(escape);
            out.extend_from_slice(escape); // doubled
            i += esc_len;
        } else {
            out.push(field[i]);
            i += 1;
        }
    }
    out.extend_from_slice(escape);
}

// ==========================================================================
// Strategy 1: Scalar — byte-by-byte scanning
// ==========================================================================

/// Check if a field contains any character that requires quoting (scalar, byte-by-byte).
#[inline]
fn field_needs_quoting_scalar(field: &[u8], separator: u8, escape: u8) -> bool {
    for &b in field {
        if b == separator || b == escape || b == b'\n' || b == b'\r' {
            return true;
        }
    }
    false
}

/// Scalar check with multiple separators
#[inline]
fn field_needs_quoting_scalar_multi_sep(field: &[u8], separators: &[u8], escape: u8) -> bool {
    for &b in field {
        if b == escape || b == b'\n' || b == b'\r' || separators.contains(&b) {
            return true;
        }
    }
    false
}

/// Encode rows to CSV using scalar scanning.
pub fn encode_csv_scalar(
    rows: &[&[&[u8]]],
    separator: u8,
    escape: u8,
    line_separator: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(estimate_output_size(rows, 1, line_separator.len()));

    for row in rows {
        for (i, field) in row.iter().enumerate() {
            if i > 0 {
                out.push(separator);
            }
            if field_needs_quoting_scalar(field, separator, escape) {
                write_quoted_field(&mut out, field, escape);
            } else {
                out.extend_from_slice(field);
            }
        }
        out.extend_from_slice(line_separator);
    }

    out
}

/// Encode rows to CSV using scalar scanning, multi-separator variant.
pub fn encode_csv_scalar_multi_sep(
    rows: &[&[&[u8]]],
    separators: &[u8],
    escape: u8,
    line_separator: &[u8],
) -> Vec<u8> {
    let dump_sep = separators[0]; // Use first separator for dumping
    let mut out = Vec::with_capacity(estimate_output_size(rows, 1, line_separator.len()));

    for row in rows {
        for (i, field) in row.iter().enumerate() {
            if i > 0 {
                out.push(dump_sep);
            }
            if field_needs_quoting_scalar_multi_sep(field, separators, escape) {
                write_quoted_field(&mut out, field, escape);
            } else {
                out.extend_from_slice(field);
            }
        }
        out.extend_from_slice(line_separator);
    }

    out
}

/// Encode rows with multi-byte separators/escape (general, scalar only).
pub fn encode_csv_general(
    rows: &[&[&[u8]]],
    separator: &[u8],
    escape: &[u8],
    line_separator: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(estimate_output_size(
        rows,
        separator.len(),
        line_separator.len(),
    ));

    for row in rows {
        for (i, field) in row.iter().enumerate() {
            if i > 0 {
                out.extend_from_slice(separator);
            }
            if field_needs_quoting_general(field, separator, escape) {
                write_quoted_field_general(&mut out, field, escape);
            } else {
                out.extend_from_slice(field);
            }
        }
        out.extend_from_slice(line_separator);
    }

    out
}

/// Multi-byte: check if field needs quoting
#[inline]
fn field_needs_quoting_general(field: &[u8], separator: &[u8], escape: &[u8]) -> bool {
    // Check for newlines byte-by-byte
    for &b in field {
        if b == b'\n' || b == b'\r' {
            return true;
        }
    }
    // Check for separator pattern
    if separator.len() == 1 {
        if field.contains(&separator[0]) {
            return true;
        }
    } else if field.len() >= separator.len() {
        for w in field.windows(separator.len()) {
            if w == separator {
                return true;
            }
        }
    }
    // Check for escape pattern
    if escape.len() == 1 {
        if field.contains(&escape[0]) {
            return true;
        }
    } else if field.len() >= escape.len() {
        for w in field.windows(escape.len()) {
            if w == escape {
                return true;
            }
        }
    }
    false
}

// ==========================================================================
// Strategy 2: SWAR — Mycroft's trick, 8 bytes at a time
// ==========================================================================

/// Broadcast a byte to all 8 positions in a u64.
#[inline]
const fn broadcast(byte: u8) -> u64 {
    (byte as u64) * 0x0101_0101_0101_0101
}

/// Mycroft's trick: detect if any byte in a u64 word is zero.
/// Returns non-zero if a zero byte exists.
#[inline]
fn has_zero_byte(v: u64) -> bool {
    const LO: u64 = 0x0101_0101_0101_0101;
    const HI: u64 = 0x8080_8080_8080_8080;
    (v.wrapping_sub(LO) & !v & HI) != 0
}

/// Check if a u64 word contains a specific byte value.
#[inline]
fn word_contains_byte(word: u64, byte: u8) -> bool {
    has_zero_byte(word ^ broadcast(byte))
}

/// SWAR: check if field needs quoting by processing 8 bytes at a time.
#[inline]
fn field_needs_quoting_swar(field: &[u8], separator: u8, escape: u8) -> bool {
    let len = field.len();
    let mut i = 0;

    // Process 8 bytes at a time
    while i + 8 <= len {
        let word = u64::from_le_bytes(field[i..i + 8].try_into().unwrap_or([0; 8]));
        if word_contains_byte(word, separator)
            || word_contains_byte(word, escape)
            || word_contains_byte(word, b'\n')
            || word_contains_byte(word, b'\r')
        {
            return true;
        }
        i += 8;
    }

    // Scalar tail
    while i < len {
        let b = field[i];
        if b == separator || b == escape || b == b'\n' || b == b'\r' {
            return true;
        }
        i += 1;
    }

    false
}

/// SWAR variant with multiple separators
#[inline]
fn field_needs_quoting_swar_multi_sep(field: &[u8], separators: &[u8], escape: u8) -> bool {
    let len = field.len();
    let mut i = 0;

    while i + 8 <= len {
        let word = u64::from_le_bytes(field[i..i + 8].try_into().unwrap_or([0; 8]));
        if word_contains_byte(word, escape)
            || word_contains_byte(word, b'\n')
            || word_contains_byte(word, b'\r')
        {
            return true;
        }
        for &sep in separators {
            if word_contains_byte(word, sep) {
                return true;
            }
        }
        i += 8;
    }

    while i < len {
        let b = field[i];
        if b == escape || b == b'\n' || b == b'\r' || separators.contains(&b) {
            return true;
        }
        i += 1;
    }

    false
}

/// Encode rows to CSV using SWAR scanning.
pub fn encode_csv_swar(
    rows: &[&[&[u8]]],
    separator: u8,
    escape: u8,
    line_separator: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(estimate_output_size(rows, 1, line_separator.len()));

    for row in rows {
        for (i, field) in row.iter().enumerate() {
            if i > 0 {
                out.push(separator);
            }
            if field_needs_quoting_swar(field, separator, escape) {
                write_quoted_field(&mut out, field, escape);
            } else {
                out.extend_from_slice(field);
            }
        }
        out.extend_from_slice(line_separator);
    }

    out
}

/// Encode rows to CSV using SWAR scanning, multi-separator variant.
pub fn encode_csv_swar_multi_sep(
    rows: &[&[&[u8]]],
    separators: &[u8],
    escape: u8,
    line_separator: &[u8],
) -> Vec<u8> {
    let dump_sep = separators[0];
    let mut out = Vec::with_capacity(estimate_output_size(rows, 1, line_separator.len()));

    for row in rows {
        for (i, field) in row.iter().enumerate() {
            if i > 0 {
                out.push(dump_sep);
            }
            if field_needs_quoting_swar_multi_sep(field, separators, escape) {
                write_quoted_field(&mut out, field, escape);
            } else {
                out.extend_from_slice(field);
            }
        }
        out.extend_from_slice(line_separator);
    }

    out
}

// ==========================================================================
// Strategy 3: SIMD — portable_simd vectorized scanning
// ==========================================================================

/// Baseline SIMD chunk size (128-bit).
const CHUNK: usize = 16;

/// Wide chunk size for AVX2 targets.
#[cfg(target_feature = "avx2")]
const WIDE: usize = 32;

/// SIMD: check if field needs quoting using vectorized comparison.
#[inline]
fn field_needs_quoting_simd(field: &[u8], separator: u8, escape: u8) -> bool {
    let len = field.len();
    let mut pos = 0;

    // AVX2 wide path: 32 bytes at a time
    #[cfg(target_feature = "avx2")]
    {
        let sep_splat = Simd::<u8, WIDE>::splat(separator);
        let esc_splat = Simd::<u8, WIDE>::splat(escape);
        let lf_splat = Simd::<u8, WIDE>::splat(b'\n');
        let cr_splat = Simd::<u8, WIDE>::splat(b'\r');

        while pos + WIDE <= len {
            let chunk = Simd::<u8, WIDE>::from_slice(&field[pos..pos + WIDE]);
            let hits = chunk.simd_eq(sep_splat)
                | chunk.simd_eq(esc_splat)
                | chunk.simd_eq(lf_splat)
                | chunk.simd_eq(cr_splat);
            if hits.any() {
                return true;
            }
            pos += WIDE;
        }
    }

    // 16-byte path
    {
        let sep_splat = Simd::<u8, CHUNK>::splat(separator);
        let esc_splat = Simd::<u8, CHUNK>::splat(escape);
        let lf_splat = Simd::<u8, CHUNK>::splat(b'\n');
        let cr_splat = Simd::<u8, CHUNK>::splat(b'\r');

        while pos + CHUNK <= len {
            let chunk = Simd::<u8, CHUNK>::from_slice(&field[pos..pos + CHUNK]);
            let hits = chunk.simd_eq(sep_splat)
                | chunk.simd_eq(esc_splat)
                | chunk.simd_eq(lf_splat)
                | chunk.simd_eq(cr_splat);
            if hits.any() {
                return true;
            }
            pos += CHUNK;
        }
    }

    // Scalar tail
    while pos < len {
        let b = field[pos];
        if b == separator || b == escape || b == b'\n' || b == b'\r' {
            return true;
        }
        pos += 1;
    }

    false
}

/// SIMD variant with multiple separators
#[inline]
fn field_needs_quoting_simd_multi_sep(field: &[u8], separators: &[u8], escape: u8) -> bool {
    let len = field.len();
    let mut pos = 0;

    // AVX2 wide path
    #[cfg(target_feature = "avx2")]
    {
        let esc_splat = Simd::<u8, WIDE>::splat(escape);
        let lf_splat = Simd::<u8, WIDE>::splat(b'\n');
        let cr_splat = Simd::<u8, WIDE>::splat(b'\r');
        let sep_splats: Vec<Simd<u8, WIDE>> = separators
            .iter()
            .map(|&s| Simd::<u8, WIDE>::splat(s))
            .collect();

        while pos + WIDE <= len {
            let chunk = Simd::<u8, WIDE>::from_slice(&field[pos..pos + WIDE]);
            let mut hits = chunk.simd_eq(esc_splat)
                | chunk.simd_eq(lf_splat)
                | chunk.simd_eq(cr_splat);
            for splat in &sep_splats {
                hits = hits | chunk.simd_eq(*splat);
            }
            if hits.any() {
                return true;
            }
            pos += WIDE;
        }
    }

    // 16-byte path
    {
        let esc_splat = Simd::<u8, CHUNK>::splat(escape);
        let lf_splat = Simd::<u8, CHUNK>::splat(b'\n');
        let cr_splat = Simd::<u8, CHUNK>::splat(b'\r');
        let sep_splats: Vec<Simd<u8, CHUNK>> = separators
            .iter()
            .map(|&s| Simd::<u8, CHUNK>::splat(s))
            .collect();

        while pos + CHUNK <= len {
            let chunk = Simd::<u8, CHUNK>::from_slice(&field[pos..pos + CHUNK]);
            let mut hits = chunk.simd_eq(esc_splat)
                | chunk.simd_eq(lf_splat)
                | chunk.simd_eq(cr_splat);
            for splat in &sep_splats {
                hits = hits | chunk.simd_eq(*splat);
            }
            if hits.any() {
                return true;
            }
            pos += CHUNK;
        }
    }

    // Scalar tail
    while pos < len {
        let b = field[pos];
        if b == escape || b == b'\n' || b == b'\r' || separators.contains(&b) {
            return true;
        }
        pos += 1;
    }

    false
}

/// Encode rows to CSV using SIMD scanning.
pub fn encode_csv_simd(
    rows: &[&[&[u8]]],
    separator: u8,
    escape: u8,
    line_separator: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(estimate_output_size(rows, 1, line_separator.len()));

    for row in rows {
        for (i, field) in row.iter().enumerate() {
            if i > 0 {
                out.push(separator);
            }
            if field_needs_quoting_simd(field, separator, escape) {
                write_quoted_field(&mut out, field, escape);
            } else {
                out.extend_from_slice(field);
            }
        }
        out.extend_from_slice(line_separator);
    }

    out
}

/// Encode rows to CSV using SIMD scanning, multi-separator variant.
pub fn encode_csv_simd_multi_sep(
    rows: &[&[&[u8]]],
    separators: &[u8],
    escape: u8,
    line_separator: &[u8],
) -> Vec<u8> {
    let dump_sep = separators[0];
    let mut out = Vec::with_capacity(estimate_output_size(rows, 1, line_separator.len()));

    for row in rows {
        for (i, field) in row.iter().enumerate() {
            if i > 0 {
                out.push(dump_sep);
            }
            if field_needs_quoting_simd_multi_sep(field, separators, escape) {
                write_quoted_field(&mut out, field, escape);
            } else {
                out.extend_from_slice(field);
            }
        }
        out.extend_from_slice(line_separator);
    }

    out
}

// ==========================================================================
// Tests
// ==========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_simple() {
        let f1: &[u8] = b"hello";
        let f2: &[u8] = b"world";
        let row: &[&[u8]] = &[f1, f2];
        let rows: &[&[&[u8]]] = &[row];

        let result = encode_csv_scalar(rows, b',', b'"', b"\n");
        assert_eq!(result, b"hello,world\n");

        let result = encode_csv_swar(rows, b',', b'"', b"\n");
        assert_eq!(result, b"hello,world\n");

        let result = encode_csv_simd(rows, b',', b'"', b"\n");
        assert_eq!(result, b"hello,world\n");
    }

    #[test]
    fn test_encode_needs_quoting() {
        // Field contains separator
        let f1: &[u8] = b"hello,world";
        let f2: &[u8] = b"plain";
        let row: &[&[u8]] = &[f1, f2];
        let rows: &[&[&[u8]]] = &[row];

        let result = encode_csv_scalar(rows, b',', b'"', b"\n");
        assert_eq!(result, b"\"hello,world\",plain\n");

        let result = encode_csv_swar(rows, b',', b'"', b"\n");
        assert_eq!(result, b"\"hello,world\",plain\n");

        let result = encode_csv_simd(rows, b',', b'"', b"\n");
        assert_eq!(result, b"\"hello,world\",plain\n");
    }

    #[test]
    fn test_encode_escape_doubling() {
        // Field contains escape character
        let f1: &[u8] = b"say \"hello\"";
        let row: &[&[u8]] = &[f1];
        let rows: &[&[&[u8]]] = &[row];

        let result = encode_csv_scalar(rows, b',', b'"', b"\n");
        assert_eq!(result, b"\"say \"\"hello\"\"\"\n");

        let result = encode_csv_swar(rows, b',', b'"', b"\n");
        assert_eq!(result, b"\"say \"\"hello\"\"\"\n");

        let result = encode_csv_simd(rows, b',', b'"', b"\n");
        assert_eq!(result, b"\"say \"\"hello\"\"\"\n");
    }

    #[test]
    fn test_encode_newline_in_field() {
        let f1: &[u8] = b"line1\nline2";
        let row: &[&[u8]] = &[f1];
        let rows: &[&[&[u8]]] = &[row];

        let result = encode_csv_scalar(rows, b',', b'"', b"\n");
        assert_eq!(result, b"\"line1\nline2\"\n");

        let result = encode_csv_swar(rows, b',', b'"', b"\n");
        assert_eq!(result, b"\"line1\nline2\"\n");

        let result = encode_csv_simd(rows, b',', b'"', b"\n");
        assert_eq!(result, b"\"line1\nline2\"\n");
    }

    #[test]
    fn test_encode_multiple_rows() {
        let r1: &[&[u8]] = &[b"a" as &[u8], b"b"];
        let r2: &[&[u8]] = &[b"1" as &[u8], b"2"];
        let rows: &[&[&[u8]]] = &[r1, r2];

        let result = encode_csv_scalar(rows, b',', b'"', b"\n");
        assert_eq!(result, b"a,b\n1,2\n");
    }

    #[test]
    fn test_encode_crlf_line_separator() {
        let row: &[&[u8]] = &[b"a" as &[u8], b"b"];
        let rows: &[&[&[u8]]] = &[row];

        let result = encode_csv_scalar(rows, b',', b'"', b"\r\n");
        assert_eq!(result, b"a,b\r\n");
    }

    #[test]
    fn test_encode_empty_field() {
        let row: &[&[u8]] = &[b"" as &[u8], b"x", b""];
        let rows: &[&[&[u8]]] = &[row];

        let result = encode_csv_scalar(rows, b',', b'"', b"\n");
        assert_eq!(result, b",x,\n");
    }

    #[test]
    fn test_encode_general_multi_byte() {
        let f1: &[u8] = b"hello";
        let f2: &[u8] = b"wor::ld";
        let row: &[&[u8]] = &[f1, f2];
        let rows: &[&[&[u8]]] = &[row];

        let result = encode_csv_general(rows, b"::", b"$$", b"\n");
        // f2 contains "::" so it needs quoting with "$$"
        assert_eq!(result, b"hello::$$wor::ld$$\n");
    }

    #[test]
    fn test_swar_field_needs_quoting() {
        // Short field (< 8 bytes) — scalar tail
        assert!(field_needs_quoting_swar(b"a,b", b',', b'"'));
        assert!(!field_needs_quoting_swar(b"abc", b',', b'"'));

        // Long field (>= 8 bytes) — SWAR path
        assert!(field_needs_quoting_swar(
            b"abcdefghij,klm",
            b',',
            b'"'
        ));
        assert!(!field_needs_quoting_swar(
            b"abcdefghijklmno",
            b',',
            b'"'
        ));
    }

    #[test]
    fn test_simd_field_needs_quoting() {
        // Short field — scalar tail
        assert!(field_needs_quoting_simd(b"a,b", b',', b'"'));
        assert!(!field_needs_quoting_simd(b"abc", b',', b'"'));

        // Medium field (>= 16 bytes) — SIMD path
        let clean = b"abcdefghijklmnopqrstuvwxyz";
        assert!(!field_needs_quoting_simd(clean, b',', b'"'));

        let dirty = b"abcdefghijklmno,qrstuvwxyz";
        assert!(field_needs_quoting_simd(dirty, b',', b'"'));
    }

    #[test]
    fn test_all_strategies_identical_output() {
        // Complex input with mixed quoting needs
        let fields: Vec<&[u8]> = vec![
            b"plain",
            b"has,comma",
            b"has\"quote",
            b"has\nnewline",
            b"has\r\ncrlf",
            b"normal field here",
            b"another,one",
            b"",
            b"last",
        ];
        let row: Vec<&[u8]> = fields;
        let row_slice: &[&[u8]] = &row;
        let rows: &[&[&[u8]]] = &[row_slice];

        let scalar = encode_csv_scalar(rows, b',', b'"', b"\n");
        let swar = encode_csv_swar(rows, b',', b'"', b"\n");
        let simd = encode_csv_simd(rows, b',', b'"', b"\n");

        assert_eq!(scalar, swar, "SWAR output must match scalar");
        assert_eq!(scalar, simd, "SIMD output must match scalar");
    }
}

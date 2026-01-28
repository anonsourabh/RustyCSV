// Shared term building utilities for converting Rust data to Elixir terms

use rustler::sys::enif_make_sub_binary;
use rustler::{Binary, Env, NewBinary, Term};
use std::borrow::Cow;

/// Convert parsed rows (borrowed slices) to Elixir term (list of lists of binaries)
#[allow(dead_code)]
pub fn rows_to_term<'a>(env: Env<'a>, rows: Vec<Vec<&[u8]>>) -> Term<'a> {
    // Build list in reverse, then reverse at the end (efficient for cons lists)
    let mut list = Term::list_new_empty(env);

    for row in rows.into_iter().rev() {
        let row_term = fields_to_term(env, row);
        list = list.list_prepend(row_term);
    }

    list
}

/// Convert a single row's fields (borrowed) to an Elixir list of binaries
#[allow(dead_code)]
pub fn fields_to_term<'a>(env: Env<'a>, fields: Vec<&[u8]>) -> Term<'a> {
    let mut list = Term::list_new_empty(env);

    for field in fields.into_iter().rev() {
        let mut binary = NewBinary::new(env, field.len());
        binary.as_mut_slice().copy_from_slice(field);
        let binary_term: Term = binary.into();
        list = list.list_prepend(binary_term);
    }

    list
}

/// Convert owned rows to Elixir term (for streaming/parallel parsers)
pub fn owned_rows_to_term<'a>(env: Env<'a>, rows: Vec<Vec<Vec<u8>>>) -> Term<'a> {
    let mut list = Term::list_new_empty(env);

    for row in rows.into_iter().rev() {
        let row_term = owned_fields_to_term(env, row);
        list = list.list_prepend(row_term);
    }

    list
}

/// Convert owned fields to an Elixir list of binaries
pub fn owned_fields_to_term<'a>(env: Env<'a>, fields: Vec<Vec<u8>>) -> Term<'a> {
    let mut list = Term::list_new_empty(env);

    for field in fields.into_iter().rev() {
        let mut binary = NewBinary::new(env, field.len());
        binary.as_mut_slice().copy_from_slice(&field);
        let binary_term: Term = binary.into();
        list = list.list_prepend(binary_term);
    }

    list
}

/// Convert Cow-based rows to Elixir term (for strategies that may need to allocate for escaped quotes)
pub fn cow_rows_to_term<'a>(env: Env<'a>, rows: Vec<Vec<Cow<'_, [u8]>>>) -> Term<'a> {
    let mut list = Term::list_new_empty(env);

    for row in rows.into_iter().rev() {
        let row_term = cow_fields_to_term(env, row);
        list = list.list_prepend(row_term);
    }

    list
}

/// Convert Cow-based fields to an Elixir list of binaries
pub fn cow_fields_to_term<'a>(env: Env<'a>, fields: Vec<Cow<'_, [u8]>>) -> Term<'a> {
    let mut list = Term::list_new_empty(env);

    for field in fields.into_iter().rev() {
        let bytes = field.as_ref();
        let mut binary = NewBinary::new(env, bytes.len());
        binary.as_mut_slice().copy_from_slice(bytes);
        let binary_term: Term = binary.into();
        list = list.list_prepend(binary_term);
    }

    list
}

// ============================================================================
// Zero-Copy Sub-Binary Support
// ============================================================================

/// Create a sub-binary term referencing the original input (zero-copy)
///
/// # Safety
/// The input binary must remain valid for the lifetime of the returned term.
/// This is guaranteed when used within a NIF call since the input binary
/// is owned by the calling process.
#[inline]
unsafe fn make_subbinary<'a>(
    env: Env<'a>,
    input_term: Term<'a>,
    start: usize,
    len: usize,
) -> Term<'a> {
    let raw_term = enif_make_sub_binary(env.as_c_arg(), input_term.as_c_arg(), start, len);
    Term::new(env, raw_term)
}

/// Unescape doubled quotes in a field: "" -> "
pub(crate) fn unescape_field(inner: &[u8], escape: u8) -> Vec<u8> {
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

/// Convert a single field to a term, using sub-binary when possible (hybrid Cow approach)
/// - Unquoted fields: sub-binary (zero-copy)
/// - Quoted without escapes: sub-binary of inner content (zero-copy)
/// - Quoted with escapes: copy and unescape (must allocate)
#[inline]
fn field_to_term_hybrid<'a>(
    env: Env<'a>,
    input_bytes: &[u8],
    input_term: Term<'a>,
    start: usize,
    end: usize,
    escape: u8,
) -> Term<'a> {
    if start >= end {
        // Empty field - create empty binary
        let binary = NewBinary::new(env, 0);
        return binary.into();
    }

    let field = &input_bytes[start..end];

    // Check if quoted
    if field.len() >= 2 && field[0] == escape && field[field.len() - 1] == escape {
        let inner = &field[1..field.len() - 1];

        if inner.contains(&escape) {
            // Must copy and unescape: "val""ue" -> val"ue
            let unescaped = unescape_field(inner, escape);
            let mut binary = NewBinary::new(env, unescaped.len());
            binary.as_mut_slice().copy_from_slice(&unescaped);
            return binary.into();
        } else {
            // Quoted but no escapes: sub-binary of inner content
            return unsafe { make_subbinary(env, input_term, start + 1, end - start - 2) };
        }
    }

    // Unquoted: direct sub-binary
    unsafe { make_subbinary(env, input_term, start, end - start) }
}

/// Convert field boundaries to Elixir terms using hybrid sub-binary/copy approach
/// boundaries: Vec of rows, each row is Vec of (start, end) pairs
pub fn boundaries_to_term_hybrid<'a>(
    env: Env<'a>,
    input: Binary<'a>,
    boundaries: Vec<Vec<(usize, usize)>>,
    escape: u8,
) -> Term<'a> {
    let input_bytes = input.as_slice();
    let input_term = input.to_term(env);
    let mut list = Term::list_new_empty(env);

    for row in boundaries.into_iter().rev() {
        let mut row_list = Term::list_new_empty(env);
        for (start, end) in row.into_iter().rev() {
            let field_term = field_to_term_hybrid(env, input_bytes, input_term, start, end, escape);
            row_list = row_list.list_prepend(field_term);
        }
        list = list.list_prepend(row_list);
    }

    list
}

// ============================================================================
// General Multi-Byte Escape Support (for zero-copy path)
// ============================================================================

use rustler::types::atom;
use rustler::Encoder;

use crate::strategy::{contains_escape, unescape_field_general};

/// Convert a single field to a term with multi-byte escape, using sub-binary when possible
#[inline]
fn field_to_term_hybrid_general<'a>(
    env: Env<'a>,
    input_bytes: &[u8],
    input_term: Term<'a>,
    start: usize,
    end: usize,
    escape: &[u8],
) -> Term<'a> {
    if start >= end {
        let binary = NewBinary::new(env, 0);
        return binary.into();
    }

    let field = &input_bytes[start..end];
    let esc_len = escape.len();

    // Check if quoted (starts and ends with escape)
    if field.len() >= 2 * esc_len
        && field[..esc_len] == *escape
        && field[field.len() - esc_len..] == *escape
    {
        let inner = &field[esc_len..field.len() - esc_len];

        if contains_escape(inner, escape) {
            // Must copy and unescape
            let unescaped = unescape_field_general(inner, escape);
            let mut binary = NewBinary::new(env, unescaped.len());
            binary.as_mut_slice().copy_from_slice(&unescaped);
            return binary.into();
        } else {
            // Quoted but no escapes: sub-binary of inner content
            return unsafe { make_subbinary(env, input_term, start + esc_len, end - start - 2 * esc_len) };
        }
    }

    // Unquoted: direct sub-binary
    unsafe { make_subbinary(env, input_term, start, end - start) }
}

/// Convert field boundaries to Elixir terms with multi-byte escape support
pub fn boundaries_to_term_hybrid_general<'a>(
    env: Env<'a>,
    input: Binary<'a>,
    boundaries: Vec<Vec<(usize, usize)>>,
    escape: &[u8],
) -> Term<'a> {
    let input_bytes = input.as_slice();
    let input_term = input.to_term(env);
    let mut list = Term::list_new_empty(env);

    for row in boundaries.into_iter().rev() {
        let mut row_list = Term::list_new_empty(env);
        for (start, end) in row.into_iter().rev() {
            let field_term = field_to_term_hybrid_general(env, input_bytes, input_term, start, end, escape);
            row_list = row_list.list_prepend(field_term);
        }
        list = list.list_prepend(row_list);
    }

    list
}

// ============================================================================
// Map Builders (for headers-to-maps feature)
// ============================================================================

/// Build a map from key/value term arrays.
/// Uses fast O(n) path, falls back to incremental for duplicate keys.
fn make_map<'a>(env: Env<'a>, keys: &[Term<'a>], values: &[Term<'a>]) -> Term<'a> {
    match Term::map_from_term_arrays(env, keys, values) {
        Ok(map) => map,
        Err(_) => {
            // Duplicate keys: build incrementally (last value wins)
            let mut map = Term::map_new(env);
            for (k, v) in keys.iter().zip(values.iter()) {
                map = map.map_put(*k, *v).unwrap_or(map);
            }
            map
        }
    }
}

/// Convert Cow rows to maps using pre-built key terms.
/// Takes a slice to avoid skip+collect copies at the call site.
pub fn cow_rows_to_maps<'a>(
    env: Env<'a>,
    keys: &[Term<'a>],
    rows: &[Vec<Cow<'_, [u8]>>],
) -> Term<'a> {
    let num_keys = keys.len();
    let nil_term = atom::nil().encode(env);
    let mut value_terms = vec![nil_term; num_keys];
    let mut list = Term::list_new_empty(env);

    for row in rows.iter().rev() {
        for i in 0..num_keys {
            value_terms[i] = if i < row.len() {
                let bytes = row[i].as_ref();
                let mut binary = NewBinary::new(env, bytes.len());
                binary.as_mut_slice().copy_from_slice(bytes);
                binary.into()
            } else {
                nil_term
            };
        }
        list = list.list_prepend(make_map(env, keys, &value_terms));
    }
    list
}

/// Convert owned rows to maps.
/// Takes a slice to avoid skip+collect copies at the call site.
pub fn owned_rows_to_maps<'a>(
    env: Env<'a>,
    keys: &[Term<'a>],
    rows: &[Vec<Vec<u8>>],
) -> Term<'a> {
    let num_keys = keys.len();
    let nil_term = atom::nil().encode(env);
    let mut value_terms = vec![nil_term; num_keys];
    let mut list = Term::list_new_empty(env);

    for row in rows.iter().rev() {
        for i in 0..num_keys {
            value_terms[i] = if i < row.len() {
                let mut binary = NewBinary::new(env, row[i].len());
                binary.as_mut_slice().copy_from_slice(&row[i]);
                binary.into()
            } else {
                nil_term
            };
        }
        list = list.list_prepend(make_map(env, keys, &value_terms));
    }
    list
}

/// Convert boundary rows to maps with sub-binary hybrid approach (single-byte escape).
/// Takes a slice to avoid skip+collect copies at the call site.
pub fn boundaries_to_maps_hybrid<'a>(
    env: Env<'a>,
    input: Binary<'a>,
    keys: &[Term<'a>],
    boundaries: &[Vec<(usize, usize)>],
    escape: u8,
) -> Term<'a> {
    let input_bytes = input.as_slice();
    let input_term = input.to_term(env);
    let num_keys = keys.len();
    let nil_term = atom::nil().encode(env);
    let mut value_terms = vec![nil_term; num_keys];
    let mut list = Term::list_new_empty(env);

    for row in boundaries.iter().rev() {
        for i in 0..num_keys {
            value_terms[i] = if i < row.len() {
                let (start, end) = row[i];
                field_to_term_hybrid(env, input_bytes, input_term, start, end, escape)
            } else {
                nil_term
            };
        }
        list = list.list_prepend(make_map(env, keys, &value_terms));
    }
    list
}

/// Convert boundary rows to maps with multi-byte escape hybrid approach.
/// Takes a slice to avoid skip+collect copies at the call site.
pub fn boundaries_to_maps_hybrid_general<'a>(
    env: Env<'a>,
    input: Binary<'a>,
    keys: &[Term<'a>],
    boundaries: &[Vec<(usize, usize)>],
    escape: &[u8],
) -> Term<'a> {
    let input_bytes = input.as_slice();
    let input_term = input.to_term(env);
    let num_keys = keys.len();
    let nil_term = atom::nil().encode(env);
    let mut value_terms = vec![nil_term; num_keys];
    let mut list = Term::list_new_empty(env);

    for row in boundaries.iter().rev() {
        for i in 0..num_keys {
            value_terms[i] = if i < row.len() {
                let (start, end) = row[i];
                field_to_term_hybrid_general(env, input_bytes, input_term, start, end, escape)
            } else {
                nil_term
            };
        }
        list = list.list_prepend(make_map(env, keys, &value_terms));
    }
    list
}


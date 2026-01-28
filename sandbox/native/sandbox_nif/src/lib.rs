// Sandbox NIF for testing headers-to-maps with key interning
//
// This is a prototype to validate performance before integrating into RustyCSV.

use memchr::memchr;
use rustler::sys::{enif_make_map_from_arrays, ERL_NIF_TERM};
use rustler::{Binary, Encoder, Env, NewBinary, NifResult, Term};
use std::borrow::Cow;

// ============================================================================
// CSV Parsing (borrowed from RustyCSV core)
// ============================================================================

/// Parse CSV into rows of fields (Cow for zero-copy when possible)
fn parse_csv_fast<'a>(input: &'a [u8], separator: u8, escape: u8) -> Vec<Vec<Cow<'a, [u8]>>> {
    let mut rows = Vec::new();
    let mut pos = 0;

    while pos < input.len() {
        let (row, next_pos) = parse_row(input, pos, separator, escape);
        if !row.is_empty() || next_pos > pos {
            rows.push(row);
        }
        pos = next_pos;
    }

    rows
}

fn parse_row<'a>(
    input: &'a [u8],
    start: usize,
    separator: u8,
    escape: u8,
) -> (Vec<Cow<'a, [u8]>>, usize) {
    let mut fields = Vec::new();
    let mut pos = start;

    loop {
        if pos >= input.len() {
            break;
        }

        let (field, next_pos, is_eol) = parse_field(input, pos, separator, escape);
        fields.push(field);
        pos = next_pos;

        if is_eol || pos >= input.len() {
            break;
        }
    }

    (fields, pos)
}

fn parse_field<'a>(
    input: &'a [u8],
    start: usize,
    separator: u8,
    escape: u8,
) -> (Cow<'a, [u8]>, usize, bool) {
    if start >= input.len() {
        return (Cow::Borrowed(&[]), start, true);
    }

    // Check if field is quoted
    if input[start] == escape {
        parse_quoted_field(input, start, separator, escape)
    } else {
        parse_unquoted_field(input, start, separator)
    }
}

fn parse_unquoted_field<'a>(
    input: &'a [u8],
    start: usize,
    separator: u8,
) -> (Cow<'a, [u8]>, usize, bool) {
    let slice = &input[start..];

    // Look for separator, \r, or \n
    for (i, &byte) in slice.iter().enumerate() {
        if byte == separator {
            return (Cow::Borrowed(&input[start..start + i]), start + i + 1, false);
        }
        if byte == b'\n' {
            return (Cow::Borrowed(&input[start..start + i]), start + i + 1, true);
        }
        if byte == b'\r' {
            let end = start + i;
            let next = if end + 1 < input.len() && input[end + 1] == b'\n' {
                end + 2
            } else {
                end + 1
            };
            return (Cow::Borrowed(&input[start..end]), next, true);
        }
    }

    // No delimiter found, rest of input is the field
    (Cow::Borrowed(&input[start..]), input.len(), true)
}

fn parse_quoted_field<'a>(
    input: &'a [u8],
    start: usize,
    separator: u8,
    escape: u8,
) -> (Cow<'a, [u8]>, usize, bool) {
    let mut pos = start + 1; // Skip opening quote
    let content_start = pos;
    let mut needs_unescape = false;

    while pos < input.len() {
        if let Some(offset) = memchr(escape, &input[pos..]) {
            pos += offset;

            if pos + 1 < input.len() && input[pos + 1] == escape {
                // Escaped quote
                needs_unescape = true;
                pos += 2;
            } else {
                // End of quoted field
                let content_end = pos;
                pos += 1; // Skip closing quote

                // Skip separator or newline
                let (next_pos, is_eol) = if pos < input.len() {
                    if input[pos] == separator {
                        (pos + 1, false)
                    } else if input[pos] == b'\n' {
                        (pos + 1, true)
                    } else if input[pos] == b'\r' {
                        if pos + 1 < input.len() && input[pos + 1] == b'\n' {
                            (pos + 2, true)
                        } else {
                            (pos + 1, true)
                        }
                    } else {
                        (pos, false)
                    }
                } else {
                    (pos, true)
                };

                let field = if needs_unescape {
                    Cow::Owned(unescape_field(&input[content_start..content_end], escape))
                } else {
                    Cow::Borrowed(&input[content_start..content_end])
                };

                return (field, next_pos, is_eol);
            }
        } else {
            break;
        }
    }

    // Unterminated quote - return rest as field
    (Cow::Borrowed(&input[content_start..]), input.len(), true)
}

fn unescape_field(input: &[u8], escape: u8) -> Vec<u8> {
    let mut result = Vec::with_capacity(input.len());
    let mut i = 0;

    while i < input.len() {
        if input[i] == escape && i + 1 < input.len() && input[i + 1] == escape {
            result.push(escape);
            i += 2;
        } else {
            result.push(input[i]);
            i += 1;
        }
    }

    result
}

// ============================================================================
// Strategy 1: Parse to maps with interned keys (keys created once, reused)
// ============================================================================

/// Parse CSV and return list of maps with interned header keys.
/// 
/// The header row is parsed first, and those binary terms are reused
/// as map keys for every subsequent row - avoiding repeated allocations.
#[rustler::nif]
fn parse_to_maps_interned<'a>(
    env: Env<'a>,
    input: Binary<'a>,
    separator: u8,
    escape: u8,
) -> NifResult<Term<'a>> {
    let bytes = input.as_slice();
    let rows = parse_csv_fast(bytes, separator, escape);

    if rows.is_empty() {
        return Ok(Term::list_new_empty(env));
    }

    // Parse header row and create interned key terms ONCE
    let header_row = &rows[0];
    let header_terms: Vec<Term<'a>> = header_row
        .iter()
        .map(|field| {
            let bytes = field.as_ref();
            let mut binary = NewBinary::new(env, bytes.len());
            binary.as_mut_slice().copy_from_slice(bytes);
            let binary_term: Term = binary.into();
            binary_term
        })
        .collect();

    // Build maps for data rows, reusing header_terms as keys
    let mut result_list = Term::list_new_empty(env);

    for row in rows[1..].iter().rev() {
        let map = build_map_with_keys(env, &header_terms, row);
        result_list = result_list.list_prepend(map);
    }

    Ok(result_list)
}

/// Build a map using pre-created key terms (interned) - FAST version
/// Uses enif_make_map_from_arrays for O(n) map creation instead of O(n^2)
fn build_map_with_keys<'a>(
    env: Env<'a>,
    keys: &[Term<'a>],
    values: &[Cow<'_, [u8]>],
) -> Term<'a> {
    let count = keys.len().min(values.len());
    
    // Pre-allocate arrays for the C API
    let key_terms: Vec<ERL_NIF_TERM> = keys.iter().take(count).map(|t| t.as_c_arg()).collect();
    
    let value_terms: Vec<ERL_NIF_TERM> = values
        .iter()
        .take(count)
        .map(|value| {
            let value_bytes = value.as_ref();
            let mut binary = NewBinary::new(env, value_bytes.len());
            binary.as_mut_slice().copy_from_slice(value_bytes);
            let value_term: Term = binary.into();
            value_term.as_c_arg()
        })
        .collect();

    // Create map in one shot - O(n) instead of O(n^2)
    let mut map_term: ERL_NIF_TERM = 0;
    let success = unsafe {
        enif_make_map_from_arrays(
            env.as_c_arg(),
            key_terms.as_ptr(),
            value_terms.as_ptr(),
            count,
            &mut map_term,
        )
    };

    if success != 0 {
        unsafe { Term::new(env, map_term) }
    } else {
        // Fallback to empty map on error
        Term::map_new(env)
    }
}

// ============================================================================
// Strategy 2: Parse to maps WITHOUT interning (baseline for comparison)
// ============================================================================

/// Parse CSV to maps, creating new key binaries for each row.
/// This simulates what Enum.zip does - no key reuse.
#[rustler::nif]
fn parse_to_maps_no_intern<'a>(
    env: Env<'a>,
    input: Binary<'a>,
    separator: u8,
    escape: u8,
) -> NifResult<Term<'a>> {
    let bytes = input.as_slice();
    let rows = parse_csv_fast(bytes, separator, escape);

    if rows.is_empty() {
        return Ok(Term::list_new_empty(env));
    }

    let header_row = &rows[0];

    // Build maps, creating NEW key binaries for each row (no interning)
    let mut result_list = Term::list_new_empty(env);

    for row in rows[1..].iter().rev() {
        let map = build_map_fresh_keys(env, header_row, row);
        result_list = result_list.list_prepend(map);
    }

    Ok(result_list)
}

/// Build a map creating fresh key binaries (no interning) - FAST version
fn build_map_fresh_keys<'a>(
    env: Env<'a>,
    keys: &[Cow<'_, [u8]>],
    values: &[Cow<'_, [u8]>],
) -> Term<'a> {
    let count = keys.len().min(values.len());

    let key_terms: Vec<ERL_NIF_TERM> = keys
        .iter()
        .take(count)
        .map(|key| {
            let key_bytes = key.as_ref();
            let mut key_binary = NewBinary::new(env, key_bytes.len());
            key_binary.as_mut_slice().copy_from_slice(key_bytes);
            let key_term: Term = key_binary.into();
            key_term.as_c_arg()
        })
        .collect();

    let value_terms: Vec<ERL_NIF_TERM> = values
        .iter()
        .take(count)
        .map(|value| {
            let value_bytes = value.as_ref();
            let mut value_binary = NewBinary::new(env, value_bytes.len());
            value_binary.as_mut_slice().copy_from_slice(value_bytes);
            let value_term: Term = value_binary.into();
            value_term.as_c_arg()
        })
        .collect();

    let mut map_term: ERL_NIF_TERM = 0;
    let success = unsafe {
        enif_make_map_from_arrays(
            env.as_c_arg(),
            key_terms.as_ptr(),
            value_terms.as_ptr(),
            count,
            &mut map_term,
        )
    };

    if success != 0 {
        unsafe { Term::new(env, map_term) }
    } else {
        Term::map_new(env)
    }
}

// ============================================================================
// Strategy 3: Parse to maps with atom keys (if headers are valid atoms)
// ============================================================================

/// Parse CSV to maps with atom keys (interned by BEAM).
/// Only works if headers are valid atom names.
#[rustler::nif]
fn parse_to_maps_atoms<'a>(
    env: Env<'a>,
    input: Binary<'a>,
    separator: u8,
    escape: u8,
) -> NifResult<Term<'a>> {
    let bytes = input.as_slice();
    let rows = parse_csv_fast(bytes, separator, escape);

    if rows.is_empty() {
        return Ok(Term::list_new_empty(env));
    }

    let header_row = &rows[0];

    // Create atom keys from headers (atoms are interned by BEAM)
    let atom_keys: Vec<Option<Term<'a>>> = header_row
        .iter()
        .map(|field| {
            let s = std::str::from_utf8(field.as_ref()).ok()?;
            rustler::Atom::from_str(env, s).ok().map(|a| a.encode(env))
        })
        .collect();

    // Check if all headers are valid atoms
    if atom_keys.iter().any(|k| k.is_none()) {
        // Fall back to interned binary keys
        let header_terms: Vec<Term<'a>> = header_row
            .iter()
            .map(|field| {
                let bytes = field.as_ref();
                let mut binary = NewBinary::new(env, bytes.len());
                binary.as_mut_slice().copy_from_slice(bytes);
                let binary_term: Term = binary.into();
                binary_term
            })
            .collect();

        let mut result_list = Term::list_new_empty(env);
        for row in rows[1..].iter().rev() {
            let map = build_map_with_keys(env, &header_terms, row);
            result_list = result_list.list_prepend(map);
        }
        return Ok(result_list);
    }

    let atom_keys: Vec<Term<'a>> = atom_keys.into_iter().flatten().collect();

    // Build maps with atom keys
    let mut result_list = Term::list_new_empty(env);

    for row in rows[1..].iter().rev() {
        let map = build_map_with_keys(env, &atom_keys, row);
        result_list = result_list.list_prepend(map);
    }

    Ok(result_list)
}

// ============================================================================
// NIF Initialization
// ============================================================================

rustler::init!("Elixir.Sandbox.Native");

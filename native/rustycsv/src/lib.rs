// RustyCSV - Fast CSV parsing with multiple strategies
//
// Strategies:
// A: Basic byte-by-byte parsing (parse_string)
// B: SIMD-accelerated via memchr (parse_string_fast)
// C: Two-phase index-then-extract (parse_string_indexed)
// D: Streaming chunked parser (streaming_*)
// E: Parallel parsing via rayon (parse_string_parallel)
// F: Zero-copy sub-binary parsing (parse_string_zero_copy)

use rustler::{Binary, Env, Error, NewBinary, NifResult, ResourceArc, Term};
use std::borrow::Cow;

mod core;
mod resource;
mod strategy;
mod term;

/// Separators: list of patterns. Each pattern can be multi-byte.
struct Separators {
    patterns: Vec<Vec<u8>>,
}

/// Escape: single pattern, possibly multi-byte.
struct Escape {
    bytes: Vec<u8>,
}

/// Decode separator from a Term.
/// Accepts: integer 44, binary <<44>>, or list [<<44>>, <<59>>], [<<58,58>>]
fn decode_separators<'a>(term: Term<'a>) -> NifResult<Separators> {
    // Try integer (single byte, single separator)
    if let Ok(byte) = term.decode::<u8>() {
        return Ok(Separators {
            patterns: vec![vec![byte]],
        });
    }
    // Try list of binaries (multiple separators, each possibly multi-byte)
    // Must check list BEFORE single binary.
    if let Ok(list) = term.decode::<Vec<Binary<'a>>>() {
        let patterns: Vec<Vec<u8>> = list.iter().map(|b| b.as_slice().to_vec()).collect();
        if patterns.is_empty() || patterns.iter().any(|p| p.is_empty()) {
            return Err(Error::BadArg);
        }
        return Ok(Separators { patterns });
    }
    // Try single binary (single separator, possibly multi-byte)
    if let Ok(binary) = term.decode::<Binary<'a>>() {
        let slice = binary.as_slice();
        if slice.is_empty() {
            return Err(Error::BadArg);
        }
        return Ok(Separators {
            patterns: vec![slice.to_vec()],
        });
    }
    Err(Error::BadArg)
}

/// Decode escape from a Term.
/// Accepts: integer 34 or binary <<34>> or binary <<36,36>>
fn decode_escape<'a>(term: Term<'a>) -> NifResult<Escape> {
    if let Ok(byte) = term.decode::<u8>() {
        return Ok(Escape {
            bytes: vec![byte],
        });
    }
    if let Ok(binary) = term.decode::<Binary<'a>>() {
        let slice = binary.as_slice();
        if slice.is_empty() {
            return Err(Error::BadArg);
        }
        return Ok(Escape {
            bytes: slice.to_vec(),
        });
    }
    Err(Error::BadArg)
}

/// Check if all separators and escape are single-byte (fast path eligible)
fn is_all_single_byte(separators: &Separators, escape: &Escape) -> bool {
    escape.bytes.len() == 1 && separators.patterns.iter().all(|p| p.len() == 1)
}

/// Extract single-byte separator values for fast path
fn single_byte_seps(separators: &Separators) -> Vec<u8> {
    separators.patterns.iter().map(|p| p[0]).collect()
}

use resource::{StreamingParserRef, StreamingParserResource};
use strategy::{
    contains_escape, parse_csv, parse_csv_boundaries_general, parse_csv_boundaries_multi_sep,
    parse_csv_boundaries_with_config, parse_csv_fast, parse_csv_fast_multi_sep,
    parse_csv_fast_with_config, parse_csv_general, parse_csv_indexed,
    parse_csv_indexed_general, parse_csv_indexed_multi_sep, parse_csv_indexed_with_config,
    parse_csv_multi_sep, parse_csv_parallel, parse_csv_parallel_general,
    parse_csv_parallel_multi_sep, parse_csv_parallel_with_config, parse_csv_with_config,
    unescape_field_general,
};
use term::{
    boundaries_to_maps_hybrid, boundaries_to_maps_hybrid_general, boundaries_to_term_hybrid,
    boundaries_to_term_hybrid_general, cow_rows_to_maps, cow_rows_to_term, owned_rows_to_maps,
    owned_rows_to_term,
};

// ============================================================================
// Allocator Configuration
// ============================================================================

// When memory_tracking is enabled, wrap the allocator to track usage
#[cfg(feature = "memory_tracking")]
mod tracking {
    use std::alloc::{GlobalAlloc, Layout};
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
    pub static PEAK_ALLOCATED: AtomicUsize = AtomicUsize::new(0);

    pub struct TrackingAllocator;

    #[cfg(feature = "mimalloc")]
    static UNDERLYING: mimalloc::MiMalloc = mimalloc::MiMalloc;

    #[cfg(not(feature = "mimalloc"))]
    static UNDERLYING: std::alloc::System = std::alloc::System;

    unsafe impl GlobalAlloc for TrackingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let ptr = UNDERLYING.alloc(layout);
            if !ptr.is_null() {
                let current = ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
                let mut peak = PEAK_ALLOCATED.load(Ordering::Relaxed);
                while current > peak {
                    match PEAK_ALLOCATED.compare_exchange_weak(
                        peak,
                        current,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(p) => peak = p,
                    }
                }
            }
            ptr
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            ALLOCATED.fetch_sub(layout.size(), Ordering::Relaxed);
            UNDERLYING.dealloc(ptr, layout)
        }
    }
}

#[cfg(feature = "memory_tracking")]
#[global_allocator]
static GLOBAL: tracking::TrackingAllocator = tracking::TrackingAllocator;

// When memory_tracking is disabled, use mimalloc directly (no overhead)
#[cfg(all(feature = "mimalloc", not(feature = "memory_tracking")))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// ============================================================================
// Memory Tracking NIFs (only available when memory_tracking feature is enabled)
// ============================================================================

#[cfg(feature = "memory_tracking")]
use std::sync::atomic::Ordering;

/// Get current Rust heap allocation in bytes (requires memory_tracking feature)
#[cfg(feature = "memory_tracking")]
#[rustler::nif]
fn get_rust_memory() -> usize {
    tracking::ALLOCATED.load(Ordering::SeqCst)
}

/// Get peak Rust heap allocation since last reset (requires memory_tracking feature)
#[cfg(feature = "memory_tracking")]
#[rustler::nif]
fn get_rust_memory_peak() -> usize {
    tracking::PEAK_ALLOCATED.load(Ordering::SeqCst)
}

/// Reset memory stats (requires memory_tracking feature)
#[cfg(feature = "memory_tracking")]
#[rustler::nif]
fn reset_rust_memory_stats() -> (usize, usize) {
    let current = tracking::ALLOCATED.load(Ordering::SeqCst);
    let peak = tracking::PEAK_ALLOCATED.swap(current, Ordering::SeqCst);
    (current, peak)
}

/// Stub: returns 0 when memory_tracking is disabled
#[cfg(not(feature = "memory_tracking"))]
#[rustler::nif]
fn get_rust_memory() -> usize {
    0
}

/// Stub: returns 0 when memory_tracking is disabled
#[cfg(not(feature = "memory_tracking"))]
#[rustler::nif]
fn get_rust_memory_peak() -> usize {
    0
}

/// Stub: returns (0, 0) when memory_tracking is disabled
#[cfg(not(feature = "memory_tracking"))]
#[rustler::nif]
fn reset_rust_memory_stats() -> (usize, usize) {
    (0, 0)
}

// ============================================================================
// Strategy A: Basic Parser
// ============================================================================

/// Parse CSV string into list of rows (basic byte-by-byte)
#[rustler::nif]
fn parse_string<'a>(env: Env<'a>, input: Binary<'a>) -> NifResult<Term<'a>> {
    let bytes = input.as_slice();
    let rows = parse_csv(bytes);
    Ok(cow_rows_to_term(env, rows))
}

/// Parse CSV with configurable separator(s) and escape
#[rustler::nif]
fn parse_string_with_config<'a>(
    env: Env<'a>,
    input: Binary<'a>,
    sep_term: Term<'a>,
    esc_term: Term<'a>,
) -> NifResult<Term<'a>> {
    let separators = decode_separators(sep_term)?;
    let escape = decode_escape(esc_term)?;
    let bytes = input.as_slice();

    let rows = if is_all_single_byte(&separators, &escape) {
        let esc = escape.bytes[0];
        let sep_bytes = single_byte_seps(&separators);
        if sep_bytes.len() == 1 {
            parse_csv_with_config(bytes, sep_bytes[0], esc)
        } else {
            parse_csv_multi_sep(bytes, &sep_bytes, esc)
        }
    } else {
        parse_csv_general(bytes, &separators.patterns, &escape.bytes)
    };
    Ok(cow_rows_to_term(env, rows))
}

// ============================================================================
// Strategy B: SIMD-Accelerated Parser
// ============================================================================

/// Parse using memchr for SIMD-accelerated delimiter scanning
#[rustler::nif]
fn parse_string_fast<'a>(env: Env<'a>, input: Binary<'a>) -> NifResult<Term<'a>> {
    let bytes = input.as_slice();
    let rows = parse_csv_fast(bytes);
    Ok(cow_rows_to_term(env, rows))
}

/// Parse using SIMD with configurable separator(s) and escape
#[rustler::nif]
fn parse_string_fast_with_config<'a>(
    env: Env<'a>,
    input: Binary<'a>,
    sep_term: Term<'a>,
    esc_term: Term<'a>,
) -> NifResult<Term<'a>> {
    let separators = decode_separators(sep_term)?;
    let escape = decode_escape(esc_term)?;
    let bytes = input.as_slice();

    let rows = if is_all_single_byte(&separators, &escape) {
        let esc = escape.bytes[0];
        let sep_bytes = single_byte_seps(&separators);
        if sep_bytes.len() == 1 {
            parse_csv_fast_with_config(bytes, sep_bytes[0], esc)
        } else {
            parse_csv_fast_multi_sep(bytes, &sep_bytes, esc)
        }
    } else {
        parse_csv_general(bytes, &separators.patterns, &escape.bytes)
    };
    Ok(cow_rows_to_term(env, rows))
}

// ============================================================================
// Strategy C: Two-Phase Index-then-Extract Parser
// ============================================================================

/// Parse using two-phase approach: build index, then extract
#[rustler::nif]
fn parse_string_indexed<'a>(env: Env<'a>, input: Binary<'a>) -> NifResult<Term<'a>> {
    let bytes = input.as_slice();
    let rows = parse_csv_indexed(bytes);
    Ok(cow_rows_to_term(env, rows))
}

/// Parse using two-phase with configurable separator(s) and escape
#[rustler::nif]
fn parse_string_indexed_with_config<'a>(
    env: Env<'a>,
    input: Binary<'a>,
    sep_term: Term<'a>,
    esc_term: Term<'a>,
) -> NifResult<Term<'a>> {
    let separators = decode_separators(sep_term)?;
    let escape = decode_escape(esc_term)?;
    let bytes = input.as_slice();

    let rows = if is_all_single_byte(&separators, &escape) {
        let esc = escape.bytes[0];
        let sep_bytes = single_byte_seps(&separators);
        if sep_bytes.len() == 1 {
            parse_csv_indexed_with_config(bytes, sep_bytes[0], esc)
        } else {
            parse_csv_indexed_multi_sep(bytes, &sep_bytes, esc)
        }
    } else {
        parse_csv_indexed_general(bytes, &separators.patterns, &escape.bytes)
    };
    Ok(cow_rows_to_term(env, rows))
}

// ============================================================================
// Strategy D: Streaming Parser
// ============================================================================

/// Create a new streaming parser with default settings
#[rustler::nif]
fn streaming_new() -> StreamingParserRef {
    ResourceArc::new(StreamingParserResource::new())
}

/// Create a new streaming parser with configurable separator(s) and escape
#[rustler::nif]
fn streaming_new_with_config<'a>(sep_term: Term<'a>, esc_term: Term<'a>) -> NifResult<StreamingParserRef> {
    let separators = decode_separators(sep_term)?;
    let escape = decode_escape(esc_term)?;
    Ok(if is_all_single_byte(&separators, &escape) {
        let esc = escape.bytes[0];
        let sep_bytes = single_byte_seps(&separators);
        if sep_bytes.len() == 1 {
            ResourceArc::new(StreamingParserResource::with_config(sep_bytes[0], esc))
        } else {
            ResourceArc::new(StreamingParserResource::with_multi_sep(&sep_bytes, esc))
        }
    } else {
        ResourceArc::new(StreamingParserResource::with_general(
            separators.patterns,
            escape.bytes,
        ))
    })
}

/// Feed a chunk of data to the streaming parser
#[rustler::nif]
fn streaming_feed(parser: StreamingParserRef, chunk: Binary) -> (usize, usize) {
    let mut inner = parser.inner.lock().unwrap();
    inner.feed(chunk.as_slice());
    (inner.available_rows(), inner.buffer_size())
}

/// Take up to `max` rows from the streaming parser
#[rustler::nif]
fn streaming_next_rows<'a>(
    env: Env<'a>,
    parser: StreamingParserRef,
    max: usize,
) -> NifResult<Term<'a>> {
    let mut inner = parser.inner.lock().unwrap();
    let rows = inner.take_rows(max);
    Ok(owned_rows_to_term(env, rows))
}

/// Finalize the streaming parser (get remaining partial row)
#[rustler::nif]
fn streaming_finalize<'a>(env: Env<'a>, parser: StreamingParserRef) -> NifResult<Term<'a>> {
    let mut inner = parser.inner.lock().unwrap();
    let rows = inner.finalize();
    Ok(owned_rows_to_term(env, rows))
}

/// Get streaming parser status (available_rows, buffer_size, has_partial)
#[rustler::nif]
fn streaming_status(parser: StreamingParserRef) -> (usize, usize, bool) {
    let inner = parser.inner.lock().unwrap();
    (
        inner.available_rows(),
        inner.buffer_size(),
        inner.has_partial(),
    )
}

// ============================================================================
// Strategy E: Parallel Parser
// ============================================================================

/// Parse CSV in parallel using rayon thread pool
/// Uses DirtyCpu scheduler since this can take significant time
#[rustler::nif(schedule = "DirtyCpu")]
fn parse_string_parallel<'a>(env: Env<'a>, input: Binary<'a>) -> NifResult<Term<'a>> {
    let bytes = input.as_slice();
    let rows = parse_csv_parallel(bytes);
    Ok(owned_rows_to_term(env, rows))
}

/// Parse CSV in parallel with configurable separator(s) and escape
#[rustler::nif(schedule = "DirtyCpu")]
fn parse_string_parallel_with_config<'a>(
    env: Env<'a>,
    input: Binary<'a>,
    sep_term: Term<'a>,
    esc_term: Term<'a>,
) -> NifResult<Term<'a>> {
    let separators = decode_separators(sep_term)?;
    let escape = decode_escape(esc_term)?;
    let bytes = input.as_slice();

    let rows = if is_all_single_byte(&separators, &escape) {
        let esc = escape.bytes[0];
        let sep_bytes = single_byte_seps(&separators);
        if sep_bytes.len() == 1 {
            parse_csv_parallel_with_config(bytes, sep_bytes[0], esc)
        } else {
            parse_csv_parallel_multi_sep(bytes, &sep_bytes, esc)
        }
    } else {
        parse_csv_parallel_general(bytes, &separators.patterns, &escape.bytes)
    };
    Ok(owned_rows_to_term(env, rows))
}

// ============================================================================
// Strategy F: Zero-Copy Parser (Sub-binary references)
// ============================================================================

/// Parse CSV using zero-copy sub-binaries where possible
#[rustler::nif]
fn parse_string_zero_copy<'a>(env: Env<'a>, input: Binary<'a>) -> NifResult<Term<'a>> {
    let bytes = input.as_slice();
    let boundaries = parse_csv_boundaries_with_config(bytes, b',', b'"');
    Ok(boundaries_to_term_hybrid(env, input, boundaries, b'"'))
}

/// Parse CSV using zero-copy with configurable separator(s) and escape
#[rustler::nif]
fn parse_string_zero_copy_with_config<'a>(
    env: Env<'a>,
    input: Binary<'a>,
    sep_term: Term<'a>,
    esc_term: Term<'a>,
) -> NifResult<Term<'a>> {
    let separators = decode_separators(sep_term)?;
    let escape = decode_escape(esc_term)?;
    let bytes = input.as_slice();

    if is_all_single_byte(&separators, &escape) {
        let esc = escape.bytes[0];
        let sep_bytes = single_byte_seps(&separators);
        let boundaries = if sep_bytes.len() == 1 {
            parse_csv_boundaries_with_config(bytes, sep_bytes[0], esc)
        } else {
            parse_csv_boundaries_multi_sep(bytes, &sep_bytes, esc)
        };
        Ok(boundaries_to_term_hybrid(env, input, boundaries, esc))
    } else {
        let boundaries =
            parse_csv_boundaries_general(bytes, &separators.patterns, &escape.bytes);
        Ok(boundaries_to_term_hybrid_general(
            env,
            input,
            boundaries,
            &escape.bytes,
        ))
    }
}

// ============================================================================
// Headers-to-Maps NIFs
// ============================================================================

use rustler::types::ListIterator;

/// Header mode: either auto (first row = keys) or explicit key terms
enum HeaderMode<'a> {
    Auto,
    Explicit(Vec<Term<'a>>),
}

/// Decode header_mode term: atom :true → Auto, list → Explicit(Vec<Term>)
fn decode_header_mode<'a>(header_mode: Term<'a>) -> NifResult<HeaderMode<'a>> {
    // Try atom :true
    if let Ok(s) = header_mode.atom_to_string() {
        if s == "true" {
            return Ok(HeaderMode::Auto);
        }
        return Err(Error::BadArg);
    }
    // Try list of terms (strings or atoms)
    if let Ok(iter) = header_mode.decode::<ListIterator<'a>>() {
        let keys: Vec<Term<'a>> = iter.collect();
        if keys.is_empty() {
            return Err(Error::BadArg);
        }
        return Ok(HeaderMode::Explicit(keys));
    }
    Err(Error::BadArg)
}

/// Convert a Cow field to a binary term (for building key terms from first row)
fn cow_field_to_binary_term<'a>(env: Env<'a>, field: &Cow<'_, [u8]>) -> Term<'a> {
    let bytes = field.as_ref();
    let mut binary = NewBinary::new(env, bytes.len());
    binary.as_mut_slice().copy_from_slice(bytes);
    binary.into()
}

/// Dispatch to Cow-returning parser based on strategy string
fn dispatch_cow_parse<'a>(
    bytes: &'a [u8],
    separators: &Separators,
    escape: &Escape,
    strategy: &str,
) -> Vec<Vec<Cow<'a, [u8]>>> {
    if is_all_single_byte(separators, escape) {
        let esc = escape.bytes[0];
        let sep_bytes = single_byte_seps(separators);
        match strategy {
            "basic" => {
                if sep_bytes.len() == 1 {
                    parse_csv_with_config(bytes, sep_bytes[0], esc)
                } else {
                    parse_csv_multi_sep(bytes, &sep_bytes, esc)
                }
            }
            "simd" => {
                if sep_bytes.len() == 1 {
                    parse_csv_fast_with_config(bytes, sep_bytes[0], esc)
                } else {
                    parse_csv_fast_multi_sep(bytes, &sep_bytes, esc)
                }
            }
            "indexed" => {
                if sep_bytes.len() == 1 {
                    parse_csv_indexed_with_config(bytes, sep_bytes[0], esc)
                } else {
                    parse_csv_indexed_multi_sep(bytes, &sep_bytes, esc)
                }
            }
            _ => unreachable!(),
        }
    } else {
        match strategy {
            "basic" | "simd" => parse_csv_general(bytes, &separators.patterns, &escape.bytes),
            "indexed" => {
                parse_csv_indexed_general(bytes, &separators.patterns, &escape.bytes)
            }
            _ => unreachable!(),
        }
    }
}

/// Parse CSV and return list of maps. Dispatches to strategy internally.
#[rustler::nif]
fn parse_to_maps<'a>(
    env: Env<'a>,
    input: Binary<'a>,
    sep_term: Term<'a>,
    esc_term: Term<'a>,
    strategy: Term<'a>,
    header_mode_term: Term<'a>,
    skip_first: bool,
) -> NifResult<Term<'a>> {
    let separators = decode_separators(sep_term)?;
    let escape = decode_escape(esc_term)?;
    let header_mode = decode_header_mode(header_mode_term)?;
    let strategy_str = strategy
        .atom_to_string()
        .map_err(|_| Error::BadArg)?;
    let bytes = input.as_slice();

    match strategy_str.as_str() {
        "basic" | "simd" | "indexed" => {
            let all_rows = dispatch_cow_parse(bytes, &separators, &escape, &strategy_str);
            if all_rows.is_empty() {
                return Ok(Term::list_new_empty(env));
            }

            match header_mode {
                HeaderMode::Auto => {
                    // First row = keys
                    let key_terms: Vec<Term<'a>> = all_rows[0]
                        .iter()
                        .map(|f| cow_field_to_binary_term(env, f))
                        .collect();
                    Ok(cow_rows_to_maps(env, &key_terms, &all_rows[1..]))
                }
                HeaderMode::Explicit(key_terms) => {
                    let start = if skip_first { 1 } else { 0 };
                    Ok(cow_rows_to_maps(env, &key_terms, &all_rows[start..]))
                }
            }
        }
        "zero_copy" => {
            if is_all_single_byte(&separators, &escape) {
                let esc = escape.bytes[0];
                let sep_bytes = single_byte_seps(&separators);
                let all_boundaries = if sep_bytes.len() == 1 {
                    parse_csv_boundaries_with_config(bytes, sep_bytes[0], esc)
                } else {
                    parse_csv_boundaries_multi_sep(bytes, &sep_bytes, esc)
                };

                if all_boundaries.is_empty() {
                    return Ok(Term::list_new_empty(env));
                }

                match header_mode {
                    HeaderMode::Auto => {
                        // Extract first row as key strings (must copy)
                        let input_bytes = input.as_slice();
                        let key_terms: Vec<Term<'a>> = all_boundaries[0]
                            .iter()
                            .map(|&(start, end)| {
                                let field = &input_bytes[start..end];
                                // Strip quotes if present
                                let content = if field.len() >= 2
                                    && field[0] == esc
                                    && field[field.len() - 1] == esc
                                {
                                    let inner = &field[1..field.len() - 1];
                                    if inner.contains(&esc) {
                                        term::unescape_field(inner, esc)
                                    } else {
                                        inner.to_vec()
                                    }
                                } else {
                                    field.to_vec()
                                };
                                let mut binary = NewBinary::new(env, content.len());
                                binary.as_mut_slice().copy_from_slice(&content);
                                let t: Term = binary.into();
                                t
                            })
                            .collect();
                        Ok(boundaries_to_maps_hybrid(
                            env,
                            input,
                            &key_terms,
                            &all_boundaries[1..],
                            esc,
                        ))
                    }
                    HeaderMode::Explicit(key_terms) => {
                        let start = if skip_first { 1 } else { 0 };
                        Ok(boundaries_to_maps_hybrid(
                            env,
                            input,
                            &key_terms,
                            &all_boundaries[start..],
                            esc,
                        ))
                    }
                }
            } else {
                // Multi-byte escape zero_copy
                let all_boundaries =
                    parse_csv_boundaries_general(bytes, &separators.patterns, &escape.bytes);

                if all_boundaries.is_empty() {
                    return Ok(Term::list_new_empty(env));
                }

                match header_mode {
                    HeaderMode::Auto => {
                        let input_bytes = input.as_slice();
                        let esc = &escape.bytes;
                        let esc_len = esc.len();
                        let key_terms: Vec<Term<'a>> = all_boundaries[0]
                            .iter()
                            .map(|&(start, end)| {
                                let field = &input_bytes[start..end];
                                let content = if field.len() >= 2 * esc_len
                                    && field[..esc_len] == *esc.as_slice()
                                    && field[field.len() - esc_len..] == *esc.as_slice()
                                {
                                    let inner = &field[esc_len..field.len() - esc_len];
                                    if contains_escape(inner, esc) {
                                        unescape_field_general(inner, esc)
                                    } else {
                                        inner.to_vec()
                                    }
                                } else {
                                    field.to_vec()
                                };
                                let mut binary = NewBinary::new(env, content.len());
                                binary.as_mut_slice().copy_from_slice(&content);
                                let t: Term = binary.into();
                                t
                            })
                            .collect();
                        Ok(boundaries_to_maps_hybrid_general(
                            env,
                            input,
                            &key_terms,
                            &all_boundaries[1..],
                            &escape.bytes,
                        ))
                    }
                    HeaderMode::Explicit(key_terms) => {
                        let start = if skip_first { 1 } else { 0 };
                        Ok(boundaries_to_maps_hybrid_general(
                            env,
                            input,
                            &key_terms,
                            &all_boundaries[start..],
                            &escape.bytes,
                        ))
                    }
                }
            }
        }
        _ => Err(Error::BadArg),
    }
}

/// Parallel variant for parse_to_maps on dirty CPU scheduler
#[rustler::nif(schedule = "DirtyCpu")]
fn parse_to_maps_parallel<'a>(
    env: Env<'a>,
    input: Binary<'a>,
    sep_term: Term<'a>,
    esc_term: Term<'a>,
    header_mode_term: Term<'a>,
    skip_first: bool,
) -> NifResult<Term<'a>> {
    let separators = decode_separators(sep_term)?;
    let escape = decode_escape(esc_term)?;
    let header_mode = decode_header_mode(header_mode_term)?;
    let bytes = input.as_slice();

    let all_rows = if is_all_single_byte(&separators, &escape) {
        let esc = escape.bytes[0];
        let sep_bytes = single_byte_seps(&separators);
        if sep_bytes.len() == 1 {
            parse_csv_parallel_with_config(bytes, sep_bytes[0], esc)
        } else {
            parse_csv_parallel_multi_sep(bytes, &sep_bytes, esc)
        }
    } else {
        parse_csv_parallel_general(bytes, &separators.patterns, &escape.bytes)
    };

    if all_rows.is_empty() {
        return Ok(Term::list_new_empty(env));
    }

    match header_mode {
        HeaderMode::Auto => {
            let key_terms: Vec<Term<'a>> = all_rows[0]
                .iter()
                .map(|f| {
                    let mut binary = NewBinary::new(env, f.len());
                    binary.as_mut_slice().copy_from_slice(f);
                    binary.into()
                })
                .collect();
            Ok(owned_rows_to_maps(env, &key_terms, &all_rows[1..]))
        }
        HeaderMode::Explicit(key_terms) => {
            let start = if skip_first { 1 } else { 0 };
            Ok(owned_rows_to_maps(env, &key_terms, &all_rows[start..]))
        }
    }
}

// ============================================================================
// NIF Initialization
// ============================================================================

#[allow(non_local_definitions)]
fn load(env: Env, _info: Term) -> bool {
    let _ = rustler::resource!(StreamingParserResource, env);
    true
}

rustler::init!("Elixir.RustyCSV.Native", load = load);

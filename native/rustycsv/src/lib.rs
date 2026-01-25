// RustyCSV - Fast CSV parsing with multiple strategies
//
// Strategies:
// A: Basic byte-by-byte parsing (parse_string)
// B: SIMD-accelerated via memchr (parse_string_fast)
// C: Two-phase index-then-extract (parse_string_indexed)
// D: Streaming chunked parser (streaming_*)
// E: Parallel parsing via rayon (parse_string_parallel)
// F: Zero-copy sub-binary parsing (parse_string_zero_copy)

use rustler::{Binary, Env, NifResult, ResourceArc, Term};

mod core;
mod resource;
mod strategy;
mod term;

use resource::{StreamingParserRef, StreamingParserResource};
use strategy::{
    parse_csv, parse_csv_boundaries_with_config, parse_csv_fast, parse_csv_fast_with_config,
    parse_csv_indexed, parse_csv_indexed_with_config, parse_csv_parallel,
    parse_csv_parallel_with_config, parse_csv_with_config,
};
use term::{boundaries_to_term_hybrid, cow_rows_to_term, owned_rows_to_term};

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

/// Parse CSV with configurable separator and escape character
#[rustler::nif]
fn parse_string_with_config<'a>(
    env: Env<'a>,
    input: Binary<'a>,
    separator: u8,
    escape: u8,
) -> NifResult<Term<'a>> {
    let bytes = input.as_slice();
    let rows = parse_csv_with_config(bytes, separator, escape);
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

/// Parse using SIMD with configurable separator and escape character
#[rustler::nif]
fn parse_string_fast_with_config<'a>(
    env: Env<'a>,
    input: Binary<'a>,
    separator: u8,
    escape: u8,
) -> NifResult<Term<'a>> {
    let bytes = input.as_slice();
    let rows = parse_csv_fast_with_config(bytes, separator, escape);
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

/// Parse using two-phase with configurable separator and escape character
#[rustler::nif]
fn parse_string_indexed_with_config<'a>(
    env: Env<'a>,
    input: Binary<'a>,
    separator: u8,
    escape: u8,
) -> NifResult<Term<'a>> {
    let bytes = input.as_slice();
    let rows = parse_csv_indexed_with_config(bytes, separator, escape);
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

/// Create a new streaming parser with configurable separator and escape
#[rustler::nif]
fn streaming_new_with_config(separator: u8, escape: u8) -> StreamingParserRef {
    ResourceArc::new(StreamingParserResource::with_config(separator, escape))
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

/// Parse CSV in parallel with configurable separator and escape
#[rustler::nif(schedule = "DirtyCpu")]
fn parse_string_parallel_with_config<'a>(
    env: Env<'a>,
    input: Binary<'a>,
    separator: u8,
    escape: u8,
) -> NifResult<Term<'a>> {
    let bytes = input.as_slice();
    let rows = parse_csv_parallel_with_config(bytes, separator, escape);
    Ok(owned_rows_to_term(env, rows))
}

// ============================================================================
// Strategy F: Zero-Copy Parser (Sub-binary references)
// ============================================================================

/// Parse CSV using zero-copy sub-binaries where possible
/// Uses sub-binary references for unquoted and simply-quoted fields,
/// only copies when quote unescaping is needed (hybrid Cow approach).
///
/// Trade-off: Sub-binaries keep the parent binary alive. Use when you
/// want maximum speed and control memory lifetime yourself.
#[rustler::nif]
fn parse_string_zero_copy<'a>(env: Env<'a>, input: Binary<'a>) -> NifResult<Term<'a>> {
    let bytes = input.as_slice();
    let boundaries = parse_csv_boundaries_with_config(bytes, b',', b'"');
    Ok(boundaries_to_term_hybrid(env, input, boundaries, b'"'))
}

/// Parse CSV using zero-copy with configurable separator and escape
#[rustler::nif]
fn parse_string_zero_copy_with_config<'a>(
    env: Env<'a>,
    input: Binary<'a>,
    separator: u8,
    escape: u8,
) -> NifResult<Term<'a>> {
    let bytes = input.as_slice();
    let boundaries = parse_csv_boundaries_with_config(bytes, separator, escape);
    Ok(boundaries_to_term_hybrid(env, input, boundaries, escape))
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

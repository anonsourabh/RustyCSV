// RustyCSV - Fast CSV parsing with multiple strategies
//
// Strategies:
// A: Basic byte-by-byte parsing (parse_string)
// B: SIMD-accelerated via memchr (parse_string_fast)
// C: Two-phase index-then-extract (parse_string_indexed)
// D: Streaming chunked parser (streaming_*)
// E: Parallel parsing via rayon (parse_string_parallel)

use rustler::{Binary, Env, NifResult, ResourceArc, Term};
use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

#[cfg(not(feature = "mimalloc"))]
use std::alloc::System;

mod core;
mod resource;
mod strategy;
mod term;

use resource::{StreamingParserRef, StreamingParserResource};
use strategy::{
    parse_csv, parse_csv_fast, parse_csv_fast_with_config, parse_csv_indexed,
    parse_csv_indexed_with_config, parse_csv_parallel, parse_csv_parallel_with_config,
    parse_csv_with_config,
};
use term::{cow_rows_to_term, owned_rows_to_term};

// ============================================================================
// Tracking Allocator - measures Rust-side memory usage (invisible to BEAM)
// ============================================================================

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static PEAK_ALLOCATED: AtomicUsize = AtomicUsize::new(0);

struct TrackingAllocator;

#[cfg(feature = "mimalloc")]
static BASE_ALLOCATOR: MiMalloc = MiMalloc;

#[cfg(not(feature = "mimalloc"))]
static BASE_ALLOCATOR: System = System;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = BASE_ALLOCATOR.alloc(layout);
        if !ptr.is_null() {
            let current = ALLOCATED.fetch_add(layout.size(), Ordering::SeqCst) + layout.size();
            // Update peak if we exceeded it
            let mut peak = PEAK_ALLOCATED.load(Ordering::SeqCst);
            while current > peak {
                match PEAK_ALLOCATED.compare_exchange_weak(
                    peak,
                    current,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(p) => peak = p,
                }
            }
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        ALLOCATED.fetch_sub(layout.size(), Ordering::SeqCst);
        BASE_ALLOCATOR.dealloc(ptr, layout)
    }
}

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator;

// ============================================================================
// Memory Tracking NIFs
// ============================================================================

/// Get current Rust heap allocation in bytes
#[rustler::nif]
fn get_rust_memory() -> usize {
    ALLOCATED.load(Ordering::SeqCst)
}

/// Get peak Rust heap allocation since last reset
#[rustler::nif]
fn get_rust_memory_peak() -> usize {
    PEAK_ALLOCATED.load(Ordering::SeqCst)
}

/// Reset memory stats (useful before benchmarking)
#[rustler::nif]
fn reset_rust_memory_stats() -> (usize, usize) {
    let current = ALLOCATED.load(Ordering::SeqCst);
    let peak = PEAK_ALLOCATED.swap(current, Ordering::SeqCst);
    (current, peak)
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
// NIF Initialization
// ============================================================================

#[allow(non_local_definitions)]
fn load(env: Env, _info: Term) -> bool {
    let _ = rustler::resource!(StreamingParserResource, env);
    true
}

rustler::init!("Elixir.RustyCSV.Native", load = load);

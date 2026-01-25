// Shared term building utilities for converting Rust data to Elixir terms

use rustler::{Env, NewBinary, Term};
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

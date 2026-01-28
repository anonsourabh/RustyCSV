# Headers-to-Maps Performance Sandbox

Testing whether `headers: true` with key interning provides performance benefits
without regressing existing functionality.

## Hypothesis

When converting CSV rows to maps, reusing the same header binary terms as map keys
(instead of allocating new binaries for each row) should reduce memory allocations
and improve performance for large datasets.

## What we're testing

1. **Baseline**: Current list-of-lists parsing + Elixir-side `Enum.zip` to maps
2. **Candidate A**: Rust-side map building with interned keys
3. **Candidate B**: Elixir-side map building with pre-allocated header terms

## Run benchmarks

```bash
cd sandbox
mix run bench_headers.exs
```

## Success criteria

- No regression for existing `parse_string` (list-of-lists) performance
- `headers: true` should be faster than manual `Enum.zip` approach
- Memory usage should be lower with interning for large datasets (10k+ rows)

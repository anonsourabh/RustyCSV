# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2025-01-25

### Added

- Initial release
- Five parsing strategies: `:simd`, `:parallel`, `:streaming`, `:indexed`, `:basic`
- Full NimbleCSV API compatibility
- RFC 4180 compliance with 147 tests
- Configurable separators (CSV, TSV, PSV, etc.)
- Bounded-memory streaming for large files
- Character encoding support: UTF-8, UTF-16 (LE/BE), UTF-32 (LE/BE), Latin-1
- Pre-defined `RustyCSV.Spreadsheet` parser for Excel-compatible UTF-16 LE TSV
- Built-in Rust memory tracking for profiling
- Comprehensive documentation

### Parsing Strategies

- **`:simd`** - SIMD-accelerated delimiter scanning via `memchr` (default)
- **`:parallel`** - Multi-threaded parsing via `rayon` for 100MB+ files
- **`:streaming`** - Stateful chunked parser for unbounded files
- **`:indexed`** - Two-phase index-then-extract for row range access
- **`:basic`** - Simple byte-by-byte parsing for debugging

### Encoding Support

- `:utf8` - UTF-8 (default, zero overhead)
- `:latin1` - ISO-8859-1 / Latin-1
- `{:utf16, :little}` - UTF-16 Little Endian (Excel/Windows)
- `{:utf16, :big}` - UTF-16 Big Endian
- `{:utf32, :little}` - UTF-32 Little Endian
- `{:utf32, :big}` - UTF-32 Big Endian

### Validation

- csv-spectrum acid test suite (12 tests)
- csv-test-data RFC 4180 suite (17 tests)
- PapaParse-inspired edge cases (53 tests)
- Encoding conversion tests (20 tests)
- Cross-strategy consistency validation
- NimbleCSV output compatibility verification

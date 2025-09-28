# Architecture Overview

Sunce is a high-performance command-line solar position calculator written in Rust. The application calculates solar coordinates and sunrise/sunset times using a streaming architecture that maintains constant memory usage regardless of input size.

## Module Structure

The codebase consists of four core modules:

- **main.rs**: Entry point, orchestrates the pipeline, handles errors
- **data.rs**: CLI parsing, input expansion, timezone handling
- **compute.rs**: Solar calculations with streaming and SPA caching
- **output.rs**: Output formatting for CSV, JSON, text
- **parquet.rs**: Output formatting for Parquet (optional feature)


## Command Structure

Sunce provides two commands:

**position**: Calculates topocentric solar coordinates (azimuth, zenith/elevation) for given coordinates and times. Supports single calculations or time series with configurable step intervals.

**sunrise**: Computes sunrise, sunset, transit times, and optional twilight times (civil, nautical, astronomical) for specified locations and dates.

Both commands share the same input processing pipeline and support identical input modes.

## Input Processing

Input handling in `data.rs` supports three modes:

**Separate inputs**: Latitude, longitude, and datetime specified as separate arguments. Each can be a single value, a range (start:end:step), or a file (@file.txt or @- for stdin).

**Paired file input**: Single file containing latitude, longitude, and datetime on each line. Uses @file.txt or @- for stdin.

**Special syntax**:

- Coordinate ranges: `52:53:0.1` (52° to 53° in 0.1° steps)
- Partial dates: `2024` (entire year), `2024-06` (entire month)
- Complete dates without time for position: `2024-06-21` expands to 24-hour time series
- Unix timestamps: integer values interpreted as seconds since epoch
- Keyword `now`: current system time

The parser uses manual string processing without external CLI frameworks. All parsing produces lazy iterators.

## Streaming Architecture

The application implements true streaming with constant memory usage:

**Lazy iterators**: Input expansion creates iterators that yield values on-demand. Coordinates, time series, and file reads all use iterator chains.

**Cartesian products**: When both coordinates and times are ranges, the application materializes the smaller dimension into a Vec and streams the larger dimension, minimizing memory usage.

**Zero intermediate buffers**: Results flow directly from calculation to output without intermediate collection.

**Immediate output**: First result appears as soon as first calculation completes, not after processing all inputs.

This design handles infinite coordinate ranges and endless time series without memory growth. Performance exceeds 1 million calculations per second on modern hardware.

## Calculation Layer

The `compute.rs` module wraps the `solar-positioning` library:

**Position calculations**: Uses the SPA (Solar Position Algorithm) with atmospheric refraction correction.

**Sunrise calculations**: Iterative refinement algorithm from NREL Appendix A.2. Supports standard horizon (-0.833°) plus civil (-6°), nautical (-12°), astronomical (-18°), and custom horizons.

**SPA caching**: Partial SPA calculation results are cached when processing multiple positions for the same datetime, significantly improving performance for time series.

**Result types**: Enum variants distinguish between position results and sunrise results (with/without twilight).

## Output Formatting

The `output.rs` module provides four output formats:

**Text format**: Human-readable tables with automatic column layout. Shows all input parameters in header section. Uses Unicode box-drawing characters.

**CSV format**: Comma-separated values with optional headers. Controlled precision (5 decimal places for coordinates, 3 for other values).

**JSON format**: JSON Lines format (one JSON object per line). Full floating-point precision (15+ digits).

**Parquet format** (optional feature): Apache Parquet columnar format with Snappy compression. Nullable fields for sunrise times that may be absent (polar day/night).

All formats support the `--show-inputs` flag to include input parameters in output. This is auto-enabled when inputs could produce multiple values (ranges, time series, files) and can be explicitly controlled with `--show-inputs` or `--no-show-inputs`.

## Timezone Handling

Timezone processing in `data.rs`:

**Input preservation**: Datetimes specified with timezone offsets (e.g., `+02:00`) preserve that timezone throughout.

**Timezone inference**: When no timezone specified, uses system local timezone via `chrono-tz` and `iana-time-zone`.

**Explicit override**: `--timezone` option forces all datetimes to specified timezone.

**DST handling**: All DST transitions handled correctly by chrono library.

Calculations internally use UTC but results are formatted in the original timezone.

## Error Handling

All errors are `String` types with user-facing messages. Errors are created inline with `format!()` macros and immediately displayed to stderr before exiting. No error recovery or structured error types are used since the application is a CLI tool that terminates on any error.

Common error cases include invalid coordinates, malformed datetimes, file I/O failures, and invalid command-line arguments.

## Data Flow

1. **CLI parsing**: `data.rs::parse_cli()` processes command-line arguments into Parameters, Command, and DataSource
   - Options can appear anywhere in the command line (before, after, or mixed with positional arguments)
   - Validation of command-specific options happens after the command is identified
2. **Iterator creation**: Data source expands into lazy iterator yielding (lat, lon, datetime) tuples
3. **Calculation**: `compute.rs::calculate_stream()` transforms input iterator into result iterator
4. **Output**: Format-specific functions consume result iterator and write to stdout
5. **Performance reporting**: Optional `--perf` flag measures throughput and reports statistics to stderr

The entire pipeline maintains lazy evaluation. Nothing is materialized into memory except where explicitly required for algorithm correctness (e.g., smaller dimension in cartesian products).

## Build Configuration

**Features**:

- `default`: Includes Parquet support
- `parquet`: Enables Apache Parquet output format (arrow + parquet dependencies)
- `minimal`: No optional dependencies

**Profile settings**:

- Release builds use LTO, single codegen unit, panic=abort
- Most dependencies optimized for size (opt-level="z")
- Performance-critical crates optimized for speed (opt-level=3): sunce, solar-positioning, chrono, chrono-tz

**Dependencies**:

- `chrono`: Date/time handling
- `chrono-tz`: Timezone support
- `solar-positioning`: Core solar calculation algorithms
- `arrow` + `parquet` (optional): Parquet format support

## Testing

Test suite includes approximately 118 tests:

**Integration tests**: End-to-end CLI testing with `assert_cmd` and `predicates`
**Critical tests**: Calculation accuracy against reference values
**Show-inputs tests**: Auto-enable logic for different input types
**Format tests**: Output correctness for all supported formats
**Parquet tests**: Schema and data validation using `bytes` crate
**Twilight tests**: All horizon calculations and output formats

All tests run with `TZ=UTC` environment variable to ensure timezone consistency.

## Performance Characteristics

**Throughput**: ~1 million calculations/second (single-core) on standard smoke test
**Memory**: Constant usage independent of input size
**Startup**: Sub-100ms for simple calculations
**Streaming**: Zero memory leaks with infinite inputs

Standard smoke test: `sunce --perf --format=CSV --no-headers 50:55:0.1 10:15:0.1 2024 position --step=3h > /dev/null`

## Compatibility Notes

Sunce maintains calculation accuracy compatibility with the original Java `solarpos` tool when using identical input parameters and Delta T values. The CLI interface has evolved beyond solarpos for improved usability while preserving calculation correctness.

Notable differences from solarpos:

- Partial date expansion behavior differs for position command
- Additional output formats (Parquet, enhanced JSON precision)
- Improved range processing performance

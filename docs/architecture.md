# Architecture Overview

Sunce is a high-performance command-line solar position calculator written in Rust. The application calculates solar coordinates and sunrise/sunset times using a streaming architecture that maintains constant memory usage regardless of input size.

## Module Structure

The codebase is organized into focused modules:

- **cli.rs**: Manual command-line parsing/validation via a small option registry that produces `Parameters`, `Command`, and `DataSource`.
- **planner.rs**: Converts parsed inputs into separate `ComputePlan` and `OutputPlan`, deriving metadata such as cache/flush policies.
- **data.rs**: Input expansion (ranges, files, cartesian products), timezone handling, and iterator utilities used by planner and compute layers.
- **compute.rs**: Solar calculations with streaming and SPA caching.
- **output.rs**: Normalizes calculation results into shared row structs and formats them (CSV/JSON/text), plus dispatch logic that chooses the appropriate writer based on the plan. Keeps field semantics aligned across formats.
- **parquet.rs**: Parquet writer (optional feature gated by `parquet` flag).
- **main.rs**: Entry point that wires the stages together (parse → plan → compute → output) and handles error reporting/perf logging.

## Command Structure

Sunce provides two commands:

**position**: Calculates topocentric solar coordinates (azimuth, zenith/elevation) for given coordinates and times. Supports single calculations or time series with configurable step intervals.

**sunrise**: Computes sunrise, sunset, transit times, and optional twilight times (civil, nautical, astronomical) for specified locations and dates.

Both commands share the same input processing pipeline and support identical input modes.

## Input Processing

CLI parsing in `cli.rs` delegates to `data.rs` helpers and supports three modes:

**Separate inputs**: Latitude, longitude, and datetime specified as separate arguments. Latitude/longitude are values or ranges (`start:end:step`); coordinate files are passed as a single `@coords.txt` argument (or `@-` for stdin). Datetime can be a single value, a partial date range, or a time file.

**Paired file input**: Single file containing latitude, longitude, and datetime on each line. Uses `@file.txt` or `@-` for stdin.

**Special syntax**:

- Coordinate ranges: `52:53:0.1` (52° to 53° in 0.1° steps)
- Partial dates: `2024` (entire year), `2024-06` (entire month)
- Complete dates without time for position: `2024-06-21` expands to 24-hour time series
- Unix timestamps: integer values interpreted as seconds since epoch
- Keyword `now`: current system time

Coordinate range steps may be positive or negative; time steps (e.g., `--step`) must be strictly positive. When combining `now` with `--step`, only a single location may be supplied so the iterator remains bounded per location.

Parsing uses manual option dispatch (no external CLI framework) and outputs strongly typed structures (enums for format/algorithm, etc.) consumed by the planner. All subsequent stages operate on lazy iterators.

## Streaming Architecture

The application implements true streaming with constant memory usage:

**Lazy iterators**: Input expansion creates iterators that yield values on-demand. Coordinates, time series, and file reads all use iterator chains.

**Cartesian products**: When both coordinates and times are ranges, the application materializes the smaller dimension into a Vec and streams the larger dimension, minimizing memory usage.

**Zero intermediate buffers**: Results flow directly from calculation to output without intermediate collection.

**Immediate output**: First result appears as soon as first calculation completes, not after processing all inputs.

This design handles large coordinate ranges and long-running time series without memory growth. Watch mode (`now` + `--step`) streams indefinitely for a single location; attempts to combine it with multiple locations are rejected up front to prevent unbounded buffering. Performance exceeds 1 million calculations per second on modern hardware.

## Calculation Layer

The `compute.rs` module wraps the `solar-positioning` library:

**Position calculations**: Uses the SPA (Solar Position Algorithm) with atmospheric refraction correction.

**Sunrise calculations**: Iterative refinement algorithm from NREL Appendix A.2. Supports standard horizon (-0.833°) plus civil (-6°), nautical (-12°), astronomical (-18°), and custom horizons.

**SPA caching**: Partial SPA calculation results are cached when processing multiple positions for the same datetime, significantly improving performance for time series. The cache uses an LRU eviction policy to keep memory usage bounded even for long-running streams.

**Result types**: Enum variants distinguish between position results and sunrise results (with/without twilight).

All calculations return `Result` values, allowing iterator consumers to surface errors (e.g., invalid refraction parameters) without panicking.

## Output Formatting

The `output.rs` module provides four output formats:

**Text format**: Whitespace-aligned columns (a “pretty CSV”) using the same fields/precision as CSV. Streaming-friendly, with optional headers.

**CSV format**: Comma-separated values with optional headers. Controlled precision (5 decimal places for coordinates, 4 for angles, 3 for most other numeric values).

**JSON format**: JSON Lines format (one JSON object per line). Angles rounded to 4 decimals.

**Parquet format** (optional feature): Apache Parquet columnar format with Snappy compression. Nullable fields for sunrise times that may be absent (polar day/night).

All formats support the `--show-inputs` flag to include input parameters in output. This is auto-enabled when inputs could produce multiple values (ranges, time series, files) and can be explicitly controlled with `--show-inputs` or `--no-show-inputs`. Timestamps are emitted consistently as RFC3339 across all formats.

## Timezone Handling

Timezone processing in `data.rs`:

**Input preservation**: Datetimes specified with timezone offsets (e.g., `+02:00`) preserve that timezone throughout.

**Timezone inference**: When no timezone specified, uses system local timezone via `chrono-tz` and `iana-time-zone`.

**Explicit override**: `--timezone` option forces all datetimes to specified timezone.

**DST handling**: All DST transitions handled correctly by chrono library.

## Error Handling

Each stage wraps its failures in a lightweight typed error (`CliError`, `PlannerError`, `OutputError`). Iterator stages still produce `Result` items, but we convert the strings that describe invalid input, timezone gaps, or file problems into the appropriate error enum before bubbling them upward. The CLI prints user-facing messages (prefixing help/version output with the standard text) while keeping internal error-handling strongly typed.

Common error cases include invalid coordinates, malformed datetimes, DST gaps in partial date expansions, invalid refraction parameters, file I/O failures, and invalid command-line arguments.

## Data Flow

1. **CLI parsing**: `cli.rs::parse_cli()` processes command-line arguments into `Parameters`, `Command`, and `DataSource`
   - Options can appear anywhere in the command line (before, after, or mixed with positional arguments)
   - Validation of command-specific options happens after the command is identified
2. **Planning**: `planner::build_job()` derives a `ComputePlan` (iterator, params, cache policy) and an `OutputPlan` (data source, flush policy). The planner centralizes cartesian expansion, watch-mode checks, and other orchestration rules.
3. **Iterator creation**: The planner calls into `data::expansion` to expand inputs into lazy iterators yielding `(lat, lon, datetime)` tuples.
4. **Calculation**: `compute.rs::calculate_stream()` transforms the iterator into results.
5. **Output**: `output::dispatch_output()` selects the appropriate writer (CSV/JSON/text/Parquet) based on plan metadata and writes results to stdout.
6. **Performance reporting**: Optional `--perf` flag measures throughput and reports statistics to stderr

The entire pipeline maintains lazy evaluation. Nothing is materialized into memory except where explicitly required for algorithm correctness (e.g., smaller dimension in cartesian products).

## Build Configuration

**Features**:

- `default`: Includes Parquet support
- `parquet`: Enables Apache Parquet output format (arrow + parquet dependencies)
- Minimal build: use `--no-default-features` to exclude Parquet dependencies

**Profile settings**:

- Release builds use LTO, single codegen unit, panic=abort, strip=true
- Most dependencies optimized for balanced size/speed (opt-level="s")
- Performance-critical crates optimized for speed (opt-level=3): sunce, solar-positioning, chrono, chrono-tz

**Dependencies**:

- `chrono`: Date/time handling
- `chrono-tz`: Timezone support
- `solar-positioning`: Core solar calculation algorithms
- `arrow` + `parquet` (optional): Parquet format support

## Testing

Test suite includes:

**Integration tests**: End-to-end CLI testing with `assert_cmd` and `predicates`
**Critical tests**: Calculation accuracy against reference values
**Show-inputs tests**: Auto-enable logic for different input types
**Format tests**: Output correctness for all supported formats
**Parquet tests**: Schema and data validation using `bytes` crate
**Twilight tests**: All horizon calculations and output formats

Tests run with explicit `TZ` environment variable to ensure timezone consistency.

## Performance Characteristics

**Throughput**: Over 1 million calculations/second (single-core) on standard smoke test
**Memory**: Constant usage independent of input size
**Streaming**: Zero memory leaks with infinite inputs

Standard smoke test: `sunce --perf --format=CSV --no-headers 50:55:0.1 10:15:0.1 2024 position --step=3h > /dev/null`

## Compatibility Notes

Sunce maintains calculation accuracy compatibility with the original Java `solarpos` tool when using identical input parameters and Delta T values. The CLI interface has evolved beyond solarpos for improved usability while preserving calculation correctness.

Notable differences from solarpos:

- Partial date expansion behavior differs for position command
- Additional output formats (Parquet, enhanced JSON precision)
- Improved range processing performance

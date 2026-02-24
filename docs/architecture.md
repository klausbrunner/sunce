# Architecture Overview

Sunce is a streaming CLI for solar position and sunrise/sunset calculations. The pipeline is:

`CLI parse -> planning -> data expansion -> calculation -> output`

Each stage works on iterators so large datasets can be processed without collecting all records in memory.

## Core Modules

- `src/main.rs`: entry point; wires parse/plan/compute/output and prints `--perf` stats.
- `src/cli.rs`: manual argument parser; validates command-specific options and builds typed `DataSource`, `Command`, and `Parameters`.
- `src/planner.rs`: derives `ComputePlan` and `OutputPlan` (cache/flush behavior).
- `src/data/`: input expansion and validation (ranges, files, time parsing, timezone handling).
- `src/compute.rs`: streaming calculations via `solar-positioning`; includes optional SPA time-part caching.
- `src/output.rs`: shared row normalization plus text/CSV/JSON writers.
- `src/parquet.rs` (feature-gated): Arrow/Parquet writer with Snappy compression.

## Commands and Inputs

Commands:

- `position`: azimuth + zenith/elevation.
- `sunrise`: sunrise/transit/sunset, plus optional twilight.

Input forms:

- Separate args: `<lat> <lon> <time>`.
- Paired file: `@file` with `lat lon datetime` per line.
- Split files: `@coords @times`.
- `@-` (stdin) is supported for one stream at a time.

Supported time syntax includes full datetime, partial dates (`YYYY`, `YYYY-MM`), unix timestamps, and `now`.

## Expansion and Streaming

- Expansion produces `Iterator<Item = Result<(lat, lon, datetime), String>>`.
- For location ranges, one coordinate axis is materialized (the smaller one) and the other axis streams, bounding memory.
- Planner selects iteration order based on replayability (file/stdin/now) and enables per-record flushing for stdin and watch mode.
- Unbounded watch mode (`now` + `--step`) is restricted to a single location.

## Calculation Layer

- Uses `solar-positioning`:
  - Position: SPA or Grena3.
  - Sunrise: sunrise/sunset for selected horizon, or multi-horizon twilight set.
- Refraction correction is validated before use.
- For SPA position runs where beneficial, datetime-dependent SPA parts are cached in a bounded FIFO-style cache (`TIME_CACHE_CAPACITY = 2048`).
- Output from compute remains a lazy result stream (`Result<CalculationResult, String>`).

## Output Layer

Formats:

- `text`: aligned table output.
- `csv`: comma-separated rows.
- `json`: JSON Lines (one object per line).
- `parquet` (feature `parquet`): columnar output.

Design notes:

- Text and CSV are generated from shared row-value extraction to keep field order and precision aligned.
- Datetime strings are emitted in RFC3339 without milliseconds.
- `show-inputs` can be explicit or auto-enabled for multi-valued inputs (ranges/files/paired).

## Timezones and Dates

- Explicit offsets in input are preserved unless `--timezone` override is supplied.
- Without explicit timezone, resolution uses override -> `TZ` -> detected system timezone.
- Named zones are handled with `chrono-tz`; fixed offsets are supported directly.
- DST handling is done during local-time resolution; non-existent local times (gaps) return explicit errors.

## Errors and Exit Behavior

- Typed top-level errors: `CliError`, `PlannerError`, `OutputError`.
- CLI exits:
  - `CliError::Exit`: prints message to stdout and exits `0` (help/version/usage).
  - `CliError::Message` and downstream failures: prints to stderr and exits `1`.

## Build Features

- Default feature set includes `parquet`.
- `--no-default-features` builds without Parquet dependencies.

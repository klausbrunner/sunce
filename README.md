# sunce

`sunce` is a command-line tool for solar position and solar event calculations. It computes topocentric solar coordinates (`position`) and daily solar events such as sunrise, sunset, transit, and twilight (`sunrise`). It is designed for scripting and bulk processing: ranges, file input, streaming, predicate checks, and machine-friendly output (CSV, JSON Lines, Parquet).

Built on the [solar-positioning](https://crates.io/crates/solar-positioning) library of high-accuracy solar position algorithms.

## Status

Under active development as of 2026. While the core functionality is stable, some features are still being added and the command-line interface may change.

## Requirements and installation

Download the latest release binary from the [releases page](https://github.com/klausbrunner/sunce/releases/latest) or install via Homebrew (macOS/Linux):

```shell
brew install klausbrunner/tap/sunce
```

To install from a checked-out copy of this repository:

```shell
cargo install --path .
```

`sunce` currently requires Rust 1.90+.

Native executables are provided for Linux, macOS, and Windows.

## Mental model

- `position` answers: "Where is the Sun at this instant?"
- `sunrise` answers: "What are this day's solar event times?"
- A full datetime means one instant.
- A date-only or partial date may expand into a time series.
- Latitude/longitude ranges and file inputs expand into multiple records.
- Output formats are different views of the same logical result.

## Quick start

```bash
# Getting today's sunrise and sunset in Madrid (UTC)
sunce 40.42 -3.70 now --timezone=UTC sunrise

# Sun position in Stockholm on 2026-01-15 at 12:30 CET
sunce 59.334 18.063 2026-01-15T12:30:00+01:00 position
```

## Common tasks

```bash
# One position fix
sunce 52.522 13.413 2026-03-28T12:00:00+01:00 position

# One day's solar events
sunce 52.522 13.413 2026-03-28 sunrise

# Time series: positions in Berlin every 10 minutes, CSV output, with delta-T estimate
sunce --format=csv --deltat --timezone=Europe/Berlin 52.522 13.413 2023-03-26 position --step=10m

# Geographic grid: positions across Central Europe at noon (1° resolution)
sunce --format=csv 45.0:50.0:1.0 5.0:15.0:1.0 2026-06-21T12:00:00Z position

# Sunrise, sunset, and twilight times for Tokyo throughout March 2027, JSON output
sunce --format=json --timezone=Asia/Tokyo 35.68 139.69 2027-03 sunrise --twilight

# High-performance data processing: large datasets with Parquet output (Snappy compressed)
sunce --format=parquet 50:55:0.1 10:15:0.1 2024 position --step=3h > solar_data.parquet
```

## Input semantics

- `position` with a date-only input like `2026-03-28` expands to a time series for that day. Year-month and year inputs expand further.
- `sunrise` treats a date-like input as a day or day series and returns event times for those days.
- `now` means the current instant. With `position --step`, it becomes a live stream and requires one explicit latitude/longitude pair.
- `--timezone` overrides timezone interpretation for parsing and output.

## File input and streaming

`sunce` accepts file input for coordinates and times and supports stdin streaming via the `@-` syntax. This is useful when composing pipelines or when input is generated programmatically.

Input modes:

- **Coordinate files:** pass `@coords.txt` in place of the lat/lon pair (e.g., `sunce @coords.txt <dateTime> position`). Each line contains a latitude and longitude (space- or comma-separated).
- **Time files:** pass `@times.txt` as the date/time parameter to read timestamps from a file, one timestamp per line.
- **Paired data files:** pass `@data.txt` to provide explicit `latitude longitude datetime` records on each line; paired input is treated as one record per line with no cartesian expansion.
- **Stdin:** use `@-` in place of a filename to read the corresponding parameter from standard input. Only one parameter may read from stdin at a time.

Examples:

```bash
# Pipe a single paired record from stdin
echo "52.0,25.0,2023-06-21T12:00:00" | sunce @- position

# Stream coordinate pairs from stdin and evaluate for a fixed time
cat coords.txt | sunce @- 2023-06-21T12:00:00 position

# Generate timestamps and pipe them in for a single location
generate-times | sunce 52.0 25.0 @- position
```

Files may include blank lines and comments (lines starting with `#`). Both space-separated and CSV-style input are accepted.

## Output formats

- `text` (default) – readable text for quick checks.
- `csv` – comma-separated values with headers by default; use `--no-headers` to omit them.
- `json` – JSON Lines (one JSON object per line), good for `jq` and similar tools.
- `parquet` – compressed Apache Parquet format for efficient columnar storage and analytics.

Field names are intended to be stable across formats where the underlying data is the same. For example, `dateTime`, `azimuth`, `zenith`, `sunrise`, and `civil_start` mean the same thing in CSV, JSON, and Parquet.

## Key options

- `--timezone=<tz>` – timezone as an offset (e.g., `+01:00`) or an IANA name (e.g., `Europe/Berlin`).
- `--deltat[=<seconds>]` – default is `0` seconds when omitted. Provide an explicit value with `--deltat=<seconds>` or pass the option without a value to request an automatic estimate. For background on delta-T see [solar-positioning](https://crates.io/crates/solar-positioning).
- `--format=<format>` – output format: `text`, `csv`, `json`, or `parquet`.
- `--[no-]headers` – include/omit header row for CSV output (default: headers on).
- `--[no-]show-inputs` – include input parameters in the output.
- `--step=<duration>` – time step for `position` time series sampling (integer seconds or a suffix like `10m`, `1h`, `1d`).

Run `sunce --help` for a brief usage summary.

## Automation and predicate mode

For automation, `sunce` can evaluate one solar condition for one explicit location and one explicit instant and report the result via the process exit code:

- `0` – predicate is true
- `1` – predicate is false
- `2` – usage or runtime error

This mode is intentionally strict: it only works with a single latitude/longitude pair and one explicit instant (`now`, full datetime, or unix timestamp). Ranges, date-only inputs, and file/stdin inputs are rejected.

Add `--wait` to keep checking a predicate on `now` until it becomes true. `--wait` is only valid together with a predicate and `now`. Completion is usually within seconds, not guaranteed at the exact transition.

Use `--after-sunset` for the practical "has the sun set yet?" check. `--is-astronomical-night` is stricter and only becomes true after astronomical twilight ends.

Examples:

```bash
# Exit 0 during daylight, 1 otherwise
sunce 52.522 13.413 now sunrise --is-daylight

# Exit 0 when the sun is above 10 degrees elevation
sunce 52.522 13.413 now position --sun-above=10

# Exit 0 during civil twilight, 1 otherwise
sunce --timezone=Europe/Berlin 52.522 13.413 2026-03-26T06:00:00 sunrise --is-civil-twilight

# Exit 0 from sunset until sunrise
sunce 52.522 13.413 now sunrise --after-sunset

# Wait until the sun is above 5 degrees elevation
sunce 52.522 13.413 now position --sun-above=5 --wait
```

For shell scripts:

```bash
if sunce 52.522 13.413 now sunrise --after-sunset; then
  echo "Sun has set"
fi
```

## Performance

`sunce` is designed for high throughput with streaming output. Memory is bounded by input expansion (the smaller range dimension), the SPA time cache, and output buffering/batching; results are not collected in full.

Standard smoke test (release build):

```bash
target/release/sunce --perf --format=csv --no-headers 50:55:0.1 10:15:0.1 2024 position --step=3h > /dev/null
```

## License

This project is distributed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contributing

Please open issues or pull requests for bugs, documentation improvements, and feature requests. Include command-line examples and expected output when reporting problems.

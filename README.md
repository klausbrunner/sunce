# sunce

A high-performance command-line solar position calculator.

Sunce calculates topocentric solar coordinates and sunrise/sunset/twilight times with high accuracy. It is designed for scripting and bulk processing, supporting time series, geographic sweeps, file input, and streaming operations with constant memory usage.

This project aims to be a drop-in replacement for the Java-based [solarpos](https://github.com/klausbrunner/solarpos) tool while (hopefully) benefitting from Rust's performance and distribution advantages.

## Key Features

The `position` command computes solar azimuth and zenith/elevation angles for given coordinates and times. The `sunrise` command calculates sunrise, sunset, and twilight times for specified locations and dates.

Input handling is flexible, supporting coordinate ranges using start:end:step syntax, time series from partial dates like "2024" or "2024-06", and file-based input including stdin streaming. The streaming architecture ensures constant memory usage regardless of input size, enabling processing of infinite coordinate and/or time streams.

Output formats include human-readable text, CSV, and JSON Lines.

## Installation

Pre-compiled binaries are available for macOS, Linux, and Windows from the [releases page](https://github.com/klausbrunner/sunce/releases/latest). Installation scripts are provided for convenience.

Alternatively, if you have a Rust toolchain, install from source using `cargo install sunce` or build locally with `cargo build --release`.

## Basic Usage

Calculate current solar position in Berlin:

```bash
sunce 52.52 13.40 now position
```

Get today's sunrise and sunset times:

```bash
sunce 52.52 13.40 today sunrise
```

Process coordinate ranges for a specific date:

```bash
sunce 50:55:0.5 10:15:0.5 2026-09-19T12:00:00 position --format=CSV
```

Stream processing from file or stdin:

```bash
echo "52.0,13.4,2024-01-01T12:00:00" | sunce @- position
cat coordinates.txt | sunce @- 2024-06-21 position
```

Check built-in help for details and more options.

## Implementation Notes

Sunce is built on the [solar-positioning](https://crates.io/crates/solar-positioning) Rust library, which implements the NREL Solar Position Algorithm (SPA) for maximum accuracy and the Grena3 algorithm for speed-accuracy balance.

## Status

Beta. This project is still under development. Core functionality looks stable and is (mostly) tested with compatibility validation against solarpos.

For applications requiring maximum stability and the most comprehensive feature set, consider using the original [solarpos](https://github.com/klausbrunner/solarpos).

## License

MIT

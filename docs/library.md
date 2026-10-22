# Rust library

Sunce calculates solar positions and events from caller-provided inputs, returning
full-precision records. Callers own I/O, clocks and scheduling.

```sh
cargo add sunce --no-default-features
```

```rust
use sunce::{PositionOptions, input, output, positions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let grid = input::CoordinateGrid::new((52.0, 53.0, 0.5), (13.0, 14.0, 0.5))?;
    let timezone = "Europe/Berlin".parse()?;
    let times = input::position_times("2026-06-21", timezone, "10m".parse()?)?;
    let options = PositionOptions::default();
    let rows = positions(input::grid_positions(grid, times), options)?;

    let mut csv = Vec::new();
    output::write_positions(
        rows,
        &options.environment,
        output::Options {
            format: output::Format::Csv,
            show_inputs: true,
            ..Default::default()
        },
        &mut csv,
    )?;
    Ok(())
}
```

## Inputs and calculations

`positions(inputs, PositionOptions)` takes `Result<PositionInput, Error>` items;
`events(inputs, EventOptions)` takes `Result<EventInput, Error>` items. Use
`.map(Ok)` for plain records. Options are validated immediately; input records
are validated and calculated lazily. Results support ordinary iterator operations
such as `filter`, `take` and `collect`. Dropping the iterator stops further work.

| Input helper | Purpose |
| --- | --- |
| `CoordinateGrid` | Constant-memory latitude/longitude grids in degrees, independent of map projection. |
| `grid_positions`, `grid_events` | Combine a grid with instants or local dates. Custom grids can supply typed input records directly. |
| `position_times` | Sample a period at elapsed-time intervals, retaining DST offset changes. |
| `daily_times` | Sample once per local date at a fixed clock time. |
| `event_dates` | Iterate inclusive local calendar dates. |
| `read_positions`, `read_events`, `read_locations`, `read_times` | Read Sunce text input from any `BufRead`; time zones are explicit and `now` must be resolved by the caller. |

Periods accept `YYYY`, `YYYY-MM`, `YYYY-MM-DD` or inclusive
`YYYY-MM-DD/YYYY-MM-DD` ranges. Position periods end at the next date's first valid
instant, exclusively. Daily sampling advances missing local times and chooses the
earlier occurrence of repeated times; entirely skipped dates produce no position
samples. Input timestamps without a UTC offset reject missing local times.

Both calculation options default to SPA and delta-T of 0 seconds; set `deltat`
to `None` for automatic estimation. Event options support twilight or a custom
horizon, which cannot be combined. Grena3 positions require zero observer elevation.

## Output and features

`output::write_positions` and `output::write_events` export text, CSV or JSON Lines
to any `std::io::Write`, returning the row count. Position exports need the same
environment as the calculation. Input, write and final flush errors propagate;
failures may leave partial output. Records retain full precision; exports apply
Sunce's usual formatting.

The default features are `cli` and `parquet`. Disable them for the core library;
enable `parquet` for bounded-batch Parquet exports, which require `Write + Send`.
The core supports `wasm32-unknown-unknown`; the application supplies browser
bindings and scheduling, and sets its own release profile.

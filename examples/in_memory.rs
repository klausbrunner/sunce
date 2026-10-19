//! A reader, lazy calculation, and CSV export without CLI or filesystem adapters.

use std::io::Cursor;
use sunce::{PositionOptions, input, output, positions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = "# latitude,longitude,datetime\n52.52,13.405,2026-06-21T12:00:00+02:00\n";
    let records = input::read_positions(Cursor::new(source.as_bytes()), "Europe/Berlin".parse()?);
    let options = PositionOptions::default();
    let rows = positions(records, options)?;

    let mut csv = Vec::new();
    let count = output::write_positions(
        rows,
        &options.environment,
        output::Options {
            format: output::Format::Csv,
            show_inputs: true,
            ..Default::default()
        },
        &mut csv,
    )?;

    assert_eq!(count, 1);
    println!("{}", String::from_utf8(csv)?);
    Ok(())
}

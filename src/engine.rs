/// Business logic coordination for sunce calculations
///
use crate::calculation::{
    PositionCalculationEngine, SunriseCalculationEngine, get_calculation_parameters,
    get_sunrise_calculation_parameters,
};
use crate::iterators::create_calculation_iterator;
use crate::output::output_position_results;
use crate::parsing::ParsedInput;
use crate::sunrise_output::output_sunrise_results;
use crate::types::OutputFormat;
use clap::ArgMatches;

/// Execute position calculation command
pub fn execute_position_command(
    input: &ParsedInput,
    matches: &ArgMatches,
    format: &OutputFormat,
    show_inputs: bool,
    show_headers: bool,
    elevation_angle: bool,
) -> Result<(), String> {
    let params = get_calculation_parameters(input, matches)?;
    let engine = PositionCalculationEngine { params };

    let position_iter = create_calculation_iterator(input, matches, &engine)?;

    // Convert Result<T, String> iterator to T iterator, handling errors
    let processed_iter = position_iter.map(|result| {
        result.unwrap_or_else(|e| {
            eprintln!("✗ {}", e);
            std::process::exit(1);
        })
    });

    output_position_results(
        processed_iter,
        format,
        show_inputs,
        show_headers,
        elevation_angle,
    );
    Ok(())
}

/// Execute sunrise calculation command
pub fn execute_sunrise_command(
    input: &ParsedInput,
    matches: &ArgMatches,
    format: &OutputFormat,
    show_inputs: bool,
    show_headers: bool,
    show_twilight: bool,
) -> Result<(), String> {
    let params = get_sunrise_calculation_parameters(input, matches, show_twilight)?;
    let engine = SunriseCalculationEngine { params };

    let sunrise_iter = create_calculation_iterator(input, matches, &engine)?;

    // Convert Result<T, String> iterator to T iterator, handling errors
    let processed_iter = sunrise_iter.map(|result| {
        result.unwrap_or_else(|e| {
            eprintln!("✗ {}", e);
            std::process::exit(1);
        })
    });

    output_sunrise_results(
        processed_iter,
        format,
        show_inputs,
        show_headers,
        show_twilight,
    );
    Ok(())
}

/// Determine output format from input options
pub fn determine_output_format(format_str: Option<&str>) -> Result<OutputFormat, String> {
    match format_str {
        Some(fmt) => OutputFormat::from_string(fmt),
        None => Ok(OutputFormat::Human),
    }
}

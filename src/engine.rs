/// Business logic coordination for sunce calculations
///
use crate::calculation::{get_calculation_parameters, get_sunrise_calculation_parameters};
use crate::iterators::{create_position_iterator, create_sunrise_iterator};
use crate::output::output_position_results;
use crate::parsing::ParsedInput;
use crate::performance::PerformanceTracker;
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

    let show_perf = matches.get_flag("perf");
    let tracker = PerformanceTracker::create(show_perf);

    let position_iter = create_position_iterator(input, matches, &params)?;

    // Convert Result<T, String> iterator to T iterator, handling errors
    let processed_iter = position_iter.map(|result| {
        result.unwrap_or_else(|e| {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        })
    });

    // Track performance if enabled
    let tracked_iter = if let Some(ref t) = tracker {
        Box::new(processed_iter.inspect(|_| {
            t.track_item();
        })) as Box<dyn Iterator<Item = _>>
    } else {
        Box::new(processed_iter) as Box<dyn Iterator<Item = _>>
    };

    output_position_results(
        tracked_iter,
        format,
        show_inputs,
        show_headers,
        elevation_angle,
    );

    PerformanceTracker::report_if_needed(&tracker);
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

    let show_perf = matches.get_flag("perf");
    let tracker = PerformanceTracker::create(show_perf);

    let sunrise_iter = create_sunrise_iterator(input, matches, &params)?;

    // Convert Result<T, String> iterator to T iterator, handling errors
    let processed_iter = sunrise_iter.map(|result| {
        result.unwrap_or_else(|e| {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        })
    });

    // Track performance if enabled
    let tracked_iter = if let Some(ref t) = tracker {
        Box::new(processed_iter.inspect(|_| {
            t.track_item();
        })) as Box<dyn Iterator<Item = _>>
    } else {
        Box::new(processed_iter) as Box<dyn Iterator<Item = _>>
    };

    output_sunrise_results(
        tracked_iter,
        format,
        show_inputs,
        show_headers,
        show_twilight,
    );

    PerformanceTracker::report_if_needed(&tracker);
    Ok(())
}

/// Determine output format from input options
pub fn determine_output_format(format_str: Option<&str>) -> Result<OutputFormat, String> {
    match format_str {
        Some(fmt) => OutputFormat::from_string(fmt),
        None => Ok(OutputFormat::Human),
    }
}

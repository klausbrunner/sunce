mod calculation;
mod cli;
mod file_input;
mod input_parsing;
mod iterators;
mod output;
#[cfg(feature = "parquet")]
mod parquet_output;
mod performance;
mod sunrise_formatters;
mod table_format;
mod time_series;
mod timezone;
mod types;

use input_parsing::{
    ParsedInput, parse_data_values, parse_input, parse_position_options, parse_sunrise_options,
};

use calculation::{get_calculation_parameters, get_sunrise_calculation_parameters};
use iterators::{create_position_iterator, create_sunrise_iterator};
use output::output_position_results;
use performance::PerformanceTracker;
use sunrise_formatters::output_sunrise_results;
use types::{DateTimeInput, InputType, OutputFormat};

/// Determine output format from input options
fn determine_output_format(format_str: Option<&str>) -> Result<OutputFormat, String> {
    match format_str {
        Some(fmt) => OutputFormat::from_string(fmt),
        None => Ok(OutputFormat::Human),
    }
}

fn main() {
    let app = cli::build_cli();
    let matches = app.get_matches();

    if let Err(e) = run_app(&matches) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

fn run_app(matches: &clap::ArgMatches) -> Result<(), String> {
    let mut input = parse_input(matches)?;
    let (cmd_name, cmd_matches) = matches.subcommand().unwrap_or(("position", matches));
    parse_data_values(&mut input, Some(cmd_name)).map_err(|e| e.to_string())?;

    let format = determine_output_format(input.global_options.format.as_deref())?;
    let show_inputs = input.global_options.show_inputs.unwrap_or(false);
    let show_headers = input.global_options.headers.unwrap_or(true);

    match cmd_name {
        "position" => {
            let elevation_angle = parse_position_options(cmd_matches).elevation_angle;

            let params = get_calculation_parameters(&input, matches)?;
            let show_perf = matches.get_flag("perf");
            let tracker = PerformanceTracker::create(show_perf);
            let position_iter = create_position_iterator(&input, matches, &params)?;

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

            // Check if stdin or watch mode is being used for adaptive buffering (low latency)
            let watch_mode = cmd_matches.get_one::<String>("step").is_some()
                && matches!(input.parsed_datetime, Some(DateTimeInput::Now));
            let is_stdin = matches!(
                input.input_type,
                InputType::StdinCoords | InputType::StdinTimes | InputType::StdinPaired
            ) || watch_mode;

            output_position_results(
                tracked_iter,
                &format,
                show_inputs,
                show_headers,
                elevation_angle,
                is_stdin,
            );

            PerformanceTracker::report_if_needed(&tracker);
            Ok(())
        }
        "sunrise" => {
            let show_twilight = parse_sunrise_options(cmd_matches).twilight;

            let params = get_sunrise_calculation_parameters(&input, matches, show_twilight)?;
            let show_perf = matches.get_flag("perf");
            let tracker = PerformanceTracker::create(show_perf);
            let sunrise_iter = create_sunrise_iterator(&input, matches, &params)?;

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

            // Check if stdin is being used for adaptive buffering
            let is_stdin = matches!(
                input.input_type,
                InputType::StdinCoords | InputType::StdinTimes | InputType::StdinPaired
            );

            output_sunrise_results(
                tracked_iter,
                &format,
                show_inputs,
                show_headers,
                show_twilight,
                is_stdin,
            );

            PerformanceTracker::report_if_needed(&tracker);
            Ok(())
        }
        _ => Err(format!("Unknown command: {}", cmd_name)),
    }
}

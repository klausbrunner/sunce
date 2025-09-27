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
    parse_data_values, parse_input, parse_position_options, parse_sunrise_options,
};
use types::ParsedInput;

use calculation::{get_calculation_parameters, get_sunrise_calculation_parameters};
use iterators::{create_position_iterator, create_sunrise_iterator};
use output::output_position_results;
use performance::PerformanceTracker;
use sunrise_formatters::output_sunrise_results;
use types::{AppError, DateTimeInput, InputType, OutputFormat};

/// Convert Result iterator to T iterator, exiting on errors
fn exit_on_error<T, E: std::fmt::Display>(result: Result<T, E>) -> T {
    result.unwrap_or_else(|e| {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    })
}

fn main() {
    let app = cli::build_cli();
    let matches = app.get_matches();

    if let Err(e) = run_app(&matches) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

fn run_app(matches: &clap::ArgMatches) -> Result<(), AppError> {
    let mut input = parse_input(matches)?;
    let (cmd_name, cmd_matches) = matches.subcommand().unwrap_or(("position", matches));
    parse_data_values(&mut input, Some(cmd_name))?;

    let format = match input.global_options.format.as_deref() {
        Some(fmt) => OutputFormat::from_string(fmt)?,
        None => OutputFormat::Human,
    };
    let show_inputs = input.global_options.show_inputs.unwrap_or(false);
    let show_headers = input.global_options.headers.unwrap_or(true);

    match cmd_name {
        "position" => {
            let elevation_angle = parse_position_options(cmd_matches).elevation_angle;

            let params = get_calculation_parameters(&input, matches)?;
            let show_perf = matches.get_flag("perf");
            let tracker = PerformanceTracker::create(show_perf);
            let position_iter = create_position_iterator(&input, matches, &params)?;

            let processed_iter = position_iter.map(exit_on_error);

            // Check if stdin or watch mode is being used for adaptive buffering (low latency)
            let watch_mode = cmd_matches.get_one::<String>("step").is_some()
                && matches!(input.parsed_datetime, Some(DateTimeInput::Now));
            let is_stdin = matches!(
                input.input_type,
                InputType::StdinCoords | InputType::StdinTimes | InputType::StdinPaired
            ) || watch_mode;

            // Track performance if enabled
            if let Some(ref t) = tracker {
                let tracked_iter = processed_iter.inspect(|_| {
                    t.track_item();
                });
                output_position_results(
                    tracked_iter,
                    &format,
                    show_inputs,
                    show_headers,
                    elevation_angle,
                    is_stdin,
                );
            } else {
                output_position_results(
                    processed_iter,
                    &format,
                    show_inputs,
                    show_headers,
                    elevation_angle,
                    is_stdin,
                );
            }

            PerformanceTracker::report_if_needed(&tracker);
            Ok(())
        }
        "sunrise" => {
            let show_twilight = parse_sunrise_options(cmd_matches).twilight;

            let params = get_sunrise_calculation_parameters(&input, matches, show_twilight)?;
            let show_perf = matches.get_flag("perf");
            let tracker = PerformanceTracker::create(show_perf);
            let sunrise_iter = create_sunrise_iterator(&input, matches, &params)?;

            let processed_iter = sunrise_iter.map(exit_on_error);

            // Check if stdin is being used for adaptive buffering
            let is_stdin = matches!(
                input.input_type,
                InputType::StdinCoords | InputType::StdinTimes | InputType::StdinPaired
            );

            // Track performance if enabled
            if let Some(ref t) = tracker {
                let tracked_iter = processed_iter.inspect(|_| {
                    t.track_item();
                });
                output_sunrise_results(
                    tracked_iter,
                    &format,
                    show_inputs,
                    show_headers,
                    show_twilight,
                    is_stdin,
                );
            } else {
                output_sunrise_results(
                    processed_iter,
                    &format,
                    show_inputs,
                    show_headers,
                    show_twilight,
                    is_stdin,
                );
            }

            PerformanceTracker::report_if_needed(&tracker);
            Ok(())
        }
        _ => Err(AppError::General(format!("Unknown command: {}", cmd_name))),
    }
}

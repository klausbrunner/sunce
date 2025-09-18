mod calculation;
mod cli;
mod coordinate_parser;
mod datetime_parser;
mod datetime_utils;
mod engine;
mod file_input;
mod input_parser;
mod iterators;
mod output;
mod parsing;
mod sunrise_formatters;
mod sunrise_output;
mod time_series;
mod timezone;
mod types;

use parsing::{
    ParsedInput, parse_data_values, parse_input, parse_position_options, parse_sunrise_options,
};

use engine::{determine_output_format, execute_position_command, execute_sunrise_command};

fn main() {
    let app = cli::build_cli();
    let matches = app.get_matches();

    if let Err(e) = run_app(&matches) {
        eprintln!("✗ {}", e);
        std::process::exit(1);
    }
}

fn run_app(matches: &clap::ArgMatches) -> Result<(), String> {
    let mut input = parse_input(matches)?;
    parse_data_values(&mut input).map_err(|e| e.to_string())?;

    let format = determine_output_format(input.global_options.format.as_deref())?;

    let (cmd_name, cmd_matches) = matches.subcommand().unwrap_or(("position", matches));
    let show_inputs = input.global_options.show_inputs.unwrap_or(false);
    let show_headers = input.global_options.headers.unwrap_or(true);

    match cmd_name {
        "position" => {
            let elevation_angle = parse_position_options(cmd_matches).elevation_angle;
            execute_position_command(
                &input,
                matches,
                &format,
                show_inputs,
                show_headers,
                elevation_angle,
            )
        }
        "sunrise" => {
            let show_twilight = parse_sunrise_options(cmd_matches).twilight;
            execute_sunrise_command(
                &input,
                matches,
                &format,
                show_inputs,
                show_headers,
                show_twilight,
            )
        }
        _ => Err(format!("Unknown command: {}", cmd_name)),
    }
}

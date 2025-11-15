//! Command-line parsing and validation.

use crate::data::{self, Command, DataSource, InputPath, LocationSource, Parameters, TimeSource};
use crate::error::CliError;
use std::path::PathBuf;

const DELTAT_MULTIPLE_ERROR: &str =
    "error: the argument '--deltat <DELTAT>' cannot be used multiple times";

type CliResult<T> = Result<T, CliError>;

pub fn parse_cli(args: Vec<String>) -> CliResult<(DataSource, Command, Parameters)> {
    if args.len() < 2 {
        return Err("Usage: sunce [options] <lat> <lon> <datetime> <command>".into());
    }

    let mut params = Parameters::default();
    let mut positional_args = Vec::new();
    let mut deltat_seen = false;

    for arg in args.iter().skip(1) {
        if let Some(stripped) = arg.strip_prefix("--") {
            if let Some(eq_pos) = stripped.find('=') {
                let option = &stripped[..eq_pos];
                let value = &stripped[eq_pos + 1..];

                match option {
                    "format" => {
                        let format_lower = value.to_lowercase();
                        #[cfg(not(feature = "parquet"))]
                        if format_lower == "parquet" {
                            return Err(
                                "PARQUET format not available. Recompile with --features parquet"
                                    .into(),
                            );
                        }
                        #[cfg(feature = "parquet")]
                        let valid_formats = ["text", "csv", "json", "parquet"];
                        #[cfg(not(feature = "parquet"))]
                        let valid_formats = ["text", "csv", "json"];

                        if !valid_formats.contains(&format_lower.as_str()) {
                            #[cfg(feature = "parquet")]
                            let supported = "text, csv, json, parquet";
                            #[cfg(not(feature = "parquet"))]
                            let supported = "text, csv, json";
                            return Err(format!(
                                "Invalid format: '{}'. Supported formats: {}",
                                value, supported
                            )
                            .into());
                        }
                        params.output.format = format_lower;
                    }
                    "deltat" => {
                        if deltat_seen {
                            return Err(DELTAT_MULTIPLE_ERROR.into());
                        }
                        deltat_seen = true;
                        params.deltat = Some(value.parse::<f64>().map_err(|_| {
                            CliError::from(format!("Invalid deltat value: {}", value))
                        })?)
                    }
                    "timezone" => params.timezone = Some(value.to_string()),
                    "algorithm" => {
                        let algo_lower = value.to_lowercase();
                        if !["spa", "grena3"].contains(&algo_lower.as_str()) {
                            return Err(format!(
                                "Invalid algorithm: '{}'. Supported algorithms: spa, grena3",
                                value
                            )
                            .into());
                        }
                        params.calculation.algorithm = algo_lower;
                    }
                    "step" => {
                        validate_step_value(value)?;
                        params.step = Some(value.to_string())
                    }
                    "elevation" => {
                        params.environment.elevation = value.parse::<f64>().map_err(|_| {
                            CliError::from(format!("Invalid elevation value: {}", value))
                        })?
                    }
                    "temperature" => {
                        params.environment.temperature = value.parse::<f64>().map_err(|_| {
                            CliError::from(format!("Invalid temperature value: {}", value))
                        })?
                    }
                    "pressure" => {
                        params.environment.pressure = value.parse::<f64>().map_err(|_| {
                            CliError::from(format!("Invalid pressure value: {}", value))
                        })?
                    }
                    "horizon" => {
                        params.calculation.horizon = Some(value.parse::<f64>().map_err(|_| {
                            CliError::from(format!("Invalid horizon value: {}", value))
                        })?)
                    }
                    _ => return Err(format!("Unknown option: --{}", option).into()),
                }
            } else {
                match stripped {
                    "headers" => params.output.headers = true,
                    "no-headers" => params.output.headers = false,
                    "show-inputs" => params.output.show_inputs = Some(true),
                    "no-show-inputs" => params.output.show_inputs = Some(false),
                    "perf" => params.perf = true,
                    "deltat" => {
                        if deltat_seen {
                            return Err(DELTAT_MULTIPLE_ERROR.into());
                        }
                        deltat_seen = true;
                        params.deltat = None;
                    }
                    "no-refraction" => params.environment.refraction = false,
                    "elevation-angle" => params.output.elevation_angle = true,
                    "twilight" => params.calculation.twilight = true,
                    "help" => return Err(get_help_text().into()),
                    "version" => return Err(get_version_text().into()),
                    _ => return Err(format!("Unknown option: --{}", stripped).into()),
                }
            }
        } else {
            positional_args.push(arg.clone());
        }
    }

    if positional_args.len() < 2 {
        return Err("Need at least command and one argument".into());
    }

    if positional_args[0] == "help" {
        if positional_args.len() >= 2 {
            return Err(get_command_help(&positional_args[1]).into());
        } else {
            return Err(get_help_text().into());
        }
    }

    let command_index = positional_args
        .iter()
        .position(|arg| arg == "position" || arg == "sunrise")
        .ok_or("No command found".to_string())?;

    let command_str = &positional_args[command_index];
    let command = match command_str.as_str() {
        "position" => Command::Position,
        "sunrise" => Command::Sunrise,
        _ => return Err(format!("Unknown command: {}", command_str).into()),
    };

    let data_args = &positional_args[..command_index];

    match command {
        Command::Position => {
            if params.calculation.horizon.is_some() {
                return Err("Option --horizon not valid for position command".into());
            }
            if params.calculation.twilight {
                return Err("Option --twilight not valid for position command".into());
            }
        }
        Command::Sunrise => {
            if params.step.is_some() {
                return Err("Option --step not valid for sunrise command".into());
            }
            if !params.environment.refraction {
                return Err("Option --no-refraction not valid for sunrise command".into());
            }
            if params.output.elevation_angle {
                return Err("Option --elevation-angle not valid for sunrise command".into());
            }
            if params.calculation.algorithm != "spa" {
                return Err("Option --algorithm not valid for sunrise command".into());
            }
            if params.environment.elevation != 0.0 {
                return Err("Option --elevation not valid for sunrise command".into());
            }
            if params.environment.temperature != 15.0 {
                return Err("Option --temperature not valid for sunrise command".into());
            }
            if params.environment.pressure != 1013.0 {
                return Err("Option --pressure not valid for sunrise command".into());
            }
        }
    }

    let data_source = match data_args.len() {
        1 => {
            let arg = &data_args[0];
            if arg.starts_with('@') {
                DataSource::Paired(parse_file_arg(arg)?)
            } else {
                return Err("Single argument must be a file (@file or @-)".into());
            }
        }
        2 => {
            let arg1 = &data_args[0];
            let arg2 = &data_args[1];

            if arg1.starts_with('@') && arg2.starts_with('@') {
                let coord_path = parse_file_arg(arg1)?;
                let time_path = parse_file_arg(arg2)?;

                let location_source = LocationSource::File(coord_path);
                let time_source = TimeSource::File(time_path);
                DataSource::Separate(location_source, time_source)
            } else if arg1.starts_with('@') {
                let location_source = LocationSource::File(parse_file_arg(arg1)?);
                let time_source = parse_time_arg(arg2, &params)?;
                DataSource::Separate(location_source, time_source)
            } else {
                return Err("Two arguments: Use @coords.txt @times.txt, @coords.txt datetime, or three arguments (lat lon datetime)".into());
            }
        }
        3 => {
            let lat_str = &data_args[0];
            let lon_str = &data_args[1];
            let time_str = &data_args[2];

            let location_source = parse_location_args(lat_str, lon_str)?;
            let time_source = parse_time_arg(time_str, &params)?;

            DataSource::Separate(location_source, time_source)
        }
        _ => return Err("Too many arguments".into()),
    };

    if params.output.show_inputs.is_none() {
        params.output.show_inputs = Some(should_auto_show_inputs(&data_source, command));
    }

    Ok((data_source, command, params))
}

fn parse_file_arg(arg: &str) -> CliResult<InputPath> {
    let Some(stripped) = arg.strip_prefix('@') else {
        return Err("Not a file argument".into());
    };

    if stripped == "-" {
        return Ok(InputPath::Stdin);
    }

    Ok(InputPath::File(PathBuf::from(stripped)))
}

fn parse_location_args(lat_str: &str, lon_str: &str) -> CliResult<LocationSource> {
    if lat_str.starts_with('@') && lon_str.starts_with('@') {
        return Err("Cannot have both lat and lon as files".into());
    }

    if lat_str.starts_with('@') {
        return Ok(LocationSource::File(parse_file_arg(lat_str)?));
    }

    if lon_str.starts_with('@') {
        return Ok(LocationSource::File(parse_file_arg(lon_str)?));
    }

    let lat_range = match parse_range(lat_str)? {
        Some(range) => Some(data::validate_latitude_range(range).map_err(CliError::from)?),
        None => None,
    };
    let lon_range = match parse_range(lon_str)? {
        Some(range) => Some(data::validate_longitude_range(range).map_err(CliError::from)?),
        None => None,
    };

    match (lat_range, lon_range) {
        (Some(lat), Some(lon)) => Ok(LocationSource::Range {
            lat,
            lon: Some(lon),
        }),
        (Some(lat), None) => {
            let lon_val = lon_str
                .parse::<f64>()
                .map_err(|_| CliError::from(format!("Invalid longitude: {}", lon_str)))?;
            let lon_valid = data::validate_longitude(lon_val).map_err(CliError::from)?;
            Ok(LocationSource::Range {
                lat,
                lon: Some((lon_valid, lon_valid, 0.0)),
            })
        }
        (None, Some(lon)) => {
            let lat_val = lat_str
                .parse::<f64>()
                .map_err(|_| CliError::from(format!("Invalid latitude: {}", lat_str)))?;
            let lat_valid = data::validate_latitude(lat_val).map_err(CliError::from)?;
            Ok(LocationSource::Range {
                lat: (lat_valid, lat_valid, 0.0),
                lon: Some(lon),
            })
        }
        (None, None) => {
            let lat_value = lat_str
                .parse::<f64>()
                .map_err(|_| CliError::from(format!("Invalid latitude: {}", lat_str)))?;
            let lon_value = lon_str
                .parse::<f64>()
                .map_err(|_| CliError::from(format!("Invalid longitude: {}", lon_str)))?;
            let lat = data::validate_latitude(lat_value).map_err(CliError::from)?;
            let lon = data::validate_longitude(lon_value).map_err(CliError::from)?;
            Ok(LocationSource::Single(lat, lon))
        }
    }
}

fn parse_time_arg(time_str: &str, params: &Parameters) -> CliResult<TimeSource> {
    if time_str.starts_with('@') {
        return Ok(TimeSource::File(parse_file_arg(time_str)?));
    }

    if time_str == "now" {
        return Ok(TimeSource::Now);
    }

    if is_partial_date(time_str) {
        return Ok(TimeSource::Range(time_str.to_string(), params.step.clone()));
    }

    if params.step.is_some() && is_date_without_time(time_str) {
        return Ok(TimeSource::Range(time_str.to_string(), params.step.clone()));
    }

    if params.step.is_some() {
        return Err(
            "Option --step requires date-only input (YYYY, YYYY-MM, or YYYY-MM-DD) or 'now'".into(),
        );
    }

    data::parse_datetime_string(time_str, params.timezone.as_deref()).map_err(CliError::from)?;
    Ok(TimeSource::Single(time_str.to_string()))
}

fn parse_range(s: &str) -> Result<Option<(f64, f64, f64)>, CliError> {
    let Some((start_str, rest)) = s.split_once(':') else {
        return Ok(None);
    };
    let Some((end_str, step_str)) = rest.split_once(':') else {
        return Err(format!("Range must be start:end:step, got: {}", s).into());
    };

    let (start, end, step) = (
        start_str
            .parse()
            .map_err(|_| CliError::from(format!("Invalid range start: {}", start_str)))?,
        end_str
            .parse()
            .map_err(|_| CliError::from(format!("Invalid range end: {}", end_str)))?,
        step_str
            .parse()
            .map_err(|_| CliError::from(format!("Invalid range step: {}", step_str)))?,
    );

    if step <= 0.0 {
        return Err("Range step must be positive".into());
    }

    Ok(Some((start, end, step)))
}

fn is_partial_date(s: &str) -> bool {
    if s.len() == 4 && s.chars().all(|c| c.is_ascii_digit()) {
        return true;
    }

    if s.len() == 7 && &s[4..5] == "-" {
        let year_part = &s[0..4];
        let month_part = &s[5..7];
        return year_part.chars().all(|c| c.is_ascii_digit())
            && month_part.chars().all(|c| c.is_ascii_digit());
    }

    false
}

fn is_date_without_time(s: &str) -> bool {
    s.len() == 10
        && s.matches('-').count() == 2
        && !s.contains('T')
        && !s.contains(' ')
        && s.chars()
            .enumerate()
            .all(|(idx, c)| matches!(idx, 4 | 7) || c.is_ascii_digit())
}

fn should_auto_show_inputs(source: &DataSource, command: Command) -> bool {
    match source {
        DataSource::Separate(loc, time) => {
            let has_location_range = matches!(loc, LocationSource::Range { .. });
            let has_location_file = matches!(loc, LocationSource::File(_));
            let has_time_range = matches!(time, TimeSource::Range(_, _));
            let has_time_file = matches!(time, TimeSource::File(_));

            let is_position_date_series = matches!(time, TimeSource::Single(s) if command == Command::Position && is_date_without_time(s));

            has_location_range
                || has_location_file
                || has_time_range
                || has_time_file
                || is_position_date_series
        }
        DataSource::Paired(_) => true,
    }
}

fn validate_step_value(step: &str) -> CliResult<()> {
    data::parse_duration_positive(step)
        .map(|_| ())
        .map_err(CliError::from)
}

fn get_version_text() -> String {
    format!(
        "sunce {}\n Build: {} ({})\n Built: {}\n Features: {}",
        env!("CARGO_PKG_VERSION"),
        env!("BUILD_PROFILE"),
        env!("BUILD_TARGET"),
        env!("BUILD_DATE"),
        env!("BUILD_FEATURES")
    )
}

fn get_help_text() -> String {
    format!(
        r#"sunce {}
Calculates topocentric solar coordinates or sunrise/sunset times.

Usage: sunce [OPTIONS] <latitude> <longitude> <dateTime> <COMMAND>

Examples:
  sunce 52.0 13.4 2024-01-01 position
  sunce 52:53:0.1 13:14:0.1 2024 position --format=csv
  sunce @coords.txt @times.txt position
  sunce @data.txt position  # paired lat,lng,datetime data
  echo '52.0 13.4 2024-01-01T12:00:00' | sunce @- position

Arguments:
  <latitude>        Latitude: decimal degrees, range, or file
                      Range:       -90° to +90°
                      52.5        single coordinate
                      52:53:0.1   range from 52° to 53° in 0.1° steps
                      @coords.txt file with coordinates (or @- for stdin)

  <longitude>       Longitude: decimal degrees, range, or file
                      Range:       -180° to +180°
                      13.4        single coordinate
                      13:14:0.1   range from 13° to 14° in 0.1° steps
                      @coords.txt file with coordinates (or @- for stdin)

  <dateTime>        Date/time: ISO format, partial dates, or file
                      2024-01-01           specific date (midnight)
                      2024-01-01T12:00:00  specific date and time
                      2024                 entire year (with --step)
                      now                  current date and time
                      @times.txt           file with times (or @- for stdin)

Options:
  --deltat[=<value>]    Delta T in seconds. Default is 0 when the option is
                        omitted. Use --deltat=<value> for an explicit value, or
                        --deltat (no value) to request an automatic estimate.
  --format=<format>     Output format: text, csv, json, parquet. Default: text
  --help                Show this help message and exit.
  --version             Print version information and exit.
  --[no-]headers        Show headers in output (CSV only). Default: true
  --[no-]show-inputs    Show all inputs in output. Auto-enabled for ranges/files
                        unless --no-show-inputs is used.
  --perf                Show performance statistics.
  --timezone=<tz>       Timezone as offset (e.g. +01:00) or zone id (e.g.
                        America/Los_Angeles). Overrides datetime timezone.

Commands:
  position              Calculate topocentric solar coordinates
  sunrise               Calculate sunrise, transit, sunset and twilight times

Run 'sunce help <command>' for command-specific options.
"#,
        env!("CARGO_PKG_VERSION")
    )
}

fn get_command_help(command: &str) -> String {
    match command {
        "position" => r#"Usage: sunce position [OPTIONS]

Calculates topocentric solar coordinates.

Options:
  --algorithm=<alg>         Algorithm: spa, grena3. Default: spa
  --elevation=<meters>      Elevation above sea level in meters. Default: 0
  --elevation-angle         Output elevation angle instead of zenith angle.
  --[no-]refraction         Apply refraction correction. Default: true
  --pressure=<hPa>          Avg. air pressure in hPa. Used for refraction. Default: 1013
  --temperature=<celsius>   Avg. air temperature in °C. Used for refraction. Default: 15
  --step=<interval>         Step interval for time series. Examples: 30s, 15m, 2h, 1d.
                            Default: 1h

Examples:
  sunce 52.0 13.4 2024-06-21T12:00:00 position
  sunce 52.0 13.4 2024 position --step=1d
  sunce 50:55:0.5 10:15:0.5 2024-06-21T12:00:00 position --algorithm=grena3
"#
        .to_string(),
        "sunrise" => r#"Usage: sunce sunrise [OPTIONS]

Calculates sunrise, transit, sunset and (optionally) twilight times.

Options:
  --twilight                Include civil, nautical, and astronomical twilight times.
  --horizon=<degrees>       Custom horizon angle in degrees (alternative to --twilight).

Examples:
  sunce 52.0 13.4 2024-06-21 sunrise
  sunce 52.0 13.4 2024-06 sunrise --twilight
  sunce 52.0 13.4 2024-06-21 sunrise --horizon=-6.0
"#
        .to_string(),
        _ => format!(
            "Unknown command: {}\n\nRun 'sunce --help' for usage.",
            command
        ),
    }
}

//! Command-line parsing and validation.

use crate::data::{
    self, CalculationAlgorithm, Command, DataSource, InputPath, LocationSource, OutputFormat,
    Parameters, Step, TimeSource, TimezoneOverride,
};
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
            let (name, value) = stripped
                .split_once('=')
                .map(|(n, v)| (n, Some(v)))
                .unwrap_or((stripped, None));
            apply_option(name, value, &mut params, &mut deltat_seen)?;
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
            if params.calculation.algorithm != CalculationAlgorithm::Spa {
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

fn apply_option(
    name: &str,
    value: Option<&str>,
    params: &mut Parameters,
    deltat_seen: &mut bool,
) -> CliResult<()> {
    match (name, value) {
        ("format", Some(v)) => {
            params.output.format = v.parse::<OutputFormat>().map_err(CliError::from)?
        }
        ("format", None) => return Err("Option --format requires a value".into()),
        ("deltat", Some(v)) => {
            if *deltat_seen {
                return Err(DELTAT_MULTIPLE_ERROR.into());
            }
            *deltat_seen = true;
            params.deltat = Some(
                v.parse::<f64>()
                    .map_err(|_| CliError::from(format!("Invalid deltat value: {}", v)))?,
            );
        }
        ("deltat", None) => {
            if *deltat_seen {
                return Err(DELTAT_MULTIPLE_ERROR.into());
            }
            *deltat_seen = true;
            params.deltat = None;
        }
        ("timezone", Some(v)) => params.timezone = Some(v.parse::<TimezoneOverride>()?),
        ("timezone", None) => return Err("Option --timezone requires a value".into()),
        ("algorithm", Some(v)) => {
            params.calculation.algorithm = v.parse::<CalculationAlgorithm>().map_err(|e| {
                // FromStr returns String directly for uniform error messages
                CliError::from(e)
            })?;
        }
        ("algorithm", None) => return Err("Option --algorithm requires a value".into()),
        ("step", Some(v)) => params.step = Some(v.parse::<Step>().map_err(CliError::from)?),
        ("step", None) => return Err("Option --step requires a value".into()),
        ("elevation", Some(v)) => params.environment.elevation = parse_f64("elevation", v)?,
        ("temperature", Some(v)) => params.environment.temperature = parse_f64("temperature", v)?,
        ("pressure", Some(v)) => params.environment.pressure = parse_f64("pressure", v)?,
        ("horizon", Some(v)) => {
            params.calculation.horizon = Some(parse_f64("horizon", v)?);
        }
        ("horizon", None) => return Err("Option --horizon requires a value".into()),
        ("headers", None) => params.output.headers = true,
        ("no-headers", None) => params.output.headers = false,
        ("show-inputs", None) => params.output.show_inputs = Some(true),
        ("no-show-inputs", None) => params.output.show_inputs = Some(false),
        ("perf", None) => params.perf = true,
        ("no-refraction", None) => params.environment.refraction = false,
        ("elevation-angle", None) => params.output.elevation_angle = true,
        ("twilight", None) => params.calculation.twilight = true,
        ("help", None) => return Err(get_help_text().into()),
        ("version", None) => return Err(get_version_text().into()),
        _ => return Err(format!("Unknown option: --{}", name).into()),
    }

    Ok(())
}

fn parse_f64(label: &str, value: &str) -> CliResult<f64> {
    value.parse::<f64>().map_err(|_| {
        CliError::from(format!(
            "Invalid {} value: {}",
            label.replace('-', " "),
            value
        ))
    })
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
        return Ok(TimeSource::Range(time_str.to_string(), params.step));
    }

    if params.step.is_some() && is_date_without_time(time_str) {
        return Ok(TimeSource::Range(time_str.to_string(), params.step));
    }

    if params.step.is_some() {
        return Err(
            "Option --step requires date-only input (YYYY, YYYY-MM, or YYYY-MM-DD) or 'now'".into(),
        );
    }

    data::parse_datetime_string(time_str, params.timezone.as_ref().map(|tz| tz.as_str()))
        .map_err(CliError::from)?;
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
    match s.len() {
        4 => s.chars().all(|c| c.is_ascii_digit()),
        7 if s.as_bytes().get(4) == Some(&b'-') => s
            .chars()
            .enumerate()
            .all(|(idx, c)| idx == 4 || c.is_ascii_digit()),
        _ => false,
    }
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
            matches!(loc, LocationSource::Range { .. } | LocationSource::File(_))
                || matches!(time, TimeSource::Range(_, _) | TimeSource::File(_))
                || (command == Command::Position
                    && matches!(time, TimeSource::Single(s) if is_date_without_time(s)))
        }
        DataSource::Paired(_) => true,
    }
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

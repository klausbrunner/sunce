//! Command-line parsing and validation.

use crate::data::{
    self, CalculationAlgorithm, Command, DataSource, InputPath, LocationSource, OutputFormat,
    Parameters, Step, TimeSource, TimezoneOverride,
};
use crate::error::CliError;
use std::path::PathBuf;

const DELTAT_MULTIPLE_ERROR: &str = "Option --deltat cannot be used multiple times";

type CliResult<T> = Result<T, CliError>;

#[derive(Default)]
struct CommandOptionUsage {
    step: bool,
    no_refraction: bool,
    elevation_angle: bool,
    elevation: bool,
    temperature: bool,
    pressure: bool,
    algorithm: bool,
    horizon: bool,
    twilight: bool,
}

pub fn parse_cli(args: Vec<String>) -> CliResult<(DataSource, Command, Parameters)> {
    if args.len() < 2 {
        return Err(CliError::Exit(
            "Usage: sunce [OPTIONS] <lat> <lon> <dateTime> <position|sunrise>".to_string(),
        ));
    }

    let mut params = Parameters::default();
    let mut positional = Vec::new();
    let mut deltat_seen = false;
    let mut option_usage = CommandOptionUsage::default();

    for arg in args.into_iter().skip(1) {
        if let Some(stripped) = arg.strip_prefix("--") {
            let (name, value) = stripped
                .split_once('=')
                .map(|(n, v)| (n, Some(v)))
                .unwrap_or((stripped, None));
            apply_option(
                name,
                value,
                &mut params,
                &mut deltat_seen,
                &mut option_usage,
            )?;
        } else {
            positional.push(arg);
        }
    }

    if let Some(first) = positional.first()
        && first == "help"
    {
        let message = positional
            .get(1)
            .map(|command| get_command_help(command))
            .unwrap_or_else(get_help_text);
        return Err(CliError::Exit(message));
    }

    let (command, data_source) = parse_positional_args(&positional, &params)?;

    validate_command_options(command, &option_usage)?;

    if params.output.show_inputs.is_none() {
        params.output.show_inputs = Some(should_auto_show_inputs(&data_source));
    }

    Ok((data_source, command, params))
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

fn apply_option(
    name: &str,
    value: Option<&str>,
    params: &mut Parameters,
    deltat_seen: &mut bool,
    option_usage: &mut CommandOptionUsage,
) -> CliResult<()> {
    let ensure_flag = |opt: &str, val: Option<&str>| -> CliResult<()> {
        if val.is_some() {
            Err(format!("Option --{} does not take a value", opt).into())
        } else {
            Ok(())
        }
    };

    match name {
        "format" => {
            let v = required_value("format", value)?;
            params.output.format = v.parse::<OutputFormat>().map_err(CliError::from)?;
        }
        "deltat" => {
            if *deltat_seen {
                return Err(DELTAT_MULTIPLE_ERROR.into());
            }
            *deltat_seen = true;
            params.deltat = match value {
                Some(v) => Some(v.parse::<f64>().map_err(|_| {
                    CliError::from("Invalid deltat value: expected floating point number")
                })?),
                None => None,
            };
        }
        "timezone" => {
            let v = required_value("timezone", value)?;
            params.timezone = Some(v.parse::<TimezoneOverride>()?);
        }
        "algorithm" => {
            let v = required_value("algorithm", value)?;
            params.calculation.algorithm =
                v.parse::<CalculationAlgorithm>().map_err(CliError::from)?;
            option_usage.algorithm = true;
        }
        "step" => {
            let v = required_value("step", value)?;
            params.step = Some(v.parse::<Step>().map_err(CliError::from)?);
            option_usage.step = true;
        }
        "elevation" => {
            let v = required_value("elevation", value)?;
            params.environment.elevation = parse_f64("elevation", v)?;
            option_usage.elevation = true;
        }
        "temperature" => {
            let v = required_value("temperature", value)?;
            params.environment.temperature = parse_f64("temperature", v)?;
            option_usage.temperature = true;
        }
        "pressure" => {
            let v = required_value("pressure", value)?;
            params.environment.pressure = parse_f64("pressure", v)?;
            option_usage.pressure = true;
        }
        "horizon" => {
            let v = required_value("horizon", value)?;
            params.calculation.horizon = Some(parse_f64("horizon", v)?);
            option_usage.horizon = true;
        }
        "headers" => {
            ensure_flag("headers", value)?;
            params.output.headers = true;
        }
        "no-headers" => {
            ensure_flag("no-headers", value)?;
            params.output.headers = false;
        }
        "show-inputs" => {
            ensure_flag("show-inputs", value)?;
            params.output.show_inputs = Some(true);
        }
        "no-show-inputs" => {
            ensure_flag("no-show-inputs", value)?;
            params.output.show_inputs = Some(false);
        }
        "perf" => {
            ensure_flag("perf", value)?;
            params.perf = true;
        }
        "no-refraction" => {
            ensure_flag("no-refraction", value)?;
            params.environment.refraction = false;
            option_usage.no_refraction = true;
        }
        "elevation-angle" => {
            ensure_flag("elevation-angle", value)?;
            params.output.elevation_angle = true;
            option_usage.elevation_angle = true;
        }
        "twilight" => {
            ensure_flag("twilight", value)?;
            params.calculation.twilight = true;
            option_usage.twilight = true;
        }
        "help" => {
            ensure_flag("help", value)?;
            return Err(CliError::Exit(get_help_text()));
        }
        "version" => {
            ensure_flag("version", value)?;
            return Err(CliError::Exit(get_version_text()));
        }
        _ => return Err(format!("Unknown option: --{}", name).into()),
    }

    Ok(())
}

fn required_value<'a>(flag: &'static str, value: Option<&'a str>) -> CliResult<&'a str> {
    value.ok_or_else(|| CliError::from(format!("Option --{} requires a value", flag)))
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
    if lat_str.starts_with('@') || lon_str.starts_with('@') {
        return Err(
            "Coordinate files must be provided as a single @file argument (use @coords.txt <dateTime>)"
                .into(),
        );
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
        (Some(lat), Some(lon)) => Ok(LocationSource::Range { lat, lon }),
        (Some(lat), None) => {
            let lon_val = lon_str
                .parse::<f64>()
                .map_err(|_| CliError::from(format!("Invalid longitude: {}", lon_str)))?;
            let lon_valid = data::validate_longitude(lon_val).map_err(CliError::from)?;
            Ok(LocationSource::Range {
                lat,
                lon: (lon_valid, lon_valid, 0.0),
            })
        }
        (None, Some(lon)) => {
            let lat_val = lat_str
                .parse::<f64>()
                .map_err(|_| CliError::from(format!("Invalid latitude: {}", lat_str)))?;
            let lat_valid = data::validate_latitude(lat_val).map_err(CliError::from)?;
            Ok(LocationSource::Range {
                lat: (lat_valid, lat_valid, 0.0),
                lon,
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

fn parse_positional_args(
    positional_args: &[String],
    params: &Parameters,
) -> CliResult<(Command, DataSource)> {
    let command_index = positional_args
        .iter()
        .position(|arg| arg == "position" || arg == "sunrise")
        .ok_or("No command found".to_string())?;
    if command_index == 0 {
        return Err("Need at least command and one argument".into());
    }

    let command = if positional_args[command_index] == "position" {
        Command::Position
    } else {
        Command::Sunrise
    };

    let data_args = &positional_args[..command_index];

    let data_source = parse_data_source(data_args, params, command)?;

    Ok((command, data_source))
}

fn parse_data_source(
    args: &[String],
    params: &Parameters,
    command: Command,
) -> CliResult<DataSource> {
    match args.len() {
        1 => {
            let arg = &args[0];
            if arg.starts_with('@') {
                Ok(DataSource::Paired(parse_file_arg(arg)?))
            } else {
                Err("Single argument must be a file (@file or @-)".into())
            }
        }
        2 => {
            let arg1 = &args[0];
            let arg2 = &args[1];

            if arg1.starts_with('@') && arg2.starts_with('@') {
                let coord_path = parse_file_arg(arg1)?;
                let time_path = parse_file_arg(arg2)?;

                let location_source = LocationSource::File(coord_path);
                let time_source = TimeSource::File(time_path);
                Ok(DataSource::Separate(location_source, time_source))
            } else if arg1.starts_with('@') {
                let location_source = LocationSource::File(parse_file_arg(arg1)?);
                let time_source = parse_time_arg(arg2, params, command)?;
                Ok(DataSource::Separate(location_source, time_source))
            } else {
                Err("Two arguments: Use @coords.txt @times.txt, @coords.txt datetime, or three arguments (lat lon datetime)".into())
            }
        }
        3 => {
            let lat_str = &args[0];
            let lon_str = &args[1];
            let time_str = &args[2];

            let location_source = parse_location_args(lat_str, lon_str)?;
            let time_source = parse_time_arg(time_str, params, command)?;

            Ok(DataSource::Separate(location_source, time_source))
        }
        _ => Err("Too many arguments".into()),
    }
}

fn parse_time_arg(time_str: &str, params: &Parameters, command: Command) -> CliResult<TimeSource> {
    if time_str.starts_with('@') {
        return Ok(TimeSource::File(parse_file_arg(time_str)?));
    }

    if time_str == "now" {
        return Ok(TimeSource::Now);
    }

    if crate::data::time_utils::is_partial_date(time_str) {
        return Ok(TimeSource::Range(time_str.to_string()));
    }

    if command == Command::Position && crate::data::time_utils::is_date_without_time(time_str) {
        return Ok(TimeSource::Range(time_str.to_string()));
    }

    if params.step.is_some() && crate::data::time_utils::is_date_without_time(time_str) {
        return Ok(TimeSource::Range(time_str.to_string()));
    }

    if params.step.is_some() {
        return Err(
            "Option --step requires date-only input (YYYY, YYYY-MM, or YYYY-MM-DD) or 'now'".into(),
        );
    }

    let dt = data::parse_datetime_string(time_str, params.timezone.as_ref().map(|tz| tz.as_str()))
        .map_err(CliError::from)?;
    Ok(TimeSource::Single(dt))
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

    if step == 0.0 {
        return Err("Range step must be non-zero".into());
    }
    if start < end && step < 0.0 {
        return Err("Range step must be positive for ascending ranges".into());
    }
    if start > end && step > 0.0 {
        return Err("Range step must be negative for descending ranges".into());
    }

    Ok(Some((start, end, step)))
}

fn should_auto_show_inputs(source: &DataSource) -> bool {
    match source {
        DataSource::Separate(loc, time) => {
            matches!(loc, LocationSource::Range { .. } | LocationSource::File(_))
                || matches!(time, TimeSource::Range(_) | TimeSource::File(_))
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

fn validate_command_options(command: Command, usage: &CommandOptionUsage) -> CliResult<()> {
    if command == Command::Position {
        if usage.horizon {
            return Err("Option --horizon not valid for position command".into());
        }
        if usage.twilight {
            return Err("Option --twilight not valid for position command".into());
        }
    }

    if command == Command::Sunrise {
        if usage.step {
            return Err("Option --step not valid for sunrise command".into());
        }
        if usage.no_refraction {
            return Err("Option --no-refraction not valid for sunrise command".into());
        }
        if usage.elevation_angle {
            return Err("Option --elevation-angle not valid for sunrise command".into());
        }
        if usage.elevation {
            return Err("Option --elevation not valid for sunrise command".into());
        }
        if usage.temperature {
            return Err("Option --temperature not valid for sunrise command".into());
        }
        if usage.pressure {
            return Err("Option --pressure not valid for sunrise command".into());
        }
        if usage.algorithm {
            return Err("Option --algorithm not valid for sunrise command".into());
        }
    }

    Ok(())
}

fn get_help_text() -> String {
    let defaults = Parameters::default();
    let formats = OutputFormat::all().join(", ");
    format!(
        r#"sunce {}
Calculates topocentric solar coordinates or sunrise/sunset times.

Usage:
  sunce [OPTIONS] <latitude> <longitude> <dateTime> <position|sunrise>
  sunce [OPTIONS] @data.txt <position|sunrise>
  sunce [OPTIONS] @coords.txt @times.txt <position|sunrise>
  sunce [OPTIONS] @coords.txt <dateTime> <position|sunrise>

Examples:
  sunce 52.0 13.4 2024-01-01 position
  sunce 52:53:0.1 13:14:0.1 2024 position --format=csv
  sunce @coords.txt @times.txt position
  sunce @data.txt position
  echo "52.0 13.4 2024-01-01T12:00:00" | sunce @- position

Arguments:
  <latitude>         Latitude: decimal degrees or range.
                       Range: -90 to +90
                       52.5            single coordinate
                       52:53:0.1       range from 52 to 53 in 0.1 steps
                       52:51:-0.5      descending range (negative step)
                       Coordinate files are passed as @coords.txt (see Usage).

  <longitude>        Longitude: decimal degrees or range.
                       Range: -180 to +180
                       13.4            single coordinate
                       13:14:0.1       range from 13 to 14 in 0.1 steps
                       13:11:-1.0      descending range (negative step)
                       Coordinate files are passed as @coords.txt (see Usage).

  <dateTime>         Date/time: ISO, partial dates, unix timestamp, or file.
                       2024-01-01           date only (position: hourly series)
                       2024-01-01T12:00:00  date and time
                       "2024-01-01 12:00"   date and time (space separator; quote it)
                       2024                 entire year (daily by default)
                       2024-06              entire month (position: hourly, sunrise: daily)
                       now                  current time (position repeats with --step
                                              for a single lat/lon only)
                       1704067200           unix timestamp (seconds)
                       @times.txt           file with times (or @- for stdin)

  File inputs:
    - Coordinates files contain lat lon per line.
    - Time files contain one datetime per line.
    - Paired data files contain lat lon datetime per line.
    - Files accept comma- or whitespace-separated fields.
    - Blank lines and lines starting with # are ignored.
    - Stdin (@-) can be used for only one input parameter.

Options:
  --deltat[=<seconds>]  Delta T in seconds. Default: 0 when omitted. Use
                        --deltat=<seconds> for an explicit value, or
                        --deltat (no value) to estimate from the date
                        (falls back to 0 if unavailable).
  --format=<format>     Output format: {}. Default: {}
  --timezone=<tz>       Timezone offset (+01:00) or IANA name (Europe/Berlin).
                        Overrides timezone for parsing and output.
  --[no-]headers        Include headers in CSV output. Default: {}
  --[no-]show-inputs    Include inputs in output. Auto-enabled for ranges,
                        files, and position date-only inputs unless
                        --no-show-inputs is used.
  --perf                Print performance statistics to stderr.
  --help                Show this help message and exit.
  --version             Print version information and exit.

Commands:
  position              Calculate topocentric solar coordinates.
  sunrise               Calculate sunrise, transit, sunset, and optional twilight.

Run 'sunce help <command>' for command-specific options.
"#,
        env!("CARGO_PKG_VERSION"),
        formats,
        defaults.output.format,
        defaults.output.headers
    )
}

fn get_command_help(command: &str) -> String {
    let defaults = Parameters::default();
    match command {
        "position" => format!(
            r#"Usage:
  sunce [OPTIONS] <latitude> <longitude> <dateTime> position
  sunce [OPTIONS] @data.txt position
  sunce [OPTIONS] @coords.txt @times.txt position
  sunce [OPTIONS] @coords.txt <dateTime> position

Calculates topocentric solar coordinates.

Options:
  --algorithm=<alg>         Algorithm: spa, grena3. Default: {}
  --elevation=<meters>      Elevation above sea level in meters. Default: {}
  --elevation-angle         Output elevation angle instead of zenith angle.
  --no-refraction           Disable refraction correction.
  --pressure=<hPa>          Air pressure in hPa (refraction). Default: {}
  --temperature=<celsius>   Air temperature in C (refraction). Default: {}
  --step=<interval>         Time step for ranges and date-only inputs.
                            Examples: 30s, 15m, 2h, 1d

Examples:
  sunce 52.0 13.4 2024-06-21T12:00:00 position
  sunce 52.0 13.4 2024-06-21 position --step=10m
  sunce 50:55:0.5 10:15:0.5 2024-06-21T12:00:00 position --algorithm=grena3
"#,
            defaults.calculation.algorithm,
            defaults.environment.elevation,
            defaults.environment.pressure,
            defaults.environment.temperature
        ),
        "sunrise" => r#"Usage:
  sunce [OPTIONS] <latitude> <longitude> <dateTime> sunrise
  sunce [OPTIONS] @data.txt sunrise
  sunce [OPTIONS] @coords.txt @times.txt sunrise
  sunce [OPTIONS] @coords.txt <dateTime> sunrise

Calculates sunrise, transit, sunset and (optionally) twilight times.

Options:
  --twilight                Include civil, nautical, and astronomical twilight times.
  --horizon=<degrees>       Custom horizon angle in degrees (ignored with --twilight).

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

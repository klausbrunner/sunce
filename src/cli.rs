//! Command-line parsing and validation.

use crate::data::{
    self, CalculationAlgorithm, Command, InputPath, LocationSource, OutputFormat, Parameters,
    Predicate, Step,
};
use crate::error::CliError;
use crate::parsed::{ParsedCommand, ParsedInput, ParsedOptionUsage, ParsedTimeSource};
use std::path::PathBuf;

const DELTAT_MULTIPLE_ERROR: &str = "Option --deltat cannot be used multiple times";
const PREDICATE_MULTIPLE_ERROR: &str = "Predicate options cannot be used multiple times";

type CliResult<T> = Result<T, CliError>;

struct Arguments<'a> {
    positional: Vec<String>,
    options: Vec<(&'a str, Option<&'a str>)>,
}

impl<'a> Arguments<'a> {
    fn new(args: &'a [String]) -> Self {
        let mut positional = Vec::new();
        let mut options = Vec::new();
        let mut args = args.iter().skip(1).peekable();
        while let Some(arg) = args.next() {
            if matches!(arg.as_str(), "-h" | "-v") {
                options.push((if arg == "-h" { "help" } else { "verbose" }, None));
                continue;
            }
            let Some(option) = arg.strip_prefix("--") else {
                positional.push(arg.clone());
                continue;
            };
            let (name, mut value) = option
                .split_once('=')
                .map(|(name, value)| (name, Some(value)))
                .unwrap_or((option, None));
            // Optional --deltat values require '=' so a latitude is never consumed.
            if value.is_none()
                && matches!(
                    name,
                    "format"
                        | "timezone"
                        | "algorithm"
                        | "step"
                        | "at"
                        | "elevation"
                        | "temperature"
                        | "pressure"
                        | "horizon"
                        | "sun-above"
                        | "sun-below"
                )
                && args.peek().is_some_and(|next| {
                    !next.starts_with("--") && !matches!(next.as_str(), "-h" | "-v")
                })
            {
                value = args.next().map(String::as_str);
            }
            options.push((name, value));
        }
        Self {
            positional,
            options,
        }
    }
}

pub fn parse_cli(args: &[String]) -> CliResult<ParsedCommand> {
    let Arguments {
        positional,
        options,
    } = Arguments::new(args);
    if positional.first().is_some_and(|arg| arg == "help") {
        if positional.len() > 2 {
            return Err("Usage: sunce help [position|events]".into());
        }
        let message = match positional.get(1) {
            Some(command) => get_command_help(command)?,
            None => get_help_text(),
        };
        return Err(CliError::Exit(message));
    }
    if options.contains(&("help", None)) {
        let message = match positional
            .iter()
            .find(|arg| matches!(arg.as_str(), "position" | "events"))
        {
            Some(command) => get_command_help(command)?,
            None => get_help_text(),
        };
        return Err(CliError::Exit(message));
    }

    let mut params = Parameters::default();
    let mut predicate = None;
    let mut deltat_seen = false;
    let mut option_usage = ParsedOptionUsage::default();

    for (name, value) in options {
        apply_option(
            name,
            value,
            &mut params,
            &mut predicate,
            &mut deltat_seen,
            &mut option_usage,
        )?;
    }

    let (command, input) = parse_positional_args(&positional)?;
    Ok(ParsedCommand {
        command,
        input,
        params,
        predicate,
        usage: option_usage,
    })
}

fn parse_f64(label: &str, value: &str) -> CliResult<f64> {
    let label = label.replace('-', " ");
    let parsed = value
        .parse::<f64>()
        .map_err(|_| CliError::from(format!("Invalid {} value: {}", label, value)))?;
    if parsed.is_finite() {
        Ok(parsed)
    } else {
        Err(format!("Invalid {} value: expected finite number", label).into())
    }
}

fn ensure_flag(opt: &str, value: Option<&str>) -> CliResult<()> {
    if value.is_some() {
        Err(format!("Option --{} does not take a value", opt).into())
    } else {
        Ok(())
    }
}

fn set_predicate(predicate: Predicate, parsed: &mut Option<Predicate>) -> CliResult<()> {
    if parsed.is_some() {
        return Err(PREDICATE_MULTIPLE_ERROR.into());
    }
    *parsed = Some(predicate);
    Ok(())
}

fn parse_predicate_option(name: &str, value: Option<&str>) -> CliResult<Option<Predicate>> {
    let parsed = match name {
        "is-daylight" => {
            ensure_flag(name, value)?;
            Some(Predicate::IsDaylight)
        }
        "is-civil-twilight" => {
            ensure_flag(name, value)?;
            Some(Predicate::IsCivilTwilight)
        }
        "is-nautical-twilight" => {
            ensure_flag(name, value)?;
            Some(Predicate::IsNauticalTwilight)
        }
        "is-astronomical-twilight" => {
            ensure_flag(name, value)?;
            Some(Predicate::IsAstronomicalTwilight)
        }
        "is-astronomical-night" => {
            ensure_flag(name, value)?;
            Some(Predicate::IsAstronomicalNight)
        }
        "after-sunset" => {
            ensure_flag(name, value)?;
            Some(Predicate::AfterSunset)
        }
        "sun-above" => Some(Predicate::SunAbove(parse_f64(
            name,
            required_value(name, value)?,
        )?)),
        "sun-below" => Some(Predicate::SunBelow(parse_f64(
            name,
            required_value(name, value)?,
        )?)),
        _ => None,
    };
    Ok(parsed)
}

fn apply_option(
    name: &str,
    value: Option<&str>,
    params: &mut Parameters,
    parsed_predicate: &mut Option<Predicate>,
    deltat_seen: &mut bool,
    option_usage: &mut ParsedOptionUsage,
) -> CliResult<()> {
    if let Some(predicate) = parse_predicate_option(name, value)? {
        return set_predicate(predicate, parsed_predicate);
    }

    match name {
        "format" => {
            let v = required_value("format", value)?;
            params.output.format = v.parse::<OutputFormat>().map_err(CliError::from)?;
            option_usage.format = true;
        }
        "deltat" => {
            if *deltat_seen {
                return Err(DELTAT_MULTIPLE_ERROR.into());
            }
            *deltat_seen = true;
            params.calculation.deltat = match value {
                Some(v) => Some(parse_f64("deltat", v)?),
                None => None,
            };
        }
        "timezone" => {
            let v = required_value("timezone", value)?;
            params.timezone = Some(data::time_utils::parse_timezone_spec(v)?);
        }
        "algorithm" => {
            let v = required_value("algorithm", value)?;
            params.calculation.algorithm =
                v.parse::<CalculationAlgorithm>().map_err(CliError::from)?;
        }
        "step" => {
            let v = required_value("step", value)?;
            params.step = Some(v.parse::<Step>().map_err(CliError::from)?);
        }
        "at" => {
            let v = required_value("at", value)?;
            params.at = Some(
                v.parse()
                    .map_err(|_| CliError::from("Option --at requires HH:MM:SS"))?,
            );
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
        }
        "headers" => {
            ensure_flag("headers", value)?;
            params.output.headers = true;
            option_usage.headers = true;
        }
        "no-headers" => {
            ensure_flag("no-headers", value)?;
            params.output.headers = false;
            option_usage.headers = true;
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
        "wait" => {
            ensure_flag("wait", value)?;
            params.wait = true;
        }
        "verbose" => {
            ensure_flag("verbose", value)?;
            params.verbose = true;
        }
        "no-refraction" => {
            ensure_flag("no-refraction", value)?;
            params.environment.refraction = false;
        }
        "elevation-angle" => {
            ensure_flag("elevation-angle", value)?;
            params.output.elevation_angle = true;
        }
        "twilight" => {
            ensure_flag("twilight", value)?;
            params.calculation.twilight = true;
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

fn required_value<'a>(flag: &str, value: Option<&'a str>) -> CliResult<&'a str> {
    value.ok_or_else(|| CliError::from(format!("Option --{} requires a value", flag)))
}

fn parse_file_arg(arg: &str) -> InputPath {
    let stripped = &arg[1..];
    if stripped == "-" {
        InputPath::Stdin
    } else {
        InputPath::File(PathBuf::from(stripped))
    }
}

fn parse_location_args(lat_str: &str, lon_str: &str) -> CliResult<LocationSource> {
    if lat_str.starts_with('@') || lon_str.starts_with('@') {
        return Err(
            "Coordinate files must be provided as a single @file argument (use @coords.txt <dateTime>)"
                .into(),
        );
    }

    fn parse_latitude(value: &str) -> CliResult<f64> {
        value
            .parse::<f64>()
            .map_err(|_| CliError::from(format!("Invalid latitude: {}", value)))
            .and_then(|value| data::validate_latitude(value).map_err(CliError::from))
    }

    fn parse_longitude(value: &str) -> CliResult<f64> {
        value
            .parse::<f64>()
            .map_err(|_| CliError::from(format!("Invalid longitude: {}", value)))
            .and_then(|value| data::validate_longitude(value).map_err(CliError::from))
    }

    let lat_range = parse_range(lat_str)?
        .map(|range| data::validate_latitude_range(range).map_err(CliError::from))
        .transpose()?;
    let lon_range = parse_range(lon_str)?
        .map(|range| data::validate_longitude_range(range).map_err(CliError::from))
        .transpose()?;

    match (lat_range, lon_range) {
        (Some(lat), Some(lon)) => Ok(LocationSource::Range { lat, lon }),
        (Some(lat), None) => parse_longitude(lon_str).map(|lon| LocationSource::Range {
            lat,
            lon: (lon, lon, 0.0),
        }),
        (None, Some(lon)) => parse_latitude(lat_str).map(|lat| LocationSource::Range {
            lat: (lat, lat, 0.0),
            lon,
        }),
        (None, None) => Ok(LocationSource::Single(
            parse_latitude(lat_str)?,
            parse_longitude(lon_str)?,
        )),
    }
}

fn parse_positional_args(positional_args: &[String]) -> CliResult<(Command, ParsedInput)> {
    let command_index = positional_args
        .iter()
        .position(|arg| arg == "position" || arg == "events")
        .ok_or("No command found\nUsage: sunce [OPTIONS] <lat> <lon> <dateTime> <position|events>\nRun 'sunce --help' for more.")?;
    if command_index == 0 {
        return Err(format!(
            "Pass inputs before the command. Run 'sunce {} --help' for usage.",
            positional_args[0]
        )
        .into());
    }
    if command_index + 1 != positional_args.len() {
        return Err(format!(
            "Unexpected arguments after command: {}",
            positional_args[command_index + 1..].join(" ")
        )
        .into());
    }

    let command = match positional_args[command_index].as_str() {
        "position" => Command::Position,
        "events" => Command::Events,
        _ => unreachable!("filtered above"),
    };
    Ok((
        command,
        parse_data_source(&positional_args[..command_index])?,
    ))
}

fn parse_data_source(args: &[String]) -> CliResult<ParsedInput> {
    match args {
        [path] if path.starts_with('@') => Ok(ParsedInput::Paired(parse_file_arg(path))),
        [_] => Err("Single argument must be a file (@file or @-)".into()),
        [path, time] if path.starts_with('@') => Ok(ParsedInput::Separate(
            LocationSource::File(parse_file_arg(path)),
            parse_time_arg(time),
        )),
        [_, _] => Err("Two arguments: Use @coords.txt @times.txt, @coords.txt datetime, or three arguments (lat lon datetime)".into()),
        [lat, lon, time] => Ok(ParsedInput::Separate(
            parse_location_args(lat, lon)?,
            parse_time_arg(time),
        )),
        _ => Err("Too many arguments".into()),
    }
}

fn parse_time_arg(time_str: &str) -> ParsedTimeSource {
    if time_str.starts_with('@') {
        ParsedTimeSource::File(parse_file_arg(time_str))
    } else if time_str == "now" {
        ParsedTimeSource::Now
    } else {
        ParsedTimeSource::Value(time_str.to_owned())
    }
}

fn parse_range(s: &str) -> Result<Option<(f64, f64, f64)>, CliError> {
    let Some((start_str, rest)) = s.split_once(':') else {
        return Ok(None);
    };
    let Some((end_str, step_str)) = rest.split_once(':') else {
        return Err(format!("Range must be start:end:step, got: {}", s).into());
    };

    let (start, end, step): (f64, f64, f64) = (
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

    if !step.is_finite() || step == 0.0 {
        return Err("Range step must be non-zero and finite".into());
    }
    if start < end && step < 0.0 {
        return Err("Range step must be positive for ascending ranges".into());
    }
    if start > end && step > 0.0 {
        return Err("Range step must be negative for descending ranges".into());
    }

    Ok(Some((start, end, step)))
}

fn get_version_text() -> String {
    format!(
        "sunce {}\n Build: {} ({})\n Features: {}",
        env!("CARGO_PKG_VERSION"),
        env!("BUILD_PROFILE"),
        env!("BUILD_TARGET"),
        env!("BUILD_FEATURES")
    )
}

fn shared_help() -> String {
    let defaults = Parameters::default();
    let formats = OutputFormat::all().join(", ");
    format!(
        r#"Shared options:
  --algorithm=<name>    Solar model: spa or grena3. Default: {}.
  --deltat[=<seconds>]  Default: 0. Bare --deltat estimates from the date
                        (falls back to 0 if unavailable).
  --format=<format>     Output format: {formats}. Default: {}.
  --timezone=<tz>       Offset (+01:00) or IANA name (Europe/Berlin).
                        Local dates and times otherwise use TZ or the system zone.
  --[no-]headers        Include headers in text/CSV. Default: {}.
  --[no-]show-inputs    Include input columns. Default: auto for ranges and files.
  --perf                Print performance statistics to stderr.
  -v, --verbose         Print status messages to stderr.
  -h, --help            Show help.
  --version             Print version information and exit.

Required option values accept '--option=value' or '--option value'.
An explicit --deltat value requires '='.

Exit status: 0 for success or a true predicate, 1 for a false predicate,
2 for errors. Predicates produce no stdout and require one location and instant.
Use --wait with a predicate and 'now' to wait for a calculated transition.
"#,
        defaults.calculation.algorithm, defaults.output.format, defaults.output.headers
    )
}

fn get_help_text() -> String {
    format!(
        r#"sunce {}
Calculates topocentric solar coordinates or solar events.

Usage:
  sunce [OPTIONS] <latitude> <longitude> <dateTime> <position|events>
  sunce [OPTIONS] @data.txt <position|events>
  sunce [OPTIONS] @coords.txt @times.txt <position|events>
  sunce [OPTIONS] @coords.txt <dateTime> <position|events>

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
                       2024-06              entire month (position: hourly, events: daily)
                       2024-06-01/2024-08-31 inclusive date range
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

Commands:
  position              Calculate topocentric solar coordinates.
  events                Calculate sunrise, transit, sunset, and optional twilight.

{}
Run 'sunce help <command>' or 'sunce <command> --help' for command-specific options.
"#,
        env!("CARGO_PKG_VERSION"),
        shared_help()
    )
}

fn get_command_help(command: &str) -> CliResult<String> {
    let defaults = Parameters::default();
    let shared = shared_help();
    Ok(match command {
        "position" => format!(
            r#"Usage:
  sunce [OPTIONS] <latitude> <longitude> <dateTime> position
  sunce [OPTIONS] @data.txt position
  sunce [OPTIONS] @coords.txt @times.txt position
  sunce [OPTIONS] @coords.txt <dateTime> position

Calculates topocentric solar coordinates.

Position options:
  --elevation=<meters>      Elevation above sea level in meters (Grena3: zero). Default: {}
  --elevation-angle         Output elevation angle instead of zenith angle.
  --no-refraction           Disable refraction correction.
  --pressure=<hPa>          Air pressure in hPa (refraction). Default: {}
  --temperature=<celsius>   Air temperature in C (refraction). Default: {}
  --step=<interval>         Time step for series or live output with 'now'.
                            Examples: 30s, 15m, 2h, 1d. Parquet needs finite input.
  --at=<HH:MM:SS>           One sample per local date at this clock time.
                            Gaps advance; overlaps use the earlier occurrence.
                            Cannot be combined with --step.
  --sun-above=<degrees>     Predicate: elevation angle is above the threshold.
  --sun-below=<degrees>     Predicate: elevation angle is below the threshold.

{shared}

Examples:
  sunce 52.0 13.4 2024-06-21T12:00:00 position
  sunce 52.0 13.4 2024-06-21 position --step=10m
  sunce 50:55:0.5 10:15:0.5 2024-06-21T12:00:00 position --algorithm=grena3
"#,
            defaults.environment.elevation,
            defaults.environment.pressure,
            defaults.environment.temperature
        ),
        "events" => format!(
            r#"Usage:
  sunce [OPTIONS] <latitude> <longitude> <dateTime> events
  sunce [OPTIONS] @data.txt events
  sunce [OPTIONS] @coords.txt @times.txt events
  sunce [OPTIONS] @coords.txt <dateTime> events

Calculates sunrise, transit, sunset and (optionally) twilight times.
Outputs one row per event: date, day_state, event, time.
Dates without events have empty event/time fields.

Event options:
  --twilight                Include civil, nautical, and astronomical twilight times.
  --horizon=<degrees>       Custom horizon angle in degrees. Not valid with --twilight.
  --is-daylight             Predicate: the instant is in daylight.
  --is-civil-twilight       Predicate: the instant is in civil twilight.
  --is-nautical-twilight    Predicate: the instant is in nautical twilight.
  --is-astronomical-twilight Predicate: the instant is in astronomical twilight.
  --is-astronomical-night   Predicate: solar elevation is below -18 degrees.
  --after-sunset            Predicate: the instant is between sunset and sunrise.

{shared}

Examples:
  sunce 52.0 13.4 2024-06-21 events
  sunce 52.0 13.4 2024-06 events --twilight
  sunce 52.0 13.4 2024-06-21 events --horizon=-6.0
"#
        ),
        _ => {
            return Err(format!(
                "Unknown command: {}\n\nRun 'sunce --help' for usage.",
                command
            )
            .into());
        }
    })
}

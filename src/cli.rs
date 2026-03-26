//! Command-line parsing and validation.

use crate::data::{
    self, CalculationAlgorithm, Command, InputPath, LocationSource, OutputFormat, Parameters,
    Predicate, Step, TimezoneOverride,
};
use crate::error::CliError;
use crate::parsed::{ParsedCommand, ParsedInput, ParsedOptionUsage, ParsedTimeSource};
use std::path::PathBuf;

const DELTAT_MULTIPLE_ERROR: &str = "Option --deltat cannot be used multiple times";
const PREDICATE_MULTIPLE_ERROR: &str = "Predicate options cannot be used multiple times";

type CliResult<T> = Result<T, CliError>;

fn predicate_error(message: impl Into<String>) -> CliError {
    CliError::MessageWithCode(message.into(), 2)
}

pub fn parse_cli(args: Vec<String>) -> CliResult<ParsedCommand> {
    if args.len() < 2 {
        return Err(CliError::Exit(
            "Usage: sunce [OPTIONS] <lat> <lon> <dateTime> <position|sunrise>".to_string(),
        ));
    }

    let mut params = Parameters::default();
    let mut predicate = None;
    let mut positional = Vec::new();
    let mut deltat_seen = false;
    let mut option_usage = ParsedOptionUsage::default();

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
                &mut predicate,
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

    let (command, input) = parse_positional_args(&positional, &params)?;
    Ok(ParsedCommand {
        command,
        input,
        params,
        predicate,
        usage: option_usage,
    })
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

fn ensure_flag(opt: &str, value: Option<&str>) -> CliResult<()> {
    if value.is_some() {
        Err(format!("Option --{} does not take a value", opt).into())
    } else {
        Ok(())
    }
}

fn set_predicate(predicate: Predicate, parsed: &mut Option<Predicate>) -> CliResult<()> {
    if parsed.is_some() {
        return Err(predicate_error(PREDICATE_MULTIPLE_ERROR));
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
        "sun-above" => Some(Predicate::SunAbove(
            parse_f64(
                "sun-above",
                value.ok_or_else(|| predicate_error("Option --sun-above requires a value"))?,
            )
            .map_err(|err| predicate_error(err.to_string()))?,
        )),
        "sun-below" => Some(Predicate::SunBelow(
            parse_f64(
                "sun-below",
                value.ok_or_else(|| predicate_error("Option --sun-below requires a value"))?,
            )
            .map_err(|err| predicate_error(err.to_string()))?,
        )),
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
            option_usage.show_inputs = true;
        }
        "no-show-inputs" => {
            ensure_flag("no-show-inputs", value)?;
            params.output.show_inputs = Some(false);
            option_usage.show_inputs = true;
        }
        "perf" => {
            ensure_flag("perf", value)?;
            params.perf = true;
            option_usage.perf = true;
        }
        "wait" => {
            ensure_flag("wait", value)?;
            params.wait = true;
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

fn parse_positional_args(
    positional_args: &[String],
    params: &Parameters,
) -> CliResult<(Command, ParsedInput)> {
    let command_index = positional_args
        .iter()
        .position(|arg| arg == "position" || arg == "sunrise")
        .ok_or("No command found".to_string())?;
    if command_index == 0 {
        return Err("Need at least command and one argument".into());
    }

    let command = match positional_args[command_index].as_str() {
        "position" => Command::Position,
        "sunrise" => Command::Sunrise,
        _ => unreachable!("filtered above"),
    };
    Ok((
        command,
        parse_data_source(&positional_args[..command_index], params)?,
    ))
}

fn parse_data_source(args: &[String], params: &Parameters) -> CliResult<ParsedInput> {
    match args.len() {
        1 => {
            if args[0].starts_with('@') {
                parse_file_arg(&args[0]).map(ParsedInput::Paired)
            } else {
                Err("Single argument must be a file (@file or @-)".into())
            }
        }
        2 => {
            if !args[0].starts_with('@') {
                Err("Two arguments: Use @coords.txt @times.txt, @coords.txt datetime, or three arguments (lat lon datetime)".into())
            } else {
                Ok(ParsedInput::Separate(
                    LocationSource::File(parse_file_arg(&args[0])?),
                    if args[1].starts_with('@') {
                        ParsedTimeSource::File(parse_file_arg(&args[1])?)
                    } else {
                        parse_time_arg(&args[1], params)?
                    },
                ))
            }
        }
        3 => Ok(ParsedInput::Separate(
            parse_location_args(&args[0], &args[1])?,
            parse_time_arg(&args[2], params)?,
        )),
        _ => Err("Too many arguments".into()),
    }
}

fn parse_time_arg(time_str: &str, _params: &Parameters) -> CliResult<ParsedTimeSource> {
    if time_str.starts_with('@') {
        return Ok(ParsedTimeSource::File(parse_file_arg(time_str)?));
    }

    if time_str == "now" {
        return Ok(ParsedTimeSource::Now);
    }

    Ok(ParsedTimeSource::Value(time_str.to_string()))
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
  Predicate mode (automation via exit status):
    Works only with one explicit lat/lon pair and one explicit instant.
    --wait                     With `now`, sleep until near the next matching
                               transition, then poll until true.
    --is-daylight              Exit 0 if the instant is daylight.
    --is-civil-twilight        Exit 0 if the instant is in civil twilight.
    --is-nautical-twilight     Exit 0 if the instant is in nautical twilight.
    --is-astronomical-twilight Exit 0 if the instant is in astronomical twilight.
    --is-astronomical-night    Exit 0 if the instant is outside astronomical twilight.
    --after-sunset             Exit 0 from sunset until the next sunrise.
    --sun-above=<degrees>      Exit 0 if the elevation angle is above the threshold.
    --sun-below=<degrees>      Exit 0 if the elevation angle is below the threshold.
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
  --sun-above=<degrees>     Predicate mode: exit 0 if elevation angle is above
                            the threshold, 1 if not.
  --sun-below=<degrees>     Predicate mode: exit 0 if elevation angle is below
                            the threshold, 1 if not.
  --wait                    Predicate mode: with `now`, sleep until near the
                            next matching transition, then poll until true.

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
  --is-daylight             Predicate mode: exit 0 if the instant is daylight.
  --is-civil-twilight       Predicate mode: exit 0 if the instant is in civil twilight.
  --is-nautical-twilight    Predicate mode: exit 0 if the instant is in nautical twilight.
  --is-astronomical-twilight Predicate mode: exit 0 if the instant is in astronomical twilight.
  --is-astronomical-night   Predicate mode: exit 0 if the instant is outside
                            astronomical twilight.
  --after-sunset            Predicate mode: exit 0 from sunset until the next
                            sunrise.
  --wait                    Predicate mode: with `now`, sleep until near the
                            next matching transition, then poll until true.

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

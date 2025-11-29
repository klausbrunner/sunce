//! Command-line parsing and validation.

use crate::data::{
    self, CalculationAlgorithm, Command, DataSource, InputPath, LocationSource, OutputFormat,
    Parameters, Step, TimeSource, TimezoneOverride,
};
use crate::error::CliError;
use std::collections::HashSet;
use std::path::PathBuf;

const DELTAT_MULTIPLE_ERROR: &str =
    "error: the argument '--deltat <DELTAT>' cannot be used multiple times";

type CliResult<T> = Result<T, CliError>;

#[derive(Copy, Clone)]
enum OptionArg {
    Required,
    Optional,
    None,
}

struct OptionSpec {
    name: &'static str,
    arg: OptionArg,
    description: &'static str,
    commands: Option<&'static [Command]>,
    apply: fn(Option<&str>, &mut Parameters, &mut bool) -> CliResult<()>,
}

static OPTION_SPECS: &[OptionSpec] = &[
    OptionSpec {
        name: "format",
        arg: OptionArg::Required,
        description: "Output format: text, csv, json, parquet",
        commands: None,
        apply: |val, params, _| {
            let v = val.ok_or("Option --format requires a value")?;
            params.output.format = v.parse::<OutputFormat>().map_err(CliError::from)?;
            Ok(())
        },
    },
    OptionSpec {
        name: "deltat",
        arg: OptionArg::Optional,
        description: "Delta T in seconds (empty for auto-estimate)",
        commands: None,
        apply: |val, params, seen| {
            if *seen {
                return Err(DELTAT_MULTIPLE_ERROR.into());
            }
            *seen = true;
            params.deltat = match val {
                Some(v) => Some(
                    v.parse::<f64>()
                        .map_err(|_| CliError::from(format!("Invalid deltat value: {}", v)))?,
                ),
                None => None,
            };
            Ok(())
        },
    },
    OptionSpec {
        name: "timezone",
        arg: OptionArg::Required,
        description: "Override timezone (offset or zone id)",
        commands: None,
        apply: |val, params, _| {
            let v = val.ok_or("Option --timezone requires a value")?;
            params.timezone = Some(v.parse::<TimezoneOverride>()?);
            Ok(())
        },
    },
    OptionSpec {
        name: "algorithm",
        arg: OptionArg::Required,
        description: "Calculation algorithm (spa, grena3)",
        commands: Some(&[Command::Position]),
        apply: |val, params, _| {
            let v = val.ok_or("Option --algorithm requires a value")?;
            params.calculation.algorithm =
                v.parse::<CalculationAlgorithm>().map_err(CliError::from)?;
            Ok(())
        },
    },
    OptionSpec {
        name: "step",
        arg: OptionArg::Required,
        description: "Time step for series (e.g. 30s, 15m, 2h)",
        commands: Some(&[Command::Position]),
        apply: |val, params, _| {
            let v = val.ok_or("Option --step requires a value")?;
            params.step = Some(v.parse::<Step>().map_err(CliError::from)?);
            Ok(())
        },
    },
    OptionSpec {
        name: "elevation",
        arg: OptionArg::Required,
        description: "Elevation above sea level in meters",
        commands: Some(&[Command::Position]),
        apply: |val, params, _| {
            let v = val.ok_or("Option --elevation requires a value")?;
            params.environment.elevation = parse_f64("elevation", v)?;
            Ok(())
        },
    },
    OptionSpec {
        name: "temperature",
        arg: OptionArg::Required,
        description: "Average air temperature in °C (refraction)",
        commands: Some(&[Command::Position]),
        apply: |val, params, _| {
            let v = val.ok_or("Option --temperature requires a value")?;
            params.environment.temperature = parse_f64("temperature", v)?;
            Ok(())
        },
    },
    OptionSpec {
        name: "pressure",
        arg: OptionArg::Required,
        description: "Average air pressure in hPa (refraction)",
        commands: Some(&[Command::Position]),
        apply: |val, params, _| {
            let v = val.ok_or("Option --pressure requires a value")?;
            params.environment.pressure = parse_f64("pressure", v)?;
            Ok(())
        },
    },
    OptionSpec {
        name: "horizon",
        arg: OptionArg::Required,
        description: "Custom horizon angle in degrees",
        commands: Some(&[Command::Sunrise]),
        apply: |val, params, _| {
            let v = val.ok_or("Option --horizon requires a value")?;
            params.calculation.horizon = Some(parse_f64("horizon", v)?);
            Ok(())
        },
    },
    OptionSpec {
        name: "headers",
        arg: OptionArg::None,
        description: "Include headers in CSV output",
        commands: None,
        apply: |_, params, _| {
            params.output.headers = true;
            Ok(())
        },
    },
    OptionSpec {
        name: "no-headers",
        arg: OptionArg::None,
        description: "Omit headers in CSV output",
        commands: None,
        apply: |_, params, _| {
            params.output.headers = false;
            Ok(())
        },
    },
    OptionSpec {
        name: "show-inputs",
        arg: OptionArg::None,
        description: "Include input columns in output",
        commands: None,
        apply: |_, params, _| {
            params.output.show_inputs = Some(true);
            Ok(())
        },
    },
    OptionSpec {
        name: "no-show-inputs",
        arg: OptionArg::None,
        description: "Suppress input columns in output",
        commands: None,
        apply: |_, params, _| {
            params.output.show_inputs = Some(false);
            Ok(())
        },
    },
    OptionSpec {
        name: "perf",
        arg: OptionArg::None,
        description: "Show performance statistics",
        commands: None,
        apply: |_, params, _| {
            params.perf = true;
            Ok(())
        },
    },
    OptionSpec {
        name: "no-refraction",
        arg: OptionArg::None,
        description: "Disable refraction correction",
        commands: Some(&[Command::Position]),
        apply: |_, params, _| {
            params.environment.refraction = false;
            Ok(())
        },
    },
    OptionSpec {
        name: "elevation-angle",
        arg: OptionArg::None,
        description: "Output elevation angle instead of zenith",
        commands: Some(&[Command::Position]),
        apply: |_, params, _| {
            params.output.elevation_angle = true;
            Ok(())
        },
    },
    OptionSpec {
        name: "twilight",
        arg: OptionArg::None,
        description: "Include civil/nautical/astronomical twilight",
        commands: Some(&[Command::Sunrise]),
        apply: |_, params, _| {
            params.calculation.twilight = true;
            Ok(())
        },
    },
    OptionSpec {
        name: "help",
        arg: OptionArg::None,
        description: "Show help message",
        commands: None,
        apply: |_, _, _| Err(get_help_text().into()),
    },
    OptionSpec {
        name: "version",
        arg: OptionArg::None,
        description: "Show version information",
        commands: None,
        apply: |_, _, _| Err(get_version_text().into()),
    },
];

pub fn parse_cli(args: Vec<String>) -> CliResult<(DataSource, Command, Parameters)> {
    if args.len() < 2 {
        return Err("Usage: sunce [options] <lat> <lon> <datetime> <command>".into());
    }

    let mut params = Parameters::default();
    let mut positional_args = Vec::new();
    let mut deltat_seen = false;
    let mut applied_options = HashSet::new();

    for arg in args.iter().skip(1) {
        if let Some(stripped) = arg.strip_prefix("--") {
            let (name, value) = stripped
                .split_once('=')
                .map(|(n, v)| (n, Some(v)))
                .unwrap_or((stripped, None));
            apply_option(name, value, &mut params, &mut deltat_seen)?;
            applied_options.insert(name.to_string());
        } else {
            positional_args.push(arg.clone());
        }
    }

    let (command, data_source) = parse_positional_args(&positional_args, &params)?;

    validate_command_options(command, &params, &applied_options)?;

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
    let Some(handler) = OPTION_SPECS.iter().find(|h| h.name == name) else {
        return Err(format!("Unknown option: --{}", name).into());
    };

    if matches!(handler.arg, OptionArg::None) && value.is_some() {
        return Err(format!("Option --{} does not take a value", name).into());
    }

    if matches!(handler.arg, OptionArg::Required) && value.is_none() {
        return Err(format!("Option --{} requires a value", name).into());
    }

    (handler.apply)(value, params, deltat_seen)
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

fn parse_positional_args(
    positional_args: &[String],
    params: &Parameters,
) -> CliResult<(Command, DataSource)> {
    if positional_args.is_empty() {
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

    if data_args.is_empty() {
        return Err("Need at least command and one argument".into());
    }

    let data_source = parse_data_source(data_args, params)?;

    Ok((command, data_source))
}

fn parse_data_source(args: &[String], params: &Parameters) -> CliResult<DataSource> {
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
                let time_source = parse_time_arg(arg2, params)?;
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
            let time_source = parse_time_arg(time_str, params)?;

            Ok(DataSource::Separate(location_source, time_source))
        }
        _ => Err("Too many arguments".into()),
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

fn option_usage(spec: &OptionSpec) -> String {
    match spec.arg {
        OptionArg::Required => format!("--{}=<value>", spec.name),
        OptionArg::Optional => format!("--{}[=<value>]", spec.name),
        OptionArg::None => format!("--{}", spec.name),
    }
}

fn render_options(command_filter: Option<Command>) -> String {
    let mut lines = Vec::new();
    for spec in OPTION_SPECS.iter() {
        if matches!(spec.name, "help" | "version") && command_filter.is_some() {
            continue;
        }
        if let Some(cmd) = command_filter
            && let Some(commands) = spec.commands
            && !commands.contains(&cmd)
        {
            continue;
        }
        lines.push(format!("  {:<22} {}", option_usage(spec), spec.description));
    }
    lines.join("\n")
}

fn command_name(command: Command) -> &'static str {
    match command {
        Command::Position => "position",
        Command::Sunrise => "sunrise",
    }
}

fn validate_command_options(
    command: Command,
    params: &Parameters,
    applied: &HashSet<String>,
) -> CliResult<()> {
    for name in applied {
        if let Some(spec) = OPTION_SPECS.iter().find(|s| s.name == name.as_str())
            && let Some(commands) = spec.commands
            && !commands.contains(&command)
        {
            return Err(format!(
                "Option --{} not valid for {} command",
                name,
                command_name(command)
            )
            .into());
        }
    }

    if command == Command::Sunrise {
        if params.calculation.algorithm != CalculationAlgorithm::Spa {
            return Err("Option --algorithm not valid for sunrise command".into());
        }
        if !params.environment.refraction {
            return Err("Option --no-refraction not valid for sunrise command".into());
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
        if params.output.elevation_angle {
            return Err("Option --elevation-angle not valid for sunrise command".into());
        }
    }

    Ok(())
}

fn get_help_text() -> String {
    let options = render_options(None);
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
{options}

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
        "position" => {
            let options = render_options(Some(Command::Position));
            format!(
                r#"Usage: sunce position [OPTIONS]

Calculates topocentric solar coordinates.

Options:
{options}

Examples:
  sunce 52.0 13.4 2024-06-21T12:00:00 position
  sunce 52.0 13.4 2024 position --step=1d
  sunce 50:55:0.5 10:15:0.5 2024-06-21T12:00:00 position --algorithm=grena3
"#
            )
        }
        "sunrise" => {
            let options = render_options(Some(Command::Sunrise));
            format!(
                r#"Usage: sunce sunrise [OPTIONS]

Calculates sunrise, transit, sunset and (optionally) twilight times.

Options:
{options}

Examples:
  sunce 52.0 13.4 2024-06-21 sunrise
  sunce 52.0 13.4 2024-06 sunrise --twilight
  sunce 52.0 13.4 2024-06-21 sunrise --horizon=-6.0
"#
            )
        }
        _ => format!(
            "Unknown command: {}\n\nRun 'sunce --help' for usage.",
            command
        ),
    }
}

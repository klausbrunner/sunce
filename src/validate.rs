//! Semantic validation that turns parsed CLI input into executable commands.

use crate::data::{self, Command, DataSource, LocationSource, Parameters, Predicate, TimeSource};
use crate::error::CliError;
use crate::parsed::{ParsedCommand, ParsedInput, ParsedOptionUsage, ParsedTimeSource};
use crate::predicate::{PredicateCheck, PredicateJob, PredicateTime};

#[derive(Debug)]
pub struct StreamRequest {
    pub command: Command,
    pub source: DataSource,
    pub params: Parameters,
}

#[derive(Debug)]
pub enum ValidCommand {
    Stream(StreamRequest),
    Predicate(PredicateJob),
}

#[derive(Debug, Clone, Copy)]
enum ValidationMode {
    Position,
    Events,
    Predicate,
}

pub fn validate(parsed: ParsedCommand) -> Result<ValidCommand, CliError> {
    let ParsedCommand {
        command,
        input,
        mut params,
        predicate,
        usage,
    } = parsed;
    let mode = match command {
        Command::Position => {
            validate_position_options(&params)?;
            ValidationMode::Position
        }
        Command::Events => {
            validate_events_options(&params, &usage)?;
            ValidationMode::Events
        }
    };
    let mode = if predicate.is_some() {
        ValidationMode::Predicate
    } else {
        mode
    };
    let source = validate_input(input, &params, mode)?;

    if let Some(predicate) = predicate {
        let (lat, lon, time) = validate_predicate_source(source, &params, &usage)?;
        validate_predicate_options(command, predicate, &params)?;
        return Ok(ValidCommand::Predicate(PredicateJob {
            lat,
            lon,
            time,
            check: PredicateCheck::from_cli(predicate),
            params,
        }));
    }

    if params.wait {
        return Err("Option --wait requires a predicate option".into());
    }
    #[cfg(feature = "parquet")]
    if matches!(params.output.format, data::OutputFormat::Parquet)
        && source.is_watch_mode(params.step.is_some())
    {
        return Err(
            "Parquet output requires finite input; use text, CSV or JSON with 'now --step'".into(),
        );
    }
    if params.output.show_inputs.is_none() {
        params.output.show_inputs = Some(should_auto_show_inputs(&source));
    }
    Ok(ValidCommand::Stream(StreamRequest {
        command,
        source,
        params,
    }))
}

fn validate_input(
    input: ParsedInput,
    params: &Parameters,
    mode: ValidationMode,
) -> Result<DataSource, CliError> {
    if params.at.is_some() {
        if params.step.is_some() {
            return Err("Option --at cannot be combined with --step".into());
        }
        if !matches!(&input, ParsedInput::Separate(_, ParsedTimeSource::Value(_))) {
            return Err("Option --at requires date-only input or a date range".into());
        }
    }
    if params.step.is_some()
        && matches!(
            &input,
            ParsedInput::Paired(_) | ParsedInput::Separate(_, ParsedTimeSource::File(_))
        )
    {
        return Err("Option --step is not valid with file input".into());
    }

    match input {
        ParsedInput::Paired(path) => Ok(DataSource::Paired(path)),
        ParsedInput::Separate(loc, time) => Ok(DataSource::Separate(
            loc,
            resolve_time_source(time, params, mode)?,
        )),
    }
}

fn resolve_time_source(
    time: ParsedTimeSource,
    params: &Parameters,
    mode: ValidationMode,
) -> Result<TimeSource, CliError> {
    match time {
        ParsedTimeSource::File(path) => Ok(TimeSource::File(path)),
        ParsedTimeSource::Now => Ok(TimeSource::Now),
        ParsedTimeSource::Value(value) => {
            let is_date_only = crate::data::time_utils::is_date_without_time(&value);
            let is_range = value.contains('/') || crate::data::time_utils::is_partial_date(&value);
            if matches!(mode, ValidationMode::Predicate) && (is_range || is_date_only) {
                return Err("Predicate mode requires a single explicit instant".into());
            }

            if is_range || (is_date_only && matches!(mode, ValidationMode::Position)) {
                if let Some(clock) = params.at {
                    return Ok(TimeSource::Daily(value, clock));
                }
                return Ok(TimeSource::Range(value));
            }

            if params.at.is_some() {
                return Err("Option --at requires date-only input or a date range".into());
            }
            if params.step.is_some() {
                return Err(
                    "Option --step requires date-only input (YYYY, YYYY-MM, or YYYY-MM-DD) or 'now'"
                        .into(),
                );
            }

            data::time_utils::parse_input_time(
                &value,
                params.timezone.as_ref().map(|tz| tz.as_str()),
            )
            .map(TimeSource::Single)
            .map_err(CliError::from)
        }
    }
}

fn validate_predicate_source(
    source: DataSource,
    params: &Parameters,
    usage: &ParsedOptionUsage,
) -> Result<(f64, f64, PredicateTime), CliError> {
    if let Some(name) = [
        (usage.format, "--format"),
        (usage.headers, "--headers/--no-headers"),
        (
            params.output.show_inputs.is_some(),
            "--show-inputs/--no-show-inputs",
        ),
        (params.perf, "--perf"),
        (params.step.is_some(), "--step"),
        (params.at.is_some(), "--at"),
    ]
    .into_iter()
    .find_map(|(used, name)| used.then_some(name))
    {
        return Err(format!("Option {name} not valid in predicate mode").into());
    }

    if params.wait && !matches!(source, DataSource::Separate(_, TimeSource::Now)) {
        return Err("Option --wait requires 'now' in predicate mode".into());
    }

    match source {
        DataSource::Paired(_) => Err(
            "Predicate mode requires explicit latitude, longitude, and datetime arguments".into(),
        ),
        DataSource::Separate(LocationSource::File(_), _) => {
            Err("Predicate mode does not support coordinate file input".into())
        }
        DataSource::Separate(_, TimeSource::File(_)) => {
            Err("Predicate mode does not support datetime file input".into())
        }
        DataSource::Separate(LocationSource::Range { .. }, _) => {
            Err("Predicate mode requires a single latitude/longitude pair".into())
        }
        DataSource::Separate(_, TimeSource::Range(_) | TimeSource::Daily(_, _)) => {
            Err("Predicate mode requires a single explicit instant".into())
        }
        DataSource::Separate(LocationSource::Single(lat, lon), TimeSource::Single(input)) => {
            Ok((lat, lon, PredicateTime::Fixed(input.datetime)))
        }
        DataSource::Separate(LocationSource::Single(lat, lon), TimeSource::Now) => {
            Ok((lat, lon, PredicateTime::Now))
        }
    }
}

fn validate_predicate_options(
    command: Command,
    predicate: Predicate,
    params: &Parameters,
) -> Result<(), CliError> {
    match predicate {
        Predicate::SunAbove(threshold) | Predicate::SunBelow(threshold) => {
            if command != Command::Position {
                return Err("Sun angle predicates require the position command".into());
            }
            if params.output.elevation_angle {
                return Err("Option --elevation-angle not valid in predicate mode".into());
            }
            if !(-90.0..=90.0).contains(&threshold) {
                return Err("Elevation threshold must be between -90 and 90 degrees".into());
            }
            Ok(())
        }
        Predicate::IsDaylight
        | Predicate::IsCivilTwilight
        | Predicate::IsNauticalTwilight
        | Predicate::IsAstronomicalTwilight
        | Predicate::IsAstronomicalNight
        | Predicate::AfterSunset => {
            if command != Command::Events {
                return Err("Event predicates require the events command".into());
            }
            if params.calculation.twilight {
                return Err("Option --twilight not valid in predicate mode".into());
            }
            if params.calculation.horizon.is_some() {
                return Err("Option --horizon not valid in predicate mode".into());
            }
            Ok(())
        }
    }
}

fn validate_position_options(params: &Parameters) -> Result<(), CliError> {
    validate_command_options(
        &[
            (params.calculation.horizon.is_some(), "--horizon"),
            (params.calculation.twilight, "--twilight"),
        ],
        "position",
    )
}

fn validate_events_options(params: &Parameters, usage: &ParsedOptionUsage) -> Result<(), CliError> {
    if params.calculation.horizon.is_some() && params.calculation.twilight {
        return Err("Option --horizon cannot be used with --twilight".into());
    }
    validate_command_options(
        &[
            (params.step.is_some(), "--step"),
            (params.at.is_some(), "--at"),
            (!params.environment.refraction, "--no-refraction"),
            (params.output.elevation_angle, "--elevation-angle"),
            (usage.elevation, "--elevation"),
            (usage.temperature, "--temperature"),
            (usage.pressure, "--pressure"),
        ],
        "events",
    )
}

fn validate_command_options(
    disallowed: &[(bool, &'static str)],
    command_name: &'static str,
) -> Result<(), CliError> {
    if let Some(name) = disallowed
        .iter()
        .find_map(|(used, name)| used.then_some(*name))
    {
        Err(format!("Option {} not valid for {} command", name, command_name).into())
    } else {
        Ok(())
    }
}

fn should_auto_show_inputs(source: &DataSource) -> bool {
    match source {
        DataSource::Separate(loc, time) => {
            matches!(loc, LocationSource::Range { .. } | LocationSource::File(_))
                || matches!(
                    time,
                    TimeSource::Range(_) | TimeSource::Daily(_, _) | TimeSource::File(_)
                )
        }
        DataSource::Paired(_) => true,
    }
}

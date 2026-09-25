//! Semantic validation that turns parsed CLI input into executable commands.

use crate::data::{self, Command, DataSource, LocationSource, Parameters, Predicate, TimeSource};
use crate::error::{CliError, predicate_error};
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
    Sunrise,
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
            validate_position_options(&usage)?;
            ValidationMode::Position
        }
        Command::Sunrise => {
            validate_sunrise_options(&usage)?;
            ValidationMode::Sunrise
        }
    };
    let mode = if predicate.is_some() {
        ValidationMode::Predicate
    } else {
        mode
    };
    let source = validate_input(input, &params, mode)?;

    if let Some(predicate) = predicate {
        match command {
            Command::Position => {
                validate_position_predicate_mode(&source, predicate, &params, &usage)?
            }
            Command::Sunrise => {
                validate_sunrise_predicate_mode(&source, predicate, &params, &usage)?
            }
        }
        return Ok(ValidCommand::Predicate(build_predicate_job(
            source, params, predicate,
        )));
    }

    if params.wait {
        return Err(predicate_error("Option --wait requires a predicate option"));
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
            if matches!(mode, ValidationMode::Predicate)
                && (crate::data::time_utils::is_partial_date(&value) || is_date_only)
            {
                return Err(predicate_error(
                    "Predicate mode requires a single explicit instant",
                ));
            }

            if crate::data::time_utils::is_partial_date(&value)
                || (is_date_only
                    && (matches!(mode, ValidationMode::Position) || params.step.is_some()))
            {
                return Ok(TimeSource::Range(value));
            }

            if params.step.is_some() {
                return Err(
                    "Option --step requires date-only input (YYYY, YYYY-MM, or YYYY-MM-DD) or 'now'"
                        .into(),
                );
            }

            data::parse_datetime_string(&value, params.timezone.as_ref().map(|tz| tz.as_str()))
                .map(TimeSource::Single)
                .map_err(|err| {
                    if matches!(mode, ValidationMode::Predicate) {
                        predicate_error(err)
                    } else {
                        CliError::from(err)
                    }
                })
        }
    }
}

fn validate_predicate_common(
    source: &DataSource,
    params: &Parameters,
    usage: &ParsedOptionUsage,
) -> Result<(), CliError> {
    if let Some(name) = [
        (usage.format, "--format"),
        (usage.headers, "--headers/--no-headers"),
        (usage.show_inputs, "--show-inputs/--no-show-inputs"),
        (usage.perf, "--perf"),
    ]
    .into_iter()
    .find_map(|(used, name)| used.then_some(name))
    {
        return Err(predicate_error(format!(
            "Option {} not valid in predicate mode",
            name
        )));
    }

    if usage.step {
        return Err(predicate_error("Option --step not valid in predicate mode"));
    }
    if params.wait && !matches!(source, DataSource::Separate(_, TimeSource::Now)) {
        return Err(predicate_error(
            "Option --wait requires 'now' in predicate mode",
        ));
    }

    match source {
        DataSource::Paired(_) => Err(predicate_error(
            "Predicate mode requires explicit latitude, longitude, and datetime arguments",
        )),
        DataSource::Separate(LocationSource::File(_), _) => Err(predicate_error(
            "Predicate mode does not support coordinate file input",
        )),
        DataSource::Separate(_, TimeSource::File(_)) => Err(predicate_error(
            "Predicate mode does not support datetime file input",
        )),
        DataSource::Separate(LocationSource::Range { .. }, _) => Err(predicate_error(
            "Predicate mode requires a single latitude/longitude pair",
        )),
        DataSource::Separate(_, TimeSource::Range(_)) => Err(predicate_error(
            "Predicate mode requires a single explicit instant",
        )),
        DataSource::Separate(
            LocationSource::Single(_, _),
            TimeSource::Single(_) | TimeSource::Now,
        ) => Ok(()),
    }
}

fn validate_position_predicate_mode(
    source: &DataSource,
    predicate: Predicate,
    params: &Parameters,
    usage: &ParsedOptionUsage,
) -> Result<(), CliError> {
    validate_predicate_common(source, params, usage)?;
    match predicate {
        Predicate::SunAbove(threshold) | Predicate::SunBelow(threshold) => {
            if usage.elevation_angle {
                return Err(predicate_error(
                    "Option --elevation-angle not valid in predicate mode",
                ));
            }
            if !(-90.0..=90.0).contains(&threshold) {
                return Err(predicate_error(
                    "Elevation threshold must be between -90 and 90 degrees",
                ));
            }
            Ok(())
        }
        _ => Err(predicate_error(
            "Sunrise predicates require the sunrise command",
        )),
    }
}

fn validate_sunrise_predicate_mode(
    source: &DataSource,
    predicate: Predicate,
    params: &Parameters,
    usage: &ParsedOptionUsage,
) -> Result<(), CliError> {
    validate_predicate_common(source, params, usage)?;
    match predicate {
        Predicate::IsDaylight
        | Predicate::IsCivilTwilight
        | Predicate::IsNauticalTwilight
        | Predicate::IsAstronomicalTwilight
        | Predicate::IsAstronomicalNight
        | Predicate::AfterSunset => {
            if usage.twilight {
                return Err(predicate_error(
                    "Option --twilight not valid in predicate mode",
                ));
            }
            if usage.horizon {
                return Err(predicate_error(
                    "Option --horizon not valid in predicate mode",
                ));
            }
            Ok(())
        }
        Predicate::SunAbove(_) | Predicate::SunBelow(_) => Err(predicate_error(
            "Sun angle predicates require the position command",
        )),
    }
}

fn validate_position_options(usage: &ParsedOptionUsage) -> Result<(), CliError> {
    validate_command_options(
        &[(usage.horizon, "--horizon"), (usage.twilight, "--twilight")],
        "position",
    )
}

fn validate_sunrise_options(usage: &ParsedOptionUsage) -> Result<(), CliError> {
    if usage.horizon && usage.twilight {
        return Err("Option --horizon cannot be used with --twilight".into());
    }
    validate_command_options(
        &[
            (usage.step, "--step"),
            (usage.no_refraction, "--no-refraction"),
            (usage.elevation_angle, "--elevation-angle"),
            (usage.elevation, "--elevation"),
            (usage.temperature, "--temperature"),
            (usage.pressure, "--pressure"),
            (usage.algorithm, "--algorithm"),
        ],
        "sunrise",
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
                || matches!(time, TimeSource::Range(_) | TimeSource::File(_))
        }
        DataSource::Paired(_) => true,
    }
}

fn build_predicate_job(
    source: DataSource,
    params: Parameters,
    predicate: Predicate,
) -> PredicateJob {
    let (lat, lon, time) = match source {
        DataSource::Separate(
            LocationSource::Single(lat, lon),
            time @ (TimeSource::Single(_) | TimeSource::Now),
        ) => (lat, lon, time),
        _ => unreachable!("validated predicate source"),
    };

    PredicateJob {
        lat,
        lon,
        time: match time {
            TimeSource::Single(dt) => PredicateTime::Fixed(dt),
            TimeSource::Now => PredicateTime::Now,
            TimeSource::Range(_) | TimeSource::File(_) => unreachable!("validated above"),
        },
        check: PredicateCheck::from_cli(predicate),
        wait: params.wait,
        params,
    }
}

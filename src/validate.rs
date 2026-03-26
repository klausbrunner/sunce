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
    Sunrise,
    Predicate,
}

pub fn validate(parsed: ParsedCommand) -> Result<ValidCommand, CliError> {
    match parsed.command {
        Command::Position => {
            validate_position(parsed.input, parsed.params, parsed.predicate, parsed.usage)
        }
        Command::Sunrise => {
            validate_sunrise(parsed.input, parsed.params, parsed.predicate, parsed.usage)
        }
    }
}

fn predicate_error(message: impl Into<String>) -> CliError {
    CliError::MessageWithCode(message.into(), 2)
}

fn validate_input(
    input: ParsedInput,
    params: &Parameters,
    mode: ValidationMode,
) -> Result<DataSource, CliError> {
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

fn validate_position(
    input: ParsedInput,
    mut params: Parameters,
    predicate: Option<Predicate>,
    usage: ParsedOptionUsage,
) -> Result<ValidCommand, CliError> {
    validate_position_options(&usage)?;
    let source = validate_input(
        input,
        &params,
        if predicate.is_some() {
            ValidationMode::Predicate
        } else {
            ValidationMode::Position
        },
    )?;

    if let Some(predicate) = predicate {
        validate_position_predicate_mode(&source, predicate, &params, &usage)?;
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
        command: Command::Position,
        source,
        params,
    }))
}

fn validate_sunrise(
    input: ParsedInput,
    mut params: Parameters,
    predicate: Option<Predicate>,
    usage: ParsedOptionUsage,
) -> Result<ValidCommand, CliError> {
    validate_sunrise_options(&usage)?;
    let source = validate_input(
        input,
        &params,
        if predicate.is_some() {
            ValidationMode::Predicate
        } else {
            ValidationMode::Sunrise
        },
    )?;

    if let Some(predicate) = predicate {
        validate_sunrise_predicate_mode(&source, predicate, &params, &usage)?;
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
        command: Command::Sunrise,
        source,
        params,
    }))
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
            if !threshold.is_finite() {
                return Err(predicate_error(
                    "Elevation threshold must be a finite number",
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
        usage,
        &[(usage.horizon, "--horizon"), (usage.twilight, "--twilight")],
        "position",
    )
}

fn validate_sunrise_options(usage: &ParsedOptionUsage) -> Result<(), CliError> {
    validate_command_options(
        usage,
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
    usage: &ParsedOptionUsage,
    disallowed: &[(bool, &'static str)],
    command_name: &'static str,
) -> Result<(), CliError> {
    fn first_used(options: &[(bool, &'static str)]) -> Option<&'static str> {
        options
            .iter()
            .find_map(|(used, name)| used.then_some(*name))
    }

    let _ = usage;
    first_used(disallowed)
        .map(|name| Err(format!("Option {} not valid for {} command", name, command_name).into()))
        .unwrap_or(Ok(()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{CalculationAlgorithm, LocationSource, OutputFormat};
    use crate::parsed::{ParsedCommand, ParsedInput, ParsedOptionUsage, ParsedTimeSource};

    fn parsed_position(input: ParsedInput) -> ParsedCommand {
        ParsedCommand {
            command: Command::Position,
            input,
            params: Parameters::default(),
            predicate: None,
            usage: ParsedOptionUsage::default(),
        }
    }

    #[test]
    fn validates_date_only_position_input_as_range() {
        let valid = validate(parsed_position(ParsedInput::Separate(
            LocationSource::Single(52.0, 13.4),
            ParsedTimeSource::Value("2024-01-01".to_string()),
        )))
        .unwrap();

        match valid {
            ValidCommand::Stream(request) => {
                assert_eq!(request.command, Command::Position);
                assert!(matches!(
                    request.source,
                    DataSource::Separate(_, TimeSource::Range(ref s)) if s == "2024-01-01"
                ));
                assert_eq!(
                    request.params.calculation.algorithm,
                    CalculationAlgorithm::Spa
                );
            }
            _ => panic!("expected position request"),
        }
    }

    #[test]
    fn rejects_predicate_date_only_input_with_code_2() {
        let err = validate(ParsedCommand {
            command: Command::Sunrise,
            input: ParsedInput::Separate(
                LocationSource::Single(52.0, 13.4),
                ParsedTimeSource::Value("2024-01-01".to_string()),
            ),
            params: Parameters::default(),
            predicate: Some(Predicate::IsDaylight),
            usage: ParsedOptionUsage::default(),
        })
        .unwrap_err();

        match err {
            CliError::MessageWithCode(message, 2) => {
                assert!(message.contains("single explicit instant"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn auto_enables_show_inputs_for_ranges() {
        let valid = validate(parsed_position(ParsedInput::Separate(
            LocationSource::Range {
                lat: (52.0, 53.0, 1.0),
                lon: (13.0, 14.0, 1.0),
            },
            ParsedTimeSource::Value("2024-01-01".to_string()),
        )))
        .unwrap();

        match valid {
            ValidCommand::Stream(request) => {
                assert_eq!(request.command, Command::Position);
                assert_eq!(request.params.output.format, OutputFormat::Text);
                assert_eq!(request.params.output.show_inputs, Some(true));
            }
            _ => panic!("expected position request"),
        }
    }
}

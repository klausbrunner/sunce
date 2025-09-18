use crate::coordinate_parser::parse_coordinate;
use crate::datetime_parser::parse_datetime;
use crate::types::{GlobalOptions, InputType, ParsedInput, PositionOptions, SunriseOptions};
use clap::ArgMatches;

pub fn parse_input(matches: &ArgMatches) -> Result<ParsedInput, String> {
    let latitude = matches
        .get_one::<String>("latitude")
        .ok_or("Latitude is required")?;
    let longitude = matches.get_one::<String>("longitude");
    let datetime = matches.get_one::<String>("dateTime");

    let input_type = determine_input_type(latitude, longitude, datetime)?;

    let global_options = parse_global_options(matches);

    // For coordinate files, the datetime is in the longitude position
    let (parsed_longitude, parsed_datetime) = match input_type {
        InputType::CoordinateFile | InputType::StdinCoords => (None, longitude.cloned()),
        _ => (longitude.cloned(), datetime.cloned()),
    };

    Ok(ParsedInput {
        input_type,
        latitude: latitude.clone(),
        longitude: parsed_longitude,
        datetime: parsed_datetime,
        global_options,
        parsed_latitude: None,
        parsed_longitude: None,
        parsed_datetime: None,
    })
}

fn determine_input_type(
    latitude: &str,
    longitude: Option<&String>,
    datetime: Option<&String>,
) -> Result<InputType, String> {
    let arg_count = count_arguments(longitude.is_some(), datetime.is_some());
    let file_locations = identify_file_inputs(latitude, longitude, datetime)?;

    match (arg_count, file_locations) {
        (1, FileInputLocation::FirstArg) => determine_single_file_type(latitude),
        (2, FileInputLocation::FirstArg) => determine_coordinate_file_type(latitude),
        (3, FileInputLocation::ThirdArg) => {
            determine_time_file_type(latitude, longitude.unwrap(), datetime.unwrap())
        }
        (3, FileInputLocation::None) => Ok(InputType::Standard),
        _ => Err(format!(
            "Invalid argument combination: {} args with file inputs at {:?}",
            arg_count, file_locations
        )),
    }
}

fn count_arguments(has_longitude: bool, has_datetime: bool) -> u8 {
    1 + has_longitude as u8 + has_datetime as u8
}

#[derive(Debug, Clone, Copy)]
enum FileInputLocation {
    None,
    FirstArg,
    ThirdArg,
}

fn identify_file_inputs(
    latitude: &str,
    longitude: Option<&String>,
    datetime: Option<&String>,
) -> Result<FileInputLocation, String> {
    let file_markers = [
        latitude.starts_with('@'),
        longitude.is_some_and(|s| s.starts_with('@')),
        datetime.is_some_and(|s| s.starts_with('@')),
    ];

    match file_markers.iter().filter(|&&x| x).count() {
        0 => Ok(FileInputLocation::None),
        1 => {
            if file_markers[0] {
                Ok(FileInputLocation::FirstArg)
            } else if file_markers[2] {
                Ok(FileInputLocation::ThirdArg)
            } else {
                Err("File input in longitude position not supported".to_string())
            }
        }
        _ => Err("Only one parameter can use file input (@file or @-)".to_string()),
    }
}

fn determine_single_file_type(latitude: &str) -> Result<InputType, String> {
    match latitude {
        "@-" => Ok(InputType::StdinPaired),
        _ => Ok(InputType::PairedDataFile),
    }
}

fn determine_coordinate_file_type(latitude: &str) -> Result<InputType, String> {
    match latitude {
        "@-" => Ok(InputType::StdinCoords),
        _ => Ok(InputType::CoordinateFile),
    }
}

fn determine_time_file_type(
    latitude: &str,
    longitude: &str,
    datetime: &str,
) -> Result<InputType, String> {
    if latitude.starts_with('@') || longitude.starts_with('@') {
        return Err("Only datetime parameter can be a file in this combination".to_string());
    }
    match datetime {
        "@-" => Ok(InputType::StdinTimes),
        _ => Ok(InputType::TimeFile),
    }
}

fn parse_global_options(matches: &ArgMatches) -> GlobalOptions {
    GlobalOptions {
        deltat: if matches.contains_id("deltat") {
            // Flag was provided, check if it has a value
            matches
                .get_one::<String>("deltat")
                .cloned()
                .or(Some("".to_string()))
        } else {
            // Flag was not provided at all
            None
        },
        format: matches.get_one::<String>("format").cloned(),
        headers: if matches.get_flag("headers") {
            Some(true)
        } else if matches.get_flag("no-headers") {
            Some(false)
        } else {
            None
        },
        show_inputs: if matches.get_flag("show-inputs") {
            Some(true)
        } else if matches.get_flag("no-show-inputs") {
            Some(false)
        } else {
            None
        },
        timezone: matches.get_one::<String>("timezone").cloned(),
    }
}

pub fn parse_position_options(matches: &ArgMatches) -> PositionOptions {
    PositionOptions {
        algorithm: matches.get_one::<String>("algorithm").cloned(),
        elevation: matches.get_one::<String>("elevation").cloned(),
        pressure: matches.get_one::<String>("pressure").cloned(),
        temperature: matches.get_one::<String>("temperature").cloned(),
        elevation_angle: matches.get_flag("elevation-angle"),
        refraction: if matches.get_flag("refraction") {
            Some(true)
        } else if matches.get_flag("no-refraction") {
            Some(false)
        } else {
            None
        },
    }
}

pub fn parse_sunrise_options(matches: &ArgMatches) -> SunriseOptions {
    SunriseOptions {
        twilight: matches.get_flag("twilight"),
    }
}

pub fn parse_data_values(input: &mut ParsedInput) -> Result<(), crate::types::ParseError> {
    match &input.input_type {
        InputType::Standard => {
            // Parse all three parameters
            input.parsed_latitude = Some(parse_coordinate(&input.latitude, "latitude")?);
            if let Some(ref lon) = input.longitude {
                input.parsed_longitude = Some(parse_coordinate(lon, "longitude")?);
            }
            if let Some(ref dt) = input.datetime {
                input.parsed_datetime = Some(parse_datetime(
                    dt,
                    input.global_options.timezone.as_deref(),
                )?);
            }
        }
        InputType::TimeFile | InputType::StdinTimes => {
            // Parse lat/lon, but not the time file (@times.txt)
            input.parsed_latitude = Some(parse_coordinate(&input.latitude, "latitude")?);
            if let Some(ref lon) = input.longitude {
                input.parsed_longitude = Some(parse_coordinate(lon, "longitude")?);
            }
            // datetime is a file reference (@times.txt) - don't parse it here
        }
        InputType::CoordinateFile | InputType::StdinCoords => {
            // Don't parse the coordinate file (@coords.txt), but parse the datetime
            if let Some(ref dt) = input.datetime {
                input.parsed_datetime = Some(parse_datetime(
                    dt,
                    input.global_options.timezone.as_deref(),
                )?);
            }
        }
        InputType::PairedDataFile | InputType::StdinPaired => {
            // Everything comes from the file - don't parse anything here
        }
    }

    // Apply auto show-inputs logic
    apply_show_inputs_auto_logic(input);

    Ok(())
}

pub fn apply_show_inputs_auto_logic(input: &mut ParsedInput) {
    if input.global_options.show_inputs.is_some() {
        return; // User explicitly set, don't override
    }

    let should_auto_enable = match (
        &input.parsed_latitude,
        &input.parsed_longitude,
        &input.parsed_datetime,
    ) {
        // Coordinate ranges auto-enable show-inputs
        (Some(crate::types::Coordinate::Range { .. }), _, _)
        | (_, Some(crate::types::Coordinate::Range { .. }), _) => true,

        // Partial dates (time series) auto-enable show-inputs
        (_, _, Some(crate::types::DateTimeInput::PartialYear(_)))
        | (_, _, Some(crate::types::DateTimeInput::PartialYearMonth(_, _)))
        | (_, _, Some(crate::types::DateTimeInput::PartialDate(_, _, _))) => true,

        // File inputs auto-enable show-inputs
        _ => matches!(
            input.input_type,
            InputType::CoordinateFile
                | InputType::TimeFile
                | InputType::PairedDataFile
                | InputType::StdinCoords
                | InputType::StdinTimes
                | InputType::StdinPaired
        ),
    };

    if should_auto_enable {
        input.global_options.show_inputs = Some(true);
    }
}

use crate::types::{Coordinate, ParseError};

pub fn parse_coordinate(input: &str, coord_type: &str) -> Result<Coordinate, ParseError> {
    if input.contains(':') {
        parse_coordinate_range(input, coord_type)
    } else {
        parse_single_coordinate(input, coord_type)
    }
}

fn parse_single_coordinate(input: &str, coord_type: &str) -> Result<Coordinate, ParseError> {
    let value: f64 = input.parse().map_err(|_| {
        ParseError::InvalidCoordinate(format!("Invalid {} format: {}", coord_type, input))
    })?;
    validate_coordinate(value, coord_type)?;
    Ok(Coordinate::Single(value))
}

fn parse_coordinate_range(input: &str, coord_type: &str) -> Result<Coordinate, ParseError> {
    let mut parts = input.split(':');
    let start_str = parts.next().ok_or_else(|| {
        ParseError::InvalidRange(format!("Missing start value in range: {}", input))
    })?;
    let end_str = parts.next().ok_or_else(|| {
        ParseError::InvalidRange(format!("Missing end value in range: {}", input))
    })?;
    let step_str = parts.next().ok_or_else(|| {
        ParseError::InvalidRange(format!("Missing step value in range: {}", input))
    })?;

    if parts.next().is_some() {
        return Err(ParseError::InvalidRange(format!(
            "Too many components in range: {}",
            input
        )));
    }

    let start: f64 = start_str.parse().map_err(|_| {
        ParseError::InvalidRange(format!("Invalid start value in range: {}", start_str))
    })?;
    let end: f64 = end_str.parse().map_err(|_| {
        ParseError::InvalidRange(format!("Invalid end value in range: {}", end_str))
    })?;
    let step: f64 = step_str.parse().map_err(|_| {
        ParseError::InvalidRange(format!("Invalid step value in range: {}", step_str))
    })?;

    if step == 0.0 {
        return Err(ParseError::ZeroStep);
    }

    validate_coordinate(start, coord_type)?;
    validate_coordinate(end, coord_type)?;

    if (step > 0.0 && start > end) || (step < 0.0 && start < end) {
        return Err(ParseError::InvalidRange(format!(
            "Step direction incompatible with range: start={}, end={}, step={}",
            start, end, step
        )));
    }

    Ok(Coordinate::Range { start, end, step })
}

fn validate_coordinate(value: f64, coord_type: &str) -> Result<(), ParseError> {
    let (min, max, name) = match coord_type {
        "latitude" => (-90.0, 90.0, "latitude range -90° to 90°"),
        "longitude" => (-180.0, 180.0, "longitude range -180° to 180°"),
        _ => return Ok(()),
    };

    if value < min || value > max {
        Err(ParseError::CoordinateOutOfBounds(value, name.to_string()))
    } else {
        Ok(())
    }
}

use crate::timezone::{apply_timezone_to_datetime, get_system_timezone};
use crate::types::DateTimeInput;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone};

pub fn parse_datetime(
    input: &str,
    timezone_override: Option<&str>,
) -> Result<DateTimeInput, ParseError> {
    if input == "now" {
        return Ok(DateTimeInput::Now);
    }

    if is_partial_date(input) {
        parse_partial_date(input)
    } else {
        parse_full_datetime(input, timezone_override)
    }
}

fn is_partial_date(input: &str) -> bool {
    !input.contains('T') && !input.contains(' ') && !input.contains('+') && !input.contains('Z')
}

fn parse_partial_date(input: &str) -> Result<DateTimeInput, ParseError> {
    if input.len() == 4 {
        // Year only: "2024"
        let year: i32 = input
            .parse()
            .map_err(|_| ParseError::InvalidDateTime(format!("Invalid year format: {}", input)))?;
        validate_year(year)?;
        return Ok(DateTimeInput::PartialYear(year));
    }

    let mut parts = input.split('-');
    let year_str = parts
        .next()
        .ok_or_else(|| ParseError::InvalidDateTime(format!("Missing year in date: {}", input)))?;
    let month_str = parts
        .next()
        .ok_or_else(|| ParseError::InvalidDateTime(format!("Missing month in date: {}", input)))?;

    let year: i32 = year_str
        .parse()
        .map_err(|_| ParseError::InvalidDateTime(format!("Invalid year: {}", year_str)))?;
    let month: u32 = month_str
        .parse()
        .map_err(|_| ParseError::InvalidDateTime(format!("Invalid month: {}", month_str)))?;

    validate_year(year)?;
    validate_month(month)?;

    if let Some(day_str) = parts.next() {
        // Year-month-day: "2024-01-15"
        let day: u32 = day_str
            .parse()
            .map_err(|_| ParseError::InvalidDateTime(format!("Invalid day: {}", day_str)))?;
        validate_day(year, month, day)?;
        Ok(DateTimeInput::PartialDate(year, month, day))
    } else {
        // Year-month: "2024-01"
        Ok(DateTimeInput::PartialYearMonth(year, month))
    }
}

fn parse_full_datetime(
    input: &str,
    timezone_override: Option<&str>,
) -> Result<DateTimeInput, ParseError> {
    // Fast path: ISO format with timezone
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(input) {
        let dt_fixed = dt.fixed_offset();
        return Ok(DateTimeInput::Single(dt_fixed));
    }

    // Parse as naive datetime and apply timezone
    let naive = if input.contains('T') {
        // ISO format with T separator
        NaiveDateTime::parse_from_str(input, "%Y-%m-%dT%H:%M:%S")
            .or_else(|_| NaiveDateTime::parse_from_str(input, "%Y-%m-%dT%H:%M"))
            .map_err(|_| {
                ParseError::InvalidDateTime(format!("Invalid datetime format: {}", input))
            })?
    } else if input.contains(' ') && input.len() > 10 {
        // Space-separated format with time
        NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M:%S")
            .or_else(|_| NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M"))
            .map_err(|_| {
                ParseError::InvalidDateTime(format!("Invalid datetime format: {}", input))
            })?
    } else {
        // Date only - assume midnight
        let date = NaiveDate::parse_from_str(input, "%Y-%m-%d")
            .map_err(|_| ParseError::InvalidDateTime(format!("Invalid date format: {}", input)))?;
        date.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap())
    };

    let dt_with_tz = if let Some(tz_str) = timezone_override {
        apply_timezone_to_datetime(naive, Some(tz_str))
            .map_err(|e| ParseError::InvalidTimezone(e.to_string()))?
    } else {
        // Use system timezone
        let tz = get_system_timezone();
        let local_dt = tz
            .from_local_datetime(&naive)
            .single()
            .ok_or_else(|| ParseError::InvalidDateTime(format!("Ambiguous datetime: {}", input)))?;
        local_dt.fixed_offset()
    };

    Ok(DateTimeInput::Single(dt_with_tz))
}

fn validate_year(year: i32) -> Result<(), ParseError> {
    if !(1800..=3000).contains(&year) {
        Err(ParseError::InvalidDateTime(format!(
            "Year {} out of valid range (1800-3000)",
            year
        )))
    } else {
        Ok(())
    }
}

fn validate_month(month: u32) -> Result<(), ParseError> {
    if !(1..=12).contains(&month) {
        Err(ParseError::InvalidDateTime(format!(
            "Month {} out of valid range (1-12)",
            month
        )))
    } else {
        Ok(())
    }
}

fn validate_day(year: i32, month: u32, day: u32) -> Result<(), ParseError> {
    if NaiveDate::from_ymd_opt(year, month, day).is_none() {
        Err(ParseError::InvalidDateTime(format!(
            "Invalid date: {}-{:02}-{:02}",
            year, month, day
        )))
    } else {
        Ok(())
    }
}

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

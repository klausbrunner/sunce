use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use clap::ArgMatches;
use std::fmt;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Invalid coordinate: {0}")]
    InvalidCoordinate(String),
    #[error("Invalid range format: {0}")]
    InvalidRange(String),
    #[error("Invalid datetime: {0}")]
    InvalidDateTime(String),
    #[error("Invalid timezone: {0}")]
    InvalidTimezone(String),
    #[error("Coordinate out of bounds: {0} (expected {1})")]
    CoordinateOutOfBounds(f64, String),
    #[error("Step cannot be zero")]
    ZeroStep,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Coordinate {
    Single(f64),
    Range { start: f64, end: f64, step: f64 },
}

#[derive(Debug, Clone, PartialEq)]
pub enum DateTimeInput {
    Single(DateTime<FixedOffset>),
    Now,
    PartialYear(i32),
    PartialYearMonth(i32, u32),
    PartialDate(i32, u32, u32), // year, month, day - generates series for that day
}

#[derive(Debug, Clone)]
pub enum InputType {
    Standard,       // lat, lon, datetime
    CoordinateFile, // @coords.txt as lat, datetime
    TimeFile,       // lat, lon, @times.txt
    PairedDataFile, // @paired.txt as lat (ignores lon, datetime)
    StdinCoords,    // @- as lat, datetime
    StdinTimes,     // lat, lon, @-
    StdinPaired,    // @- as lat (ignores lon, datetime)
}

#[derive(Debug, Clone)]
pub struct ParsedInput {
    pub input_type: InputType,
    pub latitude: String,
    pub longitude: Option<String>,
    pub datetime: Option<String>,
    pub global_options: GlobalOptions,
    // Parsed data
    pub parsed_latitude: Option<Coordinate>,
    pub parsed_longitude: Option<Coordinate>,
    pub parsed_datetime: Option<DateTimeInput>,
}

#[derive(Debug, Clone)]
pub struct GlobalOptions {
    #[allow(dead_code)] // Will be used when delta T calculation is implemented
    pub deltat: Option<String>,
    pub format: Option<String>,
    pub headers: Option<bool>,
    pub parallel: Option<bool>,
    pub show_inputs: Option<bool>,
    pub timezone: Option<String>,
}

#[derive(Debug)]
pub struct PositionOptions {
    pub algorithm: Option<String>,
    pub elevation: Option<String>,
    pub pressure: Option<String>,
    pub temperature: Option<String>,
    pub elevation_angle: bool,
    pub refraction: Option<bool>,
}

#[derive(Debug)]
pub struct SunriseOptions {
    pub twilight: bool,
}

impl fmt::Display for Coordinate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Coordinate::Single(val) => write!(f, "{:.5}°", val),
            Coordinate::Range { start, end, step } => {
                write!(f, "{:.5}° to {:.5}° step {:.5}°", start, end, step)
            }
        }
    }
}

impl fmt::Display for DateTimeInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DateTimeInput::Single(dt) => write!(f, "{}", dt.format("%Y-%m-%dT%H:%M:%S%:z")),
            DateTimeInput::Now => write!(f, "now (current time)"),
            DateTimeInput::PartialYear(year) => write!(f, "year {} (series)", year),
            DateTimeInput::PartialYearMonth(year, month) => {
                write!(f, "{:04}-{:02} (month series)", year, month)
            }
            DateTimeInput::PartialDate(year, month, day) => {
                write!(f, "{:04}-{:02}-{:02} (day series)", year, month, day)
            }
        }
    }
}

pub fn parse_coordinate(input: &str, coord_type: &str) -> Result<Coordinate, ParseError> {
    if input.contains(':') {
        parse_coordinate_range(input, coord_type)
    } else {
        parse_single_coordinate(input, coord_type)
    }
}

fn parse_single_coordinate(input: &str, coord_type: &str) -> Result<Coordinate, ParseError> {
    let value = input
        .parse::<f64>()
        .map_err(|_| ParseError::InvalidCoordinate(input.to_string()))?;

    validate_coordinate(value, coord_type)?;
    Ok(Coordinate::Single(value))
}

fn parse_coordinate_range(input: &str, coord_type: &str) -> Result<Coordinate, ParseError> {
    let parts: Vec<&str> = input.split(':').collect();
    if parts.len() != 3 {
        return Err(ParseError::InvalidRange(format!(
            "Expected format 'start:end:step', got '{}'",
            input
        )));
    }

    let start = parts[0]
        .parse::<f64>()
        .map_err(|_| ParseError::InvalidRange(format!("Invalid start value: {}", parts[0])))?;

    let end = parts[1]
        .parse::<f64>()
        .map_err(|_| ParseError::InvalidRange(format!("Invalid end value: {}", parts[1])))?;

    let step = parts[2]
        .parse::<f64>()
        .map_err(|_| ParseError::InvalidRange(format!("Invalid step value: {}", parts[2])))?;

    if step == 0.0 {
        return Err(ParseError::ZeroStep);
    }

    validate_coordinate(start, coord_type)?;
    validate_coordinate(end, coord_type)?;

    if step > 0.0 && start > end {
        return Err(ParseError::InvalidRange(format!(
            "Start {} > end {} with positive step {}",
            start, end, step
        )));
    }

    if step < 0.0 && start < end {
        return Err(ParseError::InvalidRange(format!(
            "Start {} < end {} with negative step {}",
            start, end, step
        )));
    }

    Ok(Coordinate::Range { start, end, step })
}

fn validate_coordinate(value: f64, coord_type: &str) -> Result<(), ParseError> {
    match coord_type {
        "latitude" => {
            if !(-90.0..=90.0).contains(&value) {
                return Err(ParseError::CoordinateOutOfBounds(
                    value,
                    "-90° to 90°".to_string(),
                ));
            }
        }
        "longitude" => {
            if !(-180.0..=180.0).contains(&value) {
                return Err(ParseError::CoordinateOutOfBounds(
                    value,
                    "-180° to 180°".to_string(),
                ));
            }
        }
        _ => {
            // Generic coordinate validation
            if !value.is_finite() {
                return Err(ParseError::InvalidCoordinate(format!(
                    "Non-finite value: {}",
                    value
                )));
            }
        }
    }
    Ok(())
}

pub fn parse_datetime(
    input: &str,
    timezone_override: Option<&str>,
) -> Result<DateTimeInput, ParseError> {
    if input == "now" {
        return Ok(DateTimeInput::Now);
    }

    // Try partial dates first
    if let Ok(partial) = parse_partial_date(input) {
        return Ok(partial);
    }

    // Try full datetime parsing
    parse_full_datetime(input, timezone_override)
}

fn parse_partial_date(input: &str) -> Result<DateTimeInput, ParseError> {
    // Year only: "2024"
    if input.len() == 4 && input.chars().all(|c| c.is_ascii_digit()) {
        let year = input
            .parse::<i32>()
            .map_err(|_| ParseError::InvalidDateTime(input.to_string()))?;
        validate_year(year)?;
        return Ok(DateTimeInput::PartialYear(year));
    }

    // Year-month: "2024-03"
    if input.len() == 7 && input.chars().nth(4) == Some('-') {
        let parts: Vec<&str> = input.split('-').collect();
        if parts.len() == 2 {
            let year = parts[0]
                .parse::<i32>()
                .map_err(|_| ParseError::InvalidDateTime(input.to_string()))?;
            let month = parts[1]
                .parse::<u32>()
                .map_err(|_| ParseError::InvalidDateTime(input.to_string()))?;

            validate_year(year)?;
            validate_month(month)?;
            return Ok(DateTimeInput::PartialYearMonth(year, month));
        }
    }

    // Year-month-day: "2024-06-21"
    if input.len() == 10 && input.chars().nth(4) == Some('-') && input.chars().nth(7) == Some('-') {
        let parts: Vec<&str> = input.split('-').collect();
        if parts.len() == 3 {
            let year = parts[0]
                .parse::<i32>()
                .map_err(|_| ParseError::InvalidDateTime(input.to_string()))?;
            let month = parts[1]
                .parse::<u32>()
                .map_err(|_| ParseError::InvalidDateTime(input.to_string()))?;
            let day = parts[2]
                .parse::<u32>()
                .map_err(|_| ParseError::InvalidDateTime(input.to_string()))?;

            validate_year(year)?;
            validate_month(month)?;
            validate_day(year, month, day)?;
            return Ok(DateTimeInput::PartialDate(year, month, day));
        }
    }

    Err(ParseError::InvalidDateTime(input.to_string()))
}

fn parse_full_datetime(
    input: &str,
    timezone_override: Option<&str>,
) -> Result<DateTimeInput, ParseError> {
    // Try various datetime formats that solarpos accepts
    let datetime_formats = [
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S%:z",
        "%Y-%m-%dT%H:%M:%SZ",
    ];

    for format in &datetime_formats {
        // Try parsing with timezone
        if let Ok(dt) = DateTime::parse_from_str(input, format) {
            let final_dt = if let Some(tz) = timezone_override {
                crate::timezone::apply_timezone_to_datetime(dt.naive_local(), Some(tz))?
            } else {
                dt
            };
            return Ok(DateTimeInput::Single(final_dt));
        }

        // Try parsing as naive datetime and apply timezone
        if let Ok(naive_dt) = NaiveDateTime::parse_from_str(input, format) {
            let dt = crate::timezone::apply_timezone_to_datetime(naive_dt, timezone_override)?;
            return Ok(DateTimeInput::Single(dt));
        }

        // Try parsing as date only - but for "%Y-%m-%d" format, return PartialDate
        if let Ok(date) = NaiveDate::parse_from_str(input, format) {
            if format == &"%Y-%m-%d" {
                // Date-only input like "2024-01-01" should generate a series for that day
                return Ok(DateTimeInput::PartialDate(
                    date.year(),
                    date.month(),
                    date.day(),
                ));
            } else {
                // For other formats, create a single datetime at midnight
                let naive_dt = date.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
                let dt = crate::timezone::apply_timezone_to_datetime(naive_dt, timezone_override)?;
                return Ok(DateTimeInput::Single(dt));
            }
        }
    }

    Err(ParseError::InvalidDateTime(format!(
        "Could not parse datetime: {}",
        input
    )))
}

fn validate_year(year: i32) -> Result<(), ParseError> {
    if !(1900..=2100).contains(&year) {
        return Err(ParseError::InvalidDateTime(format!(
            "Year {} out of reasonable range (1900-2100)",
            year
        )));
    }
    Ok(())
}

fn validate_month(month: u32) -> Result<(), ParseError> {
    if !(1..=12).contains(&month) {
        return Err(ParseError::InvalidDateTime(format!(
            "Month {} out of range (1-12)",
            month
        )));
    }
    Ok(())
}

fn validate_day(year: i32, month: u32, day: u32) -> Result<(), ParseError> {
    use chrono::NaiveDate;

    if NaiveDate::from_ymd_opt(year, month, day).is_none() {
        return Err(ParseError::InvalidDateTime(format!(
            "Invalid date: {}-{:02}-{:02}",
            year, month, day
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_coordinates() {
        assert_eq!(
            parse_coordinate("52.5", "latitude").unwrap(),
            Coordinate::Single(52.5)
        );
        assert_eq!(
            parse_coordinate("-13.4", "longitude").unwrap(),
            Coordinate::Single(-13.4)
        );
    }

    #[test]
    fn test_coordinate_validation() {
        // Latitude bounds
        assert!(parse_coordinate("91.0", "latitude").is_err());
        assert!(parse_coordinate("-91.0", "latitude").is_err());
        assert!(parse_coordinate("90.0", "latitude").is_ok());
        assert!(parse_coordinate("-90.0", "latitude").is_ok());

        // Longitude bounds
        assert!(parse_coordinate("181.0", "longitude").is_err());
        assert!(parse_coordinate("-181.0", "longitude").is_err());
        assert!(parse_coordinate("180.0", "longitude").is_ok());
        assert!(parse_coordinate("-180.0", "longitude").is_ok());
    }

    #[test]
    fn test_coordinate_ranges() {
        let range = parse_coordinate("52:53:0.1", "latitude").unwrap();
        assert_eq!(
            range,
            Coordinate::Range {
                start: 52.0,
                end: 53.0,
                step: 0.1
            }
        );
    }

    #[test]
    fn test_datetime_parsing() {
        assert!(matches!(
            parse_datetime("now", None).unwrap(),
            DateTimeInput::Now
        ));

        assert!(matches!(
            parse_datetime("2024", None).unwrap(),
            DateTimeInput::PartialYear(2024)
        ));

        assert!(matches!(
            parse_datetime("2024-03", None).unwrap(),
            DateTimeInput::PartialYearMonth(2024, 3)
        ));

        // Full datetime with explicit timezone
        let dt = parse_datetime("2024-01-01T12:00:00+01:00", None).unwrap();
        assert!(matches!(dt, DateTimeInput::Single(_)));

        // Verify timezone parsing works correctly
        if let DateTimeInput::Single(parsed_dt) = dt {
            assert_eq!(parsed_dt.offset().local_minus_utc(), 3600); // +01:00 = 3600 seconds
        }
    }
}

/// Parse command line input into structured ParsedInput
pub fn parse_input(matches: &ArgMatches) -> Result<ParsedInput, String> {
    let latitude = matches
        .get_one::<String>("latitude")
        .ok_or("Latitude is required")?;
    let longitude = matches.get_one::<String>("longitude");
    let datetime = matches.get_one::<String>("dateTime");

    // Determine input type and validate argument combinations
    let input_type = match (
        latitude.as_str(),
        longitude.map(|s| s.as_str()),
        datetime.map(|s| s.as_str()),
    ) {
        // Paired data file: @file as first argument, no other arguments
        (lat, None, None) if lat.starts_with('@') => {
            if lat == "@-" {
                InputType::StdinPaired
            } else {
                InputType::PairedDataFile
            }
        }

        // Coordinate file: @file as first argument, datetime as second argument
        (lat, Some(_dt), None) if lat.starts_with('@') => {
            if lat == "@-" {
                InputType::StdinCoords
            } else {
                InputType::CoordinateFile
            }
        }

        // Standard with time file: lat, lon, @times.txt
        (lat, Some(lon), Some(dt)) if dt.starts_with('@') => {
            if dt == "@-" {
                if lat.starts_with('@') || lon.starts_with('@') {
                    return Err("Only one parameter can use stdin (@-)".to_string());
                }
                InputType::StdinTimes
            } else {
                if lat.starts_with('@') || lon.starts_with('@') {
                    return Err(
                        "Only datetime parameter can be a file in this combination".to_string()
                    );
                }
                InputType::TimeFile
            }
        }

        // Standard: lat, lon, datetime (no @ prefixes)
        (lat, Some(lon), Some(dt)) => {
            if lat.starts_with('@') || lon.starts_with('@') || dt.starts_with('@') {
                return Err("Invalid file parameter combination".to_string());
            }
            InputType::Standard
        }

        // Invalid combinations
        (_lat, Some(_), None) => {
            return Err("When longitude is provided, datetime must also be provided".to_string());
        }
        _ => {
            return Err("Invalid argument combination. Use: <lat> <lon> <datetime> OR @file <datetime> OR @paired-file OR <lat> <lon> @times".to_string());
        }
    };

    // Validate that paired data doesn't have extra arguments
    if matches!(
        input_type,
        InputType::PairedDataFile | InputType::StdinPaired
    ) && (longitude.is_some() || datetime.is_some())
    {
        return Err(
            "When using paired data files, do not specify longitude or datetime parameters"
                .to_string(),
        );
    }

    // Validate that coordinate files have datetime as second parameter
    if matches!(
        input_type,
        InputType::CoordinateFile | InputType::StdinCoords
    ) {
        if longitude.is_none() {
            return Err(
                "When using coordinate files, datetime parameter is required as second argument"
                    .to_string(),
            );
        }
        if datetime.is_some() {
            return Err("When using coordinate files, only two parameters should be provided: @file datetime".to_string());
        }
    }

    // Validate standard and time file inputs
    if matches!(
        input_type,
        InputType::Standard | InputType::TimeFile | InputType::StdinTimes
    ) && (longitude.is_none() || datetime.is_none())
    {
        return Err(
            "Standard input requires latitude, longitude, and datetime parameters".to_string(),
        );
    }

    let global_options = GlobalOptions {
        deltat: if matches.contains_id("deltat") {
            matches
                .get_one::<String>("deltat")
                .cloned()
                .or_else(|| Some("ESTIMATE".to_string()))
        } else {
            None
        },
        format: matches.get_one::<String>("format").cloned(),
        headers: if matches.get_flag("headers") {
            Some(true)
        } else if matches.get_flag("no-headers") {
            Some(false)
        } else {
            Some(true) // Default: headers on for CSV
        },
        parallel: if matches.get_flag("parallel") {
            Some(true)
        } else if matches.get_flag("no-parallel") {
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
    };

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
        // Will be filled in by parse_data_values
        parsed_latitude: None,
        parsed_longitude: None,
        parsed_datetime: None,
    })
}

/// Parse position-specific command options
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

/// Parse sunrise-specific command options
pub fn parse_sunrise_options(matches: &ArgMatches) -> SunriseOptions {
    SunriseOptions {
        twilight: matches.get_flag("twilight"),
    }
}

/// Parse and validate coordinate and datetime data from command line arguments
pub fn parse_data_values(input: &mut ParsedInput) -> Result<(), ParseError> {
    match input.input_type {
        InputType::Standard | InputType::TimeFile | InputType::StdinTimes => {
            // Parse latitude and longitude
            let lat = parse_coordinate(&input.latitude, "latitude")?;
            let lon = parse_coordinate(
                input.longitude.as_ref().ok_or_else(|| {
                    ParseError::InvalidCoordinate("Missing longitude".to_string())
                })?,
                "longitude",
            )?;
            input.parsed_latitude = Some(lat);
            input.parsed_longitude = Some(lon);

            // Parse datetime
            if let Some(dt_str) = &input.datetime {
                // For time files, dt_str will be the @file reference - skip for now
                if !dt_str.starts_with('@') {
                    let dt = parse_datetime(dt_str, input.global_options.timezone.as_deref())?;
                    input.parsed_datetime = Some(dt);
                }
            }
        }
        InputType::CoordinateFile | InputType::StdinCoords => {
            // For coordinate files, we skip parsing the @file reference for now
            // But we should parse the datetime
            if let Some(dt_str) = &input.datetime {
                let dt = parse_datetime(dt_str, input.global_options.timezone.as_deref())?;
                input.parsed_datetime = Some(dt);
            }
        }
        InputType::PairedDataFile | InputType::StdinPaired => {
            // For paired data files, we skip parsing the @file reference for now
            // All data comes from the file
        }
    }

    // Auto-enable show-inputs based on parsed data
    apply_show_inputs_auto_logic(input);

    Ok(())
}

/// Apply automatic show-inputs logic based on input type and parsed data
pub fn apply_show_inputs_auto_logic(input: &mut ParsedInput) {
    // If user explicitly set --no-show-inputs, respect that
    if let Some(false) = input.global_options.show_inputs {
        return;
    }

    // If user explicitly set --show-inputs, keep it
    if let Some(true) = input.global_options.show_inputs {
        return;
    }

    // Auto-enable show-inputs for multiple value scenarios
    let file_input_check = matches!(
        input.input_type,
        InputType::CoordinateFile
            | InputType::StdinCoords
            | InputType::TimeFile
            | InputType::StdinTimes
            | InputType::PairedDataFile
            | InputType::StdinPaired
    );

    let coord_range_check = (input.parsed_latitude.is_some()
        && matches!(input.parsed_latitude, Some(Coordinate::Range { .. })))
        || (input.parsed_longitude.is_some()
            && matches!(input.parsed_longitude, Some(Coordinate::Range { .. })));

    let time_series_check = input.parsed_datetime.is_some()
        && matches!(
            input.parsed_datetime,
            Some(DateTimeInput::PartialYear(_))
                | Some(DateTimeInput::PartialYearMonth(_, _))
                | Some(DateTimeInput::PartialDate(_, _, _))
        );

    let should_auto_enable = file_input_check || coord_range_check || time_series_check;

    if should_auto_enable {
        input.global_options.show_inputs = Some(true);
    }
}

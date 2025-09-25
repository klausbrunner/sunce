// Re-export types for backward compatibility
pub use crate::types::{
    Coordinate, DateTimeInput, GlobalOptions, InputType, ParseError, ParsedInput, PositionOptions,
    SunriseOptions,
};

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

// Generic validation helper for numeric ranges
fn validate_range<T>(
    value: T,
    min: T,
    max: T,
    value_name: &str,
    range_desc: &str,
) -> Result<(), ParseError>
where
    T: PartialOrd + std::fmt::Display,
{
    if value < min || value > max {
        Err(ParseError::InvalidCoordinate(format!(
            "{} out of {}: {}",
            value_name, range_desc, value
        )))
    } else {
        Ok(())
    }
}

fn validate_coordinate(value: f64, coord_type: &str) -> Result<(), ParseError> {
    match coord_type {
        "latitude" => validate_range(value, -90.0, 90.0, coord_type, "latitude range -90° to 90°"),
        "longitude" => validate_range(
            value,
            -180.0,
            180.0,
            coord_type,
            "longitude range -180° to 180°",
        ),
        _ => Ok(()),
    }
}

use crate::timezone::{apply_timezone_to_datetime, get_system_timezone};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone};

pub fn parse_datetime(
    input: &str,
    timezone_override: Option<&str>,
) -> Result<DateTimeInput, ParseError> {
    if input == "now" {
        return Ok(DateTimeInput::Now);
    }

    if is_unix_timestamp(input) {
        return parse_unix_timestamp(input, timezone_override);
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

    // Try ISO format without seconds but with timezone (e.g., "2027-06-21T12:00Z")
    // TODO: Workaround for chrono's strict RFC3339 parsing - consider more flexible parser
    if input.ends_with('Z')
        || input.contains('+')
        || input.contains('-') && input.rfind('-').unwrap_or(0) > 10
    {
        // Add ":00" seconds if missing and try RFC3339 again
        let with_seconds = if input.contains('T') {
            let t_pos = input
                .find('T')
                .expect("T should exist since input.contains('T') was true");
            let time_part = &input[t_pos..];

            // Find where timezone starts (+ or - but not in date part)
            let tz_start = time_part.find('+').or_else(|| time_part.find('-'));
            let time_only = if let Some(tz_pos) = tz_start {
                &time_part[..tz_pos]
            } else if let Some(stripped) = time_part.strip_suffix('Z') {
                stripped
            } else {
                time_part
            };

            // Check if time part has only hour:minute (one colon)
            if time_only.matches(':').count() == 1 {
                // Add ":00" seconds
                if input.ends_with('Z') {
                    input.replace('Z', ":00Z")
                } else if let Some(offset_pos) = input
                    .find('+')
                    .or_else(|| input.rfind('-').filter(|&i| i > 10))
                {
                    format!("{}:00{}", &input[..offset_pos], &input[offset_pos..])
                } else {
                    format!("{}:00", input)
                }
            } else {
                input.to_string()
            }
        } else {
            input.to_string()
        };

        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&with_seconds) {
            let dt_fixed = dt.fixed_offset();
            return Ok(DateTimeInput::Single(dt_fixed));
        }
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
        date.and_time(NaiveTime::from_hms_opt(0, 0, 0).expect("0,0,0 time should always be valid"))
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

fn is_unix_timestamp(input: &str) -> bool {
    // Check if input contains only digits
    if input.is_empty() || !input.chars().all(|c| c.is_ascii_digit()) {
        return false;
    }

    // 4-digit numbers are more likely to be years than unix timestamps
    // Let partial date parsing handle those
    if input.len() == 4 {
        return false;
    }

    true
}

fn parse_unix_timestamp(
    input: &str,
    timezone_override: Option<&str>,
) -> Result<DateTimeInput, ParseError> {
    let timestamp: i64 = input
        .parse()
        .map_err(|_| ParseError::InvalidDateTime(format!("Invalid unix timestamp: {}", input)))?;

    // Range check: 1970-01-01 00:00:00 UTC (0) to 2300-01-01 00:00:00 UTC (~10.4B)
    const MIN_TIMESTAMP: i64 = 0;
    const MAX_TIMESTAMP: i64 = 10_413_792_000; // 2300-01-01 00:00:00 UTC

    if !(MIN_TIMESTAMP..=MAX_TIMESTAMP).contains(&timestamp) {
        return Err(ParseError::InvalidDateTime(format!(
            "Unix timestamp {} out of range (1970-2300)",
            timestamp
        )));
    }

    use chrono::DateTime;

    let utc_dt = DateTime::from_timestamp(timestamp, 0).ok_or_else(|| {
        ParseError::InvalidDateTime(format!("Invalid unix timestamp: {}", timestamp))
    })?;

    // Convert to fixed offset - default to UTC unless timezone override provided
    let dt_with_tz = if let Some(tz_str) = timezone_override {
        let naive = utc_dt.naive_utc();
        apply_timezone_to_datetime(naive, Some(tz_str))
            .map_err(|e| ParseError::InvalidTimezone(e.to_string()))?
    } else {
        utc_dt.fixed_offset()
    };

    Ok(DateTimeInput::Single(dt_with_tz))
}

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
    // Count file inputs and validate
    let file_count = [
        latitude.starts_with('@'),
        longitude.is_some_and(|s| s.starts_with('@')),
        datetime.is_some_and(|s| s.starts_with('@')),
    ]
    .iter()
    .filter(|&&x| x)
    .count();

    if file_count > 1 {
        return Err("Only one parameter can use file input (@file or @-)".to_string());
    }

    // Determine input type based on argument pattern
    match (longitude.is_some(), datetime.is_some()) {
        // 1 arg: latitude only (must be file)
        (false, false) => {
            if !latitude.starts_with('@') {
                return Err("Single argument must be a file input (@file or @-)".to_string());
            }
            if latitude == "@-" {
                Ok(InputType::StdinPaired)
            } else {
                Ok(InputType::PairedDataFile)
            }
        }
        // 2 args: latitude + longitude (latitude must be coordinate file)
        (true, false) => {
            if !latitude.starts_with('@') {
                return Err("Two arguments require coordinate file as first parameter".to_string());
            }
            if longitude
                .as_ref()
                .expect("longitude exists in (true, false) case")
                .starts_with('@')
            {
                return Err("File input in longitude position not supported".to_string());
            }
            if latitude == "@-" {
                Ok(InputType::StdinCoords)
            } else {
                Ok(InputType::CoordinateFile)
            }
        }
        // 2 args: latitude + datetime (coordinate file case, datetime in longitude position)
        (false, true) => {
            if !latitude.starts_with('@') {
                return Err("Two arguments require coordinate file as first parameter".to_string());
            }
            if latitude == "@-" {
                Ok(InputType::StdinCoords)
            } else {
                Ok(InputType::CoordinateFile)
            }
        }
        // 3 args: latitude + longitude + datetime
        (true, true) => {
            let datetime = datetime
                .as_ref()
                .expect("datetime exists in (true, true) case");
            if latitude.starts_with('@')
                || longitude
                    .as_ref()
                    .expect("longitude exists in (true, true) case")
                    .starts_with('@')
            {
                return Err("Only datetime parameter can be a file in this combination".to_string());
            }
            if datetime.starts_with('@') {
                if datetime.as_str() == "@-" {
                    Ok(InputType::StdinTimes)
                } else {
                    Ok(InputType::TimeFile)
                }
            } else {
                Ok(InputType::Standard)
            }
        }
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

pub fn parse_data_values(
    input: &mut ParsedInput,
    command_name: Option<&str>,
) -> Result<(), crate::types::ParseError> {
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
    apply_show_inputs_auto_logic(input, command_name);

    Ok(())
}

pub fn apply_show_inputs_auto_logic(input: &mut ParsedInput, command_name: Option<&str>) {
    if input.global_options.show_inputs.is_some() {
        return; // User explicitly set, don't override
    }

    let should_auto_enable =
        // Coordinate ranges auto-enable show-inputs
        matches!(input.parsed_latitude, Some(crate::types::Coordinate::Range { .. })) ||
        matches!(input.parsed_longitude, Some(crate::types::Coordinate::Range { .. })) ||

        // Partial dates (time series) auto-enable show-inputs for position command
        // For sunrise command, specific dates should NOT auto-enable show-inputs
        (matches!(input.parsed_datetime, Some(crate::types::DateTimeInput::PartialYear(_)) |
                                         Some(crate::types::DateTimeInput::PartialYearMonth(_, _))) ||
         (matches!(input.parsed_datetime, Some(crate::types::DateTimeInput::PartialDate(_, _, _))) &&
          command_name != Some("sunrise"))) ||

        // File inputs auto-enable show-inputs
        matches!(input.input_type, InputType::CoordinateFile | InputType::TimeFile |
                                   InputType::PairedDataFile | InputType::StdinCoords |
                                   InputType::StdinTimes | InputType::StdinPaired);

    if should_auto_enable {
        input.global_options.show_inputs = Some(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DateTimeInput;

    #[test]
    fn test_unix_timestamp_detection() {
        // Should detect pure numeric strings as unix timestamps
        assert!(is_unix_timestamp("1577836800")); // 2020-01-01 00:00:00 UTC
        assert!(is_unix_timestamp("0")); // Minimum
        assert!(is_unix_timestamp("123")); // Short number
        assert!(is_unix_timestamp("9999999999")); // Long number

        // Should not detect 4-digit numbers (likely years)
        assert!(!is_unix_timestamp("2024")); // 4-digit year

        // Should not detect other formats
        assert!(!is_unix_timestamp("1577836800.5")); // Decimal
        assert!(!is_unix_timestamp("2024-01-01")); // Date format
        assert!(!is_unix_timestamp("now")); // String
        assert!(!is_unix_timestamp("")); // Empty
        assert!(!is_unix_timestamp("abc123")); // Non-numeric
        assert!(!is_unix_timestamp("123abc")); // Mixed
        assert!(!is_unix_timestamp("-123")); // Negative sign
        assert!(!is_unix_timestamp("+123")); // Positive sign
    }

    #[test]
    fn test_unix_timestamp_parsing_utc() {
        // Test basic UTC parsing
        let result = parse_unix_timestamp("1577836800", None).unwrap();
        match result {
            DateTimeInput::Single(dt) => {
                assert_eq!(
                    dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                    "2020-01-01 00:00:00"
                );
                assert_eq!(dt.offset().local_minus_utc(), 0); // UTC offset
            }
            _ => panic!("Expected Single datetime"),
        }
    }

    #[test]
    fn test_unix_timestamp_parsing_with_timezone() {
        // Test with offset timezone
        let result = parse_unix_timestamp("1577836800", Some("+01:00")).unwrap();
        match result {
            DateTimeInput::Single(dt) => {
                assert_eq!(
                    dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                    "2020-01-01 00:00:00"
                );
                assert_eq!(dt.offset().local_minus_utc(), 3600); // +1 hour in seconds
            }
            _ => panic!("Expected Single datetime"),
        }
    }

    #[test]
    fn test_unix_timestamp_range_validation() {
        // Test minimum (1970-01-01)
        assert!(parse_unix_timestamp("0", None).is_ok());

        // Test maximum (2300-01-01)
        assert!(parse_unix_timestamp("10413792000", None).is_ok());

        // Test out of range (too large)
        let result = parse_unix_timestamp("10413792001", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("out of range"));

        // Test negative (would be out of range but parsing fails first)
        let result = parse_unix_timestamp("-1", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_unix_timestamp_invalid_format() {
        // Test non-numeric
        let result = parse_unix_timestamp("abc", None);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid unix timestamp")
        );

        // Test empty
        let result = parse_unix_timestamp("", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_datetime_unix_timestamp_integration() {
        // Test that parse_datetime correctly routes to unix timestamp parsing
        let result = parse_datetime("1577836800", None).unwrap();
        match result {
            DateTimeInput::Single(dt) => {
                assert_eq!(dt.format("%Y-%m-%d").to_string(), "2020-01-01");
            }
            _ => panic!("Expected Single datetime"),
        }

        // Test that regular datetime parsing still works
        let result = parse_datetime("2020-01-01T00:00:00", None).unwrap();
        match result {
            DateTimeInput::Single(_) => {} // Success
            _ => panic!("Expected Single datetime"),
        }

        // Test that partial dates still work
        let result = parse_datetime("2024", None).unwrap();
        match result {
            DateTimeInput::PartialYear(2024) => {} // Success
            _ => panic!("Expected PartialYear"),
        }
    }

    #[test]
    fn test_datetime_without_seconds_with_timezone() {
        // Test Z suffix without seconds
        let result = parse_datetime("2027-06-21T12:00Z", None).unwrap();
        match result {
            DateTimeInput::Single(dt) => {
                assert_eq!(
                    dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                    "2027-06-21 12:00:00"
                );
                assert_eq!(dt.offset().local_minus_utc(), 0); // UTC offset
            }
            _ => panic!("Expected Single datetime"),
        }

        // Test positive offset without seconds
        let result = parse_datetime("2027-06-21T12:00+02:00", None).unwrap();
        match result {
            DateTimeInput::Single(dt) => {
                assert_eq!(
                    dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                    "2027-06-21 12:00:00"
                );
                assert_eq!(dt.offset().local_minus_utc(), 7200); // +2 hours in seconds
            }
            _ => panic!("Expected Single datetime"),
        }

        // Test negative offset without seconds
        let result = parse_datetime("2027-06-21T12:00-05:00", None).unwrap();
        match result {
            DateTimeInput::Single(dt) => {
                assert_eq!(
                    dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                    "2027-06-21 12:00:00"
                );
                assert_eq!(dt.offset().local_minus_utc(), -18000); // -5 hours in seconds
            }
            _ => panic!("Expected Single datetime"),
        }
    }

    #[test]
    fn test_coordinate_parsing() {
        // Test single coordinate
        let result = parse_coordinate("52.5", "latitude").unwrap();
        match result {
            Coordinate::Single(val) => assert!((val - 52.5).abs() < f64::EPSILON),
            _ => panic!("Expected Single coordinate"),
        }

        // Test coordinate range
        let result = parse_coordinate("52:53:0.5", "latitude").unwrap();
        match result {
            Coordinate::Range { start, end, step } => {
                assert!((start - 52.0).abs() < f64::EPSILON);
                assert!((end - 53.0).abs() < f64::EPSILON);
                assert!((step - 0.5).abs() < f64::EPSILON);
            }
            _ => panic!("Expected Range coordinate"),
        }
    }

    #[test]
    fn test_coordinate_validation() {
        // Test valid latitude
        assert!(parse_coordinate("45.0", "latitude").is_ok());
        assert!(parse_coordinate("-90.0", "latitude").is_ok());
        assert!(parse_coordinate("90.0", "latitude").is_ok());

        // Test invalid latitude
        assert!(parse_coordinate("91.0", "latitude").is_err());
        assert!(parse_coordinate("-91.0", "latitude").is_err());

        // Test valid longitude
        assert!(parse_coordinate("0.0", "longitude").is_ok());
        assert!(parse_coordinate("-180.0", "longitude").is_ok());
        assert!(parse_coordinate("180.0", "longitude").is_ok());

        // Test invalid longitude
        assert!(parse_coordinate("181.0", "longitude").is_err());
        assert!(parse_coordinate("-181.0", "longitude").is_err());
    }

    #[test]
    fn test_show_inputs_auto_logic_sunrise_specific_date() {
        use crate::types::{GlobalOptions, InputType, ParsedInput};

        // Test that specific dates do NOT auto-enable show-inputs for sunrise command
        let mut input = ParsedInput {
            input_type: InputType::Standard,
            latitude: "52.0".to_string(),
            longitude: Some("13.4".to_string()),
            datetime: Some("2024-01-01".to_string()),
            global_options: GlobalOptions {
                deltat: None,
                format: None,
                headers: None,
                show_inputs: None, // Not explicitly set
                timezone: None,
            },
            parsed_latitude: Some(Coordinate::Single(52.0)),
            parsed_longitude: Some(Coordinate::Single(13.4)),
            parsed_datetime: Some(DateTimeInput::PartialDate(2024, 1, 1)),
        };

        // For sunrise command, specific date should NOT auto-enable show-inputs
        apply_show_inputs_auto_logic(&mut input, Some("sunrise"));
        assert_eq!(
            input.global_options.show_inputs, None,
            "Specific date should not auto-enable show-inputs for sunrise command"
        );

        // Reset
        input.global_options.show_inputs = None;

        // For position command, specific date SHOULD auto-enable show-inputs
        apply_show_inputs_auto_logic(&mut input, Some("position"));
        assert_eq!(
            input.global_options.show_inputs,
            Some(true),
            "Specific date should auto-enable show-inputs for position command"
        );
    }

    #[test]
    fn test_show_inputs_auto_logic_sunrise_partial_year() {
        use crate::types::{GlobalOptions, InputType, ParsedInput};

        // Test that partial years still auto-enable show-inputs even for sunrise command
        let mut input = ParsedInput {
            input_type: InputType::Standard,
            latitude: "52.0".to_string(),
            longitude: Some("13.4".to_string()),
            datetime: Some("2024".to_string()),
            global_options: GlobalOptions {
                deltat: None,
                format: None,
                headers: None,
                show_inputs: None,
                timezone: None,
            },
            parsed_latitude: Some(Coordinate::Single(52.0)),
            parsed_longitude: Some(Coordinate::Single(13.4)),
            parsed_datetime: Some(DateTimeInput::PartialYear(2024)),
        };

        // For sunrise command, partial year should still auto-enable show-inputs
        apply_show_inputs_auto_logic(&mut input, Some("sunrise"));
        assert_eq!(
            input.global_options.show_inputs,
            Some(true),
            "Partial year should auto-enable show-inputs even for sunrise command"
        );
    }
}

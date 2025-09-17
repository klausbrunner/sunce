use crate::timezone_utils::{get_system_timezone, naive_to_fixed_offset, naive_to_timezone_aware};
use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
use chrono_tz::Tz;
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
                apply_timezone_override(dt, tz)?
            } else {
                dt
            };
            return Ok(DateTimeInput::Single(final_dt));
        }

        // Try parsing as naive datetime and apply timezone
        if let Ok(naive_dt) = NaiveDateTime::parse_from_str(input, format) {
            let dt = if let Some(tz) = timezone_override {
                apply_timezone_to_naive(naive_dt, tz)?
            } else {
                // Default to local timezone like solarpos does
                let system_tz = get_system_timezone();
                naive_to_timezone_aware(naive_dt, &system_tz)?
            };
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
                let dt = if let Some(tz) = timezone_override {
                    apply_timezone_to_naive(naive_dt, tz)?
                } else {
                    let system_tz = get_system_timezone();
                    naive_to_timezone_aware(naive_dt, &system_tz)?
                };
                return Ok(DateTimeInput::Single(dt));
            }
        }
    }

    Err(ParseError::InvalidDateTime(format!(
        "Could not parse datetime: {}",
        input
    )))
}

fn apply_timezone_override(
    dt: DateTime<FixedOffset>,
    tz: &str,
) -> Result<DateTime<FixedOffset>, ParseError> {
    // Parse timezone offset like "+01:00" or "-05:00"
    if let Ok(offset) = parse_timezone_offset(tz) {
        let naive = dt.naive_utc();
        return Ok(offset.from_utc_datetime(&naive));
    }

    // Parse named timezones like "UTC", "America/New_York", etc.
    if let Ok(named_tz) = parse_named_timezone(tz) {
        let naive_local = dt.naive_local();
        let dt_in_tz = named_tz
            .from_local_datetime(&naive_local)
            .single()
            .ok_or_else(|| {
                ParseError::InvalidDateTime("Ambiguous time in target timezone".to_string())
            })?;
        // Convert to FixedOffset by recreating with the offset seconds
        let utc_time = dt_in_tz.naive_utc();
        let local_time = dt_in_tz.naive_local();
        let offset_seconds = utc_time.signed_duration_since(local_time).num_seconds() as i32;
        let fixed_offset = FixedOffset::west_opt(offset_seconds).unwrap();
        return Ok(fixed_offset.from_utc_datetime(&utc_time));
    }

    Err(ParseError::InvalidTimezone(format!(
        "Unsupported timezone format: {}. Use offset format like +01:00 or named timezone like UTC",
        tz
    )))
}

fn apply_timezone_to_naive(
    naive_dt: NaiveDateTime,
    tz: &str,
) -> Result<DateTime<FixedOffset>, ParseError> {
    if let Ok(offset) = parse_timezone_offset(tz) {
        return naive_to_fixed_offset(naive_dt, &offset);
    }

    // Parse named timezones like "UTC", "America/New_York", etc.
    if let Ok(named_tz) = parse_named_timezone(tz) {
        let dt_in_tz = named_tz
            .from_local_datetime(&naive_dt)
            .single()
            .ok_or_else(|| ParseError::InvalidDateTime("Ambiguous time in timezone".to_string()))?;
        // Convert to FixedOffset by recreating with the offset seconds
        let utc_time = dt_in_tz.naive_utc();
        let local_time = dt_in_tz.naive_local();
        let offset_seconds = utc_time.signed_duration_since(local_time).num_seconds() as i32;
        let fixed_offset = FixedOffset::west_opt(offset_seconds).unwrap();
        return Ok(fixed_offset.from_utc_datetime(&utc_time));
    }

    Err(ParseError::InvalidTimezone(format!(
        "Unsupported timezone format: {}. Use offset format like +01:00 or named timezone like UTC",
        tz
    )))
}

pub fn parse_timezone(tz: &str) -> Result<FixedOffset, ParseError> {
    // Try parsing as timezone offset first
    if let Ok(offset) = parse_timezone_offset(tz) {
        return Ok(offset);
    }

    // Try parsing as named timezone
    if let Ok(named_tz) = parse_named_timezone(tz) {
        // For named timezones, we need to convert to a FixedOffset
        // We'll use a representative time to get the offset
        let utc_time = chrono::Utc::now();
        let local_time = utc_time.with_timezone(&named_tz);

        // Convert to FixedOffset by computing the offset in seconds
        let utc_naive = local_time.naive_utc();
        let local_naive = local_time.naive_local();
        let offset_seconds = local_naive.signed_duration_since(utc_naive).num_seconds() as i32;
        let fixed_offset = FixedOffset::east_opt(offset_seconds).unwrap();
        return Ok(fixed_offset);
    }

    Err(ParseError::InvalidTimezone(format!(
        "Unsupported timezone format: {}. Use offset format like +01:00 or named timezone like UTC",
        tz
    )))
}

pub fn parse_timezone_offset(tz: &str) -> Result<FixedOffset, ParseError> {
    // Handle formats like "+01:00", "-05:00", "+0100", "-0500"
    let normalized =
        if tz.len() == 6 && (tz.starts_with('+') || tz.starts_with('-')) && tz.contains(':') {
            tz.to_string() // Already in +01:00 format
        } else if tz.len() == 5 && tz.contains(':') {
            format!("+{}", tz) // Handle 01:00 -> +01:00
        } else if tz.len() == 5 && (tz.starts_with('+') || tz.starts_with('-')) && !tz.contains(':')
        {
            format!("{}:{}", &tz[..3], &tz[3..]) // Handle +0100 -> +01:00
        } else {
            return Err(ParseError::InvalidTimezone(format!(
                "Invalid timezone format: {} (len={}, starts_with_sign={}, contains_colon={})",
                tz,
                tz.len(),
                tz.starts_with('+') || tz.starts_with('-'),
                tz.contains(':')
            )));
        };

    let test_str = format!("2000-01-01T00:00:00{}", normalized);
    DateTime::parse_from_str(&test_str, "%Y-%m-%dT%H:%M:%S%:z")
        .map(|dt| *dt.offset())
        .map_err(|e| {
            ParseError::InvalidTimezone(format!(
                "Failed to parse '{}' with format '%Y-%m-%dT%H:%M:%S%:z': {}",
                test_str, e
            ))
        })
}

pub fn parse_named_timezone(tz: &str) -> Result<Tz, ParseError> {
    // Handle common timezone aliases first
    let normalized_tz = match tz.to_uppercase().as_str() {
        "UTC" | "GMT" => "UTC",
        "CET" => "Europe/Berlin",
        "EST" => "America/New_York",
        "PST" => "America/Los_Angeles",
        _ => tz, // Use original string for full timezone names like "Europe/Berlin"
    };

    normalized_tz
        .parse::<Tz>()
        .map_err(|_| ParseError::InvalidTimezone(format!("Unknown timezone: {}", tz)))
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

    #[test]
    fn test_timezone_offset_parsing() {
        // Test various timezone offset formats
        let test_cases = [
            ("+02:00", 7200),
            ("+01:00", 3600),
            ("-05:00", -18000),
            ("+0000", 0),
            ("-0100", -3600),
        ];

        for (tz, expected_offset) in test_cases {
            let result = parse_timezone_offset(tz);
            assert!(result.is_ok(), "Failed to parse timezone: {}", tz);

            if let Ok(offset) = result {
                assert_eq!(
                    offset.local_minus_utc(),
                    expected_offset,
                    "Wrong offset for timezone {}: expected {}, got {}",
                    tz,
                    expected_offset,
                    offset.local_minus_utc()
                );
            }
        }
    }

    #[test]
    fn test_named_timezone_parsing() {
        let result = parse_named_timezone("UTC");
        assert!(result.is_ok(), "Failed to parse UTC timezone: {:?}", result);

        let result = parse_named_timezone("America/New_York");
        assert!(
            result.is_ok(),
            "Failed to parse America/New_York timezone: {:?}",
            result
        );
    }

    #[test]
    fn test_apply_timezone_override() {
        use chrono::*;

        // Create a test datetime
        let dt =
            DateTime::parse_from_str("2024-01-01T12:00:00+00:00", "%Y-%m-%dT%H:%M:%S%z").unwrap();

        // Test with UTC
        let result = apply_timezone_override(dt, "UTC");
        println!("UTC result: {:?}", result);
        assert!(
            result.is_ok(),
            "Failed to apply UTC timezone override: {:?}",
            result
        );

        // Test with offset
        let result = apply_timezone_override(dt, "+02:00");
        println!("+02:00 result: {:?}", result);
        assert!(
            result.is_ok(),
            "Failed to apply +02:00 timezone override: {:?}",
            result
        );
    }
}

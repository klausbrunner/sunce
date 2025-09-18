use crate::timezone::{apply_timezone_to_datetime, get_system_timezone};
use crate::types::{DateTimeInput, ParseError};
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

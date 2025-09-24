use crate::parsing::ParseError;
use chrono::{DateTime, FixedOffset, NaiveDateTime, TimeZone};
use chrono_tz::{Tz, UTC};
use std::sync::OnceLock;

#[derive(Debug, Clone)]
pub enum TimezoneSpec {
    Fixed(FixedOffset),
    Named(Tz),
}

impl TimezoneSpec {
    pub fn apply_to_naive(
        &self,
        naive_dt: NaiveDateTime,
    ) -> Result<DateTime<FixedOffset>, ParseError> {
        match self {
            TimezoneSpec::Fixed(offset) => offset
                .from_local_datetime(&naive_dt)
                .single()
                .ok_or_else(|| {
                    ParseError::InvalidDateTime(format!(
                        "Invalid datetime with offset: {}",
                        naive_dt
                    ))
                }),
            TimezoneSpec::Named(tz) => apply_named_timezone(naive_dt, tz),
        }
    }
}

/// Cached system timezone - computed once at first access
static SYSTEM_TIMEZONE: OnceLock<Tz> = OnceLock::new();

/// Get the system timezone using cross-platform detection (cached)
pub fn get_system_timezone() -> Tz {
    *SYSTEM_TIMEZONE.get_or_init(|| {
        // Try to get timezone from TZ environment variable first (for tests and overrides)
        if let Ok(tz_str) = std::env::var("TZ")
            && let Ok(tz) = tz_str.parse::<Tz>()
        {
            return tz;
        }

        // Use iana-time-zone for cross-platform system timezone detection
        match iana_time_zone::get_timezone() {
            Ok(tz_name) => {
                // Try to parse the IANA timezone name
                if let Ok(tz) = tz_name.parse::<Tz>() {
                    tz
                } else {
                    // Fallback to UTC if parsing fails
                    UTC
                }
            }
            Err(_) => {
                // If iana-time-zone fails, fallback to UTC
                UTC
            }
        }
    })
}

/// Apply timezone to naive datetime - single entry point for all timezone operations
pub fn apply_timezone_to_datetime(
    naive_dt: NaiveDateTime,
    timezone_str: Option<&str>,
) -> Result<DateTime<FixedOffset>, ParseError> {
    match timezone_str {
        None => apply_system_timezone(naive_dt),
        Some(tz_str) => apply_timezone_override(naive_dt, tz_str),
    }
}

/// Apply system timezone with DST handling
fn apply_system_timezone(naive_dt: NaiveDateTime) -> Result<DateTime<FixedOffset>, ParseError> {
    match get_system_timezone().from_local_datetime(&naive_dt) {
        chrono::LocalResult::Single(dt) => Ok(dt.fixed_offset()),
        chrono::LocalResult::Ambiguous(dt1, _) => Ok(dt1.fixed_offset()),
        chrono::LocalResult::None => Err(ParseError::InvalidDateTime(format!(
            "DST gap: {} does not exist in system timezone",
            naive_dt
        ))),
    }
}

/// Apply timezone override (unified handling of all timezone formats)
fn apply_timezone_override(
    naive_dt: NaiveDateTime,
    tz_str: &str,
) -> Result<DateTime<FixedOffset>, ParseError> {
    // Try offset format first (most common)
    if let Ok(offset) = parse_offset(tz_str) {
        return offset
            .from_local_datetime(&naive_dt)
            .single()
            .ok_or_else(|| {
                ParseError::InvalidDateTime(format!(
                    "Invalid datetime with offset: {} {}",
                    naive_dt, tz_str
                ))
            });
    }

    // Try named timezone
    if let Ok(tz) = parse_named_timezone(tz_str) {
        return apply_named_timezone(naive_dt, &tz);
    }

    Err(ParseError::InvalidTimezone(format!(
        "Unsupported timezone: {}. Use format like +01:00 or UTC",
        tz_str
    )))
}

/// Apply named timezone with DST handling
fn apply_named_timezone(
    naive_dt: NaiveDateTime,
    timezone: &Tz,
) -> Result<DateTime<FixedOffset>, ParseError> {
    match timezone.from_local_datetime(&naive_dt) {
        chrono::LocalResult::Single(dt) => Ok(dt.fixed_offset()),
        chrono::LocalResult::Ambiguous(dt1, _) => Ok(dt1.fixed_offset()),
        chrono::LocalResult::None => Err(ParseError::InvalidDateTime(format!(
            "DST gap: {} does not exist in timezone {}",
            naive_dt, timezone
        ))),
    }
}

/// Parse timezone offset (e.g., +01:00, -05:00)
fn parse_offset(tz_str: &str) -> Result<FixedOffset, ParseError> {
    if tz_str.len() != 6 || !tz_str.contains(':') {
        return Err(ParseError::InvalidTimezone(format!(
            "Invalid offset format: {}",
            tz_str
        )));
    }

    let test_dt = format!("2000-01-01T12:00:00{}", tz_str);
    DateTime::parse_from_str(&test_dt, "%Y-%m-%dT%H:%M:%S%:z")
        .map(|dt| *dt.offset())
        .map_err(|_| ParseError::InvalidTimezone(format!("Invalid offset: {}", tz_str)))
}

/// Parse named timezone (e.g., UTC, America/New_York)
fn parse_named_timezone(tz_str: &str) -> Result<Tz, ParseError> {
    match tz_str {
        "UTC" | "GMT" => Ok(chrono_tz::UTC),
        _ => tz_str
            .parse::<Tz>()
            .map_err(|_| ParseError::InvalidTimezone(format!("Unknown timezone: {}", tz_str))),
    }
}

/// Parse timezone string to TimezoneSpec (handles both fixed offsets and named timezones)
pub fn parse_timezone_spec(tz_str: &str) -> Result<TimezoneSpec, ParseError> {
    if let Ok(offset) = parse_offset(tz_str) {
        return Ok(TimezoneSpec::Fixed(offset));
    }

    if let Ok(tz) = parse_named_timezone(tz_str) {
        return Ok(TimezoneSpec::Named(tz));
    }

    Err(ParseError::InvalidTimezone(format!(
        "Unsupported timezone: {}. Use format like +01:00, UTC, or Europe/Berlin",
        tz_str
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use chrono_tz::Europe::Berlin;

    #[test]
    fn test_system_timezone_detection() {
        // Just verify the function returns a valid timezone
        // Don't assume specific timezone behavior in CI environments
        let tz = get_system_timezone();
        // Should be a valid timezone (any timezone is fine)
        assert!(!tz.name().is_empty());
    }

    #[test]
    fn test_offset_parsing() {
        let offset = parse_offset("+01:00").unwrap();
        assert_eq!(offset.local_minus_utc(), 3600);

        let offset = parse_offset("-05:00").unwrap();
        assert_eq!(offset.local_minus_utc(), -18000);

        assert!(parse_offset("+0100").is_err());
        assert!(parse_offset("01:00").is_err());
    }

    #[test]
    fn test_named_timezone_parsing() {
        let tz = parse_named_timezone("UTC").unwrap();
        assert_eq!(tz, chrono_tz::UTC);

        assert!(parse_named_timezone("INVALID").is_err());
    }

    #[test]
    fn test_timezone_application() {
        let naive = NaiveDate::from_ymd_opt(2024, 6, 21)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();

        // Test offset override
        let result = apply_timezone_to_datetime(naive, Some("+02:00")).unwrap();
        assert_eq!(result.offset().local_minus_utc(), 7200);

        // Test UTC override
        let result = apply_timezone_to_datetime(naive, Some("UTC")).unwrap();
        assert_eq!(result.offset().local_minus_utc(), 0);

        // Test no override (system timezone)
        let result = apply_timezone_to_datetime(naive, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_dst_handling() {
        let naive = NaiveDate::from_ymd_opt(2024, 6, 21)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let result = apply_named_timezone(naive, &Berlin).unwrap();
        assert_eq!(result.offset().local_minus_utc(), 7200); // +02:00 summer time

        let naive = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let result = apply_named_timezone(naive, &Berlin).unwrap();
        assert_eq!(result.offset().local_minus_utc(), 3600); // +01:00 standard time
    }
}

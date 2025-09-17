use crate::parsing::ParseError;
use chrono::{DateTime, FixedOffset, Local, NaiveDateTime, TimeZone};
use chrono_tz::{Tz, UTC};

/// Get the system timezone, first checking TZ environment variable, then falling back to local detection
pub fn get_system_timezone() -> Tz {
    // Try to get timezone from TZ environment variable first
    if let Ok(tz_str) = std::env::var("TZ") {
        if let Ok(tz) = tz_str.parse::<Tz>() {
            return tz;
        }
    }

    // Try to detect the system timezone by examining the local timezone offset
    // This is a best-effort approach - timezone detection is inherently complex
    // For now, we'll use UTC as a safe fallback (this should be improved)
    UTC
}

/// Convert a naive datetime to system local timezone like solarpos does
pub fn naive_to_system_local(naive: NaiveDateTime) -> Result<DateTime<FixedOffset>, ParseError> {
    // Use chrono's Local timezone to get the correct offset for this specific datetime
    match Local.from_local_datetime(&naive) {
        chrono::LocalResult::Single(dt) => Ok(dt.fixed_offset()),
        chrono::LocalResult::Ambiguous(dt1, _dt2) => {
            // During fall-back (DST ends), choose the first occurrence like solarpos
            Ok(dt1.fixed_offset())
        }
        chrono::LocalResult::None => {
            // During spring-forward (DST begins), this time doesn't exist
            // Add 1 hour to find the next valid time (standard DST behavior)
            let adjusted_naive = naive + chrono::Duration::try_hours(1).unwrap();
            match Local.from_local_datetime(&adjusted_naive) {
                chrono::LocalResult::Single(dt) => Ok(dt.fixed_offset()),
                _ => Err(ParseError::InvalidDateTime(format!(
                    "Cannot resolve DST transition for time: {}",
                    naive
                ))),
            }
        }
    }
}

/// Convert a naive datetime to a specific timezone like solarpos does (fixed DST handling)
pub fn naive_to_specific_timezone(
    naive: NaiveDateTime,
    timezone: &Tz,
) -> Result<DateTime<FixedOffset>, ParseError> {
    // Use the same logic as system local but for a specific timezone
    match timezone.from_local_datetime(&naive) {
        chrono::LocalResult::Single(dt) => Ok(dt.fixed_offset()),
        chrono::LocalResult::Ambiguous(dt1, _dt2) => {
            // During fall-back (DST ends), choose the first occurrence like solarpos
            Ok(dt1.fixed_offset())
        }
        chrono::LocalResult::None => {
            // During spring-forward (DST begins), this time doesn't exist
            // Add 1 hour to find the next valid time (standard DST behavior)
            let adjusted_naive = naive + chrono::Duration::try_hours(1).unwrap();
            match timezone.from_local_datetime(&adjusted_naive) {
                chrono::LocalResult::Single(dt) => Ok(dt.fixed_offset()),
                _ => Err(ParseError::InvalidDateTime(format!(
                    "Cannot resolve DST transition for time: {}",
                    naive
                ))),
            }
        }
    }
}

/// Convert a naive datetime to a fixed offset timezone with proper DST handling
pub fn naive_to_fixed_offset(
    naive: NaiveDateTime,
    offset: &FixedOffset,
) -> Result<DateTime<FixedOffset>, ParseError> {
    match offset.from_local_datetime(&naive) {
        chrono::LocalResult::Single(dt) => Ok(dt),
        chrono::LocalResult::Ambiguous(dt1, _dt2) => {
            // During fall-back (DST ends), choose the first occurrence like solarpos
            Ok(dt1)
        }
        chrono::LocalResult::None => {
            // During spring-forward (DST begins), this time doesn't exist
            Err(ParseError::InvalidDateTime(format!(
                "Time {} does not exist due to DST transition",
                naive.format("%Y-%m-%d %H:%M:%S")
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use chrono_tz::Europe::Berlin;

    #[test]
    fn test_get_system_timezone_with_tz_env() {
        unsafe {
            std::env::set_var("TZ", "Europe/Berlin");
        }
        let tz = get_system_timezone();
        assert_eq!(tz, Berlin);
        unsafe {
            std::env::remove_var("TZ");
        }
    }

    #[test]
    fn test_get_system_timezone_with_invalid_tz_env() {
        unsafe {
            std::env::set_var("TZ", "Invalid/Timezone");
        }
        let tz = get_system_timezone();
        assert_eq!(tz, UTC);
        unsafe {
            std::env::remove_var("TZ");
        }
    }

    #[test]
    fn test_get_system_timezone_fallback() {
        unsafe {
            std::env::remove_var("TZ");
        }
        let tz = get_system_timezone();
        assert_eq!(tz, UTC);
    }

    #[test]
    fn test_naive_to_specific_timezone_normal() {
        let naive = NaiveDate::from_ymd_opt(2024, 6, 21)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let result = naive_to_specific_timezone(naive, &Berlin).unwrap();
        assert_eq!(result.offset().local_minus_utc(), 7200); // +02:00 summer time
    }

    #[test]
    fn test_naive_to_specific_timezone_winter() {
        let naive = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let result = naive_to_specific_timezone(naive, &Berlin).unwrap();
        assert_eq!(result.offset().local_minus_utc(), 3600); // +01:00 standard time
    }

    #[test]
    fn test_naive_to_fixed_offset_success() {
        let naive = NaiveDate::from_ymd_opt(2024, 6, 21)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let offset = FixedOffset::east_opt(3600).unwrap(); // +01:00
        let result = naive_to_fixed_offset(naive, &offset).unwrap();
        assert_eq!(result.offset().local_minus_utc(), 3600);
        assert_eq!(result.naive_local(), naive);
    }
}

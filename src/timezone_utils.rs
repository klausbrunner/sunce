use crate::parsing::ParseError;
use chrono::{DateTime, FixedOffset, NaiveDateTime, TimeZone};
use chrono_tz::{Tz, UTC};

/// Get the system timezone, first checking TZ environment variable, then falling back to UTC
pub fn get_system_timezone() -> Tz {
    // Try to get timezone from TZ environment variable first
    if let Ok(tz_str) = std::env::var("TZ") {
        if let Ok(tz) = tz_str.parse::<Tz>() {
            return tz;
        }
    }

    // Fallback to detecting local timezone
    // This is a simplified approach - in practice, timezone detection is complex
    // For now, we'll use UTC as a safe fallback
    UTC
}

/// Convert a naive datetime to a timezone-aware datetime with proper DST handling
pub fn naive_to_timezone_aware(
    naive: NaiveDateTime,
    timezone: &Tz,
) -> Result<DateTime<FixedOffset>, ParseError> {
    match timezone.from_local_datetime(&naive) {
        chrono::LocalResult::Single(dt) => Ok(dt.fixed_offset()),
        chrono::LocalResult::Ambiguous(dt1, _dt2) => {
            // During fall-back (DST ends), choose the first occurrence like solarpos
            Ok(dt1.fixed_offset())
        }
        chrono::LocalResult::None => {
            // During spring-forward (DST begins), this time doesn't exist
            // Find the next valid time by adding the DST gap (typically 1 hour)
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

use crate::timezone::{TimezoneSpec, apply_timezone_to_datetime, get_system_timezone};
use crate::types::{DateTimeInput, ParseError};
use chrono::{DateTime, Duration, FixedOffset, Local, NaiveDate, NaiveDateTime};

/// Parse a duration string into a chrono Duration.
///
/// Supported formats:
/// - Plain number: interpreted as seconds (e.g., "300" = 5 minutes)
/// - Number with unit: e.g., "30s", "15m", "2h", "1d"
///
/// Supported units:
/// - s, sec, second, seconds
/// - m, min, minute, minutes
/// - h, hr, hour, hours
/// - d, day, days
pub fn parse_duration(input: &str) -> Result<Duration, ParseError> {
    if input.is_empty() {
        return Err(ParseError::InvalidDateTime(
            "Empty duration string. Expected formats: '30s', '15m', '2h', '1d', or plain number (seconds)".to_string(),
        ));
    }

    // Try parsing as plain number (seconds)
    if let Ok(seconds) = input.parse::<i64>() {
        if seconds <= 0 {
            return Err(ParseError::InvalidDateTime(format!(
                "Duration must be positive, got {} seconds. Use values like '30s', '15m', '2h'",
                seconds
            )));
        }
        if seconds > 315_576_000_000 {
            // 10,000 years in seconds
            return Err(ParseError::InvalidDateTime(format!(
                "Duration {} seconds is too large (exceeds 10,000 years)",
                seconds
            )));
        }
        return Duration::try_seconds(seconds).ok_or_else(|| {
            ParseError::InvalidDateTime(format!("Duration overflow for {} seconds", seconds))
        });
    }

    // Parse as number with unit
    let unit_start = input.find(|c: char| c.is_alphabetic()).ok_or_else(|| {
        ParseError::InvalidDateTime(format!(
            "Invalid duration format '{}'. Expected formats: '30s', '15m', '2h', '1d', or plain number (seconds)",
            input
        ))
    })?;

    let (num_str, unit) = input.split_at(unit_start);

    // Handle empty number part
    if num_str.is_empty() {
        return Err(ParseError::InvalidDateTime(format!(
            "Missing number before unit '{}'. Expected formats like '30s', '15m', '2h'",
            unit
        )));
    }

    let num: i64 = num_str.trim().parse().map_err(|_| {
        ParseError::InvalidDateTime(format!(
            "Invalid number '{}' in duration. Expected integer like '30s', '15m', '2h'",
            num_str
        ))
    })?;

    if num <= 0 {
        return Err(ParseError::InvalidDateTime(format!(
            "Duration must be positive, got {}{}. Use values like '30s', '15m', '2h'",
            num, unit
        )));
    }

    let duration = match unit.trim().to_lowercase().as_str() {
        "s" | "sec" | "second" | "seconds" => {
            if num > 315_576_000_000 {
                return Err(ParseError::InvalidDateTime(format!(
                    "Duration {}s exceeds maximum (10,000 years)",
                    num
                )));
            }
            Duration::try_seconds(num)
        }
        "m" | "min" | "minute" | "minutes" => {
            if num > 5_259_600_000 {
                // 10,000 years in minutes
                return Err(ParseError::InvalidDateTime(format!(
                    "Duration {}m exceeds maximum (10,000 years)",
                    num
                )));
            }
            Duration::try_minutes(num)
        }
        "h" | "hr" | "hour" | "hours" => {
            if num > 87_660_000 {
                // 10,000 years in hours
                return Err(ParseError::InvalidDateTime(format!(
                    "Duration {}h exceeds maximum (10,000 years)",
                    num
                )));
            }
            Duration::try_hours(num)
        }
        "d" | "day" | "days" => {
            if num > 3_652_500 {
                // 10,000 years in days
                return Err(ParseError::InvalidDateTime(format!(
                    "Duration {}d exceeds maximum (10,000 years)",
                    num
                )));
            }
            Duration::try_days(num)
        }
        _ => {
            return Err(ParseError::InvalidDateTime(format!(
                "Unknown time unit '{}'. Supported units: s/sec/second, m/min/minute, h/hr/hour, d/day",
                unit
            )));
        }
    };

    duration.ok_or_else(|| {
        ParseError::InvalidDateTime(format!("Duration overflow for {}{}", num, unit))
    })
}

#[derive(Debug, Clone, PartialEq)]
pub struct TimeStep {
    pub duration: Duration,
}

impl TimeStep {
    pub fn parse(input: &str) -> Result<Self, ParseError> {
        parse_duration(input).map(|duration| TimeStep { duration })
    }

    pub fn default() -> Self {
        TimeStep {
            // Safe: 1 hour is always valid
            duration: Duration::try_hours(1).expect("1 hour duration should be valid"),
        }
    }
}

struct TimeSeriesIterator {
    current_utc: DateTime<chrono::Utc>,
    end_utc: DateTime<chrono::Utc>,
    step: Duration,
    timezone_spec: Option<TimezoneSpec>,
    buffered_ambiguous: Option<DateTime<FixedOffset>>,
}

impl TimeSeriesIterator {
    fn new(
        start_naive: NaiveDateTime,
        end_naive: NaiveDateTime,
        step: Duration,
        timezone_spec: Option<TimezoneSpec>,
    ) -> Self {
        let start_dt = if let Some(ref tz_spec) = timezone_spec {
            tz_spec.apply_to_naive(start_naive).ok()
        } else {
            apply_timezone_to_datetime(start_naive, None).ok()
        };

        let end_dt = if let Some(ref tz_spec) = timezone_spec {
            tz_spec.apply_to_naive(end_naive).ok()
        } else {
            apply_timezone_to_datetime(end_naive, None).ok()
        };

        let program_start = crate::types::datetime_input_to_single_with_timezone(
            crate::types::DateTimeInput::Now,
            None,
        )
        .with_timezone(&chrono::Utc);
        let current_utc = start_dt
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or(program_start);
        let end_utc = end_dt
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or(program_start);

        Self {
            current_utc,
            end_utc,
            step,
            timezone_spec,
            buffered_ambiguous: None,
        }
    }
}

impl Iterator for TimeSeriesIterator {
    type Item = DateTime<FixedOffset>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(buffered) = self.buffered_ambiguous.take() {
            return Some(buffered);
        }

        if self.current_utc > self.end_utc {
            return None;
        }

        let result = match &self.timezone_spec {
            Some(TimezoneSpec::Named(tz)) => {
                use chrono::TimeZone;
                Some(
                    tz.from_utc_datetime(&self.current_utc.naive_utc())
                        .fixed_offset(),
                )
            }
            Some(TimezoneSpec::Fixed(offset)) => {
                use chrono::TimeZone;
                Some(offset.from_utc_datetime(&self.current_utc.naive_utc()))
            }
            None => {
                use chrono::TimeZone;
                let tz = get_system_timezone();
                Some(
                    tz.from_utc_datetime(&self.current_utc.naive_utc())
                        .fixed_offset(),
                )
            }
        };

        self.current_utc += self.step;

        result
    }
}

struct WatchIterator {
    step: Duration,
    timezone_spec: Option<TimezoneSpec>,
    first: bool,
}

impl WatchIterator {
    fn new(step: Duration, timezone_spec: Option<TimezoneSpec>) -> Self {
        Self {
            step,
            timezone_spec,
            first: true,
        }
    }
}

impl Iterator for WatchIterator {
    type Item = DateTime<FixedOffset>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.first {
            std::thread::sleep(self.step.to_std().ok()?);
        }
        self.first = false;

        let now = if let Some(ref tz_spec) = self.timezone_spec {
            let now_naive = chrono::Utc::now().naive_utc();
            tz_spec.apply_to_naive(now_naive).ok()?
        } else {
            Local::now().fixed_offset()
        };

        Some(now)
    }
}

pub fn expand_datetime_input(
    input: &DateTimeInput,
    step: &TimeStep,
    timezone_spec: Option<TimezoneSpec>,
) -> Result<Box<dyn Iterator<Item = DateTime<FixedOffset>>>, ParseError> {
    expand_datetime_input_with_watch(input, step, timezone_spec, false)
}

pub fn expand_datetime_input_with_watch(
    input: &DateTimeInput,
    step: &TimeStep,
    timezone_spec: Option<TimezoneSpec>,
    watch_mode: bool,
) -> Result<Box<dyn Iterator<Item = DateTime<FixedOffset>>>, ParseError> {
    match input {
        DateTimeInput::Single(dt) => {
            let adjusted_dt = if let Some(ref tz_spec) = timezone_spec {
                match tz_spec {
                    TimezoneSpec::Fixed(offset) => dt.with_timezone(offset),
                    TimezoneSpec::Named(_) => tz_spec.apply_to_naive(dt.naive_local())?,
                }
            } else {
                *dt
            };
            Ok(Box::new(std::iter::once(adjusted_dt)))
        }
        DateTimeInput::Now => {
            if watch_mode {
                Ok(Box::new(WatchIterator::new(step.duration, timezone_spec)))
            } else {
                let now = crate::types::datetime_input_to_single_with_timezone(
                    crate::types::DateTimeInput::Now,
                    timezone_spec,
                );
                Ok(Box::new(std::iter::once(now)))
            }
        }
        DateTimeInput::PartialYear(year) => {
            let start_date = NaiveDate::from_ymd_opt(*year, 1, 1)
                .ok_or_else(|| ParseError::InvalidDateTime(format!("Invalid year: {}", year)))?;
            let end_date = NaiveDate::from_ymd_opt(*year, 12, 31)
                .ok_or_else(|| ParseError::InvalidDateTime(format!("Invalid year: {}", year)))?;

            let start_naive = start_date.and_hms_opt(0, 0, 0).ok_or_else(|| {
                ParseError::InvalidDateTime(format!("Invalid date: {:?}", start_date))
            })?;
            let end_naive = end_date.and_hms_opt(23, 59, 59).ok_or_else(|| {
                ParseError::InvalidDateTime(format!("Invalid date: {:?}", end_date))
            })?;

            Ok(Box::new(TimeSeriesIterator::new(
                start_naive,
                end_naive,
                step.duration,
                timezone_spec,
            )))
        }
        DateTimeInput::PartialYearMonth(year, month) => {
            let start_date = NaiveDate::from_ymd_opt(*year, *month, 1).ok_or_else(|| {
                ParseError::InvalidDateTime(format!("Invalid year-month: {}-{:02}", year, month))
            })?;

            let end_date = if *month == 12 {
                NaiveDate::from_ymd_opt(*year + 1, 1, 1)
                    .and_then(|d| d.pred_opt())
                    .ok_or_else(|| {
                        ParseError::InvalidDateTime(format!(
                            "Invalid date calculation for year {}",
                            year
                        ))
                    })?
            } else {
                NaiveDate::from_ymd_opt(*year, *month + 1, 1)
                    .and_then(|d| d.pred_opt())
                    .ok_or_else(|| {
                        ParseError::InvalidDateTime(format!(
                            "Invalid date calculation for {}-{:02}",
                            year, month
                        ))
                    })?
            };

            let start_naive = start_date.and_hms_opt(0, 0, 0).ok_or_else(|| {
                ParseError::InvalidDateTime(format!("Invalid date: {:?}", start_date))
            })?;
            let end_naive = end_date.and_hms_opt(23, 59, 59).ok_or_else(|| {
                ParseError::InvalidDateTime(format!("Invalid date: {:?}", end_date))
            })?;

            Ok(Box::new(TimeSeriesIterator::new(
                start_naive,
                end_naive,
                step.duration,
                timezone_spec,
            )))
        }
        DateTimeInput::PartialDate(year, month, day) => {
            let target_date = NaiveDate::from_ymd_opt(*year, *month, *day).ok_or_else(|| {
                ParseError::InvalidDateTime(format!(
                    "Invalid date: {}-{:02}-{:02}",
                    year, month, day
                ))
            })?;

            let start_naive = target_date.and_hms_opt(0, 0, 0).ok_or_else(|| {
                ParseError::InvalidDateTime(format!("Invalid date: {:?}", target_date))
            })?;
            let end_naive = target_date.and_hms_opt(23, 0, 0).ok_or_else(|| {
                ParseError::InvalidDateTime(format!("Invalid date: {:?}", target_date))
            })?;

            Ok(Box::new(TimeSeriesIterator::new(
                start_naive,
                end_naive,
                step.duration,
                timezone_spec,
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, Timelike};

    #[test]
    fn test_time_step_parsing() {
        assert_eq!(
            TimeStep::parse("30s").unwrap().duration,
            Duration::try_seconds(30).unwrap()
        );
        assert_eq!(
            TimeStep::parse("15m").unwrap().duration,
            Duration::try_minutes(15).unwrap()
        );
        assert_eq!(
            TimeStep::parse("2h").unwrap().duration,
            Duration::try_hours(2).unwrap()
        );
        assert_eq!(
            TimeStep::parse("600").unwrap().duration,
            Duration::try_seconds(600).unwrap()
        );
        assert_eq!(
            TimeStep::parse("1d").unwrap().duration,
            Duration::try_days(1).unwrap()
        );

        assert!(TimeStep::parse("").is_err());
        assert!(TimeStep::parse("30x").is_err());
        assert!(TimeStep::parse("-5m").is_err());
    }

    #[test]
    fn test_duration_parsing_comprehensive() {
        // Valid durations with all unit variations
        assert!(parse_duration("30s").is_ok());
        assert!(parse_duration("30sec").is_ok());
        assert!(parse_duration("30second").is_ok());
        assert!(parse_duration("30seconds").is_ok());
        assert!(parse_duration("15m").is_ok());
        assert!(parse_duration("15min").is_ok());
        assert!(parse_duration("15minute").is_ok());
        assert!(parse_duration("15minutes").is_ok());
        assert!(parse_duration("2h").is_ok());
        assert!(parse_duration("2hr").is_ok());
        assert!(parse_duration("2hour").is_ok());
        assert!(parse_duration("2hours").is_ok());
        assert!(parse_duration("1d").is_ok());
        assert!(parse_duration("1day").is_ok());
        assert!(parse_duration("7days").is_ok());

        // Case insensitive
        assert!(parse_duration("30S").is_ok());
        assert!(parse_duration("15MIN").is_ok());
        assert!(parse_duration("2Hours").is_ok());

        // Plain numbers (seconds)
        assert!(parse_duration("300").is_ok());
        assert!(parse_duration("3600").is_ok());

        // Whitespace handling
        assert!(parse_duration("30 s").is_ok());
        assert!(parse_duration(" 30s").is_ok());
        assert!(parse_duration("30s ").is_ok());
    }

    #[test]
    fn test_duration_parsing_error_messages() {
        // Empty input
        let err = parse_duration("").unwrap_err();
        if let ParseError::InvalidDateTime(msg) = err {
            assert!(msg.contains("Empty duration string"));
            assert!(msg.contains("Expected formats"));
        } else {
            panic!("Expected InvalidDateTime error");
        }

        // Invalid unit
        let err = parse_duration("30x").unwrap_err();
        if let ParseError::InvalidDateTime(msg) = err {
            assert!(msg.contains("Unknown time unit 'x'"));
            assert!(msg.contains("Supported units"));
        } else {
            panic!("Expected InvalidDateTime error");
        }

        // Negative duration
        let err = parse_duration("-5m").unwrap_err();
        if let ParseError::InvalidDateTime(msg) = err {
            assert!(msg.contains("Duration must be positive"));
            assert!(msg.contains("-5m"));
        } else {
            panic!("Expected InvalidDateTime error");
        }

        // Zero duration
        let err = parse_duration("0s").unwrap_err();
        if let ParseError::InvalidDateTime(msg) = err {
            assert!(msg.contains("Duration must be positive"));
        } else {
            panic!("Expected InvalidDateTime error");
        }

        // Missing number
        let err = parse_duration("m").unwrap_err();
        if let ParseError::InvalidDateTime(msg) = err {
            assert!(msg.contains("Missing number before unit"));
        } else {
            panic!("Expected InvalidDateTime error");
        }

        // Invalid number
        let err = parse_duration("1.5h").unwrap_err();
        if let ParseError::InvalidDateTime(msg) = err {
            assert!(msg.contains("Invalid number '1.5'"));
            assert!(msg.contains("Expected integer"));
        } else {
            panic!("Expected InvalidDateTime error");
        }

        // Overflow duration
        let err = parse_duration("9999999999999d").unwrap_err();
        if let ParseError::InvalidDateTime(msg) = err {
            assert!(msg.contains("exceeds maximum"));
        } else {
            panic!("Expected InvalidDateTime error");
        }

        // Invalid format (no unit indicator)
        let err = parse_duration("30.5").unwrap_err();
        if let ParseError::InvalidDateTime(msg) = err {
            assert!(msg.contains("Invalid duration format"));
        } else {
            panic!("Expected InvalidDateTime error");
        }
    }

    #[test]
    fn test_time_series_iterator() {
        let start_naive = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let end_naive = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(2, 0, 0)
            .unwrap();
        let iter = TimeSeriesIterator::new(
            start_naive,
            end_naive,
            Duration::try_hours(1).unwrap(),
            None,
        );

        let mut iter = iter;
        let first = iter.next().unwrap();
        let second = iter.next().unwrap();
        let third = iter.next().unwrap();
        assert!(iter.next().is_none());

        assert_eq!(first.time().hour(), 0);
        assert_eq!(second.time().hour(), 1);
        assert_eq!(third.time().hour(), 2);
    }

    #[test]
    fn test_expand_partial_year() {
        let step = TimeStep::default();
        let input = DateTimeInput::PartialYear(2024);
        let iter = expand_datetime_input(&input, &step, None).unwrap();
        let mut iter = iter.take(25);

        let first = iter.next().unwrap();
        assert_eq!(
            first.date_naive(),
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
        );
        assert_eq!(first.time().hour(), 0);

        let count = iter.count();
        assert_eq!(count, 24); // 24 more after the first

        // Test the 25th element separately
        let mut iter = expand_datetime_input(&input, &step, None).unwrap().take(25);
        let last = iter.nth(24).unwrap();
        assert_eq!(last.time().hour(), 0);
        assert_eq!(
            last.date_naive(),
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()
        );
    }
}

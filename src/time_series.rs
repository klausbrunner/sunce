use crate::parsing::{DateTimeInput, ParseError};
use crate::timezone::apply_timezone_to_datetime;
use chrono::{DateTime, Duration, FixedOffset, Local, NaiveDate, NaiveDateTime, TimeZone};

#[derive(Debug, Clone, PartialEq)]
pub struct TimeStep {
    pub duration: Duration,
}

impl TimeStep {
    pub fn parse(input: &str) -> Result<Self, ParseError> {
        if input.is_empty() {
            return Err(ParseError::InvalidDateTime(
                "Empty step interval".to_string(),
            ));
        }

        if let Ok(seconds) = input.parse::<i64>() {
            if seconds <= 0 {
                return Err(ParseError::InvalidDateTime(
                    "Step interval must be positive".to_string(),
                ));
            }
            return Duration::try_seconds(seconds)
                .map(|d| TimeStep { duration: d })
                .ok_or_else(|| {
                    ParseError::InvalidDateTime(format!("Duration overflow: {}", input))
                });
        }

        let (num_str, unit) =
            input.split_at(input.find(|c: char| c.is_alphabetic()).ok_or_else(|| {
                ParseError::InvalidDateTime(format!("Invalid step format: {}", input))
            })?);

        let num: i64 = num_str
            .parse()
            .map_err(|_| ParseError::InvalidDateTime(format!("Invalid number: {}", num_str)))?;

        if num <= 0 {
            return Err(ParseError::InvalidDateTime(
                "Step interval must be positive".to_string(),
            ));
        }

        let duration = match unit.to_lowercase().as_str() {
            "s" | "sec" | "second" | "seconds" => Duration::try_seconds(num),
            "m" | "min" | "minute" | "minutes" => Duration::try_minutes(num),
            "h" | "hr" | "hour" | "hours" => Duration::try_hours(num),
            "d" | "day" | "days" => Duration::try_days(num),
            _ => {
                return Err(ParseError::InvalidDateTime(format!(
                    "Unknown time unit: {}",
                    unit
                )));
            }
        };

        duration
            .map(|d| TimeStep { duration: d })
            .ok_or_else(|| ParseError::InvalidDateTime(format!("Duration overflow: {}", input)))
    }

    pub fn default() -> Self {
        TimeStep {
            duration: Duration::try_hours(1).unwrap(),
        }
    }
}

struct TimeSeriesIterator {
    current_naive: NaiveDateTime,
    end_naive: NaiveDateTime,
    step: Duration,
    timezone_override: Option<FixedOffset>,
}

impl TimeSeriesIterator {
    fn new(
        start_naive: NaiveDateTime,
        end_naive: NaiveDateTime,
        step: Duration,
        timezone_override: Option<FixedOffset>,
    ) -> Self {
        Self {
            current_naive: start_naive,
            end_naive,
            step,
            timezone_override,
        }
    }
}

impl Iterator for TimeSeriesIterator {
    type Item = DateTime<FixedOffset>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_naive > self.end_naive {
            return None;
        }

        let dt = if let Some(tz) = self.timezone_override {
            tz.from_local_datetime(&self.current_naive).single()?
        } else {
            apply_timezone_to_datetime(self.current_naive, None).ok()?
        };

        self.current_naive += self.step;
        Some(dt)
    }
}

struct WatchIterator {
    step: Duration,
    timezone_override: Option<FixedOffset>,
    first: bool,
}

impl WatchIterator {
    fn new(step: Duration, timezone_override: Option<FixedOffset>) -> Self {
        Self {
            step,
            timezone_override,
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

        let now = if let Some(tz) = self.timezone_override {
            chrono::Utc::now().with_timezone(&tz)
        } else {
            Local::now().fixed_offset()
        };

        Some(now)
    }
}

pub fn expand_datetime_input(
    input: &DateTimeInput,
    step: &TimeStep,
    timezone_override: Option<FixedOffset>,
) -> Result<Box<dyn Iterator<Item = DateTime<FixedOffset>>>, ParseError> {
    expand_datetime_input_with_watch(input, step, timezone_override, false)
}

pub fn expand_datetime_input_with_watch(
    input: &DateTimeInput,
    step: &TimeStep,
    timezone_override: Option<FixedOffset>,
    watch_mode: bool,
) -> Result<Box<dyn Iterator<Item = DateTime<FixedOffset>>>, ParseError> {
    match input {
        DateTimeInput::Single(dt) => {
            let adjusted_dt = if let Some(tz) = timezone_override {
                dt.with_timezone(&tz)
            } else {
                *dt
            };
            Ok(Box::new(std::iter::once(adjusted_dt)))
        }
        DateTimeInput::Now => {
            if watch_mode {
                Ok(Box::new(WatchIterator::new(
                    step.duration,
                    timezone_override,
                )))
            } else {
                let now = if let Some(tz) = timezone_override {
                    chrono::Utc::now().with_timezone(&tz)
                } else {
                    Local::now().fixed_offset()
                };
                Ok(Box::new(std::iter::once(now)))
            }
        }
        DateTimeInput::PartialYear(year) => {
            let start_date = NaiveDate::from_ymd_opt(*year, 1, 1)
                .ok_or_else(|| ParseError::InvalidDateTime(format!("Invalid year: {}", year)))?;
            let end_date = NaiveDate::from_ymd_opt(*year, 12, 31)
                .ok_or_else(|| ParseError::InvalidDateTime(format!("Invalid year: {}", year)))?;

            let start_naive = start_date.and_hms_opt(0, 0, 0).unwrap();
            let end_naive = end_date.and_hms_opt(23, 59, 59).unwrap();

            Ok(Box::new(TimeSeriesIterator::new(
                start_naive,
                end_naive,
                step.duration,
                timezone_override,
            )))
        }
        DateTimeInput::PartialYearMonth(year, month) => {
            let start_date = NaiveDate::from_ymd_opt(*year, *month, 1).ok_or_else(|| {
                ParseError::InvalidDateTime(format!("Invalid year-month: {}-{:02}", year, month))
            })?;

            let end_date = if *month == 12 {
                NaiveDate::from_ymd_opt(*year + 1, 1, 1)
                    .unwrap()
                    .pred_opt()
                    .unwrap()
            } else {
                NaiveDate::from_ymd_opt(*year, *month + 1, 1)
                    .unwrap()
                    .pred_opt()
                    .unwrap()
            };

            let start_naive = start_date.and_hms_opt(0, 0, 0).unwrap();
            let end_naive = end_date.and_hms_opt(23, 59, 59).unwrap();

            Ok(Box::new(TimeSeriesIterator::new(
                start_naive,
                end_naive,
                step.duration,
                timezone_override,
            )))
        }
        DateTimeInput::PartialDate(year, month, day) => {
            let target_date = NaiveDate::from_ymd_opt(*year, *month, *day).ok_or_else(|| {
                ParseError::InvalidDateTime(format!(
                    "Invalid date: {}-{:02}-{:02}",
                    year, month, day
                ))
            })?;

            let start_naive = target_date.and_hms_opt(0, 0, 0).unwrap();
            let end_naive = target_date.and_hms_opt(23, 0, 0).unwrap();

            Ok(Box::new(TimeSeriesIterator::new(
                start_naive,
                end_naive,
                step.duration,
                timezone_override,
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

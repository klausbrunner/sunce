use crate::parsing::{DateTimeInput, ParseError};
use crate::timezone_utils::{
    get_system_timezone, naive_to_fixed_offset, naive_to_specific_timezone,
};
use chrono::{DateTime, Duration, FixedOffset, Local, NaiveDate, NaiveDateTime, NaiveTime};

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

        // Check if input is just a number (solarpos compatibility - assumes seconds)
        if let Ok(seconds) = input.parse::<i64>() {
            if seconds <= 0 {
                return Err(ParseError::InvalidDateTime(
                    "Step interval must be positive".to_string(),
                ));
            }
            return match Duration::try_seconds(seconds) {
                Some(d) => Ok(TimeStep { duration: d }),
                None => Err(ParseError::InvalidDateTime(format!(
                    "Duration overflow for step: {}",
                    input
                ))),
            };
        }

        let (num_str, unit) = if let Some(pos) = input.find(|c: char| c.is_alphabetic()) {
            (&input[..pos], &input[pos..])
        } else {
            return Err(ParseError::InvalidDateTime(format!(
                "Invalid step format: {}. Expected format like '30s', '15m', '2h' or raw seconds",
                input
            )));
        };

        let num: i64 = num_str.parse().map_err(|_| {
            ParseError::InvalidDateTime(format!("Invalid number in step: {}", num_str))
        })?;

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
                    "Unknown time unit: {}. Use s, m, h, or d",
                    unit
                )));
            }
        };

        match duration {
            Some(d) => Ok(TimeStep { duration: d }),
            None => Err(ParseError::InvalidDateTime(format!(
                "Duration overflow for step: {}",
                input
            ))),
        }
    }

    pub fn default() -> Self {
        TimeStep {
            duration: Duration::try_hours(1).unwrap(),
        }
    }
}

fn to_local_datetime(
    naive: NaiveDateTime,
    timezone_override: Option<FixedOffset>,
) -> Result<DateTime<FixedOffset>, ParseError> {
    if let Some(tz) = timezone_override {
        // Use the specified timezone override
        naive_to_fixed_offset(naive, &tz)
    } else {
        // Use system timezone with proper DST handling
        let system_tz = get_system_timezone();
        naive_to_specific_timezone(naive, &system_tz)
    }
}

pub struct DstAwareTimeSeriesIterator {
    current_naive: NaiveDateTime,
    end_naive: NaiveDateTime,
    step: Duration,
    finished: bool,
    timezone_override: Option<FixedOffset>,
}

impl DstAwareTimeSeriesIterator {
    pub fn new(
        start_naive: NaiveDateTime,
        end_naive: NaiveDateTime,
        step: Duration,
        timezone_override: Option<FixedOffset>,
    ) -> Self {
        Self {
            current_naive: start_naive,
            end_naive,
            step,
            finished: false,
            timezone_override,
        }
    }
}

impl Iterator for DstAwareTimeSeriesIterator {
    type Item = DateTime<FixedOffset>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished || self.current_naive > self.end_naive {
            return None;
        }

        loop {
            // Try to convert current naive time to local datetime
            match to_local_datetime(self.current_naive, self.timezone_override) {
                Ok(dt) => {
                    let result = dt;

                    // Check if this is the last iteration
                    if self.current_naive == self.end_naive {
                        self.finished = true;
                    } else {
                        self.current_naive += self.step;
                        // If we've passed the end, set to end for final iteration
                        if self.current_naive > self.end_naive {
                            self.current_naive = self.end_naive;
                        }
                    }

                    return Some(result);
                }
                Err(_) => {
                    // This time doesn't exist (DST spring-forward), skip it
                    self.current_naive += self.step;
                    if self.current_naive > self.end_naive {
                        self.finished = true;
                        return None;
                    }
                    continue;
                }
            }
        }
    }
}

pub fn expand_datetime_input(
    input: &DateTimeInput,
    step: &TimeStep,
    timezone_override: Option<FixedOffset>,
) -> Result<Box<dyn Iterator<Item = DateTime<FixedOffset>>>, ParseError> {
    match input {
        DateTimeInput::Single(dt) => {
            // For single datetime, apply timezone override if specified
            if let Some(tz) = timezone_override {
                let adjusted_dt = dt.with_timezone(&tz);
                Ok(Box::new(std::iter::once(adjusted_dt)))
            } else {
                Ok(Box::new(std::iter::once(*dt)))
            }
        }
        DateTimeInput::Now => {
            // For "now", apply timezone override if specified
            if let Some(tz) = timezone_override {
                let now = chrono::Utc::now().with_timezone(&tz);
                Ok(Box::new(std::iter::once(now)))
            } else {
                let now = Local::now().fixed_offset();
                Ok(Box::new(std::iter::once(now)))
            }
        }
        DateTimeInput::PartialYear(year) => {
            // Generate series for entire year
            let start_date = NaiveDate::from_ymd_opt(*year, 1, 1)
                .ok_or_else(|| ParseError::InvalidDateTime(format!("Invalid year: {}", year)))?;
            let end_date = NaiveDate::from_ymd_opt(*year, 12, 31)
                .ok_or_else(|| ParseError::InvalidDateTime(format!("Invalid year: {}", year)))?;

            let start_time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
            let end_time = NaiveTime::from_hms_opt(23, 59, 59).unwrap();

            let start_naive = start_date.and_time(start_time);
            let end_naive = end_date.and_time(end_time);

            Ok(Box::new(DstAwareTimeSeriesIterator::new(
                start_naive,
                end_naive,
                step.duration,
                timezone_override,
            )))
        }
        DateTimeInput::PartialYearMonth(year, month) => {
            // Generate series for entire month
            let start_date = NaiveDate::from_ymd_opt(*year, *month, 1).ok_or_else(|| {
                ParseError::InvalidDateTime(format!("Invalid year-month: {}-{:02}", year, month))
            })?;

            // Calculate last day of month
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

            let start_time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
            let end_time = NaiveTime::from_hms_opt(23, 59, 59).unwrap();

            let start_naive = start_date.and_time(start_time);
            let end_naive = end_date.and_time(end_time);

            Ok(Box::new(DstAwareTimeSeriesIterator::new(
                start_naive,
                end_naive,
                step.duration,
                timezone_override,
            )))
        }
        DateTimeInput::PartialDate(year, month, day) => {
            // Generate series for a single day
            let target_date = NaiveDate::from_ymd_opt(*year, *month, *day).ok_or_else(|| {
                ParseError::InvalidDateTime(format!(
                    "Invalid date: {}-{:02}-{:02}",
                    year, month, day
                ))
            })?;

            let start_time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
            let end_time = NaiveTime::from_hms_opt(23, 0, 0).unwrap();

            let start_naive = target_date.and_time(start_time);
            let end_naive = target_date.and_time(end_time);

            Ok(Box::new(DstAwareTimeSeriesIterator::new(
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
    use chrono::{NaiveDate, NaiveTime, Timelike};

    #[test]
    fn test_time_step_parsing() {
        // Test duration format
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

        // Test solarpos compatibility - raw seconds
        assert_eq!(
            TimeStep::parse("600").unwrap().duration,
            Duration::try_seconds(600).unwrap()
        );
        assert_eq!(
            TimeStep::parse("30").unwrap().duration,
            Duration::try_seconds(30).unwrap()
        );
        assert_eq!(
            TimeStep::parse("1d").unwrap().duration,
            Duration::try_days(1).unwrap()
        );

        // Test error cases
        assert!(TimeStep::parse("").is_err());
        assert!(TimeStep::parse("30x").is_err());
        assert!(TimeStep::parse("-5m").is_err());
    }

    #[test]
    fn test_time_series_iterator() {
        let start_naive = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let end_naive = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_time(NaiveTime::from_hms_opt(2, 0, 0).unwrap());
        let step = Duration::try_hours(1).unwrap();

        let iter = DstAwareTimeSeriesIterator::new(start_naive, end_naive, step, None);

        let times: Vec<_> = iter.collect();
        assert_eq!(times.len(), 3); // 00:00, 01:00, 02:00
        assert_eq!(times[0].time().hour(), 0);
        assert_eq!(times[1].time().hour(), 1);
        assert_eq!(times[2].time().hour(), 2);
    }

    #[test]
    fn test_expand_partial_year() {
        let step = TimeStep::default();
        let input = DateTimeInput::PartialYear(2024);

        let iter = expand_datetime_input(&input, &step, None).unwrap();
        let times: Vec<_> = iter.take(25).collect(); // Take first 25 hours

        assert_eq!(times.len(), 25);
        assert_eq!(
            times[0].date_naive(),
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
        );
        assert_eq!(times[0].time().hour(), 0);
        assert_eq!(times[24].time().hour(), 0); // Second day, hour 0
        assert_eq!(
            times[24].date_naive(),
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()
        );
    }
}

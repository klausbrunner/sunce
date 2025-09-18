use crate::parsing::DateTimeInput;
use chrono::{DateTime, FixedOffset, TimeZone};
pub fn datetime_input_to_single(datetime_input: DateTimeInput) -> DateTime<FixedOffset> {
    match datetime_input {
        DateTimeInput::Single(dt) => dt,
        DateTimeInput::Now => chrono::Utc::now().into(),
        DateTimeInput::PartialYear(year) => chrono::FixedOffset::east_opt(0)
            .unwrap()
            .with_ymd_and_hms(year, 1, 1, 0, 0, 0)
            .unwrap(),
        DateTimeInput::PartialYearMonth(year, month) => chrono::FixedOffset::east_opt(0)
            .unwrap()
            .with_ymd_and_hms(year, month, 1, 0, 0, 0)
            .unwrap(),
        DateTimeInput::PartialDate(year, month, day) => chrono::FixedOffset::east_opt(0)
            .unwrap()
            .with_ymd_and_hms(year, month, day, 0, 0, 0)
            .unwrap(),
    }
}

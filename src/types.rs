use crate::timezone::apply_timezone_to_datetime;
use chrono::{DateTime, FixedOffset, TimeZone};
use std::fmt;
use std::sync::OnceLock;
use thiserror::Error;

// Capture "now" once when the program starts
static PROGRAM_START_TIME: OnceLock<DateTime<FixedOffset>> = OnceLock::new();

fn get_program_start_time() -> DateTime<FixedOffset> {
    *PROGRAM_START_TIME.get_or_init(|| chrono::Utc::now().into())
}

/// Format datetime to match solarpos format (no subseconds)
pub fn format_datetime_solarpos(dt: &DateTime<FixedOffset>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S%:z").to_string()
}

#[derive(Debug, Clone, PartialEq)]
pub enum OutputFormat {
    Human,
    Csv,
    Json,
    Parquet,
}

impl OutputFormat {
    pub fn from_string(s: &str) -> Result<Self, String> {
        match s.to_uppercase().as_str() {
            "HUMAN" => Ok(Self::Human),
            "CSV" => Ok(Self::Csv),
            "JSON" => Ok(Self::Json),
            "PARQUET" => {
                #[cfg(feature = "parquet")]
                {
                    Ok(Self::Parquet)
                }
                #[cfg(not(feature = "parquet"))]
                {
                    Err("PARQUET format not available in minimal build. Available formats: HUMAN, CSV, JSON. Rebuild with: cargo install sunce --features parquet".to_string())
                }
            }
            _ => {
                #[cfg(feature = "parquet")]
                let available = "HUMAN, CSV, JSON, or PARQUET";
                #[cfg(not(feature = "parquet"))]
                let available = "HUMAN, CSV, or JSON";

                Err(format!("Unknown format: {}. Use {}", s, available))
            }
        }
    }
}

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
    #[error("Step cannot be zero")]
    ZeroStep,
}

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Parse error: {0}")]
    Parse(#[from] ParseError),
    #[error("Input/Output error: {0}")]
    Io(#[from] std::io::Error),
    #[error("General error: {0}")]
    General(String),
}

impl From<String> for AppError {
    fn from(s: String) -> Self {
        AppError::General(s)
    }
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
    PartialDate(i32, u32, u32),
}

#[derive(Debug, Clone)]
pub enum InputType {
    Standard,
    CoordinateFile,
    TimeFile,
    PairedDataFile,
    StdinCoords,
    StdinTimes,
    StdinPaired,
    CoordinateFileTimeFile,
}

#[derive(Debug, Clone)]
pub struct ParsedInput {
    pub input_type: InputType,
    pub latitude: String,
    pub longitude: Option<String>,
    pub datetime: Option<String>,
    pub global_options: GlobalOptions,
    pub parsed_latitude: Option<Coordinate>,
    pub parsed_longitude: Option<Coordinate>,
    pub parsed_datetime: Option<DateTimeInput>,
}

#[derive(Debug, Clone)]
pub struct GlobalOptions {
    pub deltat: Option<String>,
    pub format: Option<String>,
    pub headers: Option<bool>,
    pub show_inputs: Option<bool>,
    pub timezone: Option<String>,
}

#[derive(Debug)]
pub struct PositionOptions {
    pub algorithm: Option<String>,
    pub elevation: Option<String>,
    pub pressure: Option<String>,
    pub temperature: Option<String>,
    pub elevation_angle: bool,
    pub refraction: Option<bool>,
}

#[derive(Debug)]
pub struct SunriseOptions {
    pub twilight: bool,
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
            DateTimeInput::Now => write!(f, "now"),
            DateTimeInput::PartialYear(year) => write!(f, "{}", year),
            DateTimeInput::PartialYearMonth(year, month) => write!(f, "{:04}-{:02}", year, month),
            DateTimeInput::PartialDate(year, month, day) => {
                write!(f, "{:04}-{:02}-{:02}", year, month, day)
            }
        }
    }
}

/// Apply timezone to naive datetime with fallback to UTC
fn apply_timezone_with_fallback(
    naive_dt: chrono::NaiveDateTime,
    timezone_spec: Option<crate::timezone::TimezoneSpec>,
) -> DateTime<FixedOffset> {
    let result = if let Some(spec) = timezone_spec {
        spec.apply_to_naive(naive_dt)
    } else {
        apply_timezone_to_datetime(naive_dt, None)
    };

    result.unwrap_or_else(|_| {
        chrono::FixedOffset::east_opt(0)
            .unwrap()
            .from_local_datetime(&naive_dt)
            .unwrap()
    })
}

/// Convert DateTimeInput to a concrete DateTime<FixedOffset> with timezone support
pub fn datetime_input_to_single_with_timezone(
    datetime_input: DateTimeInput,
    timezone_spec: Option<crate::timezone::TimezoneSpec>,
) -> DateTime<FixedOffset> {
    match datetime_input {
        DateTimeInput::Single(dt) => dt,
        DateTimeInput::Now => get_program_start_time(),
        DateTimeInput::PartialYear(year) => {
            let naive_dt = chrono::NaiveDateTime::new(
                chrono::NaiveDate::from_ymd_opt(year, 1, 1)
                    .expect("Invalid year in datetime input"),
                chrono::NaiveTime::from_hms_opt(0, 0, 0).expect("Failed to create midnight time"),
            );
            apply_timezone_with_fallback(naive_dt, timezone_spec)
        }
        DateTimeInput::PartialYearMonth(year, month) => {
            let naive_dt = chrono::NaiveDateTime::new(
                chrono::NaiveDate::from_ymd_opt(year, month, 1)
                    .expect("Invalid year/month in datetime input"),
                chrono::NaiveTime::from_hms_opt(0, 0, 0).expect("Failed to create midnight time"),
            );
            apply_timezone_with_fallback(naive_dt, timezone_spec)
        }
        DateTimeInput::PartialDate(year, month, day) => {
            let naive_dt = chrono::NaiveDateTime::new(
                chrono::NaiveDate::from_ymd_opt(year, month, day)
                    .expect("Invalid date in datetime input"),
                chrono::NaiveTime::from_hms_opt(0, 0, 0).expect("Failed to create midnight time"),
            );
            apply_timezone_with_fallback(naive_dt, timezone_spec)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timezone::TimezoneSpec;
    use chrono::Datelike;

    #[test]
    fn test_datetime_input_to_single_with_timezone_valid() {
        let dt_input = DateTimeInput::PartialYear(2024);
        let result = datetime_input_to_single_with_timezone(dt_input, None);
        assert_eq!(result.year(), 2024);
        assert_eq!(result.month(), 1);
        assert_eq!(result.day(), 1);
    }

    #[test]
    fn test_datetime_input_to_single_with_timezone_with_offset() {
        let dt_input = DateTimeInput::PartialDate(2024, 6, 15);
        let offset = chrono::FixedOffset::east_opt(3600).unwrap(); // +01:00
        let tz_spec = TimezoneSpec::Fixed(offset);
        let result = datetime_input_to_single_with_timezone(dt_input, Some(tz_spec));
        assert_eq!(result.offset().local_minus_utc(), 3600);
    }

    #[test]
    fn test_apply_timezone_with_fallback_should_not_hide_errors() {
        // This test documents current behavior but shows the issue
        // When timezone application fails, it silently falls back to UTC
        let naive_dt = chrono::NaiveDateTime::new(
            chrono::NaiveDate::from_ymd_opt(2024, 3, 31).unwrap(),
            chrono::NaiveTime::from_hms_opt(2, 30, 0).unwrap(), // During DST transition
        );

        // Create an invalid timezone spec that would fail
        let result = apply_timezone_with_fallback(naive_dt, None);
        assert_eq!(result.offset().local_minus_utc(), 0); // Falls back to UTC

        // TODO: This test shows that errors are being swallowed
        // The function should either succeed properly or fail with clear error
    }
}

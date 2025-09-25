use chrono::{DateTime, FixedOffset, TimeZone};
use std::fmt;
use thiserror::Error;

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

/// Convert DateTimeInput to a concrete DateTime<FixedOffset> with timezone support
pub fn datetime_input_to_single_with_timezone(
    datetime_input: DateTimeInput,
    timezone_spec: Option<crate::timezone::TimezoneSpec>,
) -> DateTime<FixedOffset> {
    match datetime_input {
        DateTimeInput::Single(dt) => dt,
        DateTimeInput::Now => chrono::Utc::now().into(),
        DateTimeInput::PartialYear(year) => {
            let naive_dt = chrono::NaiveDateTime::new(
                chrono::NaiveDate::from_ymd_opt(year, 1, 1).unwrap(),
                chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            );
            if let Some(spec) = timezone_spec {
                spec.apply_to_naive(naive_dt).unwrap_or_else(|_| {
                    chrono::FixedOffset::east_opt(0)
                        .unwrap()
                        .from_local_datetime(&naive_dt)
                        .unwrap()
                })
            } else {
                crate::timezone::apply_timezone_to_datetime(naive_dt, None).unwrap_or_else(|_| {
                    chrono::FixedOffset::east_opt(0)
                        .unwrap()
                        .from_local_datetime(&naive_dt)
                        .unwrap()
                })
            }
        }
        DateTimeInput::PartialYearMonth(year, month) => {
            let naive_dt = chrono::NaiveDateTime::new(
                chrono::NaiveDate::from_ymd_opt(year, month, 1).unwrap(),
                chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            );
            if let Some(spec) = timezone_spec {
                spec.apply_to_naive(naive_dt).unwrap_or_else(|_| {
                    chrono::FixedOffset::east_opt(0)
                        .unwrap()
                        .from_local_datetime(&naive_dt)
                        .unwrap()
                })
            } else {
                crate::timezone::apply_timezone_to_datetime(naive_dt, None).unwrap_or_else(|_| {
                    chrono::FixedOffset::east_opt(0)
                        .unwrap()
                        .from_local_datetime(&naive_dt)
                        .unwrap()
                })
            }
        }
        DateTimeInput::PartialDate(year, month, day) => {
            let naive_dt = chrono::NaiveDateTime::new(
                chrono::NaiveDate::from_ymd_opt(year, month, day).unwrap(),
                chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            );
            if let Some(spec) = timezone_spec {
                spec.apply_to_naive(naive_dt).unwrap_or_else(|_| {
                    chrono::FixedOffset::east_opt(0)
                        .unwrap()
                        .from_local_datetime(&naive_dt)
                        .unwrap()
                })
            } else {
                crate::timezone::apply_timezone_to_datetime(naive_dt, None).unwrap_or_else(|_| {
                    chrono::FixedOffset::east_opt(0)
                        .unwrap()
                        .from_local_datetime(&naive_dt)
                        .unwrap()
                })
            }
        }
    }
}

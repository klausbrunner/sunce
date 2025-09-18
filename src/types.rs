use chrono::{DateTime, FixedOffset};
use std::fmt;
use thiserror::Error;

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
    #[error("Coordinate out of bounds: {0} (expected {1})")]
    CoordinateOutOfBounds(f64, String),
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

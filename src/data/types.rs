use chrono::{DateTime, FixedOffset};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq)]
pub enum InputPath {
    Stdin,
    File(PathBuf),
}

#[derive(Debug, Clone)]
pub enum LocationSource {
    Single(f64, f64),
    Range {
        lat: (f64, f64, f64),
        lon: Option<(f64, f64, f64)>,
    },
    File(InputPath),
}

#[derive(Debug, Clone)]
pub enum TimeSource {
    Single(String),
    Range(String, Option<String>),
    File(InputPath),
    Now,
}

#[derive(Debug, Clone)]
pub enum DataSource {
    Separate(LocationSource, TimeSource),
    Paired(InputPath),
}

impl DataSource {
    pub fn uses_stdin(&self) -> bool {
        match self {
            DataSource::Separate(loc, time) => {
                let loc_stdin = matches!(loc, LocationSource::File(InputPath::Stdin));
                let time_stdin = matches!(time, TimeSource::File(InputPath::Stdin));
                loc_stdin || time_stdin
            }
            DataSource::Paired(InputPath::Stdin) => true,
            DataSource::Paired(InputPath::File(_)) => false,
        }
    }

    pub fn is_watch_mode(&self, step: &Option<String>) -> bool {
        matches!(self, DataSource::Separate(_, TimeSource::Now)) && step.is_some()
    }
}

pub type CoordTime = (f64, f64, DateTime<FixedOffset>);
pub type CoordTimeResult = Result<CoordTime, String>;
pub type CoordTimeStream = Box<dyn Iterator<Item = CoordTimeResult>>;

pub type LocationResult = Result<(f64, f64), String>;
pub type LocationStream = Box<dyn Iterator<Item = LocationResult>>;

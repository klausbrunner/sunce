//! Core input and stream data types used across the program.

use super::time_utils::InputTime;
#[cfg(feature = "cli")]
use std::path::PathBuf;

#[cfg(feature = "cli")]
#[derive(Debug, Clone, PartialEq)]
pub enum InputPath {
    Stdin,
    File(PathBuf),
}

#[cfg(feature = "cli")]
#[derive(Debug, Clone)]
pub enum LocationSource {
    #[cfg(feature = "cli")]
    Single(f64, f64),
    Range {
        lat: (f64, f64, f64),
        lon: (f64, f64, f64),
    },
    #[cfg(feature = "cli")]
    File(InputPath),
}

#[cfg(feature = "cli")]
#[derive(Debug, Clone)]
pub enum TimeSource {
    Single(InputTime),
    Range(String),
    Daily(String, chrono::NaiveTime),
    #[cfg(feature = "cli")]
    File(InputPath),
    Now,
}

#[cfg(feature = "cli")]
#[derive(Debug, Clone)]
pub enum DataSource {
    Separate(LocationSource, TimeSource),
    Paired(InputPath),
}

#[cfg(feature = "cli")]
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

    pub fn is_watch_mode(&self, has_step: bool) -> bool {
        matches!(self, DataSource::Separate(_, TimeSource::Now)) && has_step
    }
}

pub type CoordTime = (f64, f64, InputTime);
pub type CoordTimeResult = Result<CoordTime, String>;
pub type CoordTimeStream<'a> = Box<dyn Iterator<Item = CoordTimeResult> + 'a>;

#[cfg(feature = "cli")]
pub type LocationResult = Result<(f64, f64), String>;
#[cfg(feature = "cli")]
pub type LocationStream = Box<dyn Iterator<Item = LocationResult>>;

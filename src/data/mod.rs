//! Shared data types, parsing helpers, and validation utilities.

pub mod config;
pub mod expansion;
pub mod time_utils;
pub mod types;
pub mod validation;

#[cfg(feature = "cli")]
pub use expansion::*;
#[cfg(feature = "cli")]
pub use time_utils::parse_datetime_string;
pub use types::*;
pub use validation::*;

#[cfg(feature = "cli")]
pub use config::{
    CalculationAlgorithm, Command, OutputFormat, Parameters, Predicate, Step, TimezoneOverride,
};

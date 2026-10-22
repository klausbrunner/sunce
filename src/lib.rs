#![doc = include_str!("../docs/library.md")]

#[cfg(feature = "cli")]
mod app;
#[cfg(feature = "cli")]
mod cli;
mod compute;
mod data;
mod error;
mod events;
pub mod input;
pub mod output;
#[cfg(feature = "parquet")]
pub mod parquet;
#[cfg(feature = "cli")]
mod parsed;
mod position;
#[cfg(feature = "cli")]
mod predicate;
#[cfg(feature = "cli")]
mod validate;

#[cfg(feature = "cli")]
pub use app::run;
pub use compute::{PositionRecord, events, positions};
pub use data::config::{CalculationAlgorithm, Environment, EventOptions, PositionOptions, Step};
pub use data::time_utils::TimezoneInfo as Timezone;
pub use error::Error;
pub use events::{DayState, EventKind, EventRecord};
pub use solar_positioning::{Location, SolarPosition};

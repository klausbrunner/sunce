//! Lazy calculations shared by library callers and the CLI.

use crate::events::calculate_events;
use crate::input::{EventInput, PositionInput};
use crate::position::{TIME_CACHE_CAPACITY, TimeCache, refraction_correction, time_cache_get};
use crate::{Error, EventOptions, EventRecord, Location, PositionOptions, SolarPosition};
use chrono::{DateTime, FixedOffset};
use std::collections::VecDeque;

#[cfg(feature = "cli")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SolarState {
    Daylight,
    CivilTwilight,
    NauticalTwilight,
    AstronomicalTwilight,
    Night,
}

/// A full-precision solar position and the input that produced it.
#[derive(Debug, Clone, Copy)]
pub struct PositionRecord {
    pub location: Location,
    pub datetime: DateTime<FixedOffset>,
    pub position: SolarPosition,
    pub deltat: f64,
}

/// Calculate positions lazily from a fallible input iterator.
///
/// Options are checked immediately; inputs are validated as they are pulled.
/// Dropping the iterator stops further work. For an array or vector of inputs,
/// use `inputs.into_iter().map(Ok)`.
pub fn positions<I>(
    input: I,
    options: PositionOptions,
) -> Result<impl Iterator<Item = Result<PositionRecord, Error>>, Error>
where
    I: IntoIterator<Item = Result<PositionInput, Error>>,
{
    options.validate()?;
    let refraction = refraction_correction(&options).map_err(Error::Input)?;
    let mut cache: TimeCache = TimeCache::default();
    let mut order = VecDeque::new();
    Ok(input.into_iter().map(move |item| {
        let input = item?.validate()?;
        let (prepared, deltat) = time_cache_get(
            &mut cache,
            &mut order,
            TIME_CACHE_CAPACITY,
            input.datetime,
            &options,
        )
        .map_err(Error::Calculation)?;
        let position = prepared
            .at(input.location, options.environment.elevation, refraction)
            .map_err(|err| {
                Error::Calculation(format!("Failed to calculate solar position: {err}"))
            })?;
        Ok(PositionRecord {
            location: input.location,
            datetime: input.datetime,
            position,
            deltat,
        })
    }))
}

/// Calculate events for explicit local dates and time zones.
///
/// Options are checked immediately. Each input is validated and calculated on
/// demand; event records for that date are yielded in chronological order.
pub fn events<I>(
    input: I,
    options: EventOptions,
) -> Result<impl Iterator<Item = Result<EventRecord, Error>>, Error>
where
    I: IntoIterator<Item = Result<EventInput, Error>>,
{
    options.validate()?;
    let mut input = input.into_iter();
    let mut pending = Vec::new().into_iter();
    Ok(std::iter::from_fn(move || {
        loop {
            if let Some(row) = pending.next() {
                return Some(Ok(row));
            }
            match input
                .next()?
                .and_then(EventInput::validate)
                .and_then(|input| {
                    calculate_events(
                        input.location.latitude,
                        input.location.longitude,
                        input.date,
                        input.timezone,
                        &options,
                    )
                    .map_err(Error::Calculation)
                }) {
                Ok(rows) => pending = rows.into_iter(),
                Err(err) => return Some(Err(err)),
            }
        }
    }))
}

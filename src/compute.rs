//! Stream orchestration and shared calculation result types.

use crate::data::{Command, CoordTimeStream, Parameters};
use crate::events::{EventRow, calculate_events};
use crate::position::{
    TIME_CACHE_CAPACITY, TimeCache, calculate_position_with_refraction, refraction_correction,
    time_cache_get,
};
use chrono::{DateTime, FixedOffset};
use solar_positioning::{Location, SolarPosition};
use std::collections::VecDeque;

type CalculationStream = Box<dyn Iterator<Item = Result<CalculationResult, String>>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SolarState {
    Daylight,
    CivilTwilight,
    NauticalTwilight,
    AstronomicalTwilight,
    Night,
}

pub enum CalculationResult {
    Position {
        lat: f64,
        lon: f64,
        datetime: DateTime<FixedOffset>,
        position: SolarPosition,
        deltat: f64,
    },
    Event(EventRow),
}

pub fn calculate_stream(
    data: CoordTimeStream,
    command: Command,
    params: Parameters,
    allow_time_cache: bool,
) -> CalculationStream {
    match command {
        Command::Position => {
            let refraction = match refraction_correction(&params) {
                Ok(value) => value,
                Err(err) => return Box::new(std::iter::once(Err(err))),
            };

            if allow_time_cache {
                let mut time_cache: TimeCache = TimeCache::default();
                let mut time_cache_order = VecDeque::new();

                Box::new(data.map(move |item| {
                    item.and_then(|(lat, lon, input)| {
                        let dt = input.datetime;
                        let (time_parts, deltat) = time_cache_get(
                            &mut time_cache,
                            &mut time_cache_order,
                            TIME_CACHE_CAPACITY,
                            dt,
                            &params,
                        )?;

                        let position = time_parts
                            .at(
                                Location {
                                    latitude: lat,
                                    longitude: lon,
                                },
                                params.environment.elevation,
                                refraction,
                            )
                            .map_err(|e| format!("Failed to calculate solar position: {}", e))?;

                        Ok(CalculationResult::Position {
                            lat,
                            lon,
                            datetime: dt,
                            position,
                            deltat,
                        })
                    })
                }))
            } else {
                Box::new(data.map(move |item| {
                    item.and_then(|(lat, lon, input)| {
                        let dt = input.datetime;
                        let calculation =
                            calculate_position_with_refraction(lat, lon, dt, &params, refraction)?;

                        Ok(CalculationResult::Position {
                            lat,
                            lon,
                            datetime: dt,
                            position: calculation.position,
                            deltat: calculation.deltat,
                        })
                    })
                }))
            }
        }
        Command::Events => Box::new(data.flat_map(move |item| {
            match item.and_then(|(lat, lon, dt)| calculate_events(lat, lon, dt, &params)) {
                Ok(rows) => rows
                    .into_iter()
                    .map(|row| Ok(CalculationResult::Event(row)))
                    .collect(),
                Err(err) => vec![Err(err)],
            }
        })),
    }
}

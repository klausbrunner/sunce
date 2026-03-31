//! Stream orchestration and shared calculation result types.

use crate::data::{CalculationAlgorithm, Command, CoordTimeStream, Parameters};
use crate::position::{SpaCache, TIME_CACHE_CAPACITY, refraction_correction, time_cache_get};
use crate::sunrise::calculate_sunrise as calculate_sunrise_impl;
use chrono::{DateTime, FixedOffset};
use solar_positioning::SolarPosition;
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

#[derive(Debug, Clone)]
pub enum CalculationResult {
    Position {
        lat: f64,
        lon: f64,
        datetime: DateTime<FixedOffset>,
        position: SolarPosition,
        deltat: f64,
    },
    Sunrise {
        lat: f64,
        lon: f64,
        date: DateTime<FixedOffset>,
        result: solar_positioning::SunriseResult<DateTime<FixedOffset>>,
        deltat: f64,
    },
    SunriseWithTwilight {
        lat: f64,
        lon: f64,
        date: DateTime<FixedOffset>,
        sunrise_sunset: solar_positioning::SunriseResult<DateTime<FixedOffset>>,
        civil: solar_positioning::SunriseResult<DateTime<FixedOffset>>,
        nautical: solar_positioning::SunriseResult<DateTime<FixedOffset>>,
        astronomical: solar_positioning::SunriseResult<DateTime<FixedOffset>>,
        deltat: f64,
    },
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

            if params.calculation.algorithm == CalculationAlgorithm::Spa && allow_time_cache {
                use solar_positioning::spa;

                let mut time_cache: SpaCache = SpaCache::default();
                let mut time_cache_order: VecDeque<DateTime<FixedOffset>> = VecDeque::new();

                Box::new(data.map(move |item| {
                    item.and_then(|(lat, lon, dt)| {
                        let (time_parts, deltat) = match time_cache_get(
                            &mut time_cache,
                            &mut time_cache_order,
                            TIME_CACHE_CAPACITY,
                            dt,
                            &params,
                        ) {
                            Ok(value) => value,
                            Err(err) => return Err(err),
                        };

                        let position = spa::spa_with_time_dependent_parts(
                            lat,
                            lon,
                            params.environment.elevation,
                            refraction,
                            time_parts.as_ref(),
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
                    item.and_then(|(lat, lon, dt)| {
                        let deltat = crate::position::resolve_deltat(dt, &params);
                        let position =
                            if params.calculation.algorithm == CalculationAlgorithm::Grena3 {
                                solar_positioning::grena3::solar_position(
                                    dt, lat, lon, deltat, refraction,
                                )
                                .map_err(|e| format!("Failed to calculate solar position: {}", e))?
                            } else {
                                solar_positioning::spa::solar_position(
                                    dt,
                                    lat,
                                    lon,
                                    params.environment.elevation,
                                    deltat,
                                    refraction,
                                )
                                .map_err(|e| format!("Failed to calculate solar position: {}", e))?
                            };

                        Ok(CalculationResult::Position {
                            lat,
                            lon,
                            datetime: dt,
                            position,
                            deltat,
                        })
                    })
                }))
            }
        }
        Command::Sunrise => Box::new(data.map(move |item| {
            item.and_then(|(lat, lon, dt)| calculate_sunrise_impl(lat, lon, dt, &params))
        })),
    }
}

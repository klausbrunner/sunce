//! Stream orchestration and shared calculation result types.

use crate::data::{CalculationAlgorithm, Command, CoordTimeStream, Parameters};
use crate::position::{
    SpaCache, TIME_CACHE_CAPACITY, calculate_position as calculate_position_impl,
    refraction_correction, time_cache_get,
};
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

pub fn solar_elevation_at(
    lat: f64,
    lon: f64,
    dt: DateTime<FixedOffset>,
    params: &Parameters,
) -> Result<f64, String> {
    crate::position::solar_elevation_at(lat, lon, dt, params)
}

pub fn next_state_transition(
    target: SolarState,
    lat: f64,
    lon: f64,
    now: DateTime<FixedOffset>,
    params: &Parameters,
) -> Result<DateTime<FixedOffset>, String> {
    crate::sunrise::next_state_transition(target, lat, lon, now, params)
}

pub fn solar_state_at(
    lat: f64,
    lon: f64,
    dt: DateTime<FixedOffset>,
    params: &Parameters,
) -> Result<SolarState, String> {
    crate::sunrise::solar_state_at(lat, lon, dt, params)
}

pub fn is_after_sunset(
    lat: f64,
    lon: f64,
    dt: DateTime<FixedOffset>,
    params: &Parameters,
) -> Result<bool, String> {
    crate::sunrise::is_after_sunset(lat, lon, dt, params)
}

pub fn calculate_stream(
    data: CoordTimeStream,
    command: Command,
    params: Parameters,
    allow_time_cache: bool,
) -> CalculationStream {
    match command {
        Command::Position => {
            if params.calculation.algorithm == CalculationAlgorithm::Spa && allow_time_cache {
                use solar_positioning::spa;

                let mut time_cache: SpaCache = SpaCache::default();
                let mut time_cache_order: VecDeque<DateTime<FixedOffset>> = VecDeque::new();
                let params = params.clone();

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

                        let refraction = refraction_correction(&params)?;
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
                let params = params.clone();
                Box::new(data.map(move |item| {
                    item.and_then(|(lat, lon, dt)| calculate_position_impl(lat, lon, dt, &params))
                }))
            }
        }
        Command::Sunrise => Box::new(data.map(move |item| {
            item.and_then(|(lat, lon, dt)| calculate_sunrise_impl(lat, lon, dt, &params))
        })),
    }
}

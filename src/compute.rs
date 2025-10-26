//! Solar position calculations with streaming architecture and SPA caching.

use crate::data::{Command, CoordTimeStream, Parameters};
use chrono::{DateTime, FixedOffset};
use solar_positioning::RefractionCorrection;
use solar_positioning::time::DeltaT;
use solar_positioning::{self, SolarPosition};
use std::collections::HashMap;

type CalculationStream = Box<dyn Iterator<Item = Result<CalculationResult, String>>>;

// Result types for calculations
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

fn resolve_deltat(dt: DateTime<FixedOffset>, params: &Parameters) -> f64 {
    params
        .deltat
        .unwrap_or_else(|| DeltaT::estimate_from_date_like(dt).unwrap_or(0.0))
}

fn refraction_correction(params: &Parameters) -> Result<Option<RefractionCorrection>, String> {
    if params.refraction {
        RefractionCorrection::new(params.pressure, params.temperature)
            .map(Some)
            .map_err(|err| {
                format!(
                    "Invalid refraction parameters (pressure={}, temperature={}): {}",
                    params.pressure, params.temperature, err
                )
            })
    } else {
        Ok(None)
    }
}

// Calculate solar position for a single point
pub fn calculate_position(
    lat: f64,
    lon: f64,
    dt: DateTime<FixedOffset>,
    params: &Parameters,
) -> Result<CalculationResult, String> {
    let deltat = resolve_deltat(dt, params);
    let refraction = refraction_correction(params)?;

    let position = if params.algorithm == "grena3" {
        solar_positioning::grena3::solar_position(dt, lat, lon, deltat, refraction)
            .map_err(|e| format!("Failed to calculate solar position: {}", e))?
    } else {
        solar_positioning::spa::solar_position(dt, lat, lon, params.elevation, deltat, refraction)
            .map_err(|e| format!("Failed to calculate solar position: {}", e))?
    };

    Ok(CalculationResult::Position {
        lat,
        lon,
        datetime: dt,
        position,
        deltat,
    })
}

// Calculate sunrise/sunset for a single point
pub fn calculate_sunrise(
    lat: f64,
    lon: f64,
    dt: DateTime<FixedOffset>,
    params: &Parameters,
) -> Result<CalculationResult, String> {
    use solar_positioning::Horizon;

    let deltat = resolve_deltat(dt, params);

    if params.twilight {
        let horizons = vec![
            Horizon::SunriseSunset,
            Horizon::CivilTwilight,
            Horizon::NauticalTwilight,
            Horizon::AstronomicalTwilight,
        ];

        let results =
            solar_positioning::spa::sunrise_sunset_multiple(dt, lat, lon, deltat, horizons)
                .map(|r| r.map_err(|e| format!("Failed to calculate twilight: {}", e)))
                .collect::<Result<Vec<_>, _>>()?;

        let mut iter = results.into_iter().map(|(_, result)| result);
        let Some(sunrise_sunset) = iter.next() else {
            return Err("Failed to calculate twilight: incomplete result set".to_string());
        };
        let Some(civil) = iter.next() else {
            return Err("Failed to calculate twilight: incomplete result set".to_string());
        };
        let Some(nautical) = iter.next() else {
            return Err("Failed to calculate twilight: incomplete result set".to_string());
        };
        let Some(astronomical) = iter.next() else {
            return Err("Failed to calculate twilight: incomplete result set".to_string());
        };

        Ok(CalculationResult::SunriseWithTwilight {
            lat,
            lon,
            date: dt,
            sunrise_sunset,
            civil,
            nautical,
            astronomical,
            deltat,
        })
    } else {
        let horizon = params
            .horizon
            .map(Horizon::Custom)
            .unwrap_or(Horizon::SunriseSunset);

        let result =
            solar_positioning::spa::sunrise_sunset_for_horizon(dt, lat, lon, deltat, horizon)
                .map_err(|e| format!("Failed to calculate sunrise/sunset: {}", e))?;

        Ok(CalculationResult::Sunrise {
            lat,
            lon,
            date: dt,
            result,
            deltat,
        })
    }
}

// Apply calculations to a stream of data points
pub fn calculate_stream(
    data: CoordTimeStream,
    command: Command,
    params: Parameters,
) -> CalculationStream {
    match command {
        Command::Position => {
            // Optimize for coordinate sweeps with SPA algorithm
            if params.algorithm == "spa" {
                use solar_positioning::spa;

                // Cache for time-dependent SPA calculations
                // Key must be the original DateTime with timezone to handle different TZ correctly
                let mut time_cache: HashMap<
                    DateTime<FixedOffset>,
                    Result<(spa::SpaTimeDependent, f64), String>,
                > = HashMap::new();

                Box::new(data.map(move |item| {
                    item.and_then(|(lat, lon, dt)| {
                        let entry = time_cache.entry(dt).or_insert_with(|| {
                            let deltat = resolve_deltat(dt, &params);
                            spa::spa_time_dependent_parts(dt, deltat)
                                .map(|parts| (parts, deltat))
                                .map_err(|err| {
                                    format!("Failed to calculate time-dependent parts: {}", err)
                                })
                        });

                        let (time_parts, deltat) = match entry {
                            Ok((parts, deltat)) => (parts, *deltat),
                            Err(err) => return Err(err.clone()),
                        };

                        let refraction = refraction_correction(&params)?;
                        let position = spa::spa_with_time_dependent_parts(
                            lat,
                            lon,
                            params.elevation,
                            refraction,
                            time_parts,
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
                // Regular calculation for non-SPA or when optimization isn't beneficial
                Box::new(data.map(move |item| {
                    item.and_then(|(lat, lon, dt)| calculate_position(lat, lon, dt, &params))
                }))
            }
        }
        Command::Sunrise => Box::new(data.map(move |item| {
            item.and_then(|(lat, lon, dt)| calculate_sunrise(lat, lon, dt, &params))
        })),
    }
}

//! Solar position calculations with streaming architecture and SPA caching.

use crate::data::{Command, CoordTimeStream, Parameters};
use chrono::{DateTime, FixedOffset};
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

// Calculate solar position for a single point
pub fn calculate_position(
    lat: f64,
    lon: f64,
    dt: DateTime<FixedOffset>,
    params: &Parameters,
) -> Result<CalculationResult, String> {
    use solar_positioning::RefractionCorrection;
    use solar_positioning::time::DeltaT;

    let deltat = params.deltat.unwrap_or_else(|| {
        // Auto-estimate based on year
        DeltaT::estimate_from_date_like(dt).unwrap_or(0.0)
    });

    let refraction = if params.refraction {
        match RefractionCorrection::new(params.pressure, params.temperature) {
            Ok(value) => Some(value),
            Err(err) => {
                return Err(format!(
                    "Invalid refraction parameters (pressure={}, temperature={}): {}",
                    params.pressure, params.temperature, err
                ));
            }
        }
    } else {
        None
    };

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
    use solar_positioning::time::DeltaT;

    let deltat = params
        .deltat
        .unwrap_or_else(|| DeltaT::estimate_from_date_like(dt).unwrap_or(0.0));

    if params.twilight {
        let horizons = vec![
            Horizon::SunriseSunset,
            Horizon::CivilTwilight,
            Horizon::NauticalTwilight,
            Horizon::AstronomicalTwilight,
        ];

        let results: Vec<_> =
            solar_positioning::spa::sunrise_sunset_multiple(dt, lat, lon, deltat, horizons)
                .map(|r| r.map_err(|e| format!("Failed to calculate twilight: {}", e)))
                .collect::<Result<Vec<_>, _>>()?;

        if results.len() != 4 {
            return Err("Failed to calculate twilight: incomplete result set".to_string());
        }

        let mut iter = results.into_iter();
        let sunrise_sunset = iter
            .next()
            .ok_or_else(|| "Missing sunrise/sunset result".to_string())?
            .1;
        let civil = iter
            .next()
            .ok_or_else(|| "Missing civil twilight result".to_string())?
            .1;
        let nautical = iter
            .next()
            .ok_or_else(|| "Missing nautical twilight result".to_string())?
            .1;
        let astronomical = iter
            .next()
            .ok_or_else(|| "Missing astronomical twilight result".to_string())?
            .1;

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
                use solar_positioning::RefractionCorrection;
                use solar_positioning::spa;
                use solar_positioning::time::DeltaT;

                // Cache for time-dependent SPA calculations
                // Key must be the original DateTime with timezone to handle different TZ correctly
                let mut time_cache: HashMap<
                    DateTime<FixedOffset>,
                    Result<(spa::SpaTimeDependent, f64), String>,
                > = HashMap::new();

                Box::new(data.map(move |item| {
                    let (lat, lon, dt) = match item {
                        Ok(values) => values,
                        Err(err) => return Err(err),
                    };

                    // Get or compute time-dependent parts (cached for same timestamp WITH timezone)
                    let entry = time_cache.entry(dt).or_insert_with(|| {
                        let deltat = params
                            .deltat
                            .unwrap_or_else(|| DeltaT::estimate_from_date_like(dt).unwrap_or(0.0));
                        match spa::spa_time_dependent_parts(dt, deltat) {
                            Ok(parts) => Ok((parts, deltat)),
                            Err(err) => Err(format!(
                                "Failed to calculate time-dependent parts: {}",
                                err
                            )),
                        }
                    });

                    if let Err(err) = entry {
                        return Err(err.clone());
                    }

                    let (time_parts, stored_deltat) = match entry.as_ref() {
                        Ok((parts, deltat)) => (parts, deltat),
                        Err(_) => unreachable!("entry error already returned"),
                    };
                    let deltat = *stored_deltat;

                    // Calculate position using cached time parts
                    let refraction = if params.refraction {
                        match RefractionCorrection::new(params.pressure, params.temperature) {
                            Ok(value) => Some(value),
                            Err(err) => {
                                return Err(format!(
                                    "Invalid refraction parameters (pressure={}, temperature={}): {}",
                                    params.pressure, params.temperature, err
                                ))
                            }
                        }
                    } else {
                        None
                    };

                    let position = match spa::spa_with_time_dependent_parts(
                        lat,
                        lon,
                        params.elevation,
                        refraction,
                        time_parts,
                    ) {
                        Ok(pos) => pos,
                        Err(e) => return Err(format!("Failed to calculate solar position: {}", e)),
                    };

                    Ok(CalculationResult::Position {
                        lat,
                        lon,
                        datetime: dt,
                        position,
                        deltat, // Use the stored deltaT
                    })
                }))
            } else {
                // Regular calculation for non-SPA or when optimization isn't beneficial
                Box::new(data.map(move |item| match item {
                    Ok((lat, lon, dt)) => calculate_position(lat, lon, dt, &params),
                    Err(err) => Err(err),
                }))
            }
        }
        Command::Sunrise => Box::new(data.map(move |item| match item {
            Ok((lat, lon, dt)) => calculate_sunrise(lat, lon, dt, &params),
            Err(err) => Err(err),
        })),
    }
}

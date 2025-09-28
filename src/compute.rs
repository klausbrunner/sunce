//! Solar position calculations with streaming architecture and SPA caching.

use crate::data::{Command, Parameters};
use chrono::{DateTime, FixedOffset};
use solar_positioning::{self, SolarPosition};
use std::collections::HashMap;

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
) -> CalculationResult {
    use solar_positioning::RefractionCorrection;
    use solar_positioning::time::DeltaT;

    let deltat = params.deltat.unwrap_or_else(|| {
        // Auto-estimate based on year
        DeltaT::estimate_from_date_like(dt).unwrap_or(0.0)
    });

    let refraction = if params.refraction {
        Some(RefractionCorrection::new(params.pressure, params.temperature).unwrap())
    } else {
        None
    };

    let position = if params.algorithm == "grena3" {
        solar_positioning::grena3::solar_position(dt, lat, lon, deltat, refraction)
            .expect("Failed to calculate solar position")
    } else {
        solar_positioning::spa::solar_position(dt, lat, lon, params.elevation, deltat, refraction)
            .expect("Failed to calculate solar position")
    };

    CalculationResult::Position {
        lat,
        lon,
        datetime: dt,
        position,
        deltat,
    }
}

// Calculate sunrise/sunset for a single point
pub fn calculate_sunrise(
    lat: f64,
    lon: f64,
    dt: DateTime<FixedOffset>,
    params: &Parameters,
) -> CalculationResult {
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

        let mut results: Vec<_> =
            solar_positioning::spa::sunrise_sunset_multiple(dt, lat, lon, deltat, horizons)
                .map(|r| r.expect("Failed to calculate twilight"))
                .collect();

        let astronomical = results.pop().expect("Missing astronomical twilight").1;
        let nautical = results.pop().expect("Missing nautical twilight").1;
        let civil = results.pop().expect("Missing civil twilight").1;
        let sunrise_sunset = results.pop().expect("Missing sunrise/sunset").1;

        CalculationResult::SunriseWithTwilight {
            lat,
            lon,
            date: dt,
            sunrise_sunset,
            civil,
            nautical,
            astronomical,
            deltat,
        }
    } else {
        let horizon = if let Some(h) = params.horizon {
            Horizon::Custom(h)
        } else {
            Horizon::SunriseSunset
        };

        let result =
            solar_positioning::spa::sunrise_sunset_for_horizon(dt, lat, lon, deltat, horizon)
                .expect("Failed to calculate sunrise/sunset");

        CalculationResult::Sunrise {
            lat,
            lon,
            date: dt,
            result,
            deltat,
        }
    }
}

// Apply calculations to a stream of data points
pub fn calculate_stream(
    data: Box<dyn Iterator<Item = (f64, f64, DateTime<FixedOffset>)>>,
    command: Command,
    params: Parameters,
) -> Box<dyn Iterator<Item = CalculationResult>> {
    match command {
        Command::Position => {
            // Optimize for coordinate sweeps with SPA algorithm
            if params.algorithm == "spa" {
                use solar_positioning::RefractionCorrection;
                use solar_positioning::spa;
                use solar_positioning::time::DeltaT;

                // Cache for time-dependent SPA calculations
                // Key must be the original DateTime with timezone to handle different TZ correctly
                let mut time_cache: HashMap<DateTime<FixedOffset>, (spa::SpaTimeDependent, f64)> =
                    HashMap::new();

                Box::new(data.map(move |(lat, lon, dt)| {
                    // Get or compute time-dependent parts (cached for same timestamp WITH timezone)
                    let (time_parts, deltat) = time_cache.entry(dt).or_insert_with(|| {
                        let deltat = params
                            .deltat
                            .unwrap_or_else(|| DeltaT::estimate_from_date_like(dt).unwrap_or(0.0));
                        let parts = spa::spa_time_dependent_parts(dt, deltat)
                            .expect("Failed to calculate time-dependent parts");
                        (parts, deltat)
                    });

                    // Calculate position using cached time parts
                    let refraction = if params.refraction {
                        Some(
                            RefractionCorrection::new(params.pressure, params.temperature).unwrap(),
                        )
                    } else {
                        None
                    };

                    let position = spa::spa_with_time_dependent_parts(
                        lat,
                        lon,
                        params.elevation,
                        refraction,
                        time_parts,
                    )
                    .expect("Failed to calculate solar position");

                    CalculationResult::Position {
                        lat,
                        lon,
                        datetime: dt,
                        position,
                        deltat: *deltat, // Use the stored deltaT
                    }
                }))
            } else {
                // Regular calculation for non-SPA or when optimization isn't beneficial
                Box::new(data.map(move |(lat, lon, dt)| calculate_position(lat, lon, dt, &params)))
            }
        }
        Command::Sunrise => {
            Box::new(data.map(move |(lat, lon, dt)| calculate_sunrise(lat, lon, dt, &params)))
        }
    }
}

//! Solar position calculations with streaming architecture and SPA caching.

use crate::data::{CalculationAlgorithm, Command, CoordTimeStream, Parameters};
use chrono::{DateTime, FixedOffset};
use solar_positioning::RefractionCorrection;
use solar_positioning::time::DeltaT;
use solar_positioning::{self, SolarPosition};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

type CalculationStream = Box<dyn Iterator<Item = Result<CalculationResult, String>>>;

const TIME_CACHE_CAPACITY: usize = 2048;
type SpaTimeParts = Arc<solar_positioning::spa::SpaTimeDependent>;
type SpaCacheValue = Result<(SpaTimeParts, f64), String>;
type SpaCache = HashMap<DateTime<FixedOffset>, SpaCacheValue>;

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
    if params.environment.refraction {
        RefractionCorrection::new(params.environment.pressure, params.environment.temperature)
            .map(Some)
            .map_err(|err| {
                format!(
                    "Invalid refraction parameters (pressure={}, temperature={}): {}",
                    params.environment.pressure, params.environment.temperature, err
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

    let position = if params.calculation.algorithm == CalculationAlgorithm::Grena3 {
        solar_positioning::grena3::solar_position(dt, lat, lon, deltat, refraction)
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

    if params.calculation.twilight {
        const HORIZONS: [Horizon; 4] = [
            Horizon::SunriseSunset,
            Horizon::CivilTwilight,
            Horizon::NauticalTwilight,
            Horizon::AstronomicalTwilight,
        ];

        let mut results = solar_positioning::spa::sunrise_sunset_multiple(
            dt, lat, lon, deltat, HORIZONS,
        )
        .map(|res| {
            res.map(|(_, r)| r)
                .map_err(|e| format!("Failed to calculate twilight: {}", e))
        });

        let sunrise_sunset = results
            .next()
            .transpose()?
            .ok_or_else(|| "Failed to calculate twilight: incomplete result set".to_string())?;
        let civil = results
            .next()
            .transpose()?
            .ok_or_else(|| "Failed to calculate twilight: incomplete result set".to_string())?;
        let nautical = results
            .next()
            .transpose()?
            .ok_or_else(|| "Failed to calculate twilight: incomplete result set".to_string())?;
        let astronomical = results
            .next()
            .transpose()?
            .ok_or_else(|| "Failed to calculate twilight: incomplete result set".to_string())?;

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
            .calculation
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

fn time_cache_get(
    cache: &mut SpaCache,
    order: &mut VecDeque<DateTime<FixedOffset>>,
    capacity: usize,
    dt: DateTime<FixedOffset>,
    params: &Parameters,
) -> Result<(SpaTimeParts, f64), String> {
    if let Some(existing) = cache.get(&dt) {
        if order.back().is_none_or(|back| *back != dt) {
            if let Some(pos) = order.iter().position(|key| *key == dt) {
                order.remove(pos);
            }
            order.push_back(dt);
        }
        return match existing {
            Ok((parts, deltat)) => Ok((Arc::clone(parts), *deltat)),
            Err(err) => Err(err.clone()),
        };
    }

    while cache.len() >= capacity {
        match order.pop_front() {
            Some(oldest) => {
                cache.remove(&oldest);
            }
            None => break,
        }
    }

    let entry = cache.entry(dt).or_insert_with(|| {
        let deltat = resolve_deltat(dt, params);
        solar_positioning::spa::spa_time_dependent_parts(dt, deltat)
            .map(|parts| (Arc::new(parts), deltat))
            .map_err(|err| format!("Failed to calculate time-dependent parts: {}", err))
    });

    order.push_back(dt);

    match entry {
        Ok((parts, deltat)) => Ok((Arc::clone(parts), *deltat)),
        Err(err) => Err(err.clone()),
    }
}

// Apply calculations to a stream of data points
pub fn calculate_stream(
    data: CoordTimeStream,
    command: Command,
    params: Parameters,
    allow_time_cache: bool,
) -> CalculationStream {
    match command {
        Command::Position => {
            // Optimize for coordinate sweeps with SPA algorithm
            if params.calculation.algorithm == CalculationAlgorithm::Spa && allow_time_cache {
                use solar_positioning::spa;

                // Cache for time-dependent SPA calculations
                // Key must be the original DateTime with timezone to handle different TZ correctly
                let mut time_cache: SpaCache = HashMap::new();
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
                // Regular calculation for non-SPA or when optimization isn't beneficial
                let params = params.clone();
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn spa_time_cache_enforces_capacity() {
        let mut cache: SpaCache = HashMap::new();
        let mut order = VecDeque::new();
        let params = Parameters::default();
        let tz = FixedOffset::east_opt(0).unwrap();

        for minute in 0..10 {
            let dt = tz.with_ymd_and_hms(2024, 1, 1, 0, minute, 0).unwrap();
            time_cache_get(&mut cache, &mut order, 3, dt, &params).unwrap();
        }

        assert!(
            cache.len() <= 3,
            "cache len {} exceeded capacity",
            cache.len()
        );
    }

    #[test]
    fn spa_time_cache_reuses_existing_entry() {
        let mut cache: SpaCache = HashMap::new();
        let mut order = VecDeque::new();
        let params = Parameters::default();

        let tz = FixedOffset::east_opt(0).unwrap();
        let dt = tz.with_ymd_and_hms(2024, 6, 21, 12, 0, 0).unwrap();

        let first = time_cache_get(&mut cache, &mut order, 3, dt, &params).unwrap();
        let second = time_cache_get(&mut cache, &mut order, 3, dt, &params).unwrap();

        assert!(Arc::ptr_eq(&first.0, &second.0));
    }

    #[test]
    fn spa_time_cache_order_does_not_grow_on_hits() {
        let mut cache: SpaCache = HashMap::new();
        let mut order = VecDeque::new();
        let params = Parameters::default();

        let tz = FixedOffset::east_opt(0).unwrap();
        let dt = tz.with_ymd_and_hms(2024, 6, 21, 12, 0, 0).unwrap();

        time_cache_get(&mut cache, &mut order, 3, dt, &params).unwrap();
        time_cache_get(&mut cache, &mut order, 3, dt, &params).unwrap();

        assert_eq!(order.len(), 1);
    }

    #[test]
    fn spa_time_cache_eviction_is_lru() {
        let mut cache: SpaCache = HashMap::new();
        let mut order = VecDeque::new();
        let params = Parameters::default();
        let tz = FixedOffset::east_opt(0).unwrap();

        let dt1 = tz.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let dt2 = tz.with_ymd_and_hms(2024, 1, 1, 0, 1, 0).unwrap();
        let dt3 = tz.with_ymd_and_hms(2024, 1, 1, 0, 2, 0).unwrap();

        time_cache_get(&mut cache, &mut order, 2, dt1, &params).unwrap();
        time_cache_get(&mut cache, &mut order, 2, dt2, &params).unwrap();
        time_cache_get(&mut cache, &mut order, 2, dt1, &params).unwrap(); // bump dt1
        time_cache_get(&mut cache, &mut order, 2, dt3, &params).unwrap(); // evict dt2

        assert!(cache.contains_key(&dt1));
        assert!(!cache.contains_key(&dt2));
        assert!(cache.contains_key(&dt3));
    }
}

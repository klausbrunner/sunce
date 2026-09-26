//! Solar position calculations and time-cache helpers.

use crate::data::{CalculationAlgorithm, Parameters};
use chrono::{DateTime, FixedOffset};
use solar_positioning::{
    Location, PreparedPositions, RefractionCorrection, SolarPositions, delta_t,
};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

pub(crate) const TIME_CACHE_CAPACITY: usize = 2048;
pub(crate) type TimeParts = Arc<PreparedPositions>;
// Delta-T estimates use the local date, which can differ for equal instants.
pub(crate) type TimeCacheKey = (DateTime<FixedOffset>, i32);
pub(crate) type TimeCache = HashMap<TimeCacheKey, (TimeParts, f64)>;

pub(crate) struct PositionCalculation {
    pub position: solar_positioning::SolarPosition,
    pub deltat: f64,
}

pub(crate) fn resolve_deltat(dt: DateTime<FixedOffset>, params: &Parameters) -> f64 {
    params
        .deltat
        .unwrap_or_else(|| delta_t::estimate_from_date_like(dt).unwrap_or(0.0))
}

pub(crate) fn refraction_correction(
    params: &Parameters,
) -> Result<Option<RefractionCorrection>, String> {
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

pub(crate) fn solar_elevation_at(
    lat: f64,
    lon: f64,
    dt: DateTime<FixedOffset>,
    params: &Parameters,
) -> Result<f64, String> {
    Ok(90.0
        - calculate_position(lat, lon, dt, params)?
            .position
            .zenith_angle())
}

pub(crate) fn calculate_position(
    lat: f64,
    lon: f64,
    dt: DateTime<FixedOffset>,
    params: &Parameters,
) -> Result<PositionCalculation, String> {
    let refraction = refraction_correction(params)?;
    calculate_position_with_refraction(lat, lon, dt, params, refraction)
}

pub(crate) fn calculate_position_with_refraction(
    lat: f64,
    lon: f64,
    dt: DateTime<FixedOffset>,
    params: &Parameters,
    refraction: Option<RefractionCorrection>,
) -> Result<PositionCalculation, String> {
    let deltat = resolve_deltat(dt, params);

    let position = calculator(params)
        .at(
            &dt,
            Location {
                latitude: lat,
                longitude: lon,
            },
            params.environment.elevation,
            deltat,
            refraction,
        )
        .map_err(|e| format!("Failed to calculate solar position: {e}"))?;

    Ok(PositionCalculation { position, deltat })
}

pub(crate) fn calculator(params: &Parameters) -> SolarPositions {
    match params.calculation.algorithm {
        CalculationAlgorithm::Spa => SolarPositions::new(),
        CalculationAlgorithm::Grena3 => SolarPositions::grena3(),
    }
}

pub(crate) fn time_cache_get(
    cache: &mut TimeCache,
    order: &mut VecDeque<TimeCacheKey>,
    capacity: usize,
    dt: DateTime<FixedOffset>,
    params: &Parameters,
) -> Result<(TimeParts, f64), String> {
    let key = (dt, dt.offset().local_minus_utc());
    if let Some(existing) = cache.get(&key).cloned() {
        return Ok(existing);
    }

    while cache.len() >= capacity {
        let Some(oldest) = order.pop_front() else {
            break;
        };
        cache.remove(&oldest);
    }

    let deltat = resolve_deltat(dt, params);
    let parts = Arc::new(
        calculator(params)
            .for_time(&dt, deltat)
            .map_err(|err| format!("Failed to calculate time-dependent parts: {}", err))?,
    );
    cache.insert(key, (Arc::clone(&parts), deltat));
    order.push_back(key);
    Ok((parts, deltat))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn time_cache_reuses_entries_without_growing_order() {
        let mut cache: TimeCache = HashMap::new();
        let mut order = VecDeque::new();
        let params = Parameters::default();
        let tz = FixedOffset::east_opt(0).unwrap();
        let dt = tz.with_ymd_and_hms(2024, 6, 21, 12, 0, 0).unwrap();

        let first = time_cache_get(&mut cache, &mut order, 3, dt, &params).unwrap();
        let second = time_cache_get(&mut cache, &mut order, 3, dt, &params).unwrap();

        assert!(Arc::ptr_eq(&first.0, &second.0));
        assert_eq!(order.len(), 1);
    }

    #[test]
    fn time_cache_preserves_local_date_for_delta_t_estimates() {
        let first = DateTime::parse_from_rfc3339("2024-02-01T00:30:00Z").unwrap();
        let second = first.with_timezone(&FixedOffset::west_opt(3600).unwrap());
        let params = Parameters {
            deltat: None,
            ..Parameters::default()
        };
        let mut cache = TimeCache::default();
        let mut order = VecDeque::new();
        let (_, a) = time_cache_get(&mut cache, &mut order, 2, first, &params).unwrap();
        let (_, b) = time_cache_get(&mut cache, &mut order, 2, second, &params).unwrap();
        assert_ne!(a, b);
        assert_eq!(b, resolve_deltat(second, &params));
    }

    #[test]
    fn time_cache_eviction_keeps_existing_when_not_full() {
        let mut cache: TimeCache = HashMap::new();
        let mut order = VecDeque::new();
        let params = Parameters::default();
        let tz = FixedOffset::east_opt(0).unwrap();

        let dt1 = tz.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let dt2 = tz.with_ymd_and_hms(2024, 1, 1, 0, 1, 0).unwrap();
        let dt3 = tz.with_ymd_and_hms(2024, 1, 1, 0, 2, 0).unwrap();

        time_cache_get(&mut cache, &mut order, 2, dt1, &params).unwrap();
        time_cache_get(&mut cache, &mut order, 2, dt2, &params).unwrap();
        time_cache_get(&mut cache, &mut order, 2, dt3, &params).unwrap();

        assert!(!cache.contains_key(&(dt1, 0)));
        assert!(cache.contains_key(&(dt2, 0)));
        assert!(cache.contains_key(&(dt3, 0)));
    }
}

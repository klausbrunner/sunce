//! Solar position calculations and SPA time-cache helpers.

use crate::data::{CalculationAlgorithm, Parameters};
use chrono::{DateTime, FixedOffset};
use solar_positioning::RefractionCorrection;
use solar_positioning::time::DeltaT;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

pub(crate) const TIME_CACHE_CAPACITY: usize = 2048;
pub(crate) type SpaTimeParts = Arc<solar_positioning::spa::SpaTimeDependent>;
pub(crate) type SpaCache = HashMap<DateTime<FixedOffset>, (SpaTimeParts, f64)>;

pub(crate) struct PositionCalculation {
    pub position: solar_positioning::SolarPosition,
    pub deltat: f64,
}

pub(crate) fn resolve_deltat(dt: DateTime<FixedOffset>, params: &Parameters) -> f64 {
    params
        .deltat
        .unwrap_or_else(|| DeltaT::estimate_from_date_like(dt).unwrap_or(0.0))
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

    Ok(PositionCalculation { position, deltat })
}

pub(crate) fn time_cache_get(
    cache: &mut SpaCache,
    order: &mut VecDeque<DateTime<FixedOffset>>,
    capacity: usize,
    dt: DateTime<FixedOffset>,
    params: &Parameters,
) -> Result<(SpaTimeParts, f64), String> {
    if let Some(existing) = cache.get(&dt).cloned() {
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
        solar_positioning::spa::spa_time_dependent_parts(dt, deltat)
            .map_err(|err| format!("Failed to calculate time-dependent parts: {}", err))?,
    );
    cache.insert(dt, (Arc::clone(&parts), deltat));
    order.push_back(dt);
    Ok((parts, deltat))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn spa_time_cache_reuses_entries_without_growing_order() {
        let mut cache: SpaCache = HashMap::new();
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
    fn spa_time_cache_eviction_keeps_existing_when_not_full() {
        let mut cache: SpaCache = HashMap::new();
        let mut order = VecDeque::new();
        let params = Parameters::default();
        let tz = FixedOffset::east_opt(0).unwrap();

        let dt1 = tz.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let dt2 = tz.with_ymd_and_hms(2024, 1, 1, 0, 1, 0).unwrap();
        let dt3 = tz.with_ymd_and_hms(2024, 1, 1, 0, 2, 0).unwrap();

        time_cache_get(&mut cache, &mut order, 2, dt1, &params).unwrap();
        time_cache_get(&mut cache, &mut order, 2, dt2, &params).unwrap();
        time_cache_get(&mut cache, &mut order, 2, dt3, &params).unwrap();

        assert!(!cache.contains_key(&dt1));
        assert!(cache.contains_key(&dt2));
        assert!(cache.contains_key(&dt3));
    }
}

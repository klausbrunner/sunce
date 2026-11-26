//! Solar position calculations and time-cache helpers.

#[cfg(feature = "cli")]
use crate::Location;
use crate::{CalculationAlgorithm, PositionOptions};
use jiff::{Timestamp, ZonedDateTime, civil::Date};
use solar_positioning::{PreparedPositions, RefractionCorrection, SolarPositions, delta_t};
use std::collections::{HashMap, VecDeque};

pub(crate) const TIME_CACHE_CAPACITY: usize = 2048;
// Delta-T estimates use the local date, which can differ for equal instants.
pub(crate) type TimeCacheKey = (Timestamp, Date);
pub(crate) type TimeCache = HashMap<TimeCacheKey, (PreparedPositions, f64)>;

pub(crate) fn resolve_deltat(date: Date, params: &PositionOptions) -> f64 {
    params
        .deltat
        .unwrap_or_else(|| delta_t::estimate_from_civil_date(date).unwrap_or(0.0))
}

pub(crate) fn refraction_correction(
    params: &PositionOptions,
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

#[cfg(feature = "cli")]
pub(crate) fn solar_elevation_at(
    lat: f64,
    lon: f64,
    dt: &ZonedDateTime,
    params: &PositionOptions,
) -> Result<f64, String> {
    calculator(params)
        .at(
            dt.timestamp(),
            Location {
                latitude: lat,
                longitude: lon,
            },
            params.environment.elevation,
            resolve_deltat(dt.date(), params),
            refraction_correction(params)?,
        )
        .map(|position| position.elevation_angle())
        .map_err(|e| format!("Failed to calculate solar position: {e}"))
}

pub(crate) fn calculator(params: &PositionOptions) -> SolarPositions {
    match params.algorithm {
        CalculationAlgorithm::Spa => SolarPositions::new(),
        CalculationAlgorithm::Grena3 => SolarPositions::grena3(),
    }
}

pub(crate) fn time_cache_get(
    cache: &mut TimeCache,
    order: &mut VecDeque<TimeCacheKey>,
    capacity: usize,
    dt: &ZonedDateTime,
    params: &PositionOptions,
) -> Result<(PreparedPositions, f64), String> {
    let key = (dt.timestamp(), dt.date());
    if let Some(existing) = cache.get(&key).copied() {
        return Ok(existing);
    }

    while cache.len() >= capacity {
        let Some(oldest) = order.pop_front() else {
            break;
        };
        cache.remove(&oldest);
    }

    let deltat = resolve_deltat(key.1, params);
    let parts = calculator(params)
        .for_time(key.0, deltat)
        .map_err(|err| format!("Failed to calculate time-dependent parts: {}", err))?;
    cache.insert(key, (parts, deltat));
    order.push_back(key);
    Ok((parts, deltat))
}

#[cfg(test)]
mod tests {
    use super::*;
    use jiff::tz::{self, TimeZone};

    #[test]
    fn time_cache_hits_do_not_grow_entries_or_order() {
        let mut cache: TimeCache = HashMap::new();
        let mut order = VecDeque::new();
        let params = PositionOptions::default();
        let dt: ZonedDateTime = "2024-06-21T12:00:00Z[UTC]".parse().unwrap();

        time_cache_get(&mut cache, &mut order, 3, &dt, &params).unwrap();
        time_cache_get(&mut cache, &mut order, 3, &dt, &params).unwrap();

        assert_eq!(cache.len(), 1);
        assert_eq!(order.len(), 1);
    }

    #[test]
    fn time_cache_preserves_local_date_for_delta_t_estimates() {
        let first: ZonedDateTime = "2024-02-01T00:30:00Z[UTC]".parse().unwrap();
        let second = first.with_time_zone(tz::offset(-1).to_time_zone());
        let params = PositionOptions {
            deltat: None,
            ..PositionOptions::default()
        };
        let mut cache = TimeCache::default();
        let mut order = VecDeque::new();
        let (_, a) = time_cache_get(&mut cache, &mut order, 2, &first, &params).unwrap();
        let (_, b) = time_cache_get(&mut cache, &mut order, 2, &second, &params).unwrap();
        assert_ne!(a, b);
        assert_eq!(b, resolve_deltat(second.date(), &params));
    }

    #[test]
    fn time_cache_eviction_keeps_existing_when_not_full() {
        let mut cache: TimeCache = HashMap::new();
        let mut order = VecDeque::new();
        let params = PositionOptions::default();
        let day = jiff::civil::date(2024, 1, 1);
        let time = |minute| {
            day.at(0, minute, 0, 0)
                .to_zoned_datetime(TimeZone::UTC)
                .unwrap()
        };
        let [dt1, dt2, dt3] = [0, 1, 2].map(time);

        for dt in [&dt1, &dt2, &dt3] {
            time_cache_get(&mut cache, &mut order, 2, dt, &params).unwrap();
        }

        assert!(!cache.contains_key(&(dt1.timestamp(), day)));
        assert!(cache.contains_key(&(dt2.timestamp(), day)));
        assert!(cache.contains_key(&(dt3.timestamp(), day)));
    }
}

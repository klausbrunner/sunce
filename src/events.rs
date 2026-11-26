//! Solar events and daylight predicates.

use crate::{CalculationAlgorithm, EventOptions};
#[cfg(feature = "cli")]
use crate::{
    PositionOptions,
    compute::SolarState,
    position::{calculator as position_calculator, resolve_deltat},
};
#[cfg(feature = "cli")]
use jiff::{SignedDuration, Timestamp};
use jiff::{ZonedDateTime, civil::Date, tz::TimeZone};
use solar_positioning::{Events, Horizon, HorizonState, Location, SolarEvents};

const HORIZONS: [Horizon; 4] = [
    Horizon::SunriseSunset,
    Horizon::CivilTwilight,
    Horizon::NauticalTwilight,
    Horizon::AstronomicalTwilight,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DayState {
    Crossing,
    Above,
    Below,
    OnHorizon,
    Empty,
}

impl DayState {
    fn from_events<T: PartialOrd>(day: &Events<T>) -> Self {
        if day.start == day.end {
            Self::Empty
        } else if !day.rises.is_empty() || !day.sets.is_empty() {
            Self::Crossing
        } else {
            match day.state_at_start {
                HorizonState::Above => Self::Above,
                HorizonState::Below => Self::Below,
                HorizonState::OnHorizon => Self::OnHorizon,
            }
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Crossing => "CROSSING",
            Self::Above => "ABOVE",
            Self::Below => "BELOW",
            Self::OnHorizon => "ON_HORIZON",
            Self::Empty => "EMPTY",
        }
    }
}

/// Solar event kind; `Rise` and `Set` refer to a custom horizon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    Sunrise,
    Sunset,
    Transit,
    CivilDawn,
    CivilDusk,
    NauticalDawn,
    NauticalDusk,
    AstronomicalDawn,
    AstronomicalDusk,
    Rise,
    Set,
}

impl EventKind {
    /// Label used in text, CSV, JSON and Parquet output.
    pub fn label(self) -> &'static str {
        match self {
            Self::Sunrise => "sunrise",
            Self::Sunset => "sunset",
            Self::Transit => "transit",
            Self::CivilDawn => "civil_dawn",
            Self::CivilDusk => "civil_dusk",
            Self::NauticalDawn => "nautical_dawn",
            Self::NauticalDusk => "nautical_dusk",
            Self::AstronomicalDawn => "astronomical_dawn",
            Self::AstronomicalDusk => "astronomical_dusk",
            Self::Rise => "rise",
            Self::Set => "set",
        }
    }
}

/// An event on a local date, or a status-only record when that date has no events.
#[derive(Debug, Clone)]
pub struct EventRecord {
    pub lat: f64,
    pub lon: f64,
    pub date: Date,
    pub deltat: f64,
    /// State of the requested horizon; sunrise/sunset when twilight is enabled.
    pub day_state: DayState,
    /// Event kind; `None` on status-only records.
    pub event: Option<EventKind>,
    /// Event time in the requested zone; `None` on status-only records.
    pub time: Option<ZonedDateTime>,
}

fn calculator(algorithm: CalculationAlgorithm) -> SolarEvents {
    match algorithm {
        CalculationAlgorithm::Spa => SolarEvents::new(),
        CalculationAlgorithm::Grena3 => SolarEvents::grena3(),
    }
}

fn event_kinds(horizon: Horizon) -> (EventKind, EventKind) {
    use EventKind::*;
    match horizon {
        Horizon::SunriseSunset => (Sunrise, Sunset),
        Horizon::CivilTwilight => (CivilDawn, CivilDusk),
        Horizon::NauticalTwilight => (NauticalDawn, NauticalDusk),
        Horizon::AstronomicalTwilight => (AstronomicalDawn, AstronomicalDusk),
        Horizon::Custom(_) => (Rise, Set),
    }
}

pub fn calculate_events(
    location: Location,
    date: Date,
    timezone: &TimeZone,
    params: &EventOptions,
) -> Result<Vec<EventRecord>, String> {
    let deltat = params.deltat.unwrap_or_else(|| {
        solar_positioning::delta_t::estimate_from_civil_date(date).unwrap_or(0.0)
    });
    let horizon = params
        .horizon
        .map(Horizon::Custom)
        .unwrap_or(Horizon::SunriseSunset);
    let horizons = if params.twilight {
        &HORIZONS[..]
    } else {
        std::slice::from_ref(&horizon)
    };
    let days = calculator(params.algorithm)
        .for_date_multiple(date, timezone, location, deltat, horizons.iter().copied())
        .map_err(|err| format!("Failed to calculate solar events: {err}"))?;
    let base = EventRecord {
        lat: location.latitude,
        lon: location.longitude,
        date,
        deltat,
        day_state: DayState::from_events(&days[0].1),
        event: None,
        time: None,
    };
    let mut rows = Vec::new();
    for time in &days[0].1.transits {
        rows.push(EventRecord {
            event: Some(EventKind::Transit),
            time: Some(time.clone()),
            ..base
        });
    }
    for (horizon, day) in days {
        let (rise, set) = event_kinds(horizon);
        for (event, times) in [(rise, day.rises), (set, day.sets)] {
            rows.extend(times.into_iter().map(|time| EventRecord {
                event: Some(event),
                time: Some(time),
                ..base
            }));
        }
    }
    rows.sort_by_key(|row| row.time.as_ref().map(ZonedDateTime::timestamp));
    if rows.is_empty() {
        rows.push(base);
    }
    Ok(rows)
}

#[cfg(feature = "cli")]
pub fn solar_state_at(
    lat: f64,
    lon: f64,
    dt: &ZonedDateTime,
    params: &PositionOptions,
) -> Result<SolarState, String> {
    let elevation = position_calculator(params)
        .at(
            dt.timestamp(),
            Location {
                latitude: lat,
                longitude: lon,
            },
            0.0,
            resolve_deltat(dt.date(), params),
            None,
        )
        .map_err(|err| format!("Failed to calculate solar position: {err}"))?
        .elevation_angle();
    Ok(if elevation >= Horizon::SunriseSunset.elevation_angle() {
        SolarState::Daylight
    } else if elevation >= -6.0 {
        SolarState::CivilTwilight
    } else if elevation >= -12.0 {
        SolarState::NauticalTwilight
    } else if elevation >= -18.0 {
        SolarState::AstronomicalTwilight
    } else {
        SolarState::Night
    })
}

#[cfg(feature = "cli")]
pub fn is_after_sunset(
    lat: f64,
    lon: f64,
    dt: &ZonedDateTime,
    params: &PositionOptions,
) -> Result<bool, String> {
    Ok(solar_state_at(lat, lon, dt, params)? != SolarState::Daylight)
}

#[cfg(feature = "cli")]
pub fn next_state_transition(
    target: SolarState,
    lat: f64,
    lon: f64,
    now: &ZonedDateTime,
    params: &PositionOptions,
) -> Result<Timestamp, String> {
    if solar_state_at(lat, lon, now, params)? == target {
        return Ok(now.timestamp());
    }
    // A twilight band can be entered from either side.
    let (rising, setting) = match target {
        SolarState::Daylight => (Some(Horizon::SunriseSunset), None),
        SolarState::CivilTwilight => (Some(Horizon::CivilTwilight), Some(Horizon::SunriseSunset)),
        SolarState::NauticalTwilight => (
            Some(Horizon::NauticalTwilight),
            Some(Horizon::CivilTwilight),
        ),
        SolarState::AstronomicalTwilight => (
            Some(Horizon::AstronomicalTwilight),
            Some(Horizon::NauticalTwilight),
        ),
        SolarState::Night => (None, Some(Horizon::AstronomicalTwilight)),
    };
    let start = now.timestamp();
    let end = start + SignedDuration::from_hours(370 * 24);
    let calculator = calculator(params.algorithm);
    let location = Location {
        latitude: lat,
        longitude: lon,
    };
    let deltat = resolve_deltat(now.date(), params);
    let rise = rising
        .map(|h| calculator.next_rise(start, end, location, deltat, h))
        .transpose()
        .map_err(|err| err.to_string())?
        .flatten();
    let set = setting
        .map(|h| calculator.next_set(start, end, location, deltat, h))
        .transpose()
        .map_err(|err| err.to_string())?
        .flatten();
    rise.into_iter().chain(set).min().ok_or_else(|| {
        "Predicate will not become true within the next year at this location".to_string()
    })
}

#[cfg(all(test, feature = "cli"))]
mod tests {
    use super::*;
    use jiff::civil::date;

    fn predicate_params() -> PositionOptions {
        PositionOptions::default()
    }

    #[test]
    fn next_transition_enters_each_band_from_either_direction() {
        for now in ["2024-03-21T00:00:00Z", "2024-03-21T12:00:00Z"] {
            let now = now
                .parse::<Timestamp>()
                .unwrap()
                .to_zoned_datetime(TimeZone::UTC);
            for target in [
                SolarState::Daylight,
                SolarState::CivilTwilight,
                SolarState::NauticalTwilight,
                SolarState::AstronomicalTwilight,
                SolarState::Night,
            ] {
                let params = predicate_params();
                let next = next_state_transition(target, 52.0, 13.4, &now, &params).unwrap();
                let before = (next - SignedDuration::from_secs(1)).to_zoned_datetime(TimeZone::UTC);
                let after = (next + SignedDuration::from_secs(1)).to_zoned_datetime(TimeZone::UTC);
                assert!(next >= now.timestamp());
                assert_eq!(solar_state_at(52.0, 13.4, &after, &params).unwrap(), target,);
                if next > now.timestamp() {
                    assert_ne!(
                        solar_state_at(52.0, 13.4, &before, &params).unwrap(),
                        target,
                    );
                }
            }
        }
    }

    #[test]
    fn next_state_transition_tracks_dst_offset_changes() {
        let params = PositionOptions::default();
        let zone = TimeZone::get("Europe/Berlin").unwrap();
        for (day, expected_offset) in [(date(2024, 3, 30), 7200), (date(2024, 10, 26), 3600)] {
            let now = day.at(22, 0, 0, 0).to_zoned_datetime(zone.clone()).unwrap();
            let transition =
                next_state_transition(SolarState::Daylight, 52.0, 13.4, &now, &params).unwrap();
            let transition = transition.to_zoned_datetime(zone.clone());
            assert_eq!(transition.date(), day.tomorrow().unwrap());
            assert_eq!(transition.offset().seconds(), expected_offset);
        }
    }

    #[test]
    fn next_state_transition_finds_same_day_sunrise() {
        let now = date(2024, 3, 21).to_zoned_datetime(TimeZone::UTC).unwrap();
        let transition =
            next_state_transition(SolarState::Daylight, 52.0, 13.4, &now, &predicate_params())
                .unwrap();
        assert_eq!(
            transition.to_zoned_datetime(TimeZone::UTC).date(),
            now.date()
        );
        assert!(transition > now.timestamp());
    }

    #[test]
    fn next_state_transition_rolls_to_next_day_after_sunset() {
        let now = date(2024, 3, 21)
            .at(22, 0, 0, 0)
            .to_zoned_datetime(TimeZone::UTC)
            .unwrap();
        let transition =
            next_state_transition(SolarState::Daylight, 52.0, 13.4, &now, &predicate_params())
                .unwrap();
        assert!(transition.to_zoned_datetime(TimeZone::UTC).date() > now.date());
    }
}

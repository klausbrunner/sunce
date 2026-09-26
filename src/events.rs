//! Solar events and daylight predicates.

use crate::compute::SolarState;
use crate::data::time_utils::{
    InputTime, TimezoneInfo, convert_datetime_to_timezone, get_timezone_info,
};
use crate::data::{CalculationAlgorithm, Parameters};
use crate::position::{calculator as position_calculator, resolve_deltat};
use chrono::{DateTime, Duration, FixedOffset, NaiveDate, TimeZone};
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

#[derive(Debug, Clone, Copy)]
pub struct EventRow {
    pub lat: f64,
    pub lon: f64,
    pub date: NaiveDate,
    pub deltat: f64,
    pub day_state: DayState,
    pub event: Option<&'static str>,
    pub time: Option<DateTime<FixedOffset>>,
}

fn calculator(params: &Parameters) -> SolarEvents {
    match params.calculation.algorithm {
        CalculationAlgorithm::Spa => SolarEvents::new(),
        CalculationAlgorithm::Grena3 => SolarEvents::grena3(),
    }
}

fn event_labels(horizon: Horizon) -> (&'static str, &'static str) {
    match horizon {
        Horizon::SunriseSunset => ("sunrise", "sunset"),
        Horizon::CivilTwilight => ("civil_dawn", "civil_dusk"),
        Horizon::NauticalTwilight => ("nautical_dawn", "nautical_dusk"),
        Horizon::AstronomicalTwilight => ("astronomical_dawn", "astronomical_dusk"),
        Horizon::Custom(_) => ("rise", "set"),
    }
}

pub fn calculate_events(
    lat: f64,
    lon: f64,
    input: InputTime,
    params: &Parameters,
) -> Result<Vec<EventRow>, String> {
    let date = input.datetime.date_naive();
    let deltat = resolve_deltat(input.datetime, params);
    let horizon = params
        .calculation
        .horizon
        .map(Horizon::Custom)
        .unwrap_or(Horizon::SunriseSunset);
    let horizons = if params.calculation.twilight {
        &HORIZONS[..]
    } else {
        std::slice::from_ref(&horizon)
    };
    let calculator = calculator(params);
    let location = Location {
        latitude: lat,
        longitude: lon,
    };
    match input.zone {
        TimezoneInfo::Fixed(zone) => {
            event_rows(&calculator, date, &zone, location, deltat, horizons)
        }
        TimezoneInfo::Named(zone) => {
            event_rows(&calculator, date, &zone, location, deltat, horizons)
        }
    }
    .map_err(|err| format!("Failed to calculate solar events: {err}"))
}

fn event_rows<Tz: TimeZone>(
    calculator: &SolarEvents,
    date: NaiveDate,
    zone: &Tz,
    location: Location,
    deltat: f64,
    horizons: &[Horizon],
) -> solar_positioning::Result<Vec<EventRow>> {
    let days =
        calculator.for_date_multiple(date, zone, location, deltat, horizons.iter().copied())?;
    let base = EventRow {
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
        rows.push(EventRow {
            event: Some("transit"),
            time: Some(time.fixed_offset()),
            ..base
        });
    }
    for (horizon, day) in days {
        let (rise, set) = event_labels(horizon);
        for (event, times) in [(rise, day.rises), (set, day.sets)] {
            rows.extend(times.into_iter().map(|time| EventRow {
                event: Some(event),
                time: Some(time.fixed_offset()),
                ..base
            }));
        }
    }
    rows.sort_by_key(|row| row.time);
    if rows.is_empty() {
        rows.push(base);
    }
    Ok(rows)
}

pub fn solar_state_at(
    lat: f64,
    lon: f64,
    dt: DateTime<FixedOffset>,
    params: &Parameters,
) -> Result<SolarState, String> {
    let elevation = position_calculator(params)
        .at(
            &dt,
            Location {
                latitude: lat,
                longitude: lon,
            },
            0.0,
            resolve_deltat(dt, params),
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

pub fn is_after_sunset(
    lat: f64,
    lon: f64,
    dt: DateTime<FixedOffset>,
    params: &Parameters,
) -> Result<bool, String> {
    Ok(solar_state_at(lat, lon, dt, params)? != SolarState::Daylight)
}

pub fn next_state_transition(
    target: SolarState,
    lat: f64,
    lon: f64,
    now: DateTime<FixedOffset>,
    params: &Parameters,
) -> Result<DateTime<FixedOffset>, String> {
    if solar_state_at(lat, lon, now, params)? == target {
        return Ok(now);
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
    let end = now + Duration::days(370);
    let calculator = calculator(params);
    let location = Location {
        latitude: lat,
        longitude: lon,
    };
    let deltat = resolve_deltat(now, params);
    let rise = rising
        .map(|h| calculator.next_rise(&now, &end, location, deltat, h))
        .transpose()
        .map_err(|err| err.to_string())?
        .flatten();
    let set = setting
        .map(|h| calculator.next_set(&now, &end, location, deltat, h))
        .transpose()
        .map_err(|err| err.to_string())?
        .flatten();
    let next = rise.into_iter().chain(set).min().ok_or_else(|| {
        "Predicate will not become true within the next year at this location".to_string()
    })?;
    let zone = get_timezone_info(params.timezone.as_ref().map(|tz| tz.as_str()));
    Ok(convert_datetime_to_timezone(next, &zone))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Days, TimeZone};

    fn predicate_params() -> Parameters {
        Parameters {
            timezone: Some("UTC".parse().unwrap()),
            ..Parameters::default()
        }
    }

    #[test]
    fn next_transition_enters_each_band_from_either_direction() {
        for now in ["2024-03-21T00:00:00Z", "2024-03-21T12:00:00Z"] {
            let now = DateTime::parse_from_rfc3339(now).unwrap();
            for target in [
                SolarState::Daylight,
                SolarState::CivilTwilight,
                SolarState::NauticalTwilight,
                SolarState::AstronomicalTwilight,
                SolarState::Night,
            ] {
                let params = predicate_params();
                let next = next_state_transition(target, 52.0, 13.4, now, &params).unwrap();
                assert!(next >= now);
                assert_eq!(
                    solar_state_at(52.0, 13.4, next + Duration::seconds(1), &params).unwrap(),
                    target,
                );
                if next > now {
                    assert_ne!(
                        solar_state_at(52.0, 13.4, next - Duration::seconds(1), &params).unwrap(),
                        target,
                    );
                }
            }
        }
    }

    #[test]
    fn next_state_transition_tracks_dst_offset_changes() {
        let params = Parameters {
            timezone: Some("Europe/Berlin".parse().unwrap()),
            ..Parameters::default()
        };
        for (now, expected_offset) in [
            ("2024-03-30T22:00:00+01:00", 7200),
            ("2024-10-26T22:00:00+02:00", 3600),
        ] {
            let now = DateTime::parse_from_rfc3339(now).unwrap();
            let transition =
                next_state_transition(SolarState::Daylight, 52.0, 13.4, now, &params).unwrap();
            assert_eq!(transition.date_naive(), now.date_naive() + Days::new(1));
            assert_eq!(transition.offset().local_minus_utc(), expected_offset);
        }
    }

    #[test]
    fn next_state_transition_finds_same_day_sunrise() {
        let now = FixedOffset::east_opt(0)
            .unwrap()
            .with_ymd_and_hms(2024, 3, 21, 0, 0, 0)
            .unwrap();
        let transition =
            next_state_transition(SolarState::Daylight, 52.0, 13.4, now, &predicate_params())
                .unwrap();
        assert_eq!(transition.date_naive(), now.date_naive());
        assert!(transition > now);
    }

    #[test]
    fn next_state_transition_rolls_to_next_day_after_sunset() {
        let now = FixedOffset::east_opt(0)
            .unwrap()
            .with_ymd_and_hms(2024, 3, 21, 22, 0, 0)
            .unwrap();
        let transition =
            next_state_transition(SolarState::Daylight, 52.0, 13.4, now, &predicate_params())
                .unwrap();
        assert!(transition.date_naive() > now.date_naive());
    }
}

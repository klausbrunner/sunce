//! Sunrise, twilight, and solar-state calculations.

use crate::compute::{CalculationResult, SolarState};
use crate::data::Parameters;
use crate::position::resolve_deltat;
use chrono::{DateTime, Days, FixedOffset, NaiveDate};
use solar_positioning::{Horizon, SunriseResult};

const MAX_WAIT_SEARCH_DAYS: u64 = 370;

#[derive(Debug, Clone)]
struct TwilightResults {
    sunrise_sunset: SunriseResult<DateTime<FixedOffset>>,
    civil: SunriseResult<DateTime<FixedOffset>>,
    nautical: SunriseResult<DateTime<FixedOffset>>,
    astronomical: SunriseResult<DateTime<FixedOffset>>,
}

fn calculate_twilight_results(
    lat: f64,
    lon: f64,
    dt: DateTime<FixedOffset>,
    deltat: f64,
) -> Result<TwilightResults, String> {
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

    Ok(TwilightResults {
        sunrise_sunset: results
            .next()
            .transpose()?
            .ok_or_else(|| "Failed to calculate twilight: incomplete result set".to_string())?,
        civil: results
            .next()
            .transpose()?
            .ok_or_else(|| "Failed to calculate twilight: incomplete result set".to_string())?,
        nautical: results
            .next()
            .transpose()?
            .ok_or_else(|| "Failed to calculate twilight: incomplete result set".to_string())?,
        astronomical: results
            .next()
            .transpose()?
            .ok_or_else(|| "Failed to calculate twilight: incomplete result set".to_string())?,
    })
}

fn is_above_horizon(
    result: &SunriseResult<DateTime<FixedOffset>>,
    dt: DateTime<FixedOffset>,
) -> bool {
    match result {
        SunriseResult::RegularDay {
            sunrise, sunset, ..
        } => *sunrise <= dt && dt < *sunset,
        SunriseResult::AllDay { .. } => true,
        SunriseResult::AllNight { .. } => false,
    }
}

fn classify_solar_state(results: &TwilightResults, dt: DateTime<FixedOffset>) -> SolarState {
    if is_above_horizon(&results.sunrise_sunset, dt) {
        SolarState::Daylight
    } else if is_above_horizon(&results.civil, dt) {
        SolarState::CivilTwilight
    } else if is_above_horizon(&results.nautical, dt) {
        SolarState::NauticalTwilight
    } else if is_above_horizon(&results.astronomical, dt) {
        SolarState::AstronomicalTwilight
    } else {
        SolarState::Night
    }
}

fn local_datetime(
    date: NaiveDate,
    hour: u32,
    minute: u32,
    second: u32,
    params: &Parameters,
) -> Result<DateTime<FixedOffset>, String> {
    crate::data::parse_datetime_string(
        &format!(
            "{}T{:02}:{:02}:{:02}",
            date.format("%F"),
            hour,
            minute,
            second
        ),
        params.timezone.as_ref().map(|tz| tz.as_str()),
    )
}

fn local_midnight(date: NaiveDate, params: &Parameters) -> Result<DateTime<FixedOffset>, String> {
    local_datetime(date, 0, 0, 0, params)
}

fn local_noon(date: NaiveDate, params: &Parameters) -> Result<DateTime<FixedOffset>, String> {
    local_datetime(date, 12, 0, 0, params)
}

fn next_matching_state_start(
    results: &TwilightResults,
    target: SolarState,
    day_start: DateTime<FixedOffset>,
    day_end: DateTime<FixedOffset>,
) -> Option<DateTime<FixedOffset>> {
    let mut boundaries = vec![day_start, day_end];
    for result in [
        &results.sunrise_sunset,
        &results.civil,
        &results.nautical,
        &results.astronomical,
    ] {
        if let SunriseResult::RegularDay {
            sunrise, sunset, ..
        } = result
        {
            if day_start <= *sunrise && *sunrise <= day_end {
                boundaries.push(*sunrise);
            }
            if day_start <= *sunset && *sunset <= day_end {
                boundaries.push(*sunset);
            }
        }
    }
    boundaries.sort_unstable();
    boundaries.dedup();

    boundaries.windows(2).find_map(|window| {
        let [start, end] = [window[0], window[1]];
        (start < end && classify_solar_state(results, start) == target).then_some(start)
    })
}

pub fn next_state_transition(
    target: SolarState,
    lat: f64,
    lon: f64,
    now: DateTime<FixedOffset>,
    params: &Parameters,
) -> Result<DateTime<FixedOffset>, String> {
    for day_offset in 0..=MAX_WAIT_SEARCH_DAYS {
        let date = now
            .date_naive()
            .checked_add_days(Days::new(day_offset))
            .ok_or_else(|| "Failed to search future wait dates".to_string())?;
        let day_start = local_midnight(date, params)?;
        let day_end = local_midnight(
            date.checked_add_days(Days::new(1))
                .ok_or_else(|| "Failed to search future wait dates".to_string())?,
            params,
        )?;
        let anchor = local_noon(date, params)?;
        let results = calculate_twilight_results(lat, lon, anchor, resolve_deltat(anchor, params))?;

        if let Some(next_start) =
            next_matching_state_start(&results, target, now.max(day_start), day_end)
        {
            return Ok(next_start);
        }
    }

    Err("Predicate will not become true within the next year at this location".to_string())
}

pub fn solar_state_at(
    lat: f64,
    lon: f64,
    dt: DateTime<FixedOffset>,
    params: &Parameters,
) -> Result<SolarState, String> {
    let deltat = resolve_deltat(dt, params);
    Ok(classify_solar_state(
        &calculate_twilight_results(lat, lon, dt, deltat)?,
        dt,
    ))
}

pub fn is_after_sunset(
    lat: f64,
    lon: f64,
    dt: DateTime<FixedOffset>,
    params: &Parameters,
) -> Result<bool, String> {
    let deltat = resolve_deltat(dt, params);
    let result = solar_positioning::spa::sunrise_sunset_for_horizon(
        dt,
        lat,
        lon,
        deltat,
        Horizon::SunriseSunset,
    )
    .map_err(|e| format!("Failed to calculate sunrise/sunset: {}", e))?;

    Ok(match result {
        SunriseResult::RegularDay {
            sunrise, sunset, ..
        } => dt < sunrise || dt >= sunset,
        SunriseResult::AllDay { .. } => false,
        SunriseResult::AllNight { .. } => true,
    })
}

pub fn calculate_sunrise(
    lat: f64,
    lon: f64,
    dt: DateTime<FixedOffset>,
    params: &Parameters,
) -> Result<CalculationResult, String> {
    let deltat = resolve_deltat(dt, params);

    if params.calculation.twilight {
        let TwilightResults {
            sunrise_sunset,
            civil,
            nautical,
            astronomical,
        } = calculate_twilight_results(lat, lon, dt, deltat)?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn predicate_params() -> Parameters {
        Parameters {
            timezone: Some("UTC".parse().unwrap()),
            ..Parameters::default()
        }
    }

    #[test]
    fn sunrise_interval_is_half_open() {
        let tz = FixedOffset::east_opt(0).unwrap();
        let sunrise = tz.with_ymd_and_hms(2024, 3, 21, 6, 0, 0).unwrap();
        let sunset = tz.with_ymd_and_hms(2024, 3, 21, 18, 0, 0).unwrap();
        let result = SunriseResult::RegularDay {
            sunrise,
            transit: tz.with_ymd_and_hms(2024, 3, 21, 12, 0, 0).unwrap(),
            sunset,
        };

        assert!(!is_above_horizon(
            &result,
            tz.with_ymd_and_hms(2024, 3, 21, 5, 59, 59).unwrap()
        ));
        assert!(is_above_horizon(&result, sunrise));
        assert!(!is_above_horizon(&result, sunset));
    }

    #[test]
    fn solar_state_classification_prefers_brightest_matching_horizon() {
        let tz = FixedOffset::east_opt(0).unwrap();
        let results = TwilightResults {
            sunrise_sunset: SunriseResult::RegularDay {
                sunrise: tz.with_ymd_and_hms(2024, 3, 21, 6, 0, 0).unwrap(),
                transit: tz.with_ymd_and_hms(2024, 3, 21, 12, 0, 0).unwrap(),
                sunset: tz.with_ymd_and_hms(2024, 3, 21, 18, 0, 0).unwrap(),
            },
            civil: SunriseResult::RegularDay {
                sunrise: tz.with_ymd_and_hms(2024, 3, 21, 5, 30, 0).unwrap(),
                transit: tz.with_ymd_and_hms(2024, 3, 21, 12, 0, 0).unwrap(),
                sunset: tz.with_ymd_and_hms(2024, 3, 21, 18, 30, 0).unwrap(),
            },
            nautical: SunriseResult::RegularDay {
                sunrise: tz.with_ymd_and_hms(2024, 3, 21, 5, 0, 0).unwrap(),
                transit: tz.with_ymd_and_hms(2024, 3, 21, 12, 0, 0).unwrap(),
                sunset: tz.with_ymd_and_hms(2024, 3, 21, 19, 0, 0).unwrap(),
            },
            astronomical: SunriseResult::RegularDay {
                sunrise: tz.with_ymd_and_hms(2024, 3, 21, 4, 30, 0).unwrap(),
                transit: tz.with_ymd_and_hms(2024, 3, 21, 12, 0, 0).unwrap(),
                sunset: tz.with_ymd_and_hms(2024, 3, 21, 19, 30, 0).unwrap(),
            },
        };

        assert_eq!(
            classify_solar_state(
                &results,
                tz.with_ymd_and_hms(2024, 3, 21, 6, 15, 0).unwrap()
            ),
            SolarState::Daylight
        );
        assert_eq!(
            classify_solar_state(
                &results,
                tz.with_ymd_and_hms(2024, 3, 21, 5, 45, 0).unwrap()
            ),
            SolarState::CivilTwilight
        );
        assert_eq!(
            classify_solar_state(
                &results,
                tz.with_ymd_and_hms(2024, 3, 21, 5, 15, 0).unwrap()
            ),
            SolarState::NauticalTwilight
        );
        assert_eq!(
            classify_solar_state(
                &results,
                tz.with_ymd_and_hms(2024, 3, 21, 4, 45, 0).unwrap()
            ),
            SolarState::AstronomicalTwilight
        );
        assert_eq!(
            classify_solar_state(&results, tz.with_ymd_and_hms(2024, 3, 21, 4, 0, 0).unwrap()),
            SolarState::Night
        );
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

//! Predicate evaluation and wait-until logic for automation use cases.

use crate::compute::SolarState;
use crate::data::time_utils::{convert_datetime_to_timezone, get_timezone_info};
use crate::data::{Parameters, Predicate as CliPredicate};
use crate::events::{is_after_sunset, next_state_transition, solar_state_at};
use crate::position::{calculator, refraction_correction, resolve_deltat, solar_elevation_at};
use chrono::{DateTime, FixedOffset};
use solar_positioning::{ElevationTarget, Location};

const STATE_WAIT_MARGIN: chrono::Duration = chrono::Duration::minutes(2);
const STATE_NEAR_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
const MAX_WAIT_SLEEP: std::time::Duration = std::time::Duration::from_secs(60);

#[derive(Debug, Clone)]
pub enum PredicateTime {
    Fixed(DateTime<FixedOffset>),
    Now,
}

#[derive(Debug, Clone, Copy)]
pub enum PredicateCheck {
    State(SolarState),
    AfterSunset,
    ElevationAbove(f64),
    ElevationBelow(f64),
}

#[derive(Debug)]
pub struct PredicateJob {
    pub lat: f64,
    pub lon: f64,
    pub time: PredicateTime,
    pub check: PredicateCheck,
    pub wait: bool,
    pub params: Parameters,
}

impl PredicateCheck {
    pub fn from_cli(predicate: CliPredicate) -> Self {
        match predicate {
            CliPredicate::IsDaylight => Self::State(SolarState::Daylight),
            CliPredicate::IsCivilTwilight => Self::State(SolarState::CivilTwilight),
            CliPredicate::IsNauticalTwilight => Self::State(SolarState::NauticalTwilight),
            CliPredicate::IsAstronomicalTwilight => Self::State(SolarState::AstronomicalTwilight),
            CliPredicate::IsAstronomicalNight => Self::State(SolarState::Night),
            CliPredicate::AfterSunset => Self::AfterSunset,
            CliPredicate::SunAbove(threshold) => Self::ElevationAbove(threshold),
            CliPredicate::SunBelow(threshold) => Self::ElevationBelow(threshold),
        }
    }
}

impl std::fmt::Display for PredicateCheck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::State(state) => f.write_str(match state {
                SolarState::Daylight => "daylight",
                SolarState::CivilTwilight => "civil twilight",
                SolarState::NauticalTwilight => "nautical twilight",
                SolarState::AstronomicalTwilight => "astronomical twilight",
                SolarState::Night => "astronomical night",
            }),
            Self::AfterSunset => f.write_str("after sunset"),
            Self::ElevationAbove(angle) => write!(f, "elevation above {angle} degrees"),
            Self::ElevationBelow(angle) => write!(f, "elevation below {angle} degrees"),
        }
    }
}

fn resolve_time(
    time: &PredicateTime,
    params: &Parameters,
) -> Result<DateTime<FixedOffset>, String> {
    match time {
        PredicateTime::Fixed(dt) => Ok(*dt),
        PredicateTime::Now => crate::data::parse_datetime_string(
            "now",
            params.timezone.as_ref().map(|tz| tz.as_str()),
        ),
    }
}

fn wait_duration_until(
    now: DateTime<FixedOffset>,
    target: DateTime<FixedOffset>,
) -> std::time::Duration {
    let near_start = target - STATE_WAIT_MARGIN;
    if now < near_start {
        (near_start - now)
            .to_std()
            .expect("near transition must be after current time")
            .min(MAX_WAIT_SLEEP)
    } else {
        STATE_NEAR_POLL_INTERVAL
    }
}

fn sleep_until(job: &PredicateJob, target: DateTime<FixedOffset>) -> Result<(), String> {
    loop {
        let now = resolve_time(&job.time, &job.params)?;
        if now >= target {
            return Ok(());
        }
        std::thread::sleep(wait_duration_until(now, target));
    }
}

fn next_angle_transition(
    job: &PredicateJob,
    now: DateTime<FixedOffset>,
    threshold: f64,
    rising: bool,
) -> Result<DateTime<FixedOffset>, String> {
    let calculator = calculator(&job.params.position_options());
    let end = now + chrono::Duration::days(370);
    let location = Location {
        latitude: job.lat,
        longitude: job.lon,
    };
    let deltat = resolve_deltat(now, &job.params.position_options());
    let target = ElevationTarget {
        angle: threshold,
        height: job.params.environment.elevation,
        refraction: refraction_correction(&job.params.position_options())?,
    };
    let next = if rising {
        calculator.next_elevation_rise(&now, &end, location, deltat, target)
    } else {
        calculator.next_elevation_set(&now, &end, location, deltat, target)
    };
    next.map_err(|err| format!("Failed to calculate elevation crossing: {err}"))?
        .ok_or_else(|| {
            "Predicate will not become true within the next year at this location".to_string()
        })
}

pub fn run_once(job: &PredicateJob) -> Result<bool, String> {
    let result = predicate_at(job, resolve_time(&job.time, &job.params)?)?;
    if job.params.verbose {
        eprintln!("Predicate {}: {result}.", job.check);
    }
    Ok(result)
}

fn predicate_at(job: &PredicateJob, now: DateTime<FixedOffset>) -> Result<bool, String> {
    match job.check {
        PredicateCheck::State(state) => {
            Ok(solar_state_at(job.lat, job.lon, now, &job.params.position_options())? == state)
        }
        PredicateCheck::AfterSunset => {
            is_after_sunset(job.lat, job.lon, now, &job.params.position_options())
        }
        PredicateCheck::ElevationAbove(threshold) => {
            Ok(
                solar_elevation_at(job.lat, job.lon, now, &job.params.position_options())?
                    > threshold,
            )
        }
        PredicateCheck::ElevationBelow(threshold) => {
            Ok(
                solar_elevation_at(job.lat, job.lon, now, &job.params.position_options())?
                    < threshold,
            )
        }
    }
}

pub fn wait_until_true(job: &PredicateJob) -> Result<(), String> {
    let mut waited = false;
    loop {
        let now = resolve_time(&job.time, &job.params)?;
        if predicate_at(job, now)? {
            if job.params.verbose {
                let status = if waited { "satisfied" } else { "already true" };
                eprintln!("Predicate {}: {status}.", job.check);
            }
            return Ok(());
        }
        let target = match job.check {
            PredicateCheck::AfterSunset => next_state_transition(
                SolarState::CivilTwilight,
                job.lat,
                job.lon,
                now,
                &job.params.position_options(),
                get_timezone_info(job.params.timezone.as_ref().map(|tz| tz.as_str())),
            )?,
            PredicateCheck::State(state) => next_state_transition(
                state,
                job.lat,
                job.lon,
                now,
                &job.params.position_options(),
                get_timezone_info(job.params.timezone.as_ref().map(|tz| tz.as_str())),
            )?,
            PredicateCheck::ElevationAbove(threshold) => {
                next_angle_transition(job, now, threshold, true)?
            }
            PredicateCheck::ElevationBelow(threshold) => {
                next_angle_transition(job, now, threshold, false)?
            }
        };
        if job.params.verbose {
            let zone = get_timezone_info(job.params.timezone.as_ref().map(|tz| tz.as_str()));
            let transition = convert_datetime_to_timezone(target, &zone);
            let seconds = (target - now).num_seconds().max(0);
            eprintln!(
                "Waiting for {}; estimated transition at {} (in {}h {}m {}s).",
                job.check,
                transition.to_rfc3339_opts(chrono::SecondsFormat::Secs, false),
                seconds / 3600,
                seconds / 60 % 60,
                seconds % 60
            );
        }
        // Angle predicates use strict inequalities, so wake just after the crossing.
        let deadline = if matches!(
            job.check,
            PredicateCheck::ElevationAbove(_) | PredicateCheck::ElevationBelow(_)
        ) {
            target + chrono::Duration::seconds(1)
        } else {
            target
        };
        sleep_until(job, deadline)?;
        waited = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::CalculationAlgorithm;
    use chrono::TimeZone;

    #[test]
    fn wait_duration_until_caps_sleep_before_transition_window() {
        let tz = FixedOffset::east_opt(0).unwrap();
        let now = tz.with_ymd_and_hms(2024, 3, 21, 5, 0, 0).unwrap();
        for (seconds_until_target, expected_sleep) in
            [(150, 30), (180, 60), (3600, 60), (30 * 86400, 60)]
        {
            let target = now + chrono::Duration::seconds(seconds_until_target);
            assert_eq!(
                wait_duration_until(now, target),
                std::time::Duration::from_secs(expected_sleep)
            );
        }
    }

    #[test]
    fn wait_duration_until_uses_one_second_poll_near_transition() {
        let tz = FixedOffset::east_opt(0).unwrap();
        let now = tz.with_ymd_and_hms(2024, 3, 21, 5, 58, 30).unwrap();
        let target = tz.with_ymd_and_hms(2024, 3, 21, 6, 0, 0).unwrap();
        assert_eq!(wait_duration_until(now, target), STATE_NEAR_POLL_INTERVAL);
    }

    #[test]
    fn wait_duration_until_handles_exact_transition_window() {
        let tz = FixedOffset::east_opt(0).unwrap();
        let now = tz.with_ymd_and_hms(2024, 3, 21, 5, 58, 0).unwrap();
        let target = tz.with_ymd_and_hms(2024, 3, 21, 6, 0, 0).unwrap();
        assert_eq!(wait_duration_until(now, target), STATE_NEAR_POLL_INTERVAL);
    }

    #[test]
    fn angle_transitions_match_predicates_with_observation_settings() {
        for (algorithm, height) in [
            (CalculationAlgorithm::Spa, 5000.0),
            (CalculationAlgorithm::Grena3, 0.0),
        ] {
            for refraction in [false, true] {
                let mut params = Parameters::default();
                params.calculation.deltat = Some(200.0);
                params.calculation.algorithm = algorithm;
                params.environment.elevation = height;
                params.environment.refraction = refraction;
                params.environment.pressure = 850.0;
                params.environment.temperature = -15.0;
                for (hour, check, rising) in [
                    (0, PredicateCheck::ElevationAbove(1.0), true),
                    (12, PredicateCheck::ElevationBelow(1.0), false),
                ] {
                    let now = FixedOffset::east_opt(0)
                        .unwrap()
                        .with_ymd_and_hms(2024, 12, 21, hour, 0, 0)
                        .unwrap();
                    let mut job = PredicateJob {
                        lat: 63.0,
                        lon: 13.4,
                        time: PredicateTime::Fixed(now),
                        check,
                        wait: true,
                        params: params.clone(),
                    };
                    assert!(!run_once(&job).unwrap());
                    let next = next_angle_transition(&job, now, 1.0, rising).unwrap();
                    assert!(next > now);
                    for (offset, expected) in [(-1100, false), (1100, true)] {
                        job.time =
                            PredicateTime::Fixed(next + chrono::Duration::microseconds(offset));
                        assert_eq!(run_once(&job).unwrap(), expected);
                    }
                }
            }
        }
    }
}

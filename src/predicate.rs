//! Predicate evaluation and wait-until logic for automation use cases.

use crate::compute::SolarState;
use crate::data::time_utils::get_timezone_info;
use crate::data::{Parameters, Predicate as CliPredicate};
use crate::events::{is_after_sunset, next_state_transition, solar_state_at};
use crate::position::{calculator, refraction_correction, resolve_deltat, solar_elevation_at};
use jiff::{SignedDuration, Timestamp, ZonedDateTime};
use solar_positioning::{ElevationTarget, Location};

const STATE_WAIT_MARGIN: SignedDuration = SignedDuration::from_mins(2);
const STATE_NEAR_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
const MAX_WAIT_SLEEP: std::time::Duration = std::time::Duration::from_secs(60);

#[derive(Debug, Clone)]
pub enum PredicateTime {
    Fixed(ZonedDateTime),
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

fn wait_duration_until(now: Timestamp, target: Timestamp) -> std::time::Duration {
    let near_start = target - STATE_WAIT_MARGIN;
    if now < near_start {
        std::time::Duration::try_from(near_start.duration_since(now))
            .expect("near transition must be after current time")
            .min(MAX_WAIT_SLEEP)
    } else {
        STATE_NEAR_POLL_INTERVAL
    }
}

fn sleep_until(target: Timestamp) {
    loop {
        let now = Timestamp::now();
        if now >= target {
            return;
        }
        std::thread::sleep(wait_duration_until(now, target));
    }
}

fn next_angle_transition(
    job: &PredicateJob,
    now: &ZonedDateTime,
    threshold: f64,
    rising: bool,
) -> Result<Timestamp, String> {
    let options = job.params.position_options();
    let calculator = calculator(&options);
    let start = now.timestamp();
    let end = start + SignedDuration::from_hours(370 * 24);
    let location = Location {
        latitude: job.lat,
        longitude: job.lon,
    };
    let deltat = resolve_deltat(now.date(), &options);
    let target = ElevationTarget {
        angle: threshold,
        height: job.params.environment.elevation,
        refraction: refraction_correction(&options)?,
    };
    let next = if rising {
        calculator.next_elevation_rise(start, end, location, deltat, target)
    } else {
        calculator.next_elevation_set(start, end, location, deltat, target)
    };
    next.map_err(|err| format!("Failed to calculate elevation crossing: {err}"))?
        .ok_or_else(|| {
            "Predicate will not become true within the next year at this location".to_string()
        })
}

pub fn run_once(job: &PredicateJob) -> Result<bool, String> {
    let result = match &job.time {
        PredicateTime::Fixed(time) => predicate_at(job, time),
        PredicateTime::Now => {
            let zone = get_timezone_info(job.params.timezone.as_ref());
            predicate_at(job, &Timestamp::now().to_zoned_datetime(zone))
        }
    }?;
    if job.params.verbose {
        eprintln!("Predicate {}: {result}.", job.check);
    }
    Ok(result)
}

fn predicate_at(job: &PredicateJob, now: &ZonedDateTime) -> Result<bool, String> {
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
    let zone = get_timezone_info(job.params.timezone.as_ref());
    let mut waited = false;
    loop {
        let now = Timestamp::now().to_zoned_datetime(zone.clone());
        if predicate_at(job, &now)? {
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
                &now,
                &job.params.position_options(),
            )?,
            PredicateCheck::State(state) => next_state_transition(
                state,
                job.lat,
                job.lon,
                &now,
                &job.params.position_options(),
            )?,
            PredicateCheck::ElevationAbove(threshold) => {
                next_angle_transition(job, &now, threshold, true)?
            }
            PredicateCheck::ElevationBelow(threshold) => {
                next_angle_transition(job, &now, threshold, false)?
            }
        };
        if job.params.verbose {
            let transition = crate::output::format_timestamp(
                target,
                zone.to_offset(target),
                crate::output::TimePrecision::Seconds,
            );
            let seconds = target.duration_since(now.timestamp()).as_secs().max(0);
            eprintln!(
                "Waiting for {}; estimated transition at {} (in {}h {}m {}s).",
                job.check,
                transition,
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
            target + SignedDuration::from_secs(1)
        } else {
            target
        };
        sleep_until(deadline);
        waited = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::CalculationAlgorithm;
    use jiff::{civil::date, tz::TimeZone};

    #[test]
    fn wait_duration_until_caps_sleep_before_transition_window() {
        let now: Timestamp = "2024-03-21T05:00:00Z".parse().unwrap();
        for (seconds_until_target, expected_sleep) in
            [(150, 30), (180, 60), (3600, 60), (30 * 86400, 60)]
        {
            let target = now + SignedDuration::from_secs(seconds_until_target);
            assert_eq!(
                wait_duration_until(now, target),
                std::time::Duration::from_secs(expected_sleep)
            );
        }
    }

    #[test]
    fn wait_duration_until_uses_one_second_poll_near_transition() {
        let now = "2024-03-21T05:58:30Z".parse().unwrap();
        let target = "2024-03-21T06:00:00Z".parse().unwrap();
        assert_eq!(wait_duration_until(now, target), STATE_NEAR_POLL_INTERVAL);
    }

    #[test]
    fn wait_duration_until_handles_exact_transition_window() {
        let now = "2024-03-21T05:58:00Z".parse().unwrap();
        let target = "2024-03-21T06:00:00Z".parse().unwrap();
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
                    let now = date(2024, 12, 21)
                        .at(hour, 0, 0, 0)
                        .to_zoned_datetime(TimeZone::UTC)
                        .unwrap();
                    let mut job = PredicateJob {
                        lat: 63.0,
                        lon: 13.4,
                        time: PredicateTime::Fixed(now.clone()),
                        check,
                        params: params.clone(),
                    };
                    assert!(!run_once(&job).unwrap());
                    let next = next_angle_transition(&job, &now, 1.0, rising).unwrap();
                    assert!(next > now.timestamp());
                    // The solver bounds crossings to 0.1 s; waits allow one second.
                    for (seconds, expected) in [(-1, false), (1, true)] {
                        job.time = PredicateTime::Fixed(
                            (next + SignedDuration::from_secs(seconds))
                                .to_zoned_datetime(TimeZone::UTC),
                        );
                        assert_eq!(
                            run_once(&job).unwrap(),
                            expected,
                            "{algorithm:?}, {check:?}, refraction={refraction}"
                        );
                    }
                }
            }
        }
    }
}

//! Predicate evaluation and wait-until logic for automation use cases.

use crate::compute::SolarState;
use crate::data::{Parameters, Predicate as CliPredicate};
use crate::position::solar_elevation_at;
use crate::sunrise::{is_after_sunset, next_state_transition, solar_state_at};
use chrono::{DateTime, FixedOffset};

const ANGLE_WAIT_MIN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
const ANGLE_WAIT_MAX_INTERVAL: std::time::Duration = std::time::Duration::from_secs(600);
const STATE_WAIT_MARGIN: chrono::Duration = chrono::Duration::minutes(2);
const STATE_NEAR_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

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

fn angle_wait_duration(current_elevation: f64, threshold: f64) -> std::time::Duration {
    let seconds = ((current_elevation - threshold).abs() * 120.0).clamp(
        ANGLE_WAIT_MIN_INTERVAL.as_secs_f64(),
        ANGLE_WAIT_MAX_INTERVAL.as_secs_f64(),
    );
    std::time::Duration::from_secs_f64(seconds)
}

pub fn run_once(job: &PredicateJob) -> Result<bool, String> {
    let now = resolve_time(&job.time, &job.params)?;
    match job.check {
        PredicateCheck::State(state) => {
            Ok(solar_state_at(job.lat, job.lon, now, &job.params)? == state)
        }
        PredicateCheck::AfterSunset => is_after_sunset(job.lat, job.lon, now, &job.params),
        PredicateCheck::ElevationAbove(threshold) => {
            Ok(solar_elevation_at(job.lat, job.lon, now, &job.params)? > threshold)
        }
        PredicateCheck::ElevationBelow(threshold) => {
            Ok(solar_elevation_at(job.lat, job.lon, now, &job.params)? < threshold)
        }
    }
}

pub fn wait_until_true(job: &PredicateJob) -> Result<(), String> {
    match job.check {
        PredicateCheck::AfterSunset => loop {
            let now = resolve_time(&job.time, &job.params)?;
            if is_after_sunset(job.lat, job.lon, now, &job.params)? {
                return Ok(());
            }

            let target = next_state_transition(
                SolarState::CivilTwilight,
                job.lat,
                job.lon,
                now,
                &job.params,
            )?;
            sleep_until(job, target)?;
        },
        PredicateCheck::ElevationAbove(threshold) | PredicateCheck::ElevationBelow(threshold) => {
            let wait_for_above = matches!(job.check, PredicateCheck::ElevationAbove(_));
            loop {
                let now = resolve_time(&job.time, &job.params)?;
                let elevation = solar_elevation_at(job.lat, job.lon, now, &job.params)?;
                if (wait_for_above && elevation > threshold)
                    || (!wait_for_above && elevation < threshold)
                {
                    return Ok(());
                }
                std::thread::sleep(angle_wait_duration(elevation, threshold));
            }
        }
        PredicateCheck::State(state) => loop {
            let now = resolve_time(&job.time, &job.params)?;
            if solar_state_at(job.lat, job.lon, now, &job.params)? == state {
                return Ok(());
            }

            let target = next_state_transition(state, job.lat, job.lon, now, &job.params)?;
            sleep_until(job, target)?;
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn wait_duration_until_uses_long_sleep_before_transition_window() {
        let tz = FixedOffset::east_opt(0).unwrap();
        let now = tz.with_ymd_and_hms(2024, 3, 21, 5, 0, 0).unwrap();
        let target = tz.with_ymd_and_hms(2024, 3, 21, 6, 0, 0).unwrap();
        assert_eq!(
            wait_duration_until(now, target),
            std::time::Duration::from_secs(58 * 60)
        );
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
    fn angle_wait_duration_scales_with_threshold_distance() {
        assert_eq!(
            angle_wait_duration(8.0, 10.0),
            std::time::Duration::from_secs(240)
        );
    }

    #[test]
    fn angle_wait_duration_has_one_second_floor() {
        assert_eq!(angle_wait_duration(9.995, 10.0), ANGLE_WAIT_MIN_INTERVAL);
    }

    #[test]
    fn angle_wait_duration_has_ten_minute_cap() {
        assert_eq!(angle_wait_duration(-10.0, 10.0), ANGLE_WAIT_MAX_INTERVAL);
    }
}

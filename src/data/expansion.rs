//! Input expansion for ranges, files, and cartesian products.

#[cfg(feature = "cli")]
use super::time_utils::{at_or_after_local, get_timezone_info, parse_input_time};
#[cfg(feature = "cli")]
use super::types::{CoordTimeResult, CoordTimeStream, InputPath, TimeSource};
#[cfg(feature = "cli")]
use super::types::{LocationSource, LocationStream};
#[cfg(feature = "cli")]
use super::{Command, Step};
#[cfg(feature = "cli")]
use crate::input::{self, Line, non_comment_lines, read_locations_from, read_samples};
#[cfg(feature = "cli")]
use jiff::{SignedDuration, Timestamp, ZonedDateTime, civil::Time, tz::TimeZone};
#[cfg(feature = "cli")]
use std::fs::File;
#[cfg(feature = "cli")]
use std::io::{self, BufRead, BufReader};

#[cfg(feature = "cli")]
type TimeIter = Box<dyn Iterator<Item = Result<ZonedDateTime, String>>>;

#[derive(Clone)]
pub(crate) struct CoordRangeIter {
    start: f64,
    end: f64,
    step: f64,
    index: usize,
    done: bool,
}

impl CoordRangeIter {
    pub(crate) fn new(start: f64, end: f64, step: f64) -> Self {
        Self {
            start,
            end,
            step,
            index: 0,
            done: false,
        }
    }
}

impl Iterator for CoordRangeIter {
    type Item = f64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let candidate = self.start + self.step * self.index as f64;
        let tolerance = f64::EPSILON * candidate.abs().max(self.end.abs()).max(1.0) * 8.0;
        let past_end = if self.step >= 0.0 {
            candidate > self.end + tolerance
        } else {
            candidate < self.end - tolerance
        };
        if past_end {
            self.done = true;
            return None;
        }

        let current = if (candidate - self.end).abs() <= tolerance {
            self.end
        } else {
            candidate
        };

        if current == self.end {
            self.done = true;
        } else {
            self.index += 1;
        }

        Some(current)
    }
}

#[cfg(feature = "cli")]
fn open_input(input_path: &InputPath) -> io::Result<Box<dyn BufRead>> {
    match input_path {
        InputPath::Stdin => Ok(Box::new(BufReader::new(io::stdin()))),
        InputPath::File(path) => {
            let file = File::open(path)?;
            Ok(Box::new(BufReader::new(file)))
        }
    }
}

#[cfg(feature = "cli")]
fn input_context(input_path: &InputPath) -> String {
    match input_path {
        InputPath::Stdin => "stdin".to_owned(),
        InputPath::File(path) => path.display().to_string(),
    }
}

#[cfg(feature = "cli")]
fn is_replayable(input: &InputPath) -> Result<bool, String> {
    match input {
        InputPath::Stdin => Ok(false),
        InputPath::File(path) => std::fs::metadata(path)
            .map(|metadata| metadata.is_file())
            .map_err(|err| format!("Error opening {}: {err}", path.display())),
    }
}

#[cfg(feature = "cli")]
fn read_non_comment_lines(
    input_path: &InputPath,
) -> Result<Box<dyn Iterator<Item = Result<Line, String>>>, String> {
    let ctx = input_context(input_path);
    let reader = open_input(input_path).map_err(|e| format!("Error opening {}: {}", ctx, e))?;
    Ok(non_comment_lines(reader, ctx))
}

#[cfg(feature = "cli")]
pub fn expand_location_source(source: LocationSource) -> Result<LocationStream, String> {
    match source {
        #[cfg(feature = "cli")]
        LocationSource::Single(lat, lon) => Ok(Box::new(std::iter::once(Ok((lat, lon))))),
        LocationSource::Range { lat, lon } => {
            let grid =
                crate::input::CoordinateGrid::new(lat, lon).map_err(|err| err.to_string())?;
            Ok(Box::new(grid.locations().map(|location| {
                Ok((location.latitude, location.longitude))
            })))
        }
        #[cfg(feature = "cli")]
        LocationSource::File(input_path) => {
            let context = input_context(&input_path);
            let reader =
                open_input(&input_path).map_err(|err| format!("Error opening {context}: {err}"))?;
            Ok(Box::new(read_locations_from(reader, context).map(|item| {
                item.map(|location| (location.latitude, location.longitude))
            })))
        }
    }
}

#[cfg(feature = "cli")]
pub fn expand_time_source(
    source: TimeSource,
    step_override: Option<Step>,
    override_tz: Option<TimeZone>,
    command: Command,
) -> Result<TimeIter, String> {
    match source {
        TimeSource::Single(dt) => Ok(Box::new(std::iter::once(Ok(dt)))),
        TimeSource::Daily(period, clock) => {
            let zone = get_timezone_info(override_tz.as_ref());
            let times =
                crate::input::daily_times(&period, zone, clock).map_err(|e| e.to_string())?;
            Ok(Box::new(times.map(|time| time.map_err(|e| e.to_string()))))
        }
        TimeSource::Range(period) => {
            let zone = get_timezone_info(override_tz.as_ref());
            if command == Command::Events {
                let (start, end) = input::date_bounds(&period).map_err(|e| e.to_string())?;
                let dates = input::event_dates(start, end).map_err(|e| e.to_string())?;
                return Ok(Box::new(dates.map(move |date| {
                    at_or_after_local(&zone, date.to_datetime(Time::MIN))
                        .map(|time| time.to_zoned_datetime(zone.clone()))
                        .filter(|time| time.date() == date)
                        .ok_or_else(|| format!("Date does not exist in timezone: {date}"))
                })));
            }
            let step = step_override.unwrap_or_else(|| {
                Step(SignedDuration::from_hours(if period.len() == 4 {
                    24
                } else {
                    1
                }))
            });
            let times = input::position_times(&period, zone, step).map_err(|e| e.to_string())?;
            Ok(Box::new(times.map(|time| time.map_err(|e| e.to_string()))))
        }
        TimeSource::File(path) => read_times_file(path, override_tz),
        TimeSource::Now => {
            let zone = get_timezone_info(override_tz.as_ref());
            let now = move || Ok(Timestamp::now().to_zoned_datetime(zone.clone()));
            if let Some(step) = step_override {
                let sleep_duration = std::time::Duration::try_from(step.0)
                    .expect("positive step must convert to std::time::Duration");
                let mut first = true;
                let iter = std::iter::from_fn(move || {
                    if !std::mem::take(&mut first) {
                        std::thread::sleep(sleep_duration);
                    }
                    Some(now())
                });
                Ok(Box::new(iter))
            } else {
                Ok(Box::new(std::iter::once(now())))
            }
        }
    }
}

#[cfg(feature = "cli")]
fn read_times_file(
    input_path: InputPath,
    override_tz: Option<TimeZone>,
) -> Result<TimeIter, String> {
    let iter = read_non_comment_lines(&input_path)?.map(move |line_res| {
        let line = line_res?;
        parse_input_time(&line.content, override_tz.as_ref())
            .map_err(|err| format!("{}:{}: {}", line.ctx, line.number, err))
    });

    Ok(Box::new(iter))
}

#[cfg(feature = "cli")]
fn expand_time_outer(
    loc_source: LocationSource,
    time_source: TimeSource,
    step: Option<Step>,
    override_tz: Option<TimeZone>,
    command: Command,
) -> Result<CoordTimeStream<'static>, String> {
    let iter = expand_time_source(time_source, step, override_tz.clone(), command)?.flat_map(
        move |time_res| match time_res {
            Ok(dt) => match expand_location_source(loc_source.clone()) {
                Ok(locations) => Box::new(
                    locations
                        .map(move |coord_res| coord_res.map(|(lat, lon)| (lat, lon, dt.clone()))),
                ) as Box<dyn Iterator<Item = CoordTimeResult>>,
                Err(err) => Box::new(std::iter::once(Err(err))),
            },
            Err(err) => Box::new(std::iter::once(Err(err))),
        },
    );
    Ok(Box::new(iter))
}

#[cfg(feature = "cli")]
fn expand_location_outer(
    loc_source: LocationSource,
    time_source: TimeSource,
    step: Option<Step>,
    override_tz: Option<TimeZone>,
    command: Command,
) -> Result<CoordTimeStream<'static>, String> {
    let iter = expand_location_source(loc_source)?.flat_map(move |coord_res| match coord_res {
        Ok((lat, lon)) => {
            match expand_time_source(time_source.clone(), step, override_tz.clone(), command) {
                Ok(times) => Box::new(times.map(move |time_res| time_res.map(|dt| (lat, lon, dt))))
                    as Box<dyn Iterator<Item = CoordTimeResult>>,
                Err(err) => Box::new(std::iter::once(Err(err))),
            }
        }
        Err(err) => Box::new(std::iter::once(Err(err))),
    });
    Ok(Box::new(iter))
}

#[cfg(feature = "cli")]
pub fn expand_cartesian_product(
    loc_source: LocationSource,
    time_source: TimeSource,
    step: Option<Step>,
    override_tz: Option<TimeZone>,
    command: Command,
) -> Result<CoordTimeStream<'static>, String> {
    let loc_is_single = matches!(loc_source, LocationSource::Single(_, _));

    let time_is_single = match &time_source {
        TimeSource::Single(_) => true,
        TimeSource::Now => step.is_none(),
        _ => false,
    };
    if matches!(time_source, TimeSource::Now) && step.is_some() && !loc_is_single {
        return Err(
            "Cannot use an unbounded time stream with multiple locations. Drop --step or choose a single location when combining 'now' with streaming."
                .to_string(),
        );
    }

    if time_is_single {
        let mut time_iter = expand_time_source(time_source, step, override_tz, command)?;
        let Some(first_time) = time_iter.next() else {
            return Ok(Box::new(std::iter::empty()));
        };
        let dt = first_time?;
        let iter = expand_location_source(loc_source)?
            .map(move |coord_res| coord_res.map(|(lat, lon)| (lat, lon, dt.clone())));
        return Ok(Box::new(iter));
    }

    if loc_is_single {
        return expand_location_outer(loc_source, time_source, step, override_tz, command);
    }

    if let LocationSource::File(path) = &loc_source
        && !is_replayable(path)?
    {
        if let TimeSource::File(path) = &time_source
            && !is_replayable(path)?
        {
            return Err("Cannot combine two non-replayable inputs. Use a regular file for at least one input.".into());
        }
        return expand_location_outer(loc_source, time_source, step, override_tz, command);
    }

    expand_time_outer(loc_source, time_source, step, override_tz, command)
}

#[cfg(feature = "cli")]
pub fn expand_paired_file(
    input_path: InputPath,
    override_tz: Option<TimeZone>,
) -> Result<CoordTimeStream<'static>, String> {
    let ctx = input_context(&input_path);
    let reader = open_input(&input_path).map_err(|err| format!("Error opening {ctx}: {err}"))?;
    Ok(read_samples(reader, ctx, move |time| {
        parse_input_time(time, override_tz.as_ref())
    }))
}

#[cfg(all(test, feature = "cli"))]
mod tests {
    use super::*;

    #[cfg(feature = "cli")]
    #[test]
    fn range_with_fixed_longitude_handles_single_step() {
        let source = LocationSource::Range {
            lat: (52.0, 53.0, 1.0),
            lon: (13.4, 13.4, 0.0),
        };

        let coords = expand_location_source(source).expect("expand range");
        let collected = coords
            .collect::<Result<Vec<_>, _>>()
            .expect("collect coords");

        assert_eq!(collected.len(), 2);
        assert!(
            collected
                .iter()
                .all(|(_, lon)| (*lon - 13.4).abs() < f64::EPSILON)
        );
    }

    #[cfg(feature = "cli")]
    #[test]
    fn range_supports_descending_steps() {
        let source = LocationSource::Range {
            lat: (53.0, 52.0, -1.0),
            lon: (13.0, 11.0, -1.0),
        };

        let coords = expand_location_source(source).expect("expand range");
        let collected = coords
            .collect::<Result<Vec<_>, _>>()
            .expect("collect coords");

        assert_eq!(collected.len(), 6);
        assert_eq!(collected.first().copied(), Some((53.0, 13.0)));
        assert_eq!(collected.last().copied(), Some((52.0, 11.0)));
    }

    #[cfg(feature = "cli")]
    #[test]
    fn range_does_not_overshoot_endpoint() {
        let source = LocationSource::Range {
            lat: (0.0, 1.0, 0.6),
            lon: (0.0, 0.0, 0.0),
        };

        let collected = expand_location_source(source)
            .expect("expand range")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect coordinates");

        assert_eq!(collected, vec![(0.0, 0.0), (0.6, 0.0)]);
    }

    #[cfg(feature = "cli")]
    #[test]
    fn events_ranges_stay_at_local_midnight_across_dst() {
        let times = expand_time_source(
            TimeSource::Range("2026-03".to_string()),
            None,
            Some(TimeZone::get("Europe/Berlin").unwrap()),
            Command::Events,
        )
        .expect("expand month")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect datetimes");

        for date in ["2026-03-29", "2026-03-30", "2026-03-31"] {
            assert!(
                times
                    .iter()
                    .any(|dt| { dt.datetime().to_string() == format!("{date}T00:00:00") })
            );
        }
    }
}

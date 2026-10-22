//! Typed input records, geographic grids, and reader-based text adapters.
//!
//! All times and time zones are explicit. These adapters never open a file,
//! inspect the environment, read a system clock, or sleep.

use crate::data::expansion::CoordRangeIter;
use crate::data::time_utils::{InputTime, TimezoneInfo, parse_in_timezone};
use crate::data::{CoordTimeStream, validate_latitude, validate_longitude};
use crate::{Error, Location};
use chrono::{DateTime, FixedOffset, NaiveDate};
use std::io::BufRead;

mod time;
#[cfg(feature = "cli")]
pub(crate) use time::date_bounds;
pub use time::{daily_times, event_dates, position_times};

/// A location and a specific instant for a position calculation.
#[derive(Debug, Clone, Copy)]
pub struct PositionInput {
    pub location: Location,
    pub datetime: DateTime<FixedOffset>,
}

impl PositionInput {
    pub(crate) fn validate(self) -> Result<Self, Error> {
        validate_location(self.location)?;
        Ok(self)
    }
}

/// A location, local calendar date, and time zone for solar events.
#[derive(Debug, Clone, Copy)]
pub struct EventInput {
    pub location: Location,
    pub date: NaiveDate,
    pub timezone: TimezoneInfo,
}

impl EventInput {
    pub(crate) fn validate(self) -> Result<Self, Error> {
        validate_location(self.location)?;
        Ok(self)
    }
}

fn validate_location(location: Location) -> Result<(), Error> {
    validate_latitude(location.latitude).map_err(Error::Input)?;
    validate_longitude(location.longitude).map_err(Error::Input)?;
    Ok(())
}

/// A rectangular grid in degrees, independent of map projection.
///
/// Each tuple is `(start, end, step)`. Endpoints are included when reached by
/// the step. Descending ranges require a negative step. A fixed axis may use
/// zero step. Longitude ranges are arithmetic and do not wrap at 180 degrees.
#[derive(Debug, Clone)]
pub struct CoordinateGrid {
    latitude: (f64, f64, f64),
    longitude: (f64, f64, f64),
    latitude_first: bool,
}

impl CoordinateGrid {
    pub fn new(latitude: (f64, f64, f64), longitude: (f64, f64, f64)) -> Result<Self, Error> {
        crate::data::validate_latitude_range(latitude).map_err(Error::Input)?;
        crate::data::validate_longitude_range(longitude).map_err(Error::Input)?;
        let mut counts = [0; 2];
        for (axis, (start, end, step)) in [latitude, longitude].into_iter().enumerate() {
            if !step.is_finite() || (start != end && (step == 0.0 || (end > start) != (step > 0.0)))
            {
                return Err(Error::Input(
                    "Grid step must be finite and advance toward the endpoint".into(),
                ));
            }
            if start != end && start + step == start {
                return Err(Error::Input(
                    "Grid step is too small to advance the coordinate".into(),
                ));
            }
            let intervals = if start == end {
                0.0
            } else {
                ((end - start) / step).floor()
            };
            if !intervals.is_finite()
                || intervals >= usize::MAX as f64
                || intervals >= (1_u64 << 53) as f64
            {
                return Err(Error::Input("Grid axis contains too many points".into()));
            }
            counts[axis] = (intervals as usize)
                .checked_add(1)
                .ok_or_else(|| Error::Input("Grid axis contains too many points".into()))?;
        }
        Ok(Self {
            latitude,
            longitude,
            latitude_first: counts[0] <= counts[1],
        })
    }

    /// Iterate locations in Sunce's grid order, using constant memory.
    pub fn locations(&self) -> impl Iterator<Item = Location> + Clone + use<> {
        let latitude_first = self.latitude_first;
        let (inner, outer) = if latitude_first {
            (self.latitude, self.longitude)
        } else {
            (self.longitude, self.latitude)
        };
        let inner = CoordRangeIter::new(inner.0, inner.1, inner.2);
        CoordRangeIter::new(outer.0, outer.1, outer.2).flat_map(move |outer| {
            inner.clone().map(move |inner| {
                let (latitude, longitude) = if latitude_first {
                    (inner, outer)
                } else {
                    (outer, inner)
                };
                Location {
                    latitude,
                    longitude,
                }
            })
        })
    }
}

/// Expand a grid at each supplied instant, in the same time-first order as the CLI.
pub fn grid_positions<I>(
    grid: CoordinateGrid,
    times: I,
) -> impl Iterator<Item = Result<PositionInput, Error>>
where
    I: IntoIterator<Item = Result<DateTime<FixedOffset>, Error>>,
{
    let locations = grid.locations();
    times.into_iter().flat_map(move |time| {
        let mut locations = locations.clone();
        let (datetime, mut error) = match time {
            Ok(datetime) => (Some(datetime), None),
            Err(error) => (None, Some(error)),
        };
        std::iter::from_fn(move || {
            if let Some(error) = error.take() {
                return Some(Err(error));
            }
            let datetime = datetime?;
            locations
                .next()
                .map(|location| Ok(PositionInput { location, datetime }))
        })
    })
}

/// Expand a grid for each local date, retaining the named time zone.
pub fn grid_events<I>(
    grid: CoordinateGrid,
    dates: I,
    timezone: TimezoneInfo,
) -> impl Iterator<Item = EventInput>
where
    I: IntoIterator<Item = NaiveDate>,
{
    dates.into_iter().flat_map(move |date| {
        grid.locations().map(move |location| EventInput {
            location,
            date,
            timezone,
        })
    })
}

/// Read `latitude longitude datetime` records from any buffered reader.
/// Commas or whitespace separate fields; blank lines and `#` comments are ignored.
/// Naive times use `timezone`; explicit offsets are converted to that zone.
pub fn read_positions<'a, R: BufRead + 'a>(
    reader: R,
    timezone: TimezoneInfo,
) -> impl Iterator<Item = Result<PositionInput, Error>> + 'a {
    read_samples(reader, "input".into(), move |time| {
        parse_in_timezone(time, timezone)
    })
    .map(|item| {
        item.map(|(latitude, longitude, time)| PositionInput {
            location: Location {
                latitude,
                longitude,
            },
            datetime: time.datetime,
        })
        .map_err(Error::Input)
    })
}

/// Read latitude/longitude pairs from a reader, using Sunce's coordinate-file syntax.
pub fn read_locations<'a, R: BufRead + 'a>(
    reader: R,
) -> impl Iterator<Item = Result<Location, Error>> + 'a {
    read_locations_from(reader, "input".into()).map(|item| item.map_err(Error::Input))
}

/// Read one timestamp per line, interpreting it in an explicit time zone.
pub fn read_times<'a, R: BufRead + 'a>(
    reader: R,
    timezone: TimezoneInfo,
) -> impl Iterator<Item = Result<DateTime<FixedOffset>, Error>> + 'a {
    non_comment_lines(reader, "input".into()).map(move |line| {
        let line = line.map_err(Error::Input)?;
        parse_in_timezone(&line.content, timezone)
            .map(|time| time.datetime)
            .map_err(|err| Error::Input(format!("{}:{}: {err}", line.ctx, line.number)))
    })
}

/// Read event locations and local dates using the same text syntax as the CLI.
/// If a datetime is supplied, its date in `timezone` is used.
pub fn read_events<'a, R: BufRead + 'a>(
    reader: R,
    timezone: TimezoneInfo,
) -> impl Iterator<Item = Result<EventInput, Error>> + 'a {
    read_records(reader, "input".into(), move |latitude, longitude, time| {
        let date = match time.parse::<NaiveDate>() {
            Ok(date) => date,
            Err(_) => parse_in_timezone(time, timezone)?.datetime.date_naive(),
        };
        Ok(EventInput {
            location: Location {
                latitude,
                longitude,
            },
            date,
            timezone,
        })
    })
    .map(|item| item.map_err(Error::Input))
}

pub(crate) fn parse_delimited_line(line: &str) -> Vec<&str> {
    if line.contains(',') {
        line.split(',').collect()
    } else {
        line.split_whitespace().collect()
    }
}

pub(crate) fn parse_lat_lon(
    lat: &str,
    lon: &str,
    context: &str,
    line: usize,
) -> Result<(f64, f64), String> {
    let latitude = lat
        .trim()
        .parse::<f64>()
        .map_err(|_| format!("{context}:{line}: invalid latitude '{lat}'"))?;
    let latitude = validate_latitude(latitude).map_err(|err| format!("{context}:{line}: {err}"))?;
    let longitude = lon
        .trim()
        .parse::<f64>()
        .map_err(|_| format!("{context}:{line}: invalid longitude '{lon}'"))?;
    let longitude =
        validate_longitude(longitude).map_err(|err| format!("{context}:{line}: {err}"))?;
    Ok((latitude, longitude))
}

pub(crate) struct Line {
    pub number: usize,
    pub content: String,
    pub ctx: String,
}

pub(crate) fn non_comment_lines<'a, R: BufRead + 'a>(
    reader: R,
    context: String,
) -> Box<dyn Iterator<Item = Result<Line, String>> + 'a> {
    Box::new(reader.lines().enumerate().filter_map(move |(index, line)| {
        let number = index + 1;
        match line {
            Err(err) => Some(Err(format!(
                "{context}:{number}: failed to read line: {err}"
            ))),
            Ok(content) if content.trim().is_empty() || content.trim().starts_with('#') => None,
            Ok(content) => Some(Ok(Line {
                number,
                content: content.trim().to_owned(),
                ctx: context.clone(),
            })),
        }
    }))
}

pub(crate) fn read_samples<'a, R, F>(
    reader: R,
    context: String,
    parse_time: F,
) -> CoordTimeStream<'a>
where
    R: BufRead + 'a,
    F: Fn(&str) -> Result<InputTime, String> + 'a,
{
    read_records(reader, context, move |lat, lon, time| {
        parse_time(time).map(|time| (lat, lon, time))
    })
}

fn read_records<'a, R, F, T>(
    reader: R,
    context: String,
    parse: F,
) -> Box<dyn Iterator<Item = Result<T, String>> + 'a>
where
    R: BufRead + 'a,
    F: Fn(f64, f64, &str) -> Result<T, String> + 'a,
    T: 'a,
{
    Box::new(non_comment_lines(reader, context).map(move |line| {
        let line = line?;
        let parts = parse_delimited_line(&line.content);
        if parts.len() < 3 {
            return Err(format!(
                "{}:{}: expected 3 fields (lat lon datetime), found {}",
                line.ctx,
                line.number,
                parts.len()
            ));
        }
        let (lat, lon) = parse_lat_lon(parts[0], parts[1], &line.ctx, line.number)?;
        parse(lat, lon, parts[2..].join(" ").trim())
            .map_err(|err| format!("{}:{}: {err}", line.ctx, line.number))
    }))
}

pub(crate) fn read_locations_from<'a, R: BufRead + 'a>(
    reader: R,
    context: String,
) -> impl Iterator<Item = Result<Location, String>> + 'a {
    non_comment_lines(reader, context).map(|line| {
        let line = line?;
        let parts = parse_delimited_line(&line.content);
        if parts.len() < 2 {
            return Err(format!("{}:{}: expected 2 fields (lat lon), found {}", line.ctx, line.number, parts.len()));
        }
        if parts.len() > 2 {
            return Err(format!("{}:{}: expected 2 fields (lat lon), found {}. File appears to be a paired data file (lat lon datetime), which cannot be used with a separate time source.", line.ctx, line.number, parts.len()));
        }
        let (latitude, longitude) = parse_lat_lon(parts[0], parts[1], &line.ctx, line.number)?;
        Ok(Location { latitude, longitude })
    })
}

use super::time_utils::{
    TimezoneInfo, convert_datetime_to_timezone, get_timezone_info, parse_datetime_string,
};
use super::types::{
    CoordTimeResult, CoordTimeStream, InputPath, LocationSource, LocationStream, TimeSource,
};
use super::{Command, Step, TimezoneOverride, validate_latitude, validate_longitude};
use chrono::{DateTime, Duration, FixedOffset, NaiveDate, NaiveDateTime, Utc};
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::sync::Arc;

type TimeIter = Box<dyn Iterator<Item = Result<DateTime<FixedOffset>, String>>>;

struct TimeStepIter {
    tz: TimezoneInfo,
    next: Option<DateTime<FixedOffset>>,
    end: DateTime<FixedOffset>,
    step: Duration,
}

impl TimeStepIter {
    fn new(
        start: DateTime<FixedOffset>,
        end: DateTime<FixedOffset>,
        step: Duration,
        tz: TimezoneInfo,
    ) -> Self {
        Self {
            tz,
            next: Some(start),
            end,
            step,
        }
    }
}

impl Iterator for TimeStepIter {
    type Item = DateTime<FixedOffset>;

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.next?;
        let candidate = self
            .tz
            .to_datetime_from_utc(&(current.naive_utc() + self.step));

        self.next = if candidate <= self.end {
            Some(candidate)
        } else {
            None
        };

        Some(current)
    }
}

fn parse_delimited_line(line: &str) -> Vec<&str> {
    if line.contains(',') {
        line.split(',').collect()
    } else {
        line.split_whitespace().collect()
    }
}

fn open_input(input_path: &InputPath) -> io::Result<Box<dyn BufRead>> {
    match input_path {
        InputPath::Stdin => Ok(Box::new(BufReader::new(io::stdin()))),
        InputPath::File(path) => {
            let file = File::open(path)?;
            Ok(Box::new(BufReader::new(file)))
        }
    }
}

struct Line {
    number: usize,
    content: String,
    ctx: String,
}

fn read_non_comment_lines(
    input_path: &InputPath,
) -> Result<Box<dyn Iterator<Item = Result<Line, String>>>, String> {
    let ctx = match input_path {
        InputPath::Stdin => "stdin".to_string(),
        InputPath::File(p) => p.display().to_string(),
    };

    let reader = open_input(input_path).map_err(|e| format!("Error opening {}: {}", ctx, e))?;
    let mut lines = reader.lines().enumerate();

    Ok(Box::new(std::iter::from_fn(move || {
        for (idx, line_result) in lines.by_ref() {
            let number = idx + 1;
            let line = match line_result {
                Ok(value) => value,
                Err(err) => {
                    return Some(Err(format!(
                        "{}:{}: failed to read line: {}",
                        ctx, number, err
                    )));
                }
            };
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            return Some(Ok(Line {
                number,
                content: trimmed.to_string(),
                ctx: ctx.clone(),
            }));
        }
        None
    })))
}

fn coord_range_iter(start: f64, end: f64, step: f64) -> Box<dyn Iterator<Item = f64>> {
    if step == 0.0 || start == end {
        // Single value case
        Box::new(std::iter::once(start))
    } else if step > 0.0 {
        Box::new(std::iter::successors(Some(start), move |&x| {
            let next = x + step;
            (next <= end + step * 0.5).then_some(next)
        }))
    } else {
        Box::new(std::iter::successors(Some(start), move |&x| {
            let next = x + step;
            (next >= end + step * 0.5).then_some(next)
        }))
    }
}

fn range_point_count(start: f64, end: f64, step: f64) -> usize {
    if step == 0.0 || (end - start).abs() < f64::EPSILON {
        1
    } else {
        let step_abs = step.abs();
        let span = (end - start).abs().max(0.0);
        (span / step_abs).floor() as usize + 1
    }
}

fn arc_values(values: Arc<Vec<f64>>) -> impl Iterator<Item = f64> {
    let mut idx = 0usize;
    std::iter::from_fn(move || {
        let value = values.get(idx).copied();
        idx += 1;
        value
    })
}

pub fn expand_location_source(source: LocationSource) -> Result<LocationStream, String> {
    match source {
        LocationSource::Single(lat, lon) => Ok(Box::new(std::iter::once(Ok((lat, lon))))),
        LocationSource::Range { lat, lon } => {
            let lat_iter = coord_range_iter(lat.0, lat.1, lat.2);

            let (lon_start, lon_end, lon_step) = lon;

            // Create a vector for one dimension to allow repeated iteration
            // Choose the smaller dimension to minimize memory usage
            let lat_count = range_point_count(lat.0, lat.1, lat.2);
            let lon_count = range_point_count(lon_start, lon_end, lon_step);

            if lat_count <= lon_count {
                // Collect latitudes (smaller), iterate longitudes
                let lat_coords = Arc::new(lat_iter.collect::<Vec<f64>>());
                Ok(Box::new(
                    coord_range_iter(lon_start, lon_end, lon_step).flat_map(move |lon| {
                        let lat_coords = Arc::clone(&lat_coords);
                        arc_values(lat_coords).map(move |lat| Ok::<(f64, f64), String>((lat, lon)))
                    }),
                ))
            } else {
                // Collect longitudes (smaller), iterate latitudes
                let lon_coords =
                    Arc::new(coord_range_iter(lon_start, lon_end, lon_step).collect::<Vec<f64>>());
                Ok(Box::new(lat_iter.flat_map(move |lat| {
                    let lon_coords = Arc::clone(&lon_coords);
                    arc_values(lon_coords).map(move |lon| Ok::<(f64, f64), String>((lat, lon)))
                })))
            }
        }
        LocationSource::File(input_path) => {
            let iter = read_non_comment_lines(&input_path)?.map(|line_res| {
                let line = line_res?;
                let parts = parse_delimited_line(&line.content);
                if parts.len() < 2 {
                    return Err(format!(
                        "{}:{}: expected 2 fields (lat lon), found {}",
                        line.ctx,
                        line.number,
                        parts.len()
                    ));
                }
                if parts.len() > 2 {
                    return Err(format!(
                        "{}:{}: expected 2 fields (lat lon), found {}. File appears to be a paired data file (lat lon datetime), which cannot be used with a separate time source.",
                        line.ctx,
                        line.number,
                        parts.len()
                    ));
                }

                let lat_raw: f64 = parts[0].trim().parse().map_err(|_| {
                    format!(
                        "{}:{}: invalid latitude '{}'",
                        line.ctx, line.number, parts[0]
                    )
                })?;
                let lat = validate_latitude(lat_raw)
                    .map_err(|err| format!("{}:{}: {}", line.ctx, line.number, err))?;

                let lon_raw: f64 = parts[1].trim().parse().map_err(|_| {
                    format!(
                        "{}:{}: invalid longitude '{}'",
                        line.ctx, line.number, parts[1]
                    )
                })?;
                let lon = validate_longitude(lon_raw)
                    .map_err(|err| format!("{}:{}: {}", line.ctx, line.number, err))?;

                Ok((lat, lon))
            });

            Ok(Box::new(iter))
        }
    }
}

pub fn expand_time_source(
    source: TimeSource,
    step_override: Option<Step>,
    override_tz: Option<TimezoneOverride>,
    command: Command,
) -> Result<TimeIter, String> {
    match source {
        TimeSource::Single(dt_str) => {
            if command == Command::Position && super::time_utils::is_date_without_time(&dt_str) {
                let step = step_override.unwrap_or_else(|| Step(chrono::Duration::hours(1)));
                expand_partial_date(dt_str, step, override_tz)
            } else {
                let dt =
                    parse_datetime_string(&dt_str, override_tz.as_ref().map(|tz| tz.as_str()))?;
                Ok(Box::new(std::iter::once(Ok(dt))))
            }
        }
        TimeSource::Range(partial_date, step_opt) => {
            let step = step_override.or(step_opt).unwrap_or_else(|| {
                if command == Command::Sunrise || partial_date.len() == 4 {
                    Step(chrono::Duration::days(1))
                } else {
                    Step(chrono::Duration::hours(1))
                }
            });
            expand_partial_date(partial_date, step, override_tz)
        }
        TimeSource::File(path) => read_times_file(path, override_tz),
        TimeSource::Now => {
            let tz_info = get_timezone_info(override_tz.as_ref().map(|tz| tz.as_str()));
            if let Some(step_str) = step_override {
                let step_duration: chrono::Duration = step_str.into();
                let mut first = true;
                let tz_clone = tz_info.clone();
                let iter = std::iter::from_fn(move || {
                    if !std::mem::take(&mut first) {
                        std::thread::sleep(
                            step_duration
                                .to_std()
                                .unwrap_or(std::time::Duration::from_secs(1)),
                        );
                    }
                    Some(Ok(convert_datetime_to_timezone(Utc::now(), &tz_clone)))
                });
                Ok(Box::new(iter))
            } else {
                Ok(Box::new(std::iter::once(Ok(convert_datetime_to_timezone(
                    Utc::now(),
                    &tz_info,
                )))))
            }
        }
    }
}

struct NaiveBounds {
    start: NaiveDateTime,
    end: NaiveDateTime,
}

fn naive_bounds_from_partial(date_str: &str) -> Result<NaiveBounds, String> {
    let to_day_bounds = |date: NaiveDate| -> NaiveBounds {
        let start = date
            .and_hms_opt(0, 0, 0)
            .expect("midnight must be constructible");
        let end = date
            .and_hms_opt(23, 59, 59)
            .expect("end-of-day must be constructible");
        NaiveBounds { start, end }
    };

    let parse_year = |value: &str| {
        value
            .parse::<i32>()
            .map_err(|_| format!("Invalid year value: '{}'", value))
    };

    let parse_month = |value: &str| {
        value
            .parse::<u32>()
            .map_err(|_| format!("Invalid month value: '{}'", value))
    };

    let parse_day = |value: &str| {
        value
            .parse::<u32>()
            .map_err(|_| format!("Invalid day value: '{}'", value))
    };

    match date_str.len() {
        4 => {
            let year = parse_year(date_str)?;
            let start = NaiveDate::from_ymd_opt(year, 1, 1)
                .ok_or_else(|| format!("Invalid start of year for {}", date_str))?;
            let end = NaiveDate::from_ymd_opt(year, 12, 31)
                .ok_or_else(|| format!("Invalid end of year for {}", date_str))?;
            Ok(NaiveBounds {
                start: to_day_bounds(start).start,
                end: to_day_bounds(end).end,
            })
        }
        7 => {
            let (year_str, month_str) = date_str
                .split_once('-')
                .ok_or_else(|| format!("Invalid year-month format: '{}'", date_str))?;
            let year = parse_year(year_str)?;
            let month = parse_month(month_str)?;
            let start = NaiveDate::from_ymd_opt(year, month, 1)
                .ok_or_else(|| format!("Invalid start date for {}", date_str))?;
            let (next_year, next_month) = if month == 12 {
                (year + 1, 1)
            } else {
                (year, month + 1)
            };
            let end = NaiveDate::from_ymd_opt(next_year, next_month, 1)
                .and_then(|d| d.pred_opt())
                .ok_or_else(|| format!("Cannot determine end date for {}", date_str))?;
            Ok(NaiveBounds {
                start: to_day_bounds(start).start,
                end: to_day_bounds(end).end,
            })
        }
        10 => {
            let parts: Vec<_> = date_str.split('-').collect();
            if parts.len() != 3 {
                return Err(format!(
                    "Invalid full date format: '{}'. Expected YYYY-MM-DD",
                    date_str
                ));
            }
            let date = NaiveDate::from_ymd_opt(
                parse_year(parts[0])?,
                parse_month(parts[1])?,
                parse_day(parts[2])?,
            )
            .ok_or_else(|| format!("Invalid date: '{}'", date_str))?;
            Ok(to_day_bounds(date))
        }
        _ => Err(format!(
            "Unsupported date format: '{}'. Use YYYY, YYYY-MM, or YYYY-MM-DD",
            date_str
        )),
    }
}

fn to_local_datetime(
    tz_info: &TimezoneInfo,
    naive: NaiveDateTime,
    label: &str,
    original: &str,
) -> Result<DateTime<FixedOffset>, String> {
    tz_info.to_datetime_from_local(&naive).ok_or_else(|| {
        format!(
            "{} time does not exist in timezone (likely DST gap): {}",
            label, original
        )
    })
}

fn expand_partial_date(
    date_str: String,
    step: Step,
    override_tz: Option<TimezoneOverride>,
) -> Result<TimeIter, String> {
    let step_duration: chrono::Duration = step.into();
    let tz_info = get_timezone_info(override_tz.as_ref().map(|tz| tz.as_str()));
    let bounds = naive_bounds_from_partial(&date_str)?;

    let start_dt = to_local_datetime(&tz_info, bounds.start, "Start", &date_str)?;
    let end_dt = to_local_datetime(&tz_info, bounds.end, "End", &date_str)?;

    let iter = TimeStepIter::new(start_dt, end_dt, step_duration, tz_info).map(Ok);

    Ok(Box::new(iter))
}

fn read_times_file(
    input_path: InputPath,
    override_tz: Option<TimezoneOverride>,
) -> Result<TimeIter, String> {
    let tz_override = override_tz.clone();

    let iter = read_non_comment_lines(&input_path)?.map(move |line_res| {
        let line = line_res?;
        parse_datetime_string(&line.content, tz_override.as_ref().map(|tz| tz.as_str()))
            .map_err(|err| format!("{}:{}: {}", line.ctx, line.number, err))
    });

    Ok(Box::new(iter))
}

pub fn expand_cartesian_product(
    loc_source: LocationSource,
    time_source: TimeSource,
    step: Option<Step>,
    override_tz: Option<TimezoneOverride>,
    command: Command,
) -> Result<CoordTimeStream, String> {
    #[derive(Clone)]
    struct LocationGenerator {
        source: LocationSource,
    }

    impl LocationGenerator {
        fn new(source: LocationSource) -> Self {
            Self { source }
        }

        fn iter(&self) -> Result<LocationStream, String> {
            expand_location_source(self.source.clone())
        }

        fn is_single(&self) -> bool {
            matches!(self.source, LocationSource::Single(_, _))
        }

        fn replayable(&self) -> bool {
            !matches!(self.source, LocationSource::File(InputPath::Stdin))
        }
    }

    #[derive(Clone)]
    struct TimeGenerator {
        source: TimeSource,
        step: Option<Step>,
        tz: Option<TimezoneOverride>,
        command: Command,
        bounded: bool,
        replayable: bool,
        is_single: bool,
    }

    impl TimeGenerator {
        fn new(
            source: TimeSource,
            step: Option<Step>,
            tz: Option<TimezoneOverride>,
            command: Command,
        ) -> Self {
            let is_full_date = |s: &str| {
                s.len() == 10 && s.matches('-').count() == 2 && !s.contains('T') && !s.contains(' ')
            };
            let is_single = match &source {
                TimeSource::Single(s) if command == Command::Position && is_full_date(s) => false,
                TimeSource::Single(_) => true,
                TimeSource::Now => step.is_none(),
                _ => false,
            };
            let replayable = !matches!(source, TimeSource::File(InputPath::Stdin))
                && (!matches!(source, TimeSource::Now) || step.is_none());
            let bounded = !matches!(source, TimeSource::Now) || step.is_none();

            Self {
                source,
                step,
                tz,
                command,
                bounded,
                replayable,
                is_single,
            }
        }

        fn iter(&self) -> Result<TimeIter, String> {
            expand_time_source(
                self.source.clone(),
                self.step,
                self.tz.clone(),
                self.command,
            )
        }
    }

    let loc_gen = LocationGenerator::new(loc_source);
    let time_gen = TimeGenerator::new(time_source, step, override_tz, command);

    if !time_gen.bounded && !loc_gen.is_single() {
        return Err(
            "Cannot use an unbounded time stream with multiple locations. Drop --step or choose a single location when combining 'now' with streaming."
                .to_string(),
        );
    }

    if time_gen.is_single {
        let mut time_iter = time_gen.iter()?;
        let Some(first_time) = time_iter.next() else {
            return Ok(Box::new(std::iter::empty()));
        };
        let dt = first_time?;
        let iter = loc_gen
            .iter()?
            .map(move |coord_res| coord_res.map(|(lat, lon)| (lat, lon, dt)));
        return Ok(Box::new(iter));
    }

    let time_first = |time_gen: &TimeGenerator,
                      loc_gen: &LocationGenerator|
     -> Result<CoordTimeStream, String> {
        let loc_gen_clone = loc_gen.clone();
        let iter = time_gen.iter()?.flat_map(move |time_res| match time_res {
            Ok(dt) => match loc_gen_clone.iter() {
                Ok(locations) => Box::new(
                    locations.map(move |coord_res| coord_res.map(|(lat, lon)| (lat, lon, dt))),
                ) as Box<dyn Iterator<Item = CoordTimeResult>>,
                Err(err) => Box::new(std::iter::once(Err(err))),
            },
            Err(err) => Box::new(std::iter::once(Err(err))),
        });
        Ok(Box::new(iter))
    };

    let loc_first = |time_gen: &TimeGenerator,
                     loc_gen: &LocationGenerator|
     -> Result<CoordTimeStream, String> {
        let time_gen_clone = time_gen.clone();
        let iter = loc_gen.iter()?.flat_map(move |coord_res| match coord_res {
            Ok((lat, lon)) => match time_gen_clone.iter() {
                Ok(times) => Box::new(times.map(move |time_res| time_res.map(|dt| (lat, lon, dt))))
                    as Box<dyn Iterator<Item = CoordTimeResult>>,
                Err(err) => Box::new(std::iter::once(Err(err))),
            },
            Err(err) => Box::new(std::iter::once(Err(err))),
        });
        Ok(Box::new(iter))
    };

    if time_gen.replayable {
        if loc_gen.replayable() {
            // Iterate times outermost to maximize reuse of cached time-dependent SPA parts.
            return time_first(&time_gen, &loc_gen);
        }
        // Time is replayable but locations are not (e.g., stdin) - iterate locations first.
        return loc_first(&time_gen, &loc_gen);
    }

    if loc_gen.replayable() {
        return time_first(&time_gen, &loc_gen);
    }

    if loc_gen.is_single() {
        let (lat, lon) = expand_location_source(loc_gen.source.clone())?
            .next()
            .unwrap_or(Err("No location provided".to_string()))?;
        let iter = time_gen
            .iter()?
            .map(move |time_res| time_res.map(|dt| (lat, lon, dt)));
        return Ok(Box::new(iter));
    }

    Err("Cannot combine non-replayable streams for both locations and times. Use files instead of stdin or provide bounded values.".to_string())
}

pub fn expand_paired_file(
    input_path: InputPath,
    override_tz: Option<TimezoneOverride>,
) -> Result<CoordTimeStream, String> {
    let tz_override = override_tz.clone();

    let iter = read_non_comment_lines(&input_path)?.map(move |line_res| {
        let line = line_res?;
        let parts = parse_delimited_line(&line.content);
        if parts.len() < 3 {
            return Err(format!(
                "{}:{}: expected 3 fields (lat lon datetime), found {}",
                line.ctx,
                line.number,
                parts.len()
            ));
        }

        let lat_raw = parts[0].trim().parse::<f64>().map_err(|_| {
            format!(
                "{}:{}: invalid latitude '{}'",
                line.ctx, line.number, parts[0]
            )
        })?;
        let lat = validate_latitude(lat_raw)
            .map_err(|err| format!("{}:{}: {}", line.ctx, line.number, err))?;

        let lon_raw = parts[1].trim().parse::<f64>().map_err(|_| {
            format!(
                "{}:{}: invalid longitude '{}'",
                line.ctx, line.number, parts[1]
            )
        })?;
        let lon = validate_longitude(lon_raw)
            .map_err(|err| format!("{}:{}: {}", line.ctx, line.number, err))?;

        let dt_str = parts[2..].join(" ");
        let dt = parse_datetime_string(dt_str.trim(), tz_override.as_ref().map(|tz| tz.as_str()))
            .map_err(|err| format!("{}:{}: {}", line.ctx, line.number, err))?;
        Ok((lat, lon, dt))
    });

    Ok(Box::new(iter))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_offset_timezone_accepts_dst_gap() {
        let dt = parse_datetime_string("2024-03-31T02:00:00", Some("+01:00"))
            .expect("should parse even in DST gap for fixed offset");
        assert_eq!(dt.offset().local_minus_utc(), 3600);
    }

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
}

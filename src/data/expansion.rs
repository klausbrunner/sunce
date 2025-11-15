use super::time_utils::{
    TimezoneInfo, convert_datetime_to_timezone, get_timezone_info, parse_datetime_string,
    parse_duration_positive,
};
use super::types::{
    CoordTimeResult, CoordTimeStream, InputPath, LocationSource, LocationStream, TimeSource,
};
use super::{Command, validate_latitude, validate_longitude};
use chrono::{DateTime, Duration, FixedOffset, NaiveDate, NaiveDateTime, Utc};
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::sync::Arc;

fn repeat_times(lat: f64, lon: f64, times: Arc<Vec<DateTime<FixedOffset>>>) -> CoordTimeStream {
    let len = times.len();
    Box::new((0..len).map(move |idx| Ok((lat, lon, times[idx]))))
}

struct CoordRepeat {
    coords: Arc<Vec<(f64, f64)>>,
    index: usize,
    dt: DateTime<FixedOffset>,
}

impl CoordRepeat {
    fn new(coords: Arc<Vec<(f64, f64)>>, dt: DateTime<FixedOffset>) -> Self {
        Self {
            coords,
            index: 0,
            dt,
        }
    }
}

impl Iterator for CoordRepeat {
    type Item = CoordTimeResult;

    fn next(&mut self) -> Option<Self::Item> {
        let (lat, lon) = self.coords.get(self.index).copied()?;
        self.index += 1;
        Some(Ok((lat, lon, self.dt)))
    }
}

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

pub struct TimeStream {
    iter: Box<dyn Iterator<Item = Result<DateTime<FixedOffset>, String>>>,
    bounded: bool,
}

impl TimeStream {
    fn new<I>(iter: I, bounded: bool) -> Self
    where
        I: Iterator<Item = Result<DateTime<FixedOffset>, String>> + 'static,
    {
        Self {
            iter: Box::new(iter),
            bounded,
        }
    }

    fn is_bounded(&self) -> bool {
        self.bounded
    }

    fn into_iter(self) -> Box<dyn Iterator<Item = Result<DateTime<FixedOffset>, String>>> {
        self.iter
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

fn coord_range_iter(start: f64, end: f64, step: f64) -> Box<dyn Iterator<Item = f64>> {
    if step == 0.0 || start == end {
        // Single value case
        Box::new(std::iter::once(start))
    } else {
        Box::new(std::iter::successors(Some(start), move |&x| {
            let next = x + step;
            (next <= end + step * 0.5).then_some(next)
        }))
    }
}

fn range_point_count(start: f64, end: f64, step: f64) -> usize {
    if step == 0.0 || (end - start).abs() < f64::EPSILON {
        1
    } else {
        let span = (end - start).abs().max(0.0);
        (span / step).floor() as usize + 1
    }
}

pub fn expand_location_source(source: LocationSource) -> Result<LocationStream, String> {
    match source {
        LocationSource::Single(lat, lon) => Ok(Box::new(std::iter::once(Ok((lat, lon))))),
        LocationSource::Range { lat, lon } => {
            let lat_iter = coord_range_iter(lat.0, lat.1, lat.2);

            let Some((lon_start, lon_end, lon_step)) = lon else {
                unreachable!(
                    "Range without longitude range - should be prevented by parse_cli validation"
                );
            };

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
                        (0..lat_coords.len())
                            .map(move |idx| Ok::<(f64, f64), String>((lat_coords[idx], lon)))
                    }),
                ))
            } else {
                // Collect longitudes (smaller), iterate latitudes
                let lon_coords =
                    Arc::new(coord_range_iter(lon_start, lon_end, lon_step).collect::<Vec<f64>>());
                Ok(Box::new(lat_iter.flat_map(move |lat| {
                    let lon_coords = Arc::clone(&lon_coords);
                    (0..lon_coords.len())
                        .map(move |idx| Ok::<(f64, f64), String>((lat, lon_coords[idx])))
                })))
            }
        }
        LocationSource::File(input_path) => {
            let path_display = match &input_path {
                InputPath::Stdin => "stdin".to_string(),
                InputPath::File(p) => p.display().to_string(),
            };

            let reader = open_input(&input_path)
                .map_err(|e| format!("Error opening {}: {}", path_display, e))?;

            let mut lines = reader.lines().enumerate();
            let mut finished = false;

            let iter = std::iter::from_fn(move || {
                if finished {
                    return None;
                }

                for (idx, line_result) in lines.by_ref() {
                    let line_number = idx + 1;
                    let line = match line_result {
                        Ok(l) => l,
                        Err(e) => {
                            finished = true;
                            return Some(Err(format!(
                                "{}:{}: failed to read line: {}",
                                path_display, line_number, e
                            )));
                        }
                    };

                    let trimmed = line.trim();
                    if trimmed.is_empty() || trimmed.starts_with('#') {
                        continue;
                    }

                    let parts = parse_delimited_line(trimmed);
                    if parts.len() < 2 {
                        finished = true;
                        return Some(Err(format!(
                            "{}:{}: expected 2 fields (lat lon), found {}",
                            path_display,
                            line_number,
                            parts.len()
                        )));
                    }
                    if parts.len() > 2 {
                        finished = true;
                        return Some(Err(format!(
                            "{}:{}: expected 2 fields (lat lon), found {}. File appears to be a paired data file (lat lon datetime), which cannot be used with a separate time source.",
                            path_display,
                            line_number,
                            parts.len()
                        )));
                    }

                    let lat_raw: f64 = match parts[0].trim().parse() {
                        Ok(v) => v,
                        Err(_) => {
                            finished = true;
                            return Some(Err(format!(
                                "{}:{}: invalid latitude '{}'",
                                path_display, line_number, parts[0]
                            )));
                        }
                    };
                    let lat = match validate_latitude(lat_raw) {
                        Ok(value) => value,
                        Err(err) => {
                            finished = true;
                            return Some(Err(format!("{}:{}: {}", path_display, line_number, err)));
                        }
                    };

                    let lon_raw: f64 = match parts[1].trim().parse() {
                        Ok(v) => v,
                        Err(_) => {
                            finished = true;
                            return Some(Err(format!(
                                "{}:{}: invalid longitude '{}'",
                                path_display, line_number, parts[1]
                            )));
                        }
                    };
                    let lon = match validate_longitude(lon_raw) {
                        Ok(value) => value,
                        Err(err) => {
                            finished = true;
                            return Some(Err(format!("{}:{}: {}", path_display, line_number, err)));
                        }
                    };

                    return Some(Ok((lat, lon)));
                }

                finished = true;
                None
            });

            Ok(Box::new(iter))
        }
    }
}

pub fn expand_time_source(
    source: TimeSource,
    step_override: Option<String>,
    override_tz: Option<String>,
    command: Command,
) -> Result<TimeStream, String> {
    match source {
        TimeSource::Single(dt_str) => {
            let is_date_only =
                dt_str.len() == 10 && dt_str.matches('-').count() == 2 && !dt_str.contains('T');
            if command == Command::Position && is_date_only {
                let step = step_override.unwrap_or_else(|| "1h".to_string());
                expand_partial_date(dt_str, step, override_tz)
            } else {
                let dt = parse_datetime_string(&dt_str, override_tz.as_deref())?;
                Ok(TimeStream::new(std::iter::once(Ok(dt)), true))
            }
        }
        TimeSource::Range(partial_date, step_opt) => {
            let step = step_override.or(step_opt).unwrap_or_else(|| {
                if command == Command::Sunrise || partial_date.len() == 4 {
                    "1d".to_string()
                } else {
                    "1h".to_string()
                }
            });
            expand_partial_date(partial_date, step, override_tz)
        }
        TimeSource::File(path) => read_times_file(path, override_tz),
        TimeSource::Now => {
            let tz_info = get_timezone_info(override_tz.as_deref());
            if let Some(step_str) = step_override {
                let step_duration = parse_duration_positive(&step_str)?;
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
                Ok(TimeStream::new(iter, false))
            } else {
                Ok(TimeStream::new(
                    std::iter::once(Ok(convert_datetime_to_timezone(Utc::now(), &tz_info))),
                    true,
                ))
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
    step: String,
    override_tz: Option<String>,
) -> Result<TimeStream, String> {
    let step_duration = parse_duration_positive(&step)?;
    let tz_info = get_timezone_info(override_tz.as_deref());
    let bounds = naive_bounds_from_partial(&date_str)?;

    let start_dt = to_local_datetime(&tz_info, bounds.start, "Start", &date_str)?;
    let end_dt = to_local_datetime(&tz_info, bounds.end, "End", &date_str)?;

    let iter = TimeStepIter::new(start_dt, end_dt, step_duration, tz_info).map(Ok);

    Ok(TimeStream::new(iter, true))
}

fn read_times_file(
    input_path: InputPath,
    override_tz: Option<String>,
) -> Result<TimeStream, String> {
    let path_display = match &input_path {
        InputPath::Stdin => "stdin".to_string(),
        InputPath::File(p) => p.display().to_string(),
    };

    let reader =
        open_input(&input_path).map_err(|e| format!("Error opening {}: {}", path_display, e))?;

    let mut lines = reader.lines().enumerate();
    let tz_override = override_tz.clone();

    let iter = std::iter::from_fn(move || {
        for (idx, line_result) in lines.by_ref() {
            let line_number = idx + 1;
            let line = match line_result {
                Ok(value) => value,
                Err(err) => {
                    return Some(Err(format!(
                        "{}:{}: failed to read line: {}",
                        path_display, line_number, err
                    )));
                }
            };

            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }

            match parse_datetime_string(trimmed, tz_override.as_deref()) {
                Ok(dt) => return Some(Ok(dt)),
                Err(err) => {
                    return Some(Err(format!("{}:{}: {}", path_display, line_number, err)));
                }
            }
        }
        None
    });

    Ok(TimeStream::new(iter, true))
}

pub fn expand_cartesian_product(
    loc_source: LocationSource,
    time_source: TimeSource,
    step: Option<String>,
    override_tz: Option<String>,
    command: Command,
) -> Result<CoordTimeStream, String> {
    let is_time_single = matches!(time_source, TimeSource::Single(_))
        || (matches!(time_source, TimeSource::Now) && step.is_none());
    let location_from_stdin = matches!(loc_source, LocationSource::File(InputPath::Stdin));
    let is_loc_single = matches!(loc_source, LocationSource::Single(_, _));

    let time_stream = expand_time_source(time_source, step, override_tz, command)?;
    let is_time_bounded = time_stream.is_bounded();

    if is_time_single {
        let times = Arc::new(time_stream.into_iter().collect::<Result<Vec<_>, _>>()?);
        let locations = expand_location_source(loc_source)?;
        let iter = locations.flat_map(move |coord_res| match coord_res {
            Ok((lat, lon)) => repeat_times(lat, lon, Arc::clone(&times)),
            Err(err) => Box::new(std::iter::once(Err(err))),
        });
        return Ok(Box::new(iter));
    }

    if location_from_stdin {
        if !is_time_bounded {
            return Err(
                "Cannot combine coordinate stdin input with unbounded time stream. Supply explicit times or use paired input."
                    .to_string(),
            );
        }

        let times = Arc::new(time_stream.into_iter().collect::<Result<Vec<_>, _>>()?);
        let locations = expand_location_source(loc_source)?;
        let iter = locations.flat_map(move |coord_res| match coord_res {
            Ok((lat, lon)) => repeat_times(lat, lon, Arc::clone(&times)),
            Err(err) => Box::new(std::iter::once(Err(err))),
        });
        return Ok(Box::new(iter));
    }

    if !is_time_bounded && !is_loc_single {
        return Err(
            "Cannot use an unbounded time stream with multiple locations. Drop --step or choose a single location when combining 'now' with streaming."
                .to_string(),
        );
    }

    let location_cache: Option<Arc<Vec<(f64, f64)>>> = match &loc_source {
        LocationSource::Single(lat, lon) => Some(Arc::new(vec![(*lat, *lon)])),
        LocationSource::File(InputPath::File(_)) => {
            let coords_iter = expand_location_source(loc_source.clone())?;
            let coords = coords_iter.collect::<Result<Vec<_>, _>>()?;
            Some(Arc::new(coords))
        }
        _ => None,
    };

    let loc_source_for_iter = loc_source;
    let iter =
        time_stream
            .into_iter()
            .flat_map(move |time_result| match time_result {
                Ok(dt) => {
                    if let Some(cache) = location_cache.as_ref() {
                        Box::new(CoordRepeat::new(Arc::clone(cache), dt))
                            as Box<dyn Iterator<Item = CoordTimeResult>>
                    } else {
                        let dt_value = dt;
                        match expand_location_source(loc_source_for_iter.clone()) {
                            Ok(locations) => Box::new(locations.map(move |coord_res| {
                                coord_res.map(|(lat, lon)| (lat, lon, dt_value))
                            }))
                                as Box<dyn Iterator<Item = CoordTimeResult>>,
                            Err(err) => Box::new(std::iter::once(Err(err)))
                                as Box<dyn Iterator<Item = CoordTimeResult>>,
                        }
                    }
                }
                Err(err) => {
                    Box::new(std::iter::once(Err(err))) as Box<dyn Iterator<Item = CoordTimeResult>>
                }
            });
    Ok(Box::new(iter))
}

pub fn expand_paired_file(
    input_path: InputPath,
    override_tz: Option<String>,
) -> Result<CoordTimeStream, String> {
    let path_display = match &input_path {
        InputPath::Stdin => "stdin".to_string(),
        InputPath::File(p) => p.display().to_string(),
    };

    let reader =
        open_input(&input_path).map_err(|e| format!("Error opening {}: {}", path_display, e))?;

    let mut lines = reader.lines().enumerate();
    let tz_override = override_tz.clone();

    let iter = std::iter::from_fn(move || {
        for (idx, line_result) in lines.by_ref() {
            let line_number = idx + 1;
            let line = match line_result {
                Ok(value) => value,
                Err(err) => {
                    return Some(Err(format!(
                        "{}:{}: failed to read line: {}",
                        path_display, line_number, err
                    )));
                }
            };

            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }

            let parts = parse_delimited_line(&line);
            if parts.len() < 3 {
                return Some(Err(format!(
                    "{}:{}: expected 3 fields (lat lon datetime), found {}",
                    path_display,
                    line_number,
                    parts.len()
                )));
            }

            let lat_raw = match parts[0].trim().parse::<f64>() {
                Ok(value) => value,
                Err(_) => {
                    return Some(Err(format!(
                        "{}:{}: invalid latitude '{}'",
                        path_display, line_number, parts[0]
                    )));
                }
            };
            let lat = match validate_latitude(lat_raw) {
                Ok(value) => value,
                Err(err) => {
                    return Some(Err(format!("{}:{}: {}", path_display, line_number, err)));
                }
            };

            let lon_raw = match parts[1].trim().parse::<f64>() {
                Ok(value) => value,
                Err(_) => {
                    return Some(Err(format!(
                        "{}:{}: invalid longitude '{}'",
                        path_display, line_number, parts[1]
                    )));
                }
            };
            let lon = match validate_longitude(lon_raw) {
                Ok(value) => value,
                Err(err) => {
                    return Some(Err(format!("{}:{}: {}", path_display, line_number, err)));
                }
            };

            let dt_str = parts[2..].join(" ");
            match parse_datetime_string(dt_str.trim(), tz_override.as_deref()) {
                Ok(dt) => return Some(Ok((lat, lon, dt))),
                Err(err) => {
                    return Some(Err(format!("{}:{}: {}", path_display, line_number, err)));
                }
            }
        }
        None
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
            lon: Some((13.4, 13.4, 0.0)),
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
}

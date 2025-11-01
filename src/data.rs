//! CLI parsing, data source expansion, and timezone handling.

use chrono::{
    DateTime, Duration, FixedOffset, Local, NaiveDate, NaiveDateTime, Offset, TimeZone, Utc,
};
use chrono_tz::Tz;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

static SYSTEM_TIMEZONE: OnceLock<FixedOffset> = OnceLock::new();

const DELTAT_MULTIPLE_ERROR: &str =
    "error: the argument '--deltat <DELTAT>' cannot be used multiple times";

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum InputPath {
    Stdin,
    File(PathBuf),
}

#[derive(Debug, Clone)]
pub enum LocationSource {
    Single(f64, f64),
    Range {
        lat: (f64, f64, f64),
        lon: Option<(f64, f64, f64)>,
    },
    File(InputPath),
}

#[derive(Debug, Clone)]
pub enum TimeSource {
    Single(String),
    Range(String, Option<String>), // partial date + optional step
    File(InputPath),
    Now,
}

#[derive(Debug, Clone)]
pub enum DataSource {
    Separate(LocationSource, TimeSource),
    Paired(InputPath),
}

pub type CoordTime = (f64, f64, DateTime<FixedOffset>);
pub type CoordTimeResult = Result<CoordTime, String>;
pub type CoordTimeStream = Box<dyn Iterator<Item = CoordTimeResult>>;
type LocationResult = Result<(f64, f64), String>;
type LocationStream = Box<dyn Iterator<Item = LocationResult>>;

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

impl DataSource {
    pub fn uses_stdin(&self) -> bool {
        match self {
            DataSource::Separate(loc, time) => {
                let loc_stdin = matches!(loc, LocationSource::File(InputPath::Stdin));
                let time_stdin = matches!(time, TimeSource::File(InputPath::Stdin));
                loc_stdin || time_stdin
            }
            DataSource::Paired(InputPath::Stdin) => true,
            DataSource::Paired(InputPath::File(_)) => false,
        }
    }

    pub fn is_watch_mode(&self, step: &Option<String>) -> bool {
        matches!(self, DataSource::Separate(_, TimeSource::Now)) && step.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct Parameters {
    pub deltat: Option<f64>, // None means auto-estimate
    pub format: String,
    pub headers: bool,
    pub show_inputs: Option<bool>, // None means auto-decide
    pub perf: bool,
    pub algorithm: String,
    pub refraction: bool,
    pub elevation: f64,
    pub temperature: f64,
    pub pressure: f64,
    pub horizon: Option<f64>,
    pub twilight: bool,
    pub step: Option<String>,
    pub timezone: Option<String>,
    pub elevation_angle: bool, // Show elevation instead of zenith in CSV
}

impl Default for Parameters {
    fn default() -> Self {
        Parameters {
            deltat: Some(0.0), // Default to 0.0 for compatibility
            format: "text".to_string(),
            headers: true,
            show_inputs: None,
            perf: false,
            algorithm: "spa".to_string(),
            refraction: true,
            elevation: 0.0,
            temperature: 15.0,
            pressure: 1013.0,
            horizon: None,
            twilight: false,
            step: None,
            timezone: None,
            elevation_angle: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Command {
    Position,
    Sunrise,
}

fn parse_delimited_line(line: &str) -> Vec<&str> {
    if line.contains(',') {
        line.split(',').collect()
    } else {
        line.split_whitespace().collect()
    }
}

pub fn parse_cli(args: Vec<String>) -> Result<(DataSource, Command, Parameters), String> {
    if args.len() < 2 {
        return Err("Usage: sunce [options] <lat> <lon> <datetime> <command>".to_string());
    }

    let mut params = Parameters::default();
    let mut positional_args = Vec::new();
    let mut deltat_seen = false;

    // Single pass: separate options from positionals
    for arg in args.iter().skip(1) {
        if let Some(stripped) = arg.strip_prefix("--") {
            // Parse option with = syntax
            if let Some(eq_pos) = stripped.find('=') {
                let option = &stripped[..eq_pos];
                let value = &stripped[eq_pos + 1..];

                match option {
                    "format" => {
                        let format_lower = value.to_lowercase();
                        #[cfg(not(feature = "parquet"))]
                        if format_lower == "parquet" {
                            return Err(
                                "PARQUET format not available. Recompile with --features parquet"
                                    .to_string(),
                            );
                        }
                        #[cfg(feature = "parquet")]
                        let valid_formats = ["text", "csv", "json", "parquet"];
                        #[cfg(not(feature = "parquet"))]
                        let valid_formats = ["text", "csv", "json"];

                        if !valid_formats.contains(&format_lower.as_str()) {
                            #[cfg(feature = "parquet")]
                            let supported = "text, csv, json, parquet";
                            #[cfg(not(feature = "parquet"))]
                            let supported = "text, csv, json";
                            return Err(format!(
                                "Invalid format: '{}'. Supported formats: {}",
                                value, supported
                            ));
                        }
                        params.format = format_lower;
                    }
                    "deltat" => {
                        if deltat_seen {
                            return Err(DELTAT_MULTIPLE_ERROR.to_string());
                        }
                        deltat_seen = true;
                        params.deltat = Some(
                            value
                                .parse::<f64>()
                                .map_err(|_| format!("Invalid deltat value: {}", value))?,
                        )
                    }
                    "timezone" => params.timezone = Some(value.to_string()),
                    "algorithm" => {
                        let algo_lower = value.to_lowercase();
                        if !["spa", "grena3"].contains(&algo_lower.as_str()) {
                            return Err(format!(
                                "Invalid algorithm: '{}'. Supported algorithms: spa, grena3",
                                value
                            ));
                        }
                        params.algorithm = algo_lower;
                    }
                    "step" => {
                        validate_step_value(value)?;
                        params.step = Some(value.to_string())
                    }
                    "elevation" => {
                        params.elevation = value
                            .parse::<f64>()
                            .map_err(|_| format!("Invalid elevation value: {}", value))?
                    }
                    "temperature" => {
                        params.temperature = value
                            .parse::<f64>()
                            .map_err(|_| format!("Invalid temperature value: {}", value))?
                    }
                    "pressure" => {
                        params.pressure = value
                            .parse::<f64>()
                            .map_err(|_| format!("Invalid pressure value: {}", value))?
                    }
                    "horizon" => {
                        params.horizon = Some(
                            value
                                .parse::<f64>()
                                .map_err(|_| format!("Invalid horizon value: {}", value))?,
                        )
                    }
                    _ => return Err(format!("Unknown option: --{}", option)),
                }
            } else {
                // Parse option without value
                match stripped {
                    "headers" => params.headers = true,
                    "no-headers" => params.headers = false,
                    "show-inputs" => params.show_inputs = Some(true),
                    "no-show-inputs" => params.show_inputs = Some(false),
                    "perf" => params.perf = true,
                    "deltat" => {
                        if deltat_seen {
                            return Err(DELTAT_MULTIPLE_ERROR.to_string());
                        }
                        deltat_seen = true;
                        params.deltat = None; // Auto-estimate
                    }
                    "no-refraction" => params.refraction = false,
                    "elevation-angle" => params.elevation_angle = true,
                    "twilight" => params.twilight = true,
                    "help" => return Err(get_help_text()),
                    "version" => return Err(get_version_text()),
                    _ => return Err(format!("Unknown option: --{}", stripped)),
                }
            }
        } else {
            positional_args.push(arg.clone());
        }
    }

    if positional_args.len() < 2 {
        return Err("Need at least command and one argument".to_string());
    }

    // Handle "help" command
    if positional_args[0] == "help" {
        if positional_args.len() >= 2 {
            return Err(get_command_help(&positional_args[1]));
        } else {
            return Err(get_help_text());
        }
    }

    // Find the command
    let command_index = positional_args
        .iter()
        .position(|arg| arg == "position" || arg == "sunrise")
        .ok_or("No command found".to_string())?;

    let command_str = &positional_args[command_index];
    let command = match command_str.as_str() {
        "position" => Command::Position,
        "sunrise" => Command::Sunrise,
        _ => return Err(format!("Unknown command: {}", command_str)),
    };

    // Data args are everything before the command
    let data_args = &positional_args[..command_index];

    // Validate command-specific options
    match command {
        Command::Position => {
            if params.horizon.is_some() {
                return Err("Option --horizon not valid for position command".to_string());
            }
            if params.twilight {
                return Err("Option --twilight not valid for position command".to_string());
            }
        }
        Command::Sunrise => {
            if params.step.is_some() {
                return Err("Option --step not valid for sunrise command".to_string());
            }
            if !params.refraction {
                return Err("Option --no-refraction not valid for sunrise command".to_string());
            }
            if params.elevation_angle {
                return Err("Option --elevation-angle not valid for sunrise command".to_string());
            }
            if params.algorithm != "spa" {
                return Err("Option --algorithm not valid for sunrise command".to_string());
            }
            if params.elevation != 0.0 {
                return Err("Option --elevation not valid for sunrise command".to_string());
            }
            if params.temperature != 15.0 {
                return Err("Option --temperature not valid for sunrise command".to_string());
            }
            if params.pressure != 1013.0 {
                return Err("Option --pressure not valid for sunrise command".to_string());
            }
        }
    }

    // Parse data source based on data arguments (before command)
    let data_source = match data_args.len() {
        1 => {
            // Single argument - could be paired file
            let arg = &data_args[0];
            if arg.starts_with('@') {
                DataSource::Paired(parse_file_arg(arg)?)
            } else {
                return Err("Single argument must be a file (@file or @-)".to_string());
            }
        }
        2 => {
            // Two arguments - could be coordinate file + time file
            let arg1 = &data_args[0];
            let arg2 = &data_args[1];

            // Both files: coordinate file + time file
            if arg1.starts_with('@') && arg2.starts_with('@') {
                let coord_path = parse_file_arg(arg1)?;
                let time_path = parse_file_arg(arg2)?;

                let location_source = LocationSource::File(coord_path);
                let time_source = TimeSource::File(time_path);
                DataSource::Separate(location_source, time_source)
            } else if arg1.starts_with('@') {
                // Coordinate file + time string
                let location_source = LocationSource::File(parse_file_arg(arg1)?);
                let time_source = parse_time_arg(arg2, &params)?;
                DataSource::Separate(location_source, time_source)
            } else {
                return Err("Two arguments: Use @coords.txt @times.txt, @coords.txt datetime, or three arguments (lat lon datetime)".to_string());
            }
        }
        3 => {
            // Three arguments: lat, lon, datetime
            let lat_str = &data_args[0];
            let lon_str = &data_args[1];
            let time_str = &data_args[2];

            let location_source = parse_location_args(lat_str, lon_str)?;
            let time_source = parse_time_arg(time_str, &params)?;

            DataSource::Separate(location_source, time_source)
        }
        _ => return Err("Too many arguments".to_string()),
    };

    // Auto-decide show_inputs if not explicitly set
    if params.show_inputs.is_none() {
        params.show_inputs = Some(should_auto_show_inputs(&data_source, command));
    }

    Ok((data_source, command, params))
}

fn parse_file_arg(arg: &str) -> Result<InputPath, String> {
    let Some(stripped) = arg.strip_prefix('@') else {
        return Err("Not a file argument".to_string());
    };

    if stripped == "-" {
        return Ok(InputPath::Stdin);
    }

    Ok(InputPath::File(PathBuf::from(stripped)))
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

fn parse_location_args(lat_str: &str, lon_str: &str) -> Result<LocationSource, String> {
    // Check if either is a file
    if lat_str.starts_with('@') && lon_str.starts_with('@') {
        return Err("Cannot have both lat and lon as files".to_string());
    }

    if lat_str.starts_with('@') {
        return Ok(LocationSource::File(parse_file_arg(lat_str)?));
    }

    if lon_str.starts_with('@') {
        return Ok(LocationSource::File(parse_file_arg(lon_str)?));
    }

    // Parse as ranges or single values
    let lat_range = parse_range(lat_str)?;
    let lon_range = parse_range(lon_str)?;

    match (lat_range, lon_range) {
        (Some(lat), Some(lon)) => Ok(LocationSource::Range {
            lat,
            lon: Some(lon),
        }),
        (Some(lat), None) => {
            let lon_val = lon_str
                .parse::<f64>()
                .map_err(|_| format!("Invalid longitude: {}", lon_str))?;
            Ok(LocationSource::Range {
                lat,
                lon: Some((lon_val, lon_val, 0.0)),
            })
        }
        (None, Some(lon)) => {
            let lat_val = lat_str
                .parse::<f64>()
                .map_err(|_| format!("Invalid latitude: {}", lat_str))?;
            Ok(LocationSource::Range {
                lat: (lat_val, lat_val, 0.0),
                lon: Some(lon),
            })
        }
        (None, None) => {
            let lat = lat_str
                .parse::<f64>()
                .map_err(|_| format!("Invalid latitude: {}", lat_str))?;
            let lon = lon_str
                .parse::<f64>()
                .map_err(|_| format!("Invalid longitude: {}", lon_str))?;
            Ok(LocationSource::Single(lat, lon))
        }
    }
}

fn parse_time_arg(time_str: &str, params: &Parameters) -> Result<TimeSource, String> {
    if time_str.starts_with('@') {
        return Ok(TimeSource::File(parse_file_arg(time_str)?));
    }

    if time_str == "now" {
        return Ok(TimeSource::Now);
    }

    if is_partial_date(time_str) {
        return Ok(TimeSource::Range(time_str.to_string(), params.step.clone()));
    }

    if params.step.is_some() && is_date_without_time(time_str) {
        return Ok(TimeSource::Range(time_str.to_string(), params.step.clone()));
    }

    if params.step.is_some() {
        return Err(
            "Option --step requires date-only input (YYYY, YYYY-MM, or YYYY-MM-DD) or 'now'"
                .to_string(),
        );
    }

    // Validate that this is a valid datetime string before creating TimeSource::Single
    parse_datetime_string(time_str, None)?;
    Ok(TimeSource::Single(time_str.to_string()))
}

fn parse_range(s: &str) -> Result<Option<(f64, f64, f64)>, String> {
    let Some((start_str, rest)) = s.split_once(':') else {
        return Ok(None);
    };
    let Some((end_str, step_str)) = rest.split_once(':') else {
        return Err(format!("Range must be start:end:step, got: {}", s));
    };

    let (start, end, step) = (
        start_str
            .parse()
            .map_err(|_| format!("Invalid range start: {}", start_str))?,
        end_str
            .parse()
            .map_err(|_| format!("Invalid range end: {}", end_str))?,
        step_str
            .parse()
            .map_err(|_| format!("Invalid range step: {}", step_str))?,
    );

    if step <= 0.0 {
        return Err("Range step must be positive".to_string());
    }

    Ok(Some((start, end, step)))
}

fn is_partial_date(s: &str) -> bool {
    // Only year (YYYY) and year-month (YYYY-MM) are partial dates
    // Complete dates like YYYY-MM-DD are handled specially in expand_time_source
    if s.len() == 4 && s.chars().all(|c| c.is_ascii_digit()) {
        return true;
    }

    if s.len() == 7 && &s[4..5] == "-" {
        let year_part = &s[0..4];
        let month_part = &s[5..7];
        return year_part.chars().all(|c| c.is_ascii_digit())
            && month_part.chars().all(|c| c.is_ascii_digit());
    }

    false
}

fn is_date_without_time(s: &str) -> bool {
    s.len() == 10
        && s.matches('-').count() == 2
        && !s.contains('T')
        && !s.contains(' ')
        && s.chars()
            .enumerate()
            .all(|(idx, c)| matches!(idx, 4 | 7) || c.is_ascii_digit())
}

fn should_auto_show_inputs(source: &DataSource, command: Command) -> bool {
    match source {
        DataSource::Separate(loc, time) => {
            // Auto-enable only when inputs could produce multiple values
            let has_location_range = matches!(loc, LocationSource::Range { .. });
            let has_location_file = matches!(loc, LocationSource::File(_));
            let has_time_range = matches!(time, TimeSource::Range(_, _));
            let has_time_file = matches!(time, TimeSource::File(_));

            // Special case: position command with YYYY-MM-DD date (no time) expands to time series
            let is_position_date_series = matches!(time, TimeSource::Single(s) if command == Command::Position && is_date_without_time(s));

            has_location_range
                || has_location_file
                || has_time_range
                || has_time_file
                || is_position_date_series
        }
        DataSource::Paired(_) => {
            // Paired files always could have multiple rows
            true
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
            let lat_count = ((lat.1 - lat.0) / lat.2 + 1.0) as usize;
            let lon_count = ((lon_end - lon_start) / lon_step + 1.0) as usize;

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

                    let lat: f64 = match parts[0].trim().parse() {
                        Ok(v) => v,
                        Err(_) => {
                            finished = true;
                            return Some(Err(format!(
                                "{}:{}: invalid latitude '{}'",
                                path_display, line_number, parts[0]
                            )));
                        }
                    };

                    let lon: f64 = match parts[1].trim().parse() {
                        Ok(v) => v,
                        Err(_) => {
                            finished = true;
                            return Some(Err(format!(
                                "{}:{}: invalid longitude '{}'",
                                path_display, line_number, parts[1]
                            )));
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

#[derive(Clone)]
enum TimezoneInfo {
    Fixed(FixedOffset),
    Named(Tz),
}

impl TimezoneInfo {
    fn to_datetime_from_utc(&self, dt: &NaiveDateTime) -> DateTime<FixedOffset> {
        match self {
            TimezoneInfo::Fixed(offset) => offset.from_utc_datetime(dt),
            TimezoneInfo::Named(tz) => {
                let dt_utc = Utc.from_utc_datetime(dt);
                dt_utc.with_timezone(tz).fixed_offset()
            }
        }
    }

    fn to_datetime_from_local(&self, dt: &NaiveDateTime) -> Option<DateTime<FixedOffset>> {
        match self {
            TimezoneInfo::Fixed(offset) => match offset.from_local_datetime(dt) {
                chrono::LocalResult::Single(dt) => Some(dt),
                chrono::LocalResult::None => None,
                chrono::LocalResult::Ambiguous(dt1, _dt2) => Some(dt1),
            },
            TimezoneInfo::Named(tz) => {
                // Handle DST transitions:
                // - LocalResult::None: time doesn't exist (spring forward) - skip it
                // - LocalResult::Single: unambiguous time
                // - LocalResult::Ambiguous: time exists twice (fall back) - use earlier time
                match tz.from_local_datetime(dt) {
                    chrono::LocalResult::None => None, // DST gap - skip
                    chrono::LocalResult::Single(dt) => Some(dt.fixed_offset()),
                    chrono::LocalResult::Ambiguous(dt1, _dt2) => Some(dt1.fixed_offset()), // Use first occurrence
                }
            }
        }
    }
}

fn get_timezone_info(override_tz: Option<&str>) -> TimezoneInfo {
    let tz_env = std::env::var("TZ").ok();
    let tz_str = override_tz.or(tz_env.as_deref());

    tz_str
        .and_then(|s| {
            parse_tz_offset(s)
                .map(TimezoneInfo::Fixed)
                .or_else(|| s.parse::<Tz>().ok().map(TimezoneInfo::Named))
        })
        .unwrap_or_else(|| {
            let offset = SYSTEM_TIMEZONE.get_or_init(|| Local::now().offset().fix());
            TimezoneInfo::Fixed(*offset)
        })
}

fn get_local_timezone(override_tz: Option<&str>) -> FixedOffset {
    match get_timezone_info(override_tz) {
        TimezoneInfo::Fixed(offset) => offset,
        TimezoneInfo::Named(tz) => {
            let now = Utc::now();
            tz.offset_from_utc_datetime(&now.naive_utc()).fix()
        }
    }
}

fn parse_tz_offset(tz: &str) -> Option<FixedOffset> {
    let (sign, rest) = match tz.as_bytes().first()? {
        b'+' => (1, &tz[1..]),
        b'-' => (-1, &tz[1..]),
        _ => return None,
    };

    let (hours, minutes) = if let Some((h, m)) = rest.split_once(':') {
        (h.parse::<i32>().ok()?, m.parse::<i32>().ok()?)
    } else {
        (rest.parse::<i32>().ok()?, 0)
    };

    FixedOffset::east_opt(sign * (hours * 3600 + minutes * 60))
}

fn parse_datetime_string(
    dt_str: &str,
    override_tz: Option<&str>,
) -> Result<DateTime<FixedOffset>, String> {
    // Handle "now" specially
    if dt_str == "now" {
        let local_tz = get_local_timezone(override_tz);
        return Ok(Utc::now().with_timezone(&local_tz));
    }

    // Try ISO 8601 parsing with timezone
    if dt_str.contains('T') || dt_str.contains(' ') && dt_str.matches(':').count() >= 2 {
        // Has time component (either T or space separator with at least HH:MM:SS)
        if dt_str.ends_with('Z') {
            // UTC timezone
            let utc_dt = dt_str
                .parse::<DateTime<Utc>>()
                .map_err(|e| format!("Failed to parse UTC datetime: {}", e))?;
            let target_tz = get_local_timezone(override_tz);
            return Ok(utc_dt.with_timezone(&target_tz));
        } else if dt_str.contains('+') || dt_str.rfind('-').is_some_and(|i| i > 10) {
            // Has timezone offset
            let fixed_dt = dt_str
                .parse::<DateTime<FixedOffset>>()
                .map_err(|e| format!("Failed to parse datetime with timezone: {}", e))?;

            // If we have an override timezone, convert to that
            if override_tz.is_some() {
                let target_tz = get_local_timezone(override_tz);
                return Ok(fixed_dt.with_timezone(&target_tz));
            }

            return Ok(fixed_dt);
        } else {
            // No timezone, assume local or override
            let naive_dt = NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%dT%H:%M:%S")
                .or_else(|_| NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%dT%H:%M"))
                .or_else(|_| NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%d %H:%M:%S"))
                .or_else(|_| NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%d %H:%M"))
                .map_err(|e| format!("Failed to parse naive datetime: {}", e))?;

            let tz_info = get_timezone_info(override_tz);
            return tz_info.to_datetime_from_local(&naive_dt).ok_or_else(|| {
                format!(
                    "Datetime does not exist in timezone (likely DST gap): {}",
                    dt_str
                )
            });
        }
    }

    // Try parsing as unix timestamp (integer seconds since epoch)
    // Timestamps have at least 5 digits (10000+) to distinguish from years
    if let Ok(timestamp) = dt_str.parse::<i64>()
        && timestamp.abs() >= 10000
    {
        let utc_dt = DateTime::<Utc>::from_timestamp(timestamp, 0)
            .ok_or_else(|| format!("Invalid unix timestamp: {}", timestamp))?;

        if override_tz.is_some() {
            let tz_info = get_timezone_info(override_tz);
            let naive_utc = utc_dt.naive_utc();
            return Ok(tz_info.to_datetime_from_utc(&naive_utc));
        }

        return Ok(utc_dt.fixed_offset());
    }

    // Date only, assume midnight
    let naive_date = NaiveDate::parse_from_str(dt_str, "%Y-%m-%d")
        .map_err(|e| format!("Failed to parse date: {}", e))?;
    let naive_dt = naive_date
        .and_hms_opt(0, 0, 0)
        .expect("Midnight time creation cannot fail");
    let tz_info = get_timezone_info(override_tz);
    tz_info.to_datetime_from_local(&naive_dt).ok_or_else(|| {
        format!(
            "Datetime does not exist in timezone (likely DST gap): {}",
            dt_str
        )
    })
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
            let local_tz = get_local_timezone(override_tz.as_deref());
            if let Some(step_str) = step_override {
                let step_duration = parse_duration_positive(&step_str)?;
                let mut first = true;
                let iter = std::iter::from_fn(move || {
                    if !std::mem::take(&mut first) {
                        std::thread::sleep(
                            step_duration
                                .to_std()
                                .unwrap_or(std::time::Duration::from_secs(1)),
                        );
                    }
                    Some(Ok(Utc::now().with_timezone(&local_tz)))
                });
                Ok(TimeStream::new(iter, false))
            } else {
                Ok(TimeStream::new(
                    std::iter::once(Ok(Utc::now().with_timezone(&local_tz))),
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

fn parse_duration_positive(s: &str) -> Result<Duration, String> {
    let ensure_positive = |num: i64| {
        if num <= 0 {
            Err(format!("Step must be positive, got '{}'", s))
        } else {
            Ok(num)
        }
    };

    if let Ok(raw_seconds) = s.parse::<i64>() {
        let seconds = ensure_positive(raw_seconds)?;
        return Ok(Duration::seconds(seconds));
    }

    if s.len() < 2 {
        return Err(format!(
            "Invalid step format: '{}'. Expected <number><unit> such as 1h or 30m",
            s
        ));
    }

    let (num_str, unit) = s.split_at(s.len() - 1);
    let value = num_str.parse::<i64>().map_err(|_| {
        format!(
            "Invalid step value in '{}'. Use an integer before the unit (e.g., 15m)",
            s
        )
    })?;
    let positive = ensure_positive(value)?;

    let duration = match unit {
        "s" => Duration::seconds(positive),
        "m" => Duration::minutes(positive),
        "h" => Duration::hours(positive),
        "d" => Duration::days(positive),
        _ => {
            return Err(format!(
                "Invalid step unit in '{}'. Supported units: s, m, h, d",
                s
            ));
        }
    };

    Ok(duration)
}

fn validate_step_value(step: &str) -> Result<(), String> {
    parse_duration_positive(step).map(|_| ())
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

            let lat = match parts[0].trim().parse::<f64>() {
                Ok(value) => value,
                Err(_) => {
                    return Some(Err(format!(
                        "{}:{}: invalid latitude '{}'",
                        path_display, line_number, parts[0]
                    )));
                }
            };

            let lon = match parts[1].trim().parse::<f64>() {
                Ok(value) => value,
                Err(_) => {
                    return Some(Err(format!(
                        "{}:{}: invalid longitude '{}'",
                        path_display, line_number, parts[1]
                    )));
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

fn get_version_text() -> String {
    format!(
        "sunce {}\n Build: {} ({})\n Features: {}",
        env!("CARGO_PKG_VERSION"),
        env!("BUILD_PROFILE"),
        env!("BUILD_TARGET"),
        env!("BUILD_FEATURES")
    )
}

fn get_help_text() -> String {
    format!(
        r#"sunce {}
Calculates topocentric solar coordinates or sunrise/sunset times.

Usage: sunce [OPTIONS] <latitude> <longitude> <dateTime> <COMMAND>

Examples:
  sunce 52.0 13.4 2024-01-01 position
  sunce 52:53:0.1 13:14:0.1 2024 position --format=csv
  sunce @coords.txt @times.txt position
  sunce @data.txt position  # paired lat,lng,datetime data
  echo '52.0 13.4 2024-01-01T12:00:00' | sunce @- position

Arguments:
  <latitude>        Latitude: decimal degrees, range, or file
                      52.5        single coordinate
                      52:53:0.1   range from 52° to 53° in 0.1° steps
                      @coords.txt file with coordinates (or @- for stdin)

  <longitude>       Longitude: decimal degrees, range, or file
                      13.4        single coordinate
                      13:14:0.1   range from 13° to 14° in 0.1° steps
                      @coords.txt file with coordinates (or @- for stdin)

  <dateTime>        Date/time: ISO format, partial dates, or file
                      2024-01-01           specific date (midnight)
                      2024-01-01T12:00:00  specific date and time
                      2024                 entire year (with --step)
                      now                  current date and time
                      @times.txt           file with times (or @- for stdin)

Options:
  --deltat[=<value>]    Delta T in seconds; auto-estimate if no value given.
                        Use --deltat=<value> for explicit value.
  --format=<format>     Output format: text, csv, json, parquet. Default: text
  --help                Show this help message and exit.
  --version             Print version information and exit.
  --[no-]headers        Show headers in output (CSV only). Default: true
  --[no-]show-inputs    Show all inputs in output. Auto-enabled for ranges/files
                        unless --no-show-inputs is used.
  --perf                Show performance statistics.
  --timezone=<tz>       Timezone as offset (e.g. +01:00) or zone id (e.g.
                        America/Los_Angeles). Overrides datetime timezone.

Commands:
  position              Calculate topocentric solar coordinates
  sunrise               Calculate sunrise, transit, sunset and twilight times

Run 'sunce help <command>' for command-specific options.
"#,
        env!("CARGO_PKG_VERSION")
    )
}

pub fn get_command_help(command: &str) -> String {
    match command {
        "position" => r#"Usage: sunce position [OPTIONS]

Calculates topocentric solar coordinates.

Options:
  --algorithm=<alg>         Algorithm: spa, grena3. Default: spa
  --elevation=<meters>      Elevation above sea level in meters. Default: 0
  --elevation-angle         Output elevation angle instead of zenith angle.
  --[no-]refraction         Apply refraction correction. Default: true
  --pressure=<hPa>          Avg. air pressure in hPa. Used for refraction. Default: 1013
  --temperature=<celsius>   Avg. air temperature in °C. Used for refraction. Default: 15
  --step=<interval>         Step interval for time series. Examples: 30s, 15m, 2h, 1d.
                            Default: 1h

Examples:
  sunce 52.0 13.4 2024-06-21T12:00:00 position
  sunce 52.0 13.4 2024 position --step=1d
  sunce 50:55:0.5 10:15:0.5 2024-06-21T12:00:00 position --algorithm=grena3
"#
        .to_string(),
        "sunrise" => r#"Usage: sunce sunrise [OPTIONS]

Calculates sunrise, transit, sunset and (optionally) twilight times.

Options:
  --twilight                Include civil, nautical, and astronomical twilight times.
  --horizon=<degrees>       Custom horizon angle in degrees (alternative to --twilight).

Examples:
  sunce 52.0 13.4 2024-06-21 sunrise
  sunce 52.0 13.4 2024-06 sunrise --twilight
  sunce 52.0 13.4 2024-06-21 sunrise --horizon=-6.0
"#
        .to_string(),
        _ => format!(
            "Unknown command: {}\n\nRun 'sunce --help' for usage.",
            command
        ),
    }
}

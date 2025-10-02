//! CLI parsing, data source expansion, and timezone handling.

use chrono::{DateTime, Duration, FixedOffset, NaiveDate, NaiveDateTime, Offset, TimeZone, Utc};
use chrono_tz::Tz;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::PathBuf;

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

    #[allow(dead_code)]
    pub fn debug_repr(&self) -> String {
        match self {
            DataSource::Separate(loc, time) => {
                let loc_str = match loc {
                    LocationSource::Single(lat, lon) => format!("Single({}, {})", lat, lon),
                    LocationSource::Range { lat, lon } => {
                        if let Some((lon_start, lon_end, lon_step)) = lon {
                            format!(
                                "Range({}, {}, {}) x Range({}, {}, {})",
                                lat.0, lat.1, lat.2, lon_start, lon_end, lon_step
                            )
                        } else {
                            format!("Range({}, {}, {}) x Single", lat.0, lat.1, lat.2)
                        }
                    }
                    LocationSource::File(InputPath::Stdin) => "File(stdin)".to_string(),
                    LocationSource::File(InputPath::File(path)) => {
                        format!("File({})", path.display())
                    }
                };

                let time_str = match time {
                    TimeSource::Single(s) => format!("Single({})", s),
                    TimeSource::Range(date, step) => {
                        if let Some(s) = step {
                            format!("Range({}, step={})", date, s)
                        } else {
                            format!("Range({})", date)
                        }
                    }
                    TimeSource::File(InputPath::Stdin) => "File(stdin)".to_string(),
                    TimeSource::File(InputPath::File(path)) => format!("File({})", path.display()),
                    TimeSource::Now => "Now".to_string(),
                };

                format!(
                    "SOURCE: Separate\nLOCATION: {}\nTIME: {}",
                    loc_str, time_str
                )
            }
            DataSource::Paired(InputPath::Stdin) => "SOURCE: Paired\nFILE: stdin".to_string(),
            DataSource::Paired(InputPath::File(path)) => {
                format!("SOURCE: Paired\nFILE: {}", path.display())
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Parameters {
    pub deltat: Option<f64>, // None means auto-estimate
    pub format: String,
    pub headers: bool,
    pub show_inputs: Option<bool>, // None means auto-decide
    pub parallel: bool,
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
            parallel: false,
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
                    "step" => params.step = Some(value.to_string()),
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
                    "parallel" => params.parallel = true,
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
    let stripped = arg
        .strip_prefix('@')
        .ok_or_else(|| "Not a file argument".to_string())?;

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

    if is_partial_date(time_str) || params.step.is_some() {
        // Partial dates or any date with --step become time series
        Ok(TimeSource::Range(time_str.to_string(), params.step.clone()))
    } else {
        // Validate that this is a valid datetime string before creating TimeSource::Single
        parse_datetime_string(time_str, None)?;
        Ok(TimeSource::Single(time_str.to_string()))
    }
}

fn parse_range(s: &str) -> Result<Option<(f64, f64, f64)>, String> {
    let Some((start_str, rest)) = s.split_once(':') else {
        return Ok(None);
    };
    let Some((end_str, step_str)) = rest.split_once(':') else {
        return Err(format!("Range must be start:end:step, got: {}", s));
    };

    let start = start_str
        .parse()
        .map_err(|_| format!("Invalid range start: {}", start_str))?;
    let end = end_str
        .parse()
        .map_err(|_| format!("Invalid range end: {}", end_str))?;
    let step = step_str
        .parse()
        .map_err(|_| format!("Invalid range step: {}", step_str))?;

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

fn should_auto_show_inputs(source: &DataSource, command: Command) -> bool {
    match source {
        DataSource::Separate(loc, time) => {
            // Auto-enable only when inputs could produce multiple values
            let has_location_range = matches!(loc, LocationSource::Range { .. });
            let has_location_file = matches!(loc, LocationSource::File(_));
            let has_time_range = matches!(time, TimeSource::Range(_, _));
            let has_time_file = matches!(time, TimeSource::File(_));

            // Special case: position command with YYYY-MM-DD date (no time) expands to time series
            let is_position_date_series = match time {
                TimeSource::Single(s) => {
                    command == Command::Position
                        && s.len() == 10
                        && s.matches('-').count() == 2
                        && !s.contains('T')
                }
                _ => false,
            };

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

pub fn expand_location_source(source: LocationSource) -> Box<dyn Iterator<Item = (f64, f64)>> {
    match source {
        LocationSource::Single(lat, lon) => Box::new(std::iter::once((lat, lon))),
        LocationSource::Range { lat, lon } => {
            let lat_iter = coord_range_iter(lat.0, lat.1, lat.2);

            if let Some((lon_start, lon_end, lon_step)) = lon {
                // Create a vector for one dimension to allow repeated iteration
                // Choose the smaller dimension to minimize memory usage
                let lat_count = ((lat.1 - lat.0) / lat.2 + 1.0) as usize;
                let lon_count = ((lon_end - lon_start) / lon_step + 1.0) as usize;

                if lat_count <= lon_count {
                    // Collect latitudes (smaller), iterate longitudes
                    let lat_coords: Vec<f64> = lat_iter.collect();
                    Box::new(
                        coord_range_iter(lon_start, lon_end, lon_step).flat_map(move |lon| {
                            lat_coords.clone().into_iter().map(move |lat| (lat, lon))
                        }),
                    )
                } else {
                    // Collect longitudes (smaller), iterate latitudes
                    let lon_coords: Vec<f64> =
                        coord_range_iter(lon_start, lon_end, lon_step).collect();
                    Box::new(lat_iter.flat_map(move |lat| {
                        lon_coords.clone().into_iter().map(move |lon| (lat, lon))
                    }))
                }
            } else {
                unreachable!(
                    "Range without longitude range - should be prevented by parse_cli validation"
                )
            }
        }
        LocationSource::File(input_path) => {
            let path_display = match &input_path {
                InputPath::Stdin => "stdin".to_string(),
                InputPath::File(p) => p.display().to_string(),
            };

            let reader = match open_input(&input_path) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("Error opening {}: {}", path_display, e);
                    std::process::exit(1);
                }
            };

            let mut line_num = 0;

            Box::new(reader.lines().filter_map(move |line_result| {
                line_num += 1;

                let line = match line_result {
                    Ok(l) => l,
                    Err(e) => {
                        eprintln!("Error reading {}:{}: {}", path_display, line_num, e);
                        std::process::exit(1);
                    }
                };

                let trimmed = line.trim();
                if trimmed.is_empty() || trimmed.starts_with('#') {
                    return None;
                }

                let parts = parse_delimited_line(trimmed);
                if parts.len() < 2 {
                    eprintln!(
                        "Error: {}:{}: expected 2 fields (lat lon), found {}",
                        path_display,
                        line_num,
                        parts.len()
                    );
                    std::process::exit(1);
                }
                if parts.len() > 2 {
                    eprintln!(
                        "Error: {}:{}: expected 2 fields (lat lon), found {}. File appears to be a paired data file (lat lon datetime), which cannot be used with a separate time source.",
                        path_display,
                        line_num,
                        parts.len()
                    );
                    std::process::exit(1);
                }

                let lat: f64 = match parts[0].trim().parse() {
                    Ok(v) => v,
                    Err(_) => {
                        eprintln!(
                            "Error: {}:{}: invalid latitude '{}'",
                            path_display,
                            line_num,
                            parts[0]
                        );
                        std::process::exit(1);
                    }
                };

                let lon: f64 = match parts[1].trim().parse() {
                    Ok(v) => v,
                    Err(_) => {
                        eprintln!(
                            "Error: {}:{}: invalid longitude '{}'",
                            path_display,
                            line_num,
                            parts[1]
                        );
                        std::process::exit(1);
                    }
                };

                Some((lat, lon))
            }))
        }
    }
}

#[derive(Clone)]
enum TimezoneInfo {
    Fixed(FixedOffset),
    Named(Tz),
}

impl TimezoneInfo {
    #[allow(clippy::wrong_self_convention)]
    fn from_utc_datetime(&self, dt: &NaiveDateTime) -> DateTime<FixedOffset> {
        match self {
            TimezoneInfo::Fixed(offset) => offset.from_utc_datetime(dt),
            TimezoneInfo::Named(tz) => {
                let dt_utc = Utc.from_utc_datetime(dt);
                dt_utc.with_timezone(tz).fixed_offset()
            }
        }
    }

    #[allow(clippy::wrong_self_convention)]
    fn from_local_datetime(&self, dt: &NaiveDateTime) -> Option<DateTime<FixedOffset>> {
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
        .and_then(|s| parse_tz_offset(s).map(TimezoneInfo::Fixed))
        .or_else(|| tz_str.and_then(|s| s.parse::<Tz>().ok().map(TimezoneInfo::Named)))
        .unwrap_or_else(|| {
            TimezoneInfo::Fixed(FixedOffset::east_opt(0).expect("UTC offset creation cannot fail"))
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

    let (hours, minutes): (i32, i32) = if let Some((h, m)) = rest.split_once(':') {
        (h.parse().ok()?, m.parse().ok()?)
    } else {
        (rest.parse().ok()?, 0)
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
            return tz_info.from_local_datetime(&naive_dt).ok_or_else(|| {
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
            return Ok(tz_info.from_utc_datetime(&naive_utc));
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
    tz_info.from_local_datetime(&naive_dt).ok_or_else(|| {
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
) -> Box<dyn Iterator<Item = DateTime<FixedOffset>>> {
    match source {
        TimeSource::Single(dt_str) => {
            // For position command, treat YYYY-MM-DD as time series (24 hours)
            if command == Command::Position
                && dt_str.len() == 10
                && dt_str.matches('-').count() == 2
                && !dt_str.contains('T')
            {
                let step = step_override.unwrap_or_else(|| "1h".to_string());
                Box::new(expand_partial_date(dt_str, step, override_tz.clone()))
            } else {
                let dt = parse_datetime_string(&dt_str, override_tz.as_deref());
                match dt {
                    Ok(dt) => Box::new(std::iter::once(dt)),
                    Err(_) => Box::new(std::iter::empty()),
                }
            }
        }
        TimeSource::Range(partial_date, step_opt) => {
            // Expand into time series with appropriate step
            let step = step_override.or(step_opt).unwrap_or_else(|| {
                if partial_date.len() == 4 {
                    "1d".to_string() // Year -> daily
                } else {
                    "1h".to_string() // Month/Day -> hourly
                }
            });

            Box::new(expand_partial_date(partial_date, step, override_tz.clone()))
        }
        TimeSource::File(path) => Box::new(read_times_file(path, override_tz.clone())),
        TimeSource::Now => {
            let local_tz = get_local_timezone(override_tz.as_deref());
            Box::new(std::iter::once(Utc::now().with_timezone(&local_tz)))
        }
    }
}

fn expand_partial_date(
    date_str: String,
    step: String,
    override_tz: Option<String>,
) -> Box<dyn Iterator<Item = DateTime<FixedOffset>>> {
    let step_duration = parse_duration(&step).unwrap_or_else(|| {
        eprintln!(
            "Error: Invalid step format: '{}'. Expected format: <number><unit> where unit is s, m, h, or d (e.g., 1h, 30m, 1d)",
            step
        );
        std::process::exit(1);
    });

    let tz_info = get_timezone_info(override_tz.as_deref());

    let to_naive_range = |date: NaiveDate| {
        (
            date.and_hms_opt(0, 0, 0)
                .expect("Midnight creation cannot fail"),
            date.and_hms_opt(23, 59, 59)
                .expect("End of day creation cannot fail"),
        )
    };

    let (start_naive, end_naive) = match date_str.len() {
        4 => {
            let year = date_str
                .parse::<i32>()
                .expect("Year must be valid 4-digit integer");
            (
                to_naive_range(
                    NaiveDate::from_ymd_opt(year, 1, 1).expect("Year start must be valid"),
                )
                .0,
                to_naive_range(
                    NaiveDate::from_ymd_opt(year, 12, 31).expect("Year end must be valid"),
                )
                .1,
            )
        }
        7 => {
            let (year_str, month_str) = date_str.split_once('-').expect("Invalid month format");
            let (year, month) = (
                year_str.parse::<i32>().expect("Year must be valid integer"),
                month_str
                    .parse::<u32>()
                    .expect("Month must be valid integer"),
            );
            let start_date =
                NaiveDate::from_ymd_opt(year, month, 1).expect("Month start must be valid");
            let next_month_date = if month == 12 {
                NaiveDate::from_ymd_opt(year + 1, 1, 1)
            } else {
                NaiveDate::from_ymd_opt(year, month + 1, 1)
            }
            .expect("Next month must be valid");
            let end_date = next_month_date.pred_opt().expect("Previous day must exist");

            (to_naive_range(start_date).0, to_naive_range(end_date).1)
        }
        10 => {
            let parts: Vec<_> = date_str.split('-').collect();
            let date = NaiveDate::from_ymd_opt(
                parts[0].parse().expect("Year must be valid"),
                parts[1].parse().expect("Month must be valid"),
                parts[2].parse().expect("Day must be valid"),
            )
            .expect("Date must be valid");
            to_naive_range(date)
        }
        _ => return Box::new(std::iter::empty()),
    };

    let to_local_or_exit = |naive: NaiveDateTime, desc: &str| {
        tz_info.from_local_datetime(&naive).unwrap_or_else(|| {
            eprintln!(
                "Error: {} time does not exist in timezone (likely DST gap): {}",
                desc, date_str
            );
            std::process::exit(1);
        })
    };

    let start_dt = to_local_or_exit(start_naive, "Start");
    let end_dt = to_local_or_exit(end_naive, "End");

    Box::new(std::iter::successors(Some(start_dt), move |current_dt| {
        let next_dt = tz_info.from_utc_datetime(&(current_dt.naive_utc() + step_duration));
        (next_dt <= end_dt).then_some(next_dt)
    })) as Box<dyn Iterator<Item = DateTime<FixedOffset>>>
}

fn parse_duration(s: &str) -> Option<Duration> {
    let (num_str, unit) = s.split_at(s.len().checked_sub(1)?);
    let num = num_str.parse::<i64>().ok()?;
    Some(match unit {
        "s" => Duration::seconds(num),
        "m" => Duration::minutes(num),
        "h" => Duration::hours(num),
        "d" => Duration::days(num),
        _ => return None,
    })
}

fn read_times_file(
    input_path: InputPath,
    override_tz: Option<String>,
) -> impl Iterator<Item = DateTime<FixedOffset>> {
    match open_input(&input_path) {
        Ok(reader) => Box::new(reader.lines().filter_map(move |line| {
            if let Ok(line) = line {
                parse_datetime_string(line.trim(), override_tz.as_deref()).ok()
            } else {
                None
            }
        })) as Box<dyn Iterator<Item = DateTime<FixedOffset>>>,
        Err(_) => Box::new(std::iter::empty()) as Box<dyn Iterator<Item = DateTime<FixedOffset>>>,
    }
}

pub fn expand_cartesian_product(
    loc_source: LocationSource,
    time_source: TimeSource,
    step: Option<String>,
    override_tz: Option<String>,
    command: Command,
) -> Box<dyn Iterator<Item = (f64, f64, DateTime<FixedOffset>)>> {
    // For single location + time range: materialize locations (1 item), stream times
    // For location range + single time: materialize times (1 item), stream locations
    // For both ranges: estimate sizes and materialize the smaller one

    let is_loc_single = matches!(loc_source, LocationSource::Single(_, _));
    let is_time_single = matches!(time_source, TimeSource::Single(_) | TimeSource::Now);

    if is_loc_single && !is_time_single {
        // Single location, multiple times - stream times
        let locs: Vec<(f64, f64)> = expand_location_source(loc_source).collect();
        Box::new(
            expand_time_source(time_source, step, override_tz, command).flat_map(move |dt| {
                locs.clone()
                    .into_iter()
                    .map(move |(lat, lon)| (lat, lon, dt))
            }),
        )
    } else if !is_loc_single && is_time_single {
        // Multiple locations, single time - stream locations
        let times: Vec<DateTime<FixedOffset>> =
            expand_time_source(time_source, step, override_tz, command).collect();
        Box::new(expand_location_source(loc_source).flat_map(move |coord| {
            times
                .clone()
                .into_iter()
                .map(move |dt| (coord.0, coord.1, dt))
        }))
    } else {
        // Both ranges or both single - materialize times (typically smaller)
        let times: Vec<DateTime<FixedOffset>> =
            expand_time_source(time_source, step, override_tz, command).collect();
        Box::new(expand_location_source(loc_source).flat_map(move |coord| {
            times
                .clone()
                .into_iter()
                .map(move |dt| (coord.0, coord.1, dt))
        }))
    }
}

pub fn expand_paired_file(
    input_path: InputPath,
    override_tz: Option<String>,
) -> Box<dyn Iterator<Item = (f64, f64, DateTime<FixedOffset>)>> {
    let path_display = match &input_path {
        InputPath::Stdin => "stdin".to_string(),
        InputPath::File(p) => p.display().to_string(),
    };

    let reader = match open_input(&input_path) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error opening {}: {}", path_display, e);
            std::process::exit(1);
        }
    };

    let mut line_num = 0;

    Box::new(reader.lines().filter_map(move |line_result| {
        line_num += 1;

        let line = match line_result {
            Ok(l) => l,
            Err(e) => {
                eprintln!("Error reading {}:{}: {}", path_display, line_num, e);
                std::process::exit(1);
            }
        };

        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            return None;
        }

        let parts = parse_delimited_line(&line);
        if parts.len() < 3 {
            eprintln!(
                "Error: line {}: expected 3 fields (lat lon datetime), found {}",
                line_num,
                parts.len()
            );
            std::process::exit(1);
        }

        let lat: f64 = match parts[0].trim().parse() {
            Ok(v) => v,
            Err(_) => {
                eprintln!("Error: line {}: invalid latitude '{}'", line_num, parts[0]);
                std::process::exit(1);
            }
        };

        let lon: f64 = match parts[1].trim().parse() {
            Ok(v) => v,
            Err(_) => {
                eprintln!("Error: line {}: invalid longitude '{}'", line_num, parts[1]);
                std::process::exit(1);
            }
        };

        let dt_str = parts[2..].join(" ").trim().to_string();
        let dt = match parse_datetime_string(&dt_str, override_tz.as_deref()) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("Error: line {}: {}", line_num, e);
                std::process::exit(1);
            }
        };

        Some((lat, lon, dt))
    }))
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
  --[no-]parallel       Enable parallel processing (experimental). Default: false
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

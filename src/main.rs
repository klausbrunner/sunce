use chrono::TimeZone;
use clap::{Arg, ArgAction, ArgMatches, Command};
use rayon::prelude::*;
use solar_positioning::{grena3, spa};

mod parsing;
use parsing::{Coordinate, DateTimeInput, ParseError, parse_coordinate, parse_datetime};

mod output;
use output::{EnvironmentalParams, OutputFormat, PositionResult, output_position_results};

mod sunrise_output;
use sunrise_output::{SunriseResultData, TwilightResults, output_sunrise_results};

mod file_input;
mod time_series;
mod timezone_utils;
use file_input::{
    CoordinateFileIterator, PairedFileIterator, TimeFileIterator, create_file_reader,
};
use time_series::{TimeStep, expand_datetime_input};

/// Streaming iterator for coordinate ranges (no Vec collection)
struct CoordinateRangeIterator {
    current: f64,
    end: f64,
    step: f64,
    ascending: bool,
    finished: bool,
}

impl CoordinateRangeIterator {
    fn new(start: f64, end: f64, step: f64) -> Self {
        let ascending = if step > 0.0 {
            start <= end
        } else {
            start >= end
        };
        Self {
            current: start,
            end,
            step: step.abs(),
            ascending,
            finished: false,
        }
    }
}

impl Iterator for CoordinateRangeIterator {
    type Item = f64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let should_continue = if self.ascending {
            self.current <= self.end
        } else {
            self.current >= self.end
        };

        if !should_continue {
            self.finished = true;
            return None;
        }

        let value = self.current;

        if self.ascending {
            self.current += self.step;
        } else {
            self.current -= self.step;
        }

        Some(value)
    }
}

/// Create a streaming iterator for a coordinate (single value or range)
fn create_coordinate_iterator(coord: &Coordinate) -> Box<dyn Iterator<Item = f64>> {
    match coord {
        Coordinate::Single(val) => Box::new(std::iter::once(*val)),
        Coordinate::Range { start, end, step } => {
            Box::new(CoordinateRangeIterator::new(*start, *end, *step))
        }
    }
}

fn main() {
    let app = build_cli();
    let matches = app.get_matches();

    // Parse and validate the input arguments
    match parse_input(&matches) {
        Ok(mut input) => {
            // Parse the actual data values
            match parse_data_values(&mut input) {
                Ok(()) => {
                    // Determine output format
                    let format = match input.global_options.format.as_deref() {
                        Some(fmt) => match OutputFormat::from_string(fmt) {
                            Ok(f) => f,
                            Err(e) => {
                                eprintln!("✗ {}", e);
                                std::process::exit(1);
                            }
                        },
                        None => OutputFormat::Human,
                    };

                    // Determine command type and calculate accordingly
                    let (cmd_name, cmd_matches) =
                        matches.subcommand().unwrap_or(("position", &matches));

                    match cmd_name {
                        "position" => {
                            let show_inputs = input.global_options.show_inputs.unwrap_or(false);
                            let show_headers = input.global_options.headers.unwrap_or(true);
                            let elevation_angle =
                                parse_position_options(cmd_matches).elevation_angle;

                            let parallel = input.global_options.parallel.unwrap_or(false);
                            match calculate_and_output_positions(
                                &input,
                                &matches,
                                &format,
                                show_inputs,
                                show_headers,
                                elevation_angle,
                                parallel,
                            ) {
                                Ok(_) => {}
                                Err(e) => {
                                    eprintln!("✗ Error calculating positions: {}", e);
                                    std::process::exit(1);
                                }
                            }
                        }
                        "sunrise" => {
                            let show_inputs = input.global_options.show_inputs.unwrap_or(false);
                            let show_headers = input.global_options.headers.unwrap_or(true);
                            let show_twilight = parse_sunrise_options(cmd_matches).twilight;

                            let parallel = input.global_options.parallel.unwrap_or(false);
                            match calculate_and_output_sunrise(
                                &input,
                                &matches,
                                &format,
                                show_inputs,
                                show_headers,
                                show_twilight,
                                parallel,
                            ) {
                                Ok(_) => {}
                                Err(e) => {
                                    eprintln!("✗ Error calculating sunrise: {}", e);
                                    std::process::exit(1);
                                }
                            }
                        }
                        _ => {
                            eprintln!("✗ Unknown command: {}", cmd_name);
                            std::process::exit(1);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("✗ Error parsing data values: {}", e);
                    std::process::exit(1);
                }
            }
        }
        Err(e) => {
            eprintln!("✗ Error parsing input: {}", e);
            std::process::exit(1);
        }
    }
}

#[derive(Debug, Clone)]
enum InputType {
    Standard,       // lat, lon, datetime
    CoordinateFile, // @coords.txt as lat, datetime
    TimeFile,       // lat, lon, @times.txt
    PairedDataFile, // @paired.txt as lat (ignores lon, datetime)
    StdinCoords,    // @- as lat, datetime
    StdinTimes,     // lat, lon, @-
    StdinPaired,    // @- as lat (ignores lon, datetime)
}

#[derive(Debug, Clone)]
struct ParsedInput {
    input_type: InputType,
    latitude: String,
    longitude: Option<String>,
    datetime: Option<String>,
    global_options: GlobalOptions,
    // Parsed data
    parsed_latitude: Option<Coordinate>,
    parsed_longitude: Option<Coordinate>,
    parsed_datetime: Option<DateTimeInput>,
}

#[derive(Debug, Clone)]
struct GlobalOptions {
    #[allow(dead_code)] // Will be used when delta T calculation is implemented
    deltat: Option<String>,
    format: Option<String>,
    headers: Option<bool>,
    parallel: Option<bool>,
    show_inputs: Option<bool>,
    timezone: Option<String>,
}

#[derive(Debug)]
struct PositionOptions {
    algorithm: Option<String>,
    elevation: Option<String>,
    pressure: Option<String>,
    temperature: Option<String>,
    elevation_angle: bool,
    refraction: Option<bool>,
}

#[derive(Debug)]
struct SunriseOptions {
    twilight: bool,
}

fn parse_input(matches: &ArgMatches) -> Result<ParsedInput, String> {
    let latitude = matches
        .get_one::<String>("latitude")
        .ok_or("Latitude is required")?;
    let longitude = matches.get_one::<String>("longitude");
    let datetime = matches.get_one::<String>("dateTime");

    // Determine input type and validate argument combinations
    let input_type = match (
        latitude.as_str(),
        longitude.map(|s| s.as_str()),
        datetime.map(|s| s.as_str()),
    ) {
        // Paired data file: @file as first argument, no other arguments
        (lat, None, None) if lat.starts_with('@') => {
            if lat == "@-" {
                InputType::StdinPaired
            } else {
                InputType::PairedDataFile
            }
        }

        // Coordinate file: @file as first argument, datetime as second argument
        (lat, Some(_dt), None) if lat.starts_with('@') => {
            if lat == "@-" {
                InputType::StdinCoords
            } else {
                InputType::CoordinateFile
            }
        }

        // Standard with time file: lat, lon, @times.txt
        (lat, Some(lon), Some(dt)) if dt.starts_with('@') => {
            if dt == "@-" {
                if lat.starts_with('@') || lon.starts_with('@') {
                    return Err("Only one parameter can use stdin (@-)".to_string());
                }
                InputType::StdinTimes
            } else {
                if lat.starts_with('@') || lon.starts_with('@') {
                    return Err(
                        "Only datetime parameter can be a file in this combination".to_string()
                    );
                }
                InputType::TimeFile
            }
        }

        // Standard: lat, lon, datetime (no @ prefixes)
        (lat, Some(lon), Some(dt)) => {
            if lat.starts_with('@') || lon.starts_with('@') || dt.starts_with('@') {
                return Err("Invalid file parameter combination".to_string());
            }
            InputType::Standard
        }

        // Invalid combinations
        (_lat, Some(_), None) => {
            return Err("When longitude is provided, datetime must also be provided".to_string());
        }
        // These patterns are handled by the Standard pattern above, so they're unreachable
        // Removed to fix clippy warnings
        _ => {
            return Err("Invalid argument combination. Use: <lat> <lon> <datetime> OR @file <datetime> OR @paired-file OR <lat> <lon> @times".to_string());
        }
    };

    // Validate that paired data doesn't have extra arguments
    if matches!(
        input_type,
        InputType::PairedDataFile | InputType::StdinPaired
    ) && (longitude.is_some() || datetime.is_some())
    {
        return Err(
            "When using paired data files, do not specify longitude or datetime parameters"
                .to_string(),
        );
    }

    // Validate that coordinate files have datetime as second parameter
    if matches!(
        input_type,
        InputType::CoordinateFile | InputType::StdinCoords
    ) {
        if longitude.is_none() {
            return Err(
                "When using coordinate files, datetime parameter is required as second argument"
                    .to_string(),
            );
        }
        if datetime.is_some() {
            return Err("When using coordinate files, only two parameters should be provided: @file datetime".to_string());
        }
    }

    // Validate standard and time file inputs
    if matches!(
        input_type,
        InputType::Standard | InputType::TimeFile | InputType::StdinTimes
    ) && (longitude.is_none() || datetime.is_none())
    {
        return Err(
            "Standard input requires latitude, longitude, and datetime parameters".to_string(),
        );
    }

    let global_options = GlobalOptions {
        deltat: if matches.contains_id("deltat") {
            matches
                .get_one::<String>("deltat")
                .cloned()
                .or_else(|| Some("ESTIMATE".to_string()))
        } else {
            None
        },
        format: matches.get_one::<String>("format").cloned(),
        headers: if matches.get_flag("headers") {
            Some(true)
        } else if matches.get_flag("no-headers") {
            Some(false)
        } else {
            Some(true) // Default: headers on for CSV
        },
        parallel: if matches.get_flag("parallel") {
            Some(true)
        } else if matches.get_flag("no-parallel") {
            Some(false)
        } else {
            None
        },
        show_inputs: if matches.get_flag("show-inputs") {
            Some(true)
        } else if matches.get_flag("no-show-inputs") {
            Some(false)
        } else {
            None
        },
        timezone: matches.get_one::<String>("timezone").cloned(),
    };

    // For coordinate files, the datetime is in the longitude position
    let (parsed_longitude, parsed_datetime) = match input_type {
        InputType::CoordinateFile | InputType::StdinCoords => (None, longitude.cloned()),
        _ => (longitude.cloned(), datetime.cloned()),
    };

    Ok(ParsedInput {
        input_type,
        latitude: latitude.clone(),
        longitude: parsed_longitude,
        datetime: parsed_datetime,
        global_options,
        // Will be filled in by parse_data_values
        parsed_latitude: None,
        parsed_longitude: None,
        parsed_datetime: None,
    })
}

fn parse_position_options(matches: &ArgMatches) -> PositionOptions {
    PositionOptions {
        algorithm: matches.get_one::<String>("algorithm").cloned(),
        elevation: matches.get_one::<String>("elevation").cloned(),
        pressure: matches.get_one::<String>("pressure").cloned(),
        temperature: matches.get_one::<String>("temperature").cloned(),
        elevation_angle: matches.get_flag("elevation-angle"),
        refraction: if matches.get_flag("refraction") {
            Some(true)
        } else if matches.get_flag("no-refraction") {
            Some(false)
        } else {
            None
        },
    }
}

fn parse_sunrise_options(matches: &ArgMatches) -> SunriseOptions {
    SunriseOptions {
        twilight: matches.get_flag("twilight"),
    }
}

fn parse_data_values(input: &mut ParsedInput) -> Result<(), ParseError> {
    match input.input_type {
        InputType::Standard | InputType::TimeFile | InputType::StdinTimes => {
            // Parse latitude and longitude
            let lat = parse_coordinate(&input.latitude, "latitude")?;
            let lon = parse_coordinate(
                input.longitude.as_ref().ok_or_else(|| {
                    ParseError::InvalidCoordinate("Missing longitude".to_string())
                })?,
                "longitude",
            )?;
            input.parsed_latitude = Some(lat);
            input.parsed_longitude = Some(lon);

            // Parse datetime
            if let Some(dt_str) = &input.datetime {
                // For time files, dt_str will be the @file reference - skip for now
                if !dt_str.starts_with('@') {
                    let dt = parse_datetime(dt_str, input.global_options.timezone.as_deref())?;
                    input.parsed_datetime = Some(dt);
                }
            }
        }
        InputType::CoordinateFile | InputType::StdinCoords => {
            // For coordinate files, we skip parsing the @file reference for now
            // But we should parse the datetime
            if let Some(dt_str) = &input.datetime {
                let dt = parse_datetime(dt_str, input.global_options.timezone.as_deref())?;
                input.parsed_datetime = Some(dt);
            }
        }
        InputType::PairedDataFile | InputType::StdinPaired => {
            // For paired data files, we skip parsing the @file reference for now
            // All data comes from the file
        }
    }

    // Auto-enable show-inputs based on parsed data
    apply_show_inputs_auto_logic(input);

    Ok(())
}

/// Generic calculation trait for streaming architecture
trait CalculationEngine<T>: Sync {
    fn calculate_single(
        &self,
        datetime: chrono::DateTime<chrono::FixedOffset>,
        lat: f64,
        lon: f64,
    ) -> T;
}

/// Position calculation engine
struct PositionCalculationEngine {
    params: CalculationParameters,
}

impl CalculationEngine<PositionResult> for PositionCalculationEngine {
    fn calculate_single(
        &self,
        datetime: chrono::DateTime<chrono::FixedOffset>,
        lat: f64,
        lon: f64,
    ) -> PositionResult {
        calculate_single_position(datetime, lat, lon, &self.params)
    }
}

/// Sunrise calculation engine
struct SunriseCalculationEngine {
    params: SunriseCalculationParameters,
}

impl CalculationEngine<SunriseResultData> for SunriseCalculationEngine {
    fn calculate_single(
        &self,
        datetime: chrono::DateTime<chrono::FixedOffset>,
        lat: f64,
        lon: f64,
    ) -> SunriseResultData {
        calculate_single_sunrise(datetime, lat, lon, &self.params)
    }
}

/// Unified streaming function for position calculations
fn calculate_and_output_positions(
    input: &ParsedInput,
    matches: &ArgMatches,
    format: &OutputFormat,
    show_inputs: bool,
    show_headers: bool,
    elevation_angle: bool,
    parallel: bool,
) -> Result<(), String> {
    // Create position calculation engine
    let engine = PositionCalculationEngine {
        params: get_calculation_parameters(input, matches)?,
    };

    // Create a streaming iterator using the unified engine
    let position_iter = create_calculation_iterator(input, matches, &engine, parallel)?;

    // Always stream to output - parallel processing handled transparently
    output_position_results(
        position_iter,
        format,
        show_inputs,
        show_headers,
        elevation_angle,
    );
    Ok(())
}

/// Unified streaming function for sunrise calculations
fn calculate_and_output_sunrise(
    input: &ParsedInput,
    matches: &ArgMatches,
    format: &OutputFormat,
    show_inputs: bool,
    show_headers: bool,
    show_twilight: bool,
    parallel: bool,
) -> Result<(), String> {
    // Create sunrise calculation engine
    let engine = SunriseCalculationEngine {
        params: get_sunrise_calculation_parameters(input, matches, show_twilight)?,
    };

    // Create a streaming iterator using the unified engine
    let sunrise_iter = create_calculation_iterator(input, matches, &engine, parallel)?;

    // Always stream to output - parallel processing handled transparently
    output_sunrise_results(
        sunrise_iter,
        format,
        show_inputs,
        show_headers,
        show_twilight,
    );
    Ok(())
}

/// Unified calculation iterator that works for any calculation engine
fn create_calculation_iterator<'a, T>(
    input: &'a ParsedInput,
    matches: &'a ArgMatches,
    engine: &'a dyn CalculationEngine<T>,
    parallel: bool,
) -> Result<Box<dyn Iterator<Item = T> + 'a>, String>
where
    T: Send,
{
    // Extract step parameter from subcommand matches (only for position command)
    let (cmd_name, cmd_matches) = matches.subcommand().unwrap_or(("position", matches));
    let step = if cmd_name == "position" {
        if let Some(step_str) = cmd_matches.get_one::<String>("step") {
            TimeStep::parse(step_str).map_err(|e| format!("Invalid step parameter: {}", e))?
        } else {
            TimeStep::default()
        }
    } else {
        // For sunrise command, use default step (sunrise calculations typically don't use time series)
        TimeStep::default()
    };

    match input.input_type {
        InputType::PairedDataFile | InputType::StdinPaired => Ok(Box::new(
            create_paired_file_calculation_iterator(input, engine)?,
        )),
        InputType::CoordinateFile | InputType::StdinCoords => Ok(Box::new(
            create_coordinate_file_calculation_iterator(input, engine)?,
        )),
        InputType::TimeFile | InputType::StdinTimes => Ok(Box::new(
            create_time_file_calculation_iterator(input, engine)?,
        )),
        InputType::Standard => Ok(Box::new(create_standard_calculation_iterator(
            input, engine, parallel, step,
        )?)),
    }
}

/// Unified paired file calculation iterator
fn create_paired_file_calculation_iterator<'a, T>(
    input: &'a ParsedInput,
    engine: &'a dyn CalculationEngine<T>,
) -> Result<impl Iterator<Item = T> + 'a, String> {
    let file_path = &input.latitude;
    let reader = create_file_reader(file_path)
        .map_err(|e| format!("Failed to open paired data file {}: {}", file_path, e))?;
    let paired_iter = PairedFileIterator::new(reader, input.global_options.timezone.clone());

    // Stream each file line through calculation immediately using the same pattern as other iterators
    Ok(paired_iter.flat_map(move |paired_result| {
        let (lat, lon, datetime_input) = paired_result.expect("Error reading paired data");

        // For sunrise calculations, convert DateTimeInput to a single representative datetime
        // (no time series expansion - one sunrise calculation per day)
        let datetime = match datetime_input {
            DateTimeInput::Single(dt) => dt,
            DateTimeInput::Now => chrono::Utc::now().into(),
            DateTimeInput::PartialYear(year) => {
                // Use start of year for single sunrise calculation
                chrono::FixedOffset::east_opt(0)
                    .unwrap()
                    .with_ymd_and_hms(year, 1, 1, 0, 0, 0)
                    .unwrap()
            }
            DateTimeInput::PartialYearMonth(year, month) => {
                // Use start of month for single sunrise calculation
                chrono::FixedOffset::east_opt(0)
                    .unwrap()
                    .with_ymd_and_hms(year, month, 1, 0, 0, 0)
                    .unwrap()
            }
            DateTimeInput::PartialDate(year, month, day) => {
                // Use start of day for single sunrise calculation
                chrono::FixedOffset::east_opt(0)
                    .unwrap()
                    .with_ymd_and_hms(year, month, day, 0, 0, 0)
                    .unwrap()
            }
        };

        std::iter::once(engine.calculate_single(datetime, lat, lon))
    }))
}

/// Unified coordinate file calculation iterator
fn create_coordinate_file_calculation_iterator<'a, T>(
    input: &'a ParsedInput,
    engine: &'a dyn CalculationEngine<T>,
) -> Result<impl Iterator<Item = T> + 'a, String> {
    let file_path = &input.latitude;
    let datetime_str = input
        .datetime
        .as_ref()
        .ok_or("Datetime parameter required for coordinate files")?;
    let datetime_input = parse_datetime(datetime_str, input.global_options.timezone.as_deref())
        .map_err(|e| format!("Failed to parse datetime: {}", e))?;

    let reader = create_file_reader(file_path)
        .map_err(|e| format!("Failed to open coordinate file {}: {}", file_path, e))?;
    let coord_iter = CoordinateFileIterator::new(reader);

    // For sunrise calculations, we need single datetime per day, not time series
    let datetime = match datetime_input {
        DateTimeInput::Single(dt) => dt,
        DateTimeInput::Now => chrono::Utc::now().into(),
        DateTimeInput::PartialDate(year, month, day) => {
            // Use start of day for single sunrise calculation
            chrono::FixedOffset::east_opt(0)
                .unwrap()
                .with_ymd_and_hms(year, month, day, 0, 0, 0)
                .unwrap()
        }
        DateTimeInput::PartialYear(year) => chrono::FixedOffset::east_opt(0)
            .unwrap()
            .with_ymd_and_hms(year, 1, 1, 0, 0, 0)
            .unwrap(),
        DateTimeInput::PartialYearMonth(year, month) => chrono::FixedOffset::east_opt(0)
            .unwrap()
            .with_ymd_and_hms(year, month, 1, 0, 0, 0)
            .unwrap(),
    };

    // Stream each coordinate through calculation immediately (one calculation per coordinate)
    Ok(coord_iter.map(move |coord_result| {
        let (lat, lon) = coord_result.expect("Error reading coordinates");
        engine.calculate_single(datetime, lat, lon)
    }))
}

/// Unified time file calculation iterator
fn create_time_file_calculation_iterator<'a, T>(
    input: &'a ParsedInput,
    engine: &'a dyn CalculationEngine<T>,
) -> Result<impl Iterator<Item = T> + 'a, String> {
    let file_path = input.datetime.as_ref().ok_or("Time file path not found")?;
    let lat = parse_coordinate(&input.latitude, "latitude")
        .map_err(|e| format!("Failed to parse latitude: {}", e))?;
    let lon = parse_coordinate(
        input
            .longitude
            .as_ref()
            .ok_or("Longitude required for time files")?,
        "longitude",
    )
    .map_err(|e| format!("Failed to parse longitude: {}", e))?;

    let reader = create_file_reader(file_path)
        .map_err(|e| format!("Failed to open time file {}: {}", file_path, e))?;
    let time_iter = TimeFileIterator::new(reader, input.global_options.timezone.clone());

    // Create coordinate combinations - small collections for coordinate ranges are acceptable
    let lat_iter = create_coordinate_iterator(&lat);
    let coords: Vec<_> = lat_iter
        .flat_map(|lat_val| {
            let lon_iter = create_coordinate_iterator(&lon);
            lon_iter.map(move |lon_val| (lat_val, lon_val))
        })
        .collect(); // Small collection for coordinate combinations - acceptable for coordinate ranges

    // Stream each time through calculation immediately
    Ok(time_iter.flat_map(move |time_result| {
        let datetime_input = time_result.expect("Error reading time");
        let datetime = match datetime_input {
            DateTimeInput::Single(dt) => dt,
            _ => panic!("Expected specific datetime from time file"),
        };
        let coords = coords.clone(); // Clone the coordinate vector for each time
        coords
            .into_iter()
            .map(move |(lat_val, lon_val)| engine.calculate_single(datetime, lat_val, lon_val))
    }))
}

/// Unified standard calculation iterator
fn create_standard_calculation_iterator<'a, T>(
    input: &'a ParsedInput,
    engine: &'a dyn CalculationEngine<T>,
    parallel: bool,
    step: TimeStep,
) -> Result<impl Iterator<Item = T> + 'a, String>
where
    T: Send,
{
    if let (Some(lat), Some(lon), Some(dt)) = (
        &input.parsed_latitude,
        &input.parsed_longitude,
        &input.parsed_datetime,
    ) {
        // Create streaming coordinate iterators (no Vec collection!)
        let lat_iter = create_coordinate_iterator(lat);
        let lon_iter = create_coordinate_iterator(lon);

        // Parse timezone override once for reuse
        let timezone_override = if let Some(tz_str) = &input.global_options.timezone {
            if let Ok(offset) = crate::parsing::parse_timezone_offset(tz_str) {
                Some(offset)
            } else {
                // For named timezones like UTC, use UTC offset
                Some(chrono::FixedOffset::east_opt(0).unwrap())
            }
        } else {
            None
        };

        // Determine step and datetime expansion based on calculation type
        let (_step, datetime_iter): (
            TimeStep,
            Box<dyn Iterator<Item = chrono::DateTime<chrono::FixedOffset>>>,
        ) = if std::any::type_name::<T>().contains("SunriseResultData") {
            // Sunrise calculations: single datetime for specific dates, daily series for partial dates
            match dt {
                DateTimeInput::Single(dt) => (step, Box::new(std::iter::once(*dt))),
                DateTimeInput::Now => (step, Box::new(std::iter::once(chrono::Utc::now().into()))),
                DateTimeInput::PartialDate(year, month, day) => {
                    // For specific dates in sunrise, create single datetime at midnight
                    let datetime = if let Some(offset) = timezone_override {
                        offset
                            .with_ymd_and_hms(*year, *month, *day, 0, 0, 0)
                            .unwrap()
                    } else {
                        let system_tz = crate::timezone_utils::get_system_timezone();
                        let naive_dt = chrono::NaiveDate::from_ymd_opt(*year, *month, *day)
                            .unwrap()
                            .and_hms_opt(0, 0, 0)
                            .unwrap();
                        crate::timezone_utils::naive_to_specific_timezone(naive_dt, &system_tz)
                            .map_err(|e| format!("Failed to convert to system timezone: {}", e))?
                    };
                    (step, Box::new(std::iter::once(datetime)))
                }
                DateTimeInput::PartialYear(_) | DateTimeInput::PartialYearMonth(_, _) => {
                    // For partial dates in sunrise, generate daily time series
                    let daily_step = TimeStep {
                        duration: chrono::Duration::try_days(1).unwrap(),
                    };
                    let expanded = expand_datetime_input(dt, &daily_step, timezone_override)
                        .map_err(|e| format!("Failed to expand datetime: {}", e))?;
                    (daily_step, Box::new(expanded))
                }
            }
        } else {
            // Position calculations: always expand to time series with user step
            let expanded = expand_datetime_input(dt, &step, timezone_override)
                .map_err(|e| format!("Failed to expand datetime: {}", e))?;
            (step, Box::new(expanded))
        };

        // Create streaming Cartesian product iterator
        Ok(create_cartesian_calculation_iterator(
            datetime_iter,
            lat_iter,
            lon_iter,
            engine,
            parallel,
        ))
    } else {
        Err("Missing required coordinate or datetime data".to_string())
    }
}

#[derive(Clone)]
struct CalculationParameters {
    algorithm: String,
    elevation: f64,
    delta_t: f64,
    pressure: f64,
    temperature: f64,
    apply_refraction: bool,
}

#[derive(Clone)]
struct SunriseCalculationParameters {
    #[allow(dead_code)]
    algorithm: String,
    elevation: f64,
    delta_t: f64,
    #[allow(dead_code)]
    pressure: f64,
    #[allow(dead_code)]
    temperature: f64,
    #[allow(dead_code)]
    apply_refraction: bool,
    show_twilight: bool,
}

fn get_calculation_parameters(
    input: &ParsedInput,
    matches: &ArgMatches,
) -> Result<CalculationParameters, String> {
    let (_, cmd_matches) = matches.subcommand().unwrap_or(("position", matches));
    let pos_options = parse_position_options(cmd_matches);

    Ok(CalculationParameters {
        algorithm: pos_options
            .algorithm
            .as_deref()
            .unwrap_or("SPA")
            .to_string(),
        elevation: pos_options
            .elevation
            .as_deref()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0),
        delta_t: input
            .global_options
            .deltat
            .as_deref()
            .and_then(|s| {
                if s == "ESTIMATE" {
                    Some(69.0)
                } else {
                    s.parse::<f64>().ok()
                }
            })
            .unwrap_or(0.0),
        pressure: pos_options
            .pressure
            .as_deref()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(1013.0),
        temperature: pos_options
            .temperature
            .as_deref()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(15.0),
        apply_refraction: pos_options.refraction.unwrap_or(true),
    })
}

fn get_sunrise_calculation_parameters(
    input: &ParsedInput,
    matches: &ArgMatches,
    show_twilight: bool,
) -> Result<SunriseCalculationParameters, String> {
    let (_, cmd_matches) = matches.subcommand().unwrap_or(("sunrise", matches));
    let _sunrise_options = parse_sunrise_options(cmd_matches);

    Ok(SunriseCalculationParameters {
        algorithm: "SPA".to_string(), // Sunrise always uses SPA
        elevation: 0.0,               // Sunrise uses sea level by default
        delta_t: input
            .global_options
            .deltat
            .as_deref()
            .and_then(|s| {
                if s == "ESTIMATE" {
                    Some(69.0)
                } else {
                    s.parse::<f64>().ok()
                }
            })
            .unwrap_or(0.0),
        pressure: 1013.0,       // Default atmospheric pressure
        temperature: 15.0,      // Default temperature
        apply_refraction: true, // Sunrise typically includes refraction
        show_twilight,
    })
}

/// Calculate a single sunrise result (the core calculation)
fn calculate_single_sunrise(
    datetime: chrono::DateTime<chrono::FixedOffset>,
    lat: f64,
    lon: f64,
    params: &SunriseCalculationParameters,
) -> SunriseResultData {
    use solar_positioning::types::Horizon;

    let delta_t = params.delta_t;
    let _elevation = params.elevation;

    // Calculate sunrise for standard horizon
    let sunrise_result = solar_positioning::spa::sunrise_sunset_for_horizon(
        datetime,
        lat,
        lon,
        delta_t,
        Horizon::SunriseSunset,
    )
    .unwrap();

    // Calculate twilight if requested
    let twilight_results = if params.show_twilight {
        let civil = solar_positioning::spa::sunrise_sunset_for_horizon(
            datetime,
            lat,
            lon,
            delta_t,
            Horizon::CivilTwilight,
        )
        .unwrap();
        let nautical = solar_positioning::spa::sunrise_sunset_for_horizon(
            datetime,
            lat,
            lon,
            delta_t,
            Horizon::NauticalTwilight,
        )
        .unwrap();
        let astronomical = solar_positioning::spa::sunrise_sunset_for_horizon(
            datetime,
            lat,
            lon,
            delta_t,
            Horizon::AstronomicalTwilight,
        )
        .unwrap();

        Some(TwilightResults {
            civil,
            nautical,
            astronomical,
        })
    } else {
        None
    };

    SunriseResultData {
        datetime,
        latitude: lat,
        longitude: lon,
        delta_t,
        sunrise_result,
        twilight_results,
    }
}

/// Calculate a single solar position (the core calculation)
fn calculate_single_position(
    datetime: chrono::DateTime<chrono::FixedOffset>,
    lat: f64,
    lon: f64,
    params: &CalculationParameters,
) -> PositionResult {
    let position = match params.algorithm.to_uppercase().as_str() {
        "SPA" => {
            if params.apply_refraction {
                spa::solar_position(
                    datetime,
                    lat,
                    lon,
                    params.elevation,
                    params.delta_t,
                    params.pressure,
                    params.temperature,
                )
            } else {
                spa::solar_position_no_refraction(
                    datetime,
                    lat,
                    lon,
                    params.elevation,
                    params.delta_t,
                )
            }
        }
        "GRENA3" => {
            if params.apply_refraction {
                grena3::solar_position_with_refraction(
                    datetime,
                    lat,
                    lon,
                    params.delta_t,
                    Some(params.pressure),
                    Some(params.temperature),
                )
            } else {
                grena3::solar_position(datetime, lat, lon, params.delta_t)
            }
        }
        _ => panic!("Unknown algorithm: {}", params.algorithm),
    };

    let pos = position.expect("Solar calculation should not fail with validated inputs");

    PositionResult::new(
        datetime,
        pos,
        lat,
        lon,
        EnvironmentalParams {
            elevation: params.elevation,
            pressure: params.pressure,
            temperature: params.temperature,
        },
        params.delta_t,
    )
}

/// Create unified Cartesian product iterator for any calculation engine
fn create_cartesian_calculation_iterator<T>(
    datetime_iter: impl Iterator<Item = chrono::DateTime<chrono::FixedOffset>>,
    lat_iter: Box<dyn Iterator<Item = f64>>,
    lon_iter: Box<dyn Iterator<Item = f64>>,
    engine: &dyn CalculationEngine<T>,
    parallel: bool,
) -> Box<dyn Iterator<Item = T> + '_>
where
    T: Send,
{
    // For coordinate ranges and time series, collect small datasets for Cartesian products
    // This is acceptable because coordinate ranges (e.g., 52:53:0.1) are typically small
    let datetimes: Vec<_> = datetime_iter.collect();
    let latitudes: Vec<_> = lat_iter.collect();
    let longitudes: Vec<_> = lon_iter.collect();

    // Create all combinations and return as an owned iterator
    let mut combinations = Vec::new();
    for dt in datetimes {
        for &lat in &latitudes {
            for &lon in &longitudes {
                combinations.push((dt, lat, lon));
            }
        }
    }

    // Stream the Cartesian product with optional parallel processing
    if parallel {
        // Use parallel iterator for coordinate ranges
        let results: Vec<T> = combinations
            .into_par_iter()
            .map(|(dt, lat, lon)| engine.calculate_single(dt, lat, lon))
            .collect();
        Box::new(results.into_iter())
    } else {
        // Pure streaming - calculations happen lazily as output is consumed
        Box::new(
            combinations
                .into_iter()
                .map(move |(dt, lat, lon)| engine.calculate_single(dt, lat, lon)),
        )
    }
}

fn apply_show_inputs_auto_logic(input: &mut ParsedInput) {
    // If user explicitly set --no-show-inputs, respect that
    if let Some(false) = input.global_options.show_inputs {
        return;
    }

    // If user explicitly set --show-inputs, keep it
    if let Some(true) = input.global_options.show_inputs {
        return;
    }

    // Auto-enable show-inputs for multiple value scenarios
    let should_auto_enable =
        // Any file input
        matches!(input.input_type,
            InputType::CoordinateFile | InputType::StdinCoords |
            InputType::TimeFile | InputType::StdinTimes |
            InputType::PairedDataFile | InputType::StdinPaired) ||
        // Coordinate ranges
        (input.parsed_latitude.is_some() && matches!(input.parsed_latitude, Some(Coordinate::Range { .. }))) ||
        (input.parsed_longitude.is_some() && matches!(input.parsed_longitude, Some(Coordinate::Range { .. }))) ||
        // Time series (partial dates)
        (input.parsed_datetime.is_some() && matches!(input.parsed_datetime,
            Some(DateTimeInput::PartialYear(_)) | Some(DateTimeInput::PartialYearMonth(_, _)) | Some(DateTimeInput::PartialDate(_, _, _))));

    if should_auto_enable {
        input.global_options.show_inputs = Some(true);
    }
}

fn build_cli() -> Command {
    Command::new("sunce")
        .version("0.1.0")
        .about("Calculates topocentric solar coordinates or sunrise/sunset times.")
        .long_about(Some(concat!(
            "Examples:\n",
            "  sunce 52.0 13.4 2024-01-01 position\n",
            "  sunce 52:53:0.1 13:14:0.1 2024 position --format=csv\n",
            "  sunce @coords.txt @times.txt position\n",
            "  sunce @data.txt position  # paired lat,lng,datetime data\n",
            "  echo '52.0 13.4 2024-01-01T12:00:00' | sunce @- now position"
        )))
        .arg(Arg::new("latitude")
            .help(concat!(
                "Latitude: decimal degrees, range, or file\n",
                "  52.5        single coordinate\n",
                "  52:53:0.1   range from 52° to 53° in 0.1° steps\n",
                "  @coords.txt file with coordinates (or @- for stdin)"
            ))
            .required(true)
            .allow_hyphen_values(true)
            .index(1))
        .arg(Arg::new("longitude")
            .help(concat!(
                "Longitude: decimal degrees, range, or file\n",
                "  13.4        single coordinate\n",
                "  13:14:0.1   range from 13° to 14° in 0.1° steps\n",
                "  @coords.txt file with coordinates (or @- for stdin)"
            ))
            .required(false)
            .allow_hyphen_values(true)
            .index(2))
        .arg(Arg::new("dateTime")
            .help(concat!(
                "Date/time: ISO format, partial dates, or file\n",
                "  2024-01-01           specific date (midnight)\n",
                "  2024-01-01T12:00:00  specific date and time\n",
                "  2024                 entire year (with --step)\n",
                "  now                  current date and time\n",
                "  @times.txt           file with times (or @- for stdin)\n",
                "                       (files require explicit dates like 2024-01-15)"
            ))
            .required(false)
            .index(3))

        // Global options
        .arg(Arg::new("deltat")
            .long("deltat")
            .help("Delta T in seconds; an estimate is used if this option is given without a value.")
            .num_args(0..=1)
            .require_equals(true)
            .value_name("deltaT"))
        .arg(Arg::new("format")
            .long("format")
            .help("Output format, one of HUMAN, CSV, JSON.")
            .require_equals(true)
            .value_name("format"))
        .arg(Arg::new("headers")
            .long("headers")
            .action(ArgAction::SetTrue)
            .help("Show headers in output (CSV only). Default: true"))
        .arg(Arg::new("no-headers")
            .long("no-headers")
            .action(ArgAction::SetTrue)
            .help("Don't show headers in output (CSV only)"))
        .arg(Arg::new("parallel")
            .long("parallel")
            .action(ArgAction::SetTrue)
            .help("Enable parallel processing for better performance on multi-core systems. May cause memory pressure with large datasets. Default: false."))
        .arg(Arg::new("no-parallel")
            .long("no-parallel")
            .action(ArgAction::SetTrue)
            .help("Disable parallel processing"))
        .arg(Arg::new("show-inputs")
            .long("show-inputs")
            .action(ArgAction::SetTrue)
            .help("Show all inputs in output. Automatically enabled for coordinate ranges unless --no-show-inputs is used."))
        .arg(Arg::new("no-show-inputs")
            .long("no-show-inputs")
            .action(ArgAction::SetTrue)
            .help("Don't show inputs in output"))
        .arg(Arg::new("timezone")
            .long("timezone")
            .help("Timezone as offset (e.g. +01:00) and/or zone id (e.g. America/Los_Angeles). Overrides any timezone info found in dateTime.")
            .require_equals(true)
            .value_name("timezone"))

        // Commands
        .subcommand(
            Command::new("position")
                .about("Calculates topocentric solar coordinates.")
                .arg(Arg::new("elevation-angle")
                    .long("elevation-angle")
                    .action(ArgAction::SetTrue)
                    .help("Output elevation angle instead of zenith angle."))
                .arg(Arg::new("refraction")
                    .long("refraction")
                    .action(ArgAction::SetTrue)
                    .help("Apply refraction correction. Default: true"))
                .arg(Arg::new("no-refraction")
                    .long("no-refraction")
                    .action(ArgAction::SetTrue)
                    .help("Don't apply refraction correction"))
                .arg(Arg::new("algorithm")
                    .short('a')
                    .long("algorithm")
                    .help("One of SPA, GRENA3. Default: spa.")
                    .require_equals(true)
                    .value_name("algorithm"))
                .arg(Arg::new("elevation")
                    .long("elevation")
                    .help("Elevation above sea level, in meters. Default: 0.")
                    .require_equals(true)
                    .value_name("elevation"))
                .arg(Arg::new("pressure")
                    .long("pressure")
                    .help("Avg. air pressure in millibars/hectopascals. Used for refraction correction. Default: 1013.")
                    .require_equals(true)
                    .value_name("pressure"))
                .arg(Arg::new("step")
                    .long("step")
                    .help("Step interval for time series. Examples: 30s, 15m, 2h. Default: 1h.")
                    .require_equals(true)
                    .value_name("step"))
                .arg(Arg::new("temperature")
                    .long("temperature")
                    .help("Avg. air temperature in degrees Celsius. Used for refraction correction. Default: 15.")
                    .require_equals(true)
                    .value_name("temperature"))
        )
        .subcommand(
            Command::new("sunrise")
                .about("Calculates sunrise, transit, sunset and (optionally) twilight times.")
                .arg(Arg::new("twilight")
                    .long("twilight")
                    .action(ArgAction::SetTrue)
                    .help("Show twilight times."))
        )
}

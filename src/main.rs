use chrono::TimeZone;
use clap::{Arg, ArgAction, ArgMatches, Command};
use solar_positioning::{grena3, spa, types::Horizon};

mod parsing;
use parsing::{
    Coordinate, DateTimeInput, ParseError, apply_timezone_to_naive, parse_coordinate,
    parse_datetime,
};

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
use timezone_utils::naive_to_system_local;

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

                            match calculate_and_output_positions(
                                &input,
                                &matches,
                                &format,
                                show_inputs,
                                show_headers,
                                elevation_angle,
                            ) {
                                Ok(_) => {}
                                Err(e) => {
                                    eprintln!("✗ Error calculating positions: {}", e);
                                    std::process::exit(1);
                                }
                            }
                        }
                        "sunrise" => match calculate_sunrise(&input, &matches) {
                            Ok(results) => {
                                let show_inputs = input.global_options.show_inputs.unwrap_or(false);
                                let show_headers = input.global_options.headers.unwrap_or(true);
                                let show_twilight = parse_sunrise_options(cmd_matches).twilight;

                                output_sunrise_results(
                                    results.into_iter(),
                                    &format,
                                    show_inputs,
                                    show_headers,
                                    show_twilight,
                                );
                            }
                            Err(e) => {
                                eprintln!("✗ Error calculating sunrise: {}", e);
                                std::process::exit(1);
                            }
                        },
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
    #[allow(dead_code)] // Will be used when parallel processing is implemented
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
    step: Option<String>,
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
        step: matches.get_one::<String>("step").cloned(),
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

/// Unified streaming function for all position calculations
fn calculate_and_output_positions(
    input: &ParsedInput,
    matches: &ArgMatches,
    format: &OutputFormat,
    show_inputs: bool,
    show_headers: bool,
    elevation_angle: bool,
) -> Result<(), String> {
    // Create a streaming iterator for all input types
    let position_iter = create_position_iterator(input, matches)?;

    // Stream directly to output - no Vec collection
    output_position_results(
        position_iter,
        format,
        show_inputs,
        show_headers,
        elevation_angle,
    );
    Ok(())
}

fn create_position_iterator<'a>(
    input: &'a ParsedInput,
    matches: &'a ArgMatches,
) -> Result<Box<dyn Iterator<Item = PositionResult> + 'a>, String> {
    match input.input_type {
        InputType::PairedDataFile | InputType::StdinPaired => Ok(Box::new(
            create_paired_file_position_iterator(input, matches)?,
        )),
        InputType::CoordinateFile | InputType::StdinCoords => Ok(Box::new(
            create_coordinate_file_position_iterator(input, matches)?,
        )),
        InputType::TimeFile | InputType::StdinTimes => Ok(Box::new(
            create_time_file_position_iterator(input, matches)?,
        )),
        InputType::Standard => Ok(Box::new(create_standard_position_iterator(input, matches)?)),
    }
}

fn create_paired_file_position_iterator(
    input: &ParsedInput,
    matches: &ArgMatches,
) -> Result<impl Iterator<Item = PositionResult>, String> {
    let file_path = &input.latitude;
    let reader = create_file_reader(file_path)
        .map_err(|e| format!("Failed to open paired data file {}: {}", file_path, e))?;
    let paired_iter = PairedFileIterator::new(reader, input.global_options.timezone.clone());

    // Get calculation parameters once
    let calc_params = get_calculation_parameters(input, matches)?;

    // Stream each file line through calculation immediately
    Ok(paired_iter.map(move |paired_result| {
        let (lat, lon, datetime_input) = paired_result.expect("Error reading paired data");
        let datetime = match datetime_input {
            DateTimeInput::Single(dt) => dt,
            _ => panic!("Expected specific datetime from paired file"),
        };

        calculate_single_position(datetime, lat, lon, &calc_params)
    }))
}

fn create_coordinate_file_position_iterator(
    input: &ParsedInput,
    matches: &ArgMatches,
) -> Result<impl Iterator<Item = PositionResult>, String> {
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

    // Get calculation parameters once
    let calc_params = get_calculation_parameters(input, matches)?;

    // Get datetime iterator
    let step = TimeStep::default();
    let datetime_iter = expand_datetime_input(&datetime_input, &step, None)
        .map_err(|e| format!("Failed to expand datetime: {}", e))?;
    let datetimes: Vec<_> = datetime_iter.collect(); // Small collection for time series - acceptable for time ranges

    // Stream each coordinate through calculation immediately
    Ok(coord_iter.flat_map(move |coord_result| {
        let (lat, lon) = coord_result.expect("Error reading coordinates");
        let calc_params = calc_params.clone();
        let datetimes = datetimes.clone(); // Clone the time vector for each coordinate
        datetimes
            .into_iter()
            .map(move |datetime| calculate_single_position(datetime, lat, lon, &calc_params))
    }))
}

fn create_time_file_position_iterator(
    input: &ParsedInput,
    matches: &ArgMatches,
) -> Result<impl Iterator<Item = PositionResult>, String> {
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

    // Get calculation parameters once
    let calc_params = get_calculation_parameters(input, matches)?;

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
        let calc_params = calc_params.clone();
        let coords = coords.clone(); // Clone the coordinate vector for each time
        coords.into_iter().map(move |(lat_val, lon_val)| {
            calculate_single_position(datetime, lat_val, lon_val, &calc_params)
        })
    }))
}

fn create_standard_position_iterator(
    input: &ParsedInput,
    matches: &ArgMatches,
) -> Result<impl Iterator<Item = PositionResult>, String> {
    if let (Some(lat), Some(lon), Some(dt)) = (
        &input.parsed_latitude,
        &input.parsed_longitude,
        &input.parsed_datetime,
    ) {
        let (cmd_name, cmd_matches) = matches.subcommand().unwrap_or(("position", matches));
        if cmd_name != "position" {
            return Err("Position calculation not available for sunrise command".to_string());
        }

        // Get calculation parameters
        let calc_params = get_calculation_parameters(input, matches)?;

        // Parse step option
        let pos_options = parse_position_options(cmd_matches);
        let step = pos_options
            .step
            .as_deref()
            .map(TimeStep::parse)
            .transpose()
            .map_err(|e| format!("Invalid step: {}", e))?
            .unwrap_or_else(TimeStep::default);

        // Create streaming coordinate iterators (no Vec collection!)
        let lat_iter = create_coordinate_iterator(lat);
        let lon_iter = create_coordinate_iterator(lon);

        let datetime_iter = expand_datetime_input(dt, &step, None)
            .map_err(|e| format!("Failed to expand datetime: {}", e))?;

        // Create streaming Cartesian product iterator
        Ok(create_cartesian_position_iterator(
            datetime_iter,
            lat_iter,
            lon_iter,
            calc_params,
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

/// Create Cartesian product iterator for all coordinate/time combinations
fn create_cartesian_position_iterator(
    datetime_iter: impl Iterator<Item = chrono::DateTime<chrono::FixedOffset>>,
    lat_iter: Box<dyn Iterator<Item = f64>>,
    lon_iter: Box<dyn Iterator<Item = f64>>,
    params: CalculationParameters,
) -> impl Iterator<Item = PositionResult> {
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

    combinations
        .into_iter()
        .map(move |(dt, lat, lon)| calculate_single_position(dt, lat, lon, &params))
}

fn calculate_sunrise(
    input: &ParsedInput,
    matches: &ArgMatches,
) -> Result<Vec<SunriseResultData>, String> {
    // Handle file inputs with streaming
    match input.input_type {
        InputType::CoordinateFile | InputType::StdinCoords => {
            return calculate_sunrise_from_coordinate_file(input, matches);
        }
        InputType::TimeFile | InputType::StdinTimes => {
            return calculate_sunrise_from_time_file(input, matches);
        }
        InputType::PairedDataFile | InputType::StdinPaired => {
            return calculate_sunrise_from_paired_file(input, matches);
        }
        InputType::Standard => {
            // Continue with standard processing below
        }
    }

    if let (Some(lat), Some(lon), Some(dt)) = (
        &input.parsed_latitude,
        &input.parsed_longitude,
        &input.parsed_datetime,
    ) {
        // Extract coordinate values (same pattern as position command)
        let lat_values = match lat {
            Coordinate::Single(val) => vec![*val],
            Coordinate::Range { start, end, step } => {
                let mut values = Vec::new();
                let mut current = *start;
                while current <= *end {
                    values.push(current);
                    current += step;
                }
                values
            }
        };

        let lon_values = match lon {
            Coordinate::Single(val) => vec![*val],
            Coordinate::Range { start, end, step } => {
                let mut values = Vec::new();
                let mut current = *start;
                while current <= *end {
                    values.push(current);
                    current += step;
                }
                values
            }
        };

        // For sunrise, we use daily step by default for partial dates (year, year-month)
        let step = TimeStep {
            duration: chrono::Duration::try_days(1).unwrap(),
        };

        let mut results = Vec::new();

        // Generate all combinations
        for lat_val in &lat_values {
            for lon_val in &lon_values {
                match dt {
                    DateTimeInput::PartialDate(year, month, day) => {
                        // For single dates, only calculate sunrise once at 00:00:00
                        let target_date = chrono::NaiveDate::from_ymd_opt(*year, *month, *day)
                            .ok_or_else(|| {
                                format!("Invalid date: {}-{:02}-{:02}", year, month, day)
                            })?;
                        let start_time = chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap();
                        let start_naive = target_date.and_time(start_time);

                        let datetime = if let Some(tz_str) = &input.global_options.timezone {
                            apply_timezone_to_naive(start_naive, tz_str)
                                .map_err(|e| format!("Timezone error: {}", e))?
                        } else {
                            naive_to_system_local(start_naive)
                                .map_err(|e| format!("Timezone error: {}", e))?
                        };

                        let sunrise_result = calculate_sunrise_for_single_datetime(
                            *lat_val, *lon_val, datetime, input, matches,
                        )?;
                        results.extend(sunrise_result);
                    }
                    _ => {
                        // For other datetime types (partial year, partial month, single datetime), use time series
                        let datetime_iter = expand_datetime_input(dt, &step, None)
                            .map_err(|e| format!("Failed to expand datetime: {}", e))?;

                        for datetime in datetime_iter {
                            // Calculate sunrise for this coordinate and datetime
                            let sunrise_result = calculate_sunrise_for_single_datetime(
                                *lat_val, *lon_val, datetime, input, matches,
                            )?;
                            results.extend(sunrise_result);
                        }
                    }
                }
            }
        }

        Ok(results)
    } else {
        Err("Missing required coordinate or datetime data".to_string())
    }
}

fn calculate_sunrise_for_single_datetime(
    lat_val: f64,
    lon_val: f64,
    datetime: chrono::DateTime<chrono::FixedOffset>,
    input: &ParsedInput,
    matches: &ArgMatches,
) -> Result<Vec<SunriseResultData>, String> {
    // For sunrise calculations, we need datetime that represents the start of the local date
    // We take the local date and create a datetime at 00:00:00 in the original timezone
    let local_date = datetime.date_naive();
    let start_of_day = local_date.and_hms_opt(0, 0, 0).unwrap();
    let start_datetime = datetime
        .timezone()
        .from_local_datetime(&start_of_day)
        .single()
        .unwrap();

    // Get delta_t parameter
    let delta_t = input
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
        .unwrap_or(0.0); // Default to 0.0 like solarpos

    // Get command options
    let (_, cmd_matches) = matches.subcommand().unwrap_or(("sunrise", matches));
    let sunrise_options = parse_sunrise_options(cmd_matches);

    // Calculate main sunrise/sunset using standard horizon
    let sunrise_result = spa::sunrise_sunset_for_horizon(
        start_datetime,
        lat_val,
        lon_val,
        delta_t,
        Horizon::SunriseSunset,
    );

    match sunrise_result {
        Ok(sunrise_data) => {
            // The sunrise_result is already in the correct timezone (start_datetime's timezone)
            let sunrise_result_tz = sunrise_data;

            // Calculate twilight if requested
            let twilight_results = if sunrise_options.twilight {
                Some(calculate_twilight_times(
                    start_datetime,
                    lat_val,
                    lon_val,
                    delta_t,
                )?)
            } else {
                None
            };

            let result = SunriseResultData::new(
                datetime,
                lat_val,
                lon_val,
                delta_t,
                sunrise_result_tz,
                twilight_results,
            );

            Ok(vec![result])
        }
        Err(e) => Err(format!("Sunrise calculation failed: {}", e)),
    }
}

fn calculate_twilight_times(
    start_datetime: chrono::DateTime<chrono::FixedOffset>,
    lat_val: f64,
    lon_val: f64,
    delta_t: f64,
) -> Result<TwilightResults, String> {
    // Calculate civil twilight
    let civil_result = spa::sunrise_sunset_for_horizon(
        start_datetime,
        lat_val,
        lon_val,
        delta_t,
        Horizon::CivilTwilight,
    )
    .map_err(|e| format!("Civil twilight calculation failed: {}", e))?;

    // Calculate nautical twilight
    let nautical_result = spa::sunrise_sunset_for_horizon(
        start_datetime,
        lat_val,
        lon_val,
        delta_t,
        Horizon::NauticalTwilight,
    )
    .map_err(|e| format!("Nautical twilight calculation failed: {}", e))?;

    // Calculate astronomical twilight
    let astronomical_result = spa::sunrise_sunset_for_horizon(
        start_datetime,
        lat_val,
        lon_val,
        delta_t,
        Horizon::AstronomicalTwilight,
    )
    .map_err(|e| format!("Astronomical twilight calculation failed: {}", e))?;

    Ok(TwilightResults {
        civil: civil_result,
        nautical: nautical_result,
        astronomical: astronomical_result,
    })
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

fn calculate_sunrise_from_coordinate_file(
    input: &ParsedInput,
    matches: &ArgMatches,
) -> Result<Vec<SunriseResultData>, String> {
    let file_path = &input.latitude;
    let datetime_str = input
        .datetime
        .as_ref()
        .ok_or("Datetime parameter required for coordinate files")?;
    let datetime_input = parse_datetime(datetime_str, input.global_options.timezone.as_deref())
        .map_err(|e| format!("Failed to parse datetime: {}", e))?;

    process_coordinate_file(
        file_path,
        &datetime_input,
        input,
        matches,
        calculate_sunrise,
    )
}

fn calculate_sunrise_from_time_file(
    input: &ParsedInput,
    matches: &ArgMatches,
) -> Result<Vec<SunriseResultData>, String> {
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

    process_time_file(file_path, &lat, &lon, input, matches, calculate_sunrise)
}

fn calculate_sunrise_from_paired_file(
    input: &ParsedInput,
    matches: &ArgMatches,
) -> Result<Vec<SunriseResultData>, String> {
    let file_path = &input.latitude;
    process_paired_file(file_path, input, matches, calculate_sunrise)
}

// Generic file processing functions to eliminate duplication

fn process_coordinate_file<T>(
    file_path: &str,
    datetime_input: &DateTimeInput,
    input: &ParsedInput,
    matches: &ArgMatches,
    calculator: fn(&ParsedInput, &ArgMatches) -> Result<Vec<T>, String>,
) -> Result<Vec<T>, String> {
    let reader = create_file_reader(file_path)
        .map_err(|e| format!("Failed to open coordinate file {}: {}", file_path, e))?;
    let coord_iter = CoordinateFileIterator::new(reader);

    let mut results = Vec::new();
    for coord_result in coord_iter {
        let (lat, lon) = coord_result.map_err(|e| format!("Error reading coordinates: {}", e))?;
        let calculation_result =
            calculate_with_single_coords(lat, lon, datetime_input, input, matches, calculator)?;
        results.extend(calculation_result);
    }
    Ok(results)
}

fn process_time_file<T>(
    file_path: &str,
    lat: &Coordinate,
    lon: &Coordinate,
    input: &ParsedInput,
    matches: &ArgMatches,
    calculator: fn(&ParsedInput, &ArgMatches) -> Result<Vec<T>, String>,
) -> Result<Vec<T>, String> {
    let reader = create_file_reader(file_path)
        .map_err(|e| format!("Failed to open time file {}: {}", file_path, e))?;
    let time_iter = TimeFileIterator::new(reader, input.global_options.timezone.clone());

    let mut results = Vec::new();
    for time_result in time_iter {
        let datetime_input = time_result.map_err(|e| format!("Error reading time: {}", e))?;
        let calculation_result = calculate_with_coords(
            lat.clone(),
            lon.clone(),
            &datetime_input,
            input,
            matches,
            calculator,
        )?;
        results.extend(calculation_result);
    }
    Ok(results)
}

fn process_paired_file<T>(
    file_path: &str,
    input: &ParsedInput,
    matches: &ArgMatches,
    calculator: fn(&ParsedInput, &ArgMatches) -> Result<Vec<T>, String>,
) -> Result<Vec<T>, String> {
    let reader = create_file_reader(file_path)
        .map_err(|e| format!("Failed to open paired data file {}: {}", file_path, e))?;
    let paired_iter = PairedFileIterator::new(reader, input.global_options.timezone.clone());

    let mut results = Vec::new();
    for paired_result in paired_iter {
        let (lat, lon, datetime_input) =
            paired_result.map_err(|e| format!("Error reading paired data: {}", e))?;
        let calculation_result =
            calculate_with_single_coords(lat, lon, &datetime_input, input, matches, calculator)?;
        results.extend(calculation_result);
    }
    Ok(results)
}

fn calculate_with_single_coords<T>(
    lat: f64,
    lon: f64,
    datetime_input: &DateTimeInput,
    input: &ParsedInput,
    matches: &ArgMatches,
    calculator: fn(&ParsedInput, &ArgMatches) -> Result<Vec<T>, String>,
) -> Result<Vec<T>, String> {
    let lat_coord = Coordinate::Single(lat);
    let lon_coord = Coordinate::Single(lon);
    calculate_with_coords(
        lat_coord,
        lon_coord,
        datetime_input,
        input,
        matches,
        calculator,
    )
}

fn calculate_with_coords<T>(
    lat: Coordinate,
    lon: Coordinate,
    datetime_input: &DateTimeInput,
    input: &ParsedInput,
    matches: &ArgMatches,
    calculator: fn(&ParsedInput, &ArgMatches) -> Result<Vec<T>, String>,
) -> Result<Vec<T>, String> {
    let mut temp_input = input.clone();
    temp_input.parsed_latitude = Some(lat);
    temp_input.parsed_longitude = Some(lon);
    temp_input.parsed_datetime = Some(datetime_input.clone());
    temp_input.input_type = InputType::Standard;
    calculator(&temp_input, matches)
}

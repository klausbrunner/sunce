use chrono::TimeZone;
use clap::ArgMatches;

mod coordinate_parser;
mod datetime_parser;
mod input_parser;
mod parsing;
mod sunrise_formatters;
mod types;

use parsing::{
    Coordinate, DateTimeInput, InputType, ParsedInput, parse_coordinate, parse_data_values,
    parse_datetime, parse_input, parse_position_options, parse_sunrise_options,
};

mod output;
use output::{OutputFormat, output_position_results};

mod sunrise_output;
use sunrise_output::{OutputFormat as SunriseOutputFormat, output_sunrise_results};

mod calculation;
use calculation::{
    CalculationEngine, PositionCalculationEngine, SunriseCalculationEngine,
    get_calculation_parameters, get_sunrise_calculation_parameters,
};

mod cli;
mod file_input;
mod iterators;
mod time_series;
mod timezone;
use file_input::{
    CoordinateFileIterator, PairedFileIterator, TimeFileIterator, create_file_reader,
};
use iterators::create_coordinate_iterator;
use time_series::{TimeStep, expand_datetime_input};

fn main() {
    let app = cli::build_cli();
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

                            // Note: --parallel flag is accepted for compatibility but ignored
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
                        "sunrise" => {
                            let show_inputs = input.global_options.show_inputs.unwrap_or(false);
                            let show_headers = input.global_options.headers.unwrap_or(true);
                            let show_twilight = parse_sunrise_options(cmd_matches).twilight;

                            // Note: --parallel flag is accepted for compatibility but ignored
                            match calculate_and_output_sunrise(
                                &input,
                                &matches,
                                &format,
                                show_inputs,
                                show_headers,
                                show_twilight,
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

/// Unified streaming function for position calculations
fn calculate_and_output_positions(
    input: &ParsedInput,
    matches: &ArgMatches,
    format: &OutputFormat,
    show_inputs: bool,
    show_headers: bool,
    elevation_angle: bool,
) -> Result<(), String> {
    // Create position calculation engine
    let params = get_calculation_parameters(input, matches)?;
    let engine = PositionCalculationEngine { params };

    // Create a streaming iterator using the unified engine (sequential processing)
    let position_iter = create_calculation_iterator(input, matches, &engine)?;

    // Stream to output
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
) -> Result<(), String> {
    // Create sunrise calculation engine
    let params = get_sunrise_calculation_parameters(input, matches, show_twilight)?;
    let engine = SunriseCalculationEngine { params };

    // Create a streaming iterator using the unified engine (sequential processing)
    let sunrise_iter = create_calculation_iterator(input, matches, &engine)?;

    // Convert format
    let sunrise_format = match format {
        OutputFormat::Human => SunriseOutputFormat::Human,
        OutputFormat::Csv => SunriseOutputFormat::Csv,
        OutputFormat::Json => SunriseOutputFormat::Json,
    };

    // Stream to output
    output_sunrise_results(
        sunrise_iter,
        &sunrise_format,
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
) -> Result<Box<dyn Iterator<Item = T> + 'a>, String> {
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
            input, engine, step,
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

    // Stream each time through calculation immediately using Cartesian product
    Ok(time_iter.flat_map(move |time_result| {
        let datetime_input = time_result.expect("Error reading time");
        let datetime = match datetime_input {
            DateTimeInput::Single(dt) => dt,
            _ => panic!("Expected specific datetime from time file"),
        };

        // Create streaming coordinate combinations for each time
        let lat_clone = lat.clone();
        let lon_clone = lon.clone();
        let lat_iter = create_coordinate_iterator(&lat_clone);
        lat_iter.flat_map(move |lat_val| {
            let lon_iter = create_coordinate_iterator(&lon_clone);
            lon_iter.map(move |lon_val| engine.calculate_single(datetime, lat_val, lon_val))
        })
    }))
}

/// Unified standard calculation iterator
fn create_standard_calculation_iterator<'a, T>(
    input: &'a ParsedInput,
    engine: &'a dyn CalculationEngine<T>,
    step: TimeStep,
) -> Result<impl Iterator<Item = T> + 'a, String> {
    if let (Some(lat), Some(lon), Some(dt)) = (
        &input.parsed_latitude,
        &input.parsed_longitude,
        &input.parsed_datetime,
    ) {
        // Note: Coordinate iterators are now created directly in streaming function

        // Parse timezone override once for reuse
        let timezone_override = if let Some(tz_str) = &input.global_options.timezone {
            Some(
                crate::timezone::parse_timezone_to_offset(tz_str)
                    .map_err(|e| format!("Invalid timezone: {}", e))?,
            )
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
                        let naive_dt = chrono::NaiveDate::from_ymd_opt(*year, *month, *day)
                            .unwrap()
                            .and_hms_opt(0, 0, 0)
                            .unwrap();
                        crate::timezone::apply_timezone_to_datetime(naive_dt, None)
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

        // Create optimized calculation iterator (sequential processing)
        Ok(create_optimized_calculation_iterator(
            lat,
            lon,
            datetime_iter,
            engine,
        ))
    } else {
        Err("Missing required coordinate or datetime data".to_string())
    }
}

/// Create optimized calculation iterator - prioritizes streaming for common patterns
fn create_optimized_calculation_iterator<'a, T>(
    lat: &'a Coordinate,
    lon: &'a Coordinate,
    datetime_iter: Box<dyn Iterator<Item = chrono::DateTime<chrono::FixedOffset>>>,
    engine: &'a dyn CalculationEngine<T>,
) -> Box<dyn Iterator<Item = T> + 'a> {
    // Determine optimal pattern based on coordinate types
    match (lat, lon) {
        // Single coordinates: pure streaming over time (optimal for time series)
        (Coordinate::Single(lat_val), Coordinate::Single(lon_val)) => {
            let lat_val = *lat_val;
            let lon_val = *lon_val;
            Box::new(datetime_iter.map(move |dt| engine.calculate_single(dt, lat_val, lon_val)))
        }

        // For coordinate ranges: use true streaming Cartesian product
        _ => {
            // Use nested flat_map for true streaming Cartesian product
            Box::new(datetime_iter.flat_map(move |dt| {
                let lat_iter = create_coordinate_iterator(lat);
                lat_iter.flat_map(move |lat_val| {
                    let lon_iter = create_coordinate_iterator(lon);
                    lon_iter.map(move |lon_val| engine.calculate_single(dt, lat_val, lon_val))
                })
            }))
        }
    }
}

use crate::calculation::create_refraction_correction;
use crate::calculation::{
    CalculationParameters, CoordinateSweepCalculator, SunriseCalculationParameters,
    calculate_single_position, calculate_single_sunrise,
};
use crate::file_input::{
    CoordinateFileIterator, PairedFileIterator, TimeFileIterator, create_file_reader,
};
use crate::output::PositionResult;
use crate::sunrise_formatters::SunriseResultData;
use crate::time_series::{TimeStep, expand_datetime_input};
use crate::timezone::parse_timezone_spec;
use crate::types::datetime_input_to_single_with_timezone;
use crate::types::{Coordinate, DateTimeInput, InputType, ParsedInput};
use chrono::{DateTime, FixedOffset};
use clap::ArgMatches;
use solar_positioning::RefractionCorrection;
use std::cell::RefCell;
use std::rc::Rc;

/// Streaming iterator for coordinate ranges (no Vec collection)
pub struct CoordinateRangeIterator {
    current: f64,
    end: f64,
    step: f64,
    ascending: bool,
    finished: bool,
}

impl CoordinateRangeIterator {
    pub fn new(start: f64, end: f64, step: f64) -> Self {
        let ascending = step > 0.0;
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

        let current = self.current;

        // Use epsilon for floating point comparison to handle precision issues
        const EPSILON: f64 = 1e-10;

        // Check if current value is beyond the end (should not be included)
        let past_end = if self.ascending {
            current > self.end + EPSILON
        } else {
            current < self.end - EPSILON
        };

        if past_end {
            None
        } else {
            // Check if we're at or very close to the end
            let at_end = if self.ascending {
                current >= self.end - EPSILON
            } else {
                current <= self.end + EPSILON
            };

            if at_end {
                self.finished = true;
            } else {
                // Advance to next value
                if self.ascending {
                    self.current += self.step;
                } else {
                    self.current -= self.step;
                }
            }
            Some(current)
        }
    }
}

/// Concrete iterator type for coordinates (avoids boxing)
pub enum CoordinateIterator {
    Single(std::iter::Once<f64>),
    Range(CoordinateRangeIterator),
}

impl Iterator for CoordinateIterator {
    type Item = f64;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            CoordinateIterator::Single(iter) => iter.next(),
            CoordinateIterator::Range(iter) => iter.next(),
        }
    }
}

/// Create a streaming iterator for a coordinate (single value or range)
pub fn create_coordinate_iterator(coord: &Coordinate) -> CoordinateIterator {
    match coord {
        Coordinate::Single(val) => CoordinateIterator::Single(std::iter::once(*val)),
        Coordinate::Range { start, end, step } => {
            CoordinateIterator::Range(CoordinateRangeIterator::new(*start, *end, *step))
        }
    }
}

pub fn create_position_iterator<'a>(
    input: &'a ParsedInput,
    matches: &'a ArgMatches,
    params: &'a CalculationParameters,
) -> Result<Box<dyn Iterator<Item = Result<PositionResult, String>> + 'a>, String> {
    let (cmd_name, cmd_matches) = matches.subcommand().unwrap_or(("position", matches));
    let (step, watch_mode) = if cmd_name == "position" {
        if let Some(step_str) = cmd_matches.get_one::<String>("step") {
            let step =
                TimeStep::parse(step_str).map_err(|e| format!("Invalid step parameter: {}", e))?;
            let watch = matches!(input.parsed_datetime, Some(DateTimeInput::Now));
            (step, watch)
        } else {
            (TimeStep::default(), false)
        }
    } else {
        (TimeStep::default(), false)
    };

    match input.input_type {
        InputType::PairedDataFile | InputType::StdinPaired => Ok(Box::new(
            create_paired_file_position_iterator(input, params)?,
        )),
        InputType::CoordinateFile | InputType::StdinCoords => Ok(Box::new(
            create_coordinate_file_position_iterator(input, params)?,
        )),
        InputType::TimeFile | InputType::StdinTimes => {
            Ok(Box::new(create_time_file_position_iterator(input, params)?))
        }
        InputType::Standard => Ok(Box::new(
            create_standard_position_iterator(input, params, step, false, watch_mode)?.map(Ok),
        )),
    }
}

pub fn create_sunrise_iterator<'a>(
    input: &'a ParsedInput,
    matches: &'a ArgMatches,
    params: &'a SunriseCalculationParameters,
) -> Result<Box<dyn Iterator<Item = Result<SunriseResultData, String>> + 'a>, String> {
    let (_cmd_name, _cmd_matches) = matches.subcommand().unwrap_or(("sunrise", matches));
    let step = TimeStep::default();

    match input.input_type {
        InputType::PairedDataFile | InputType::StdinPaired => Ok(Box::new(
            create_paired_file_sunrise_iterator(input, params)?,
        )),
        InputType::CoordinateFile | InputType::StdinCoords => Ok(Box::new(
            create_coordinate_file_sunrise_iterator(input, params)?,
        )),
        InputType::TimeFile | InputType::StdinTimes => {
            Ok(Box::new(create_time_file_sunrise_iterator(input, params)?))
        }
        InputType::Standard => Ok(Box::new(
            create_standard_sunrise_iterator(input, params, step, true)?.map(Ok),
        )),
    }
}

fn create_paired_file_position_iterator<'a>(
    input: &'a ParsedInput,
    params: &'a CalculationParameters,
) -> Result<impl Iterator<Item = Result<PositionResult, String>> + 'a, String> {
    let file_path = &input.latitude;
    let reader = create_file_reader(file_path)
        .map_err(|e| format!("Failed to open paired data file {}: {}", file_path, e))?;
    let paired_iter = PairedFileIterator::new(reader, input.global_options.timezone.clone());
    let timezone_spec = input
        .global_options
        .timezone
        .as_deref()
        .map(parse_timezone_spec)
        .transpose()
        .map_err(|e| format!("Invalid timezone: {}", e))?;

    Ok(paired_iter.map(move |paired_result| match paired_result {
        Ok((lat, lon, datetime_input)) => {
            let datetime =
                datetime_input_to_single_with_timezone(datetime_input, timezone_spec.clone());
            Ok(calculate_single_position(datetime, lat, lon, params))
        }
        Err(e) => Err(format!("Error reading paired data: {}", e)),
    }))
}

fn create_paired_file_sunrise_iterator<'a>(
    input: &'a ParsedInput,
    params: &'a SunriseCalculationParameters,
) -> Result<impl Iterator<Item = Result<SunriseResultData, String>> + 'a, String> {
    let file_path = &input.latitude;
    let reader = create_file_reader(file_path)
        .map_err(|e| format!("Failed to open paired data file {}: {}", file_path, e))?;
    let paired_iter = PairedFileIterator::new(reader, input.global_options.timezone.clone());
    let timezone_spec = input
        .global_options
        .timezone
        .as_deref()
        .map(parse_timezone_spec)
        .transpose()
        .map_err(|e| format!("Invalid timezone: {}", e))?;

    Ok(paired_iter.map(move |paired_result| match paired_result {
        Ok((lat, lon, datetime_input)) => {
            let datetime =
                datetime_input_to_single_with_timezone(datetime_input, timezone_spec.clone());
            Ok(calculate_single_sunrise(datetime, lat, lon, params))
        }
        Err(e) => Err(format!("Error reading paired data: {}", e)),
    }))
}

fn create_coordinate_file_position_iterator<'a>(
    input: &'a ParsedInput,
    params: &'a CalculationParameters,
) -> Result<impl Iterator<Item = Result<PositionResult, String>> + 'a, String> {
    let file_path = &input.latitude;

    let datetime = input
        .parsed_datetime
        .as_ref()
        .ok_or("Parsed datetime not available")?
        .clone();
    let timezone_spec = input
        .global_options
        .timezone
        .as_deref()
        .map(parse_timezone_spec)
        .transpose()
        .map_err(|e| format!("Invalid timezone: {}", e))?;
    let datetime = datetime_input_to_single_with_timezone(datetime, timezone_spec);

    let reader = create_file_reader(file_path)
        .map_err(|e| format!("Failed to open coordinate file {}: {}", file_path, e))?;
    let coord_iter = CoordinateFileIterator::new(reader);

    // Use optimized calculator for coordinate sweeps
    let calculator = Rc::new(RefCell::new(CoordinateSweepCalculator::new(params.clone())));

    Ok(coord_iter.map(move |coord_result| match coord_result {
        Ok((lat, lon)) => Ok(calculator
            .borrow_mut()
            .calculate_position(datetime, lat, lon)),
        Err(e) => Err(format!("Error reading coordinate data: {}", e)),
    }))
}

fn create_coordinate_file_sunrise_iterator<'a>(
    input: &'a ParsedInput,
    params: &'a SunriseCalculationParameters,
) -> Result<impl Iterator<Item = Result<SunriseResultData, String>> + 'a, String> {
    let file_path = &input.latitude;

    let datetime = input
        .parsed_datetime
        .as_ref()
        .ok_or("Parsed datetime not available")?
        .clone();
    let timezone_spec = input
        .global_options
        .timezone
        .as_deref()
        .map(parse_timezone_spec)
        .transpose()
        .map_err(|e| format!("Invalid timezone: {}", e))?;
    let datetime = datetime_input_to_single_with_timezone(datetime, timezone_spec);

    let reader = create_file_reader(file_path)
        .map_err(|e| format!("Failed to open coordinate file {}: {}", file_path, e))?;
    let coord_iter = CoordinateFileIterator::new(reader);

    Ok(coord_iter.map(move |coord_result| match coord_result {
        Ok((lat, lon)) => Ok(calculate_single_sunrise(datetime, lat, lon, params)),
        Err(e) => Err(format!("Error reading coordinate data: {}", e)),
    }))
}

fn create_time_file_position_iterator<'a>(
    input: &'a ParsedInput,
    params: &'a CalculationParameters,
) -> Result<impl Iterator<Item = Result<PositionResult, String>> + 'a, String> {
    let lat = input
        .parsed_latitude
        .as_ref()
        .ok_or("Parsed latitude not available")?;
    let lon = input
        .parsed_longitude
        .as_ref()
        .ok_or("Parsed longitude not available")?;

    let file_path = input.datetime.as_ref().unwrap();
    let reader = create_file_reader(file_path)
        .map_err(|e| format!("Failed to open time file {}: {}", file_path, e))?;
    let time_iter = TimeFileIterator::new(reader, input.global_options.timezone.clone());
    let timezone_spec = input
        .global_options
        .timezone
        .as_deref()
        .map(parse_timezone_spec)
        .transpose()
        .map_err(|e| format!("Invalid timezone: {}", e))?;

    Ok(time_iter.flat_map(move |time_result| match time_result {
        Ok(datetime_input) => {
            let datetime =
                datetime_input_to_single_with_timezone(datetime_input, timezone_spec.clone());
            let lat_iter = create_coordinate_iterator(lat);
            // Use streaming approach - no collect()!
            Box::new(lat_iter.flat_map(move |lat_val| {
                let lon_iter = create_coordinate_iterator(lon);
                lon_iter.map(move |lon_val| {
                    Ok(calculate_single_position(
                        datetime, lat_val, lon_val, params,
                    ))
                })
            })) as Box<dyn Iterator<Item = Result<PositionResult, String>>>
        }
        Err(e) => Box::new(std::iter::once(Err(format!(
            "Error reading time data: {}",
            e
        )))) as Box<dyn Iterator<Item = Result<PositionResult, String>>>,
    }))
}

fn create_time_file_sunrise_iterator<'a>(
    input: &'a ParsedInput,
    params: &'a SunriseCalculationParameters,
) -> Result<impl Iterator<Item = Result<SunriseResultData, String>> + 'a, String> {
    let lat = input
        .parsed_latitude
        .as_ref()
        .ok_or("Parsed latitude not available")?;
    let lon = input
        .parsed_longitude
        .as_ref()
        .ok_or("Parsed longitude not available")?;

    let file_path = input.datetime.as_ref().unwrap();
    let reader = create_file_reader(file_path)
        .map_err(|e| format!("Failed to open time file {}: {}", file_path, e))?;
    let time_iter = TimeFileIterator::new(reader, input.global_options.timezone.clone());
    let timezone_spec = input
        .global_options
        .timezone
        .as_deref()
        .map(parse_timezone_spec)
        .transpose()
        .map_err(|e| format!("Invalid timezone: {}", e))?;

    Ok(time_iter.flat_map(move |time_result| match time_result {
        Ok(datetime_input) => {
            let datetime =
                datetime_input_to_single_with_timezone(datetime_input, timezone_spec.clone());
            let lat_iter = create_coordinate_iterator(lat);
            // Use streaming approach - no collect()!
            Box::new(lat_iter.flat_map(move |lat_val| {
                let lon_iter = create_coordinate_iterator(lon);
                lon_iter.map(move |lon_val| {
                    Ok(calculate_single_sunrise(datetime, lat_val, lon_val, params))
                })
            })) as Box<dyn Iterator<Item = Result<SunriseResultData, String>>>
        }
        Err(e) => Box::new(std::iter::once(Err(format!(
            "Error reading time data: {}",
            e
        )))) as Box<dyn Iterator<Item = Result<SunriseResultData, String>>>,
    }))
}

fn create_standard_position_iterator<'a>(
    input: &'a ParsedInput,
    params: &'a CalculationParameters,
    step: TimeStep,
    is_sunrise_command: bool,
    watch_mode: bool,
) -> Result<impl Iterator<Item = PositionResult> + 'a, String> {
    let lat = input
        .parsed_latitude
        .as_ref()
        .ok_or("Parsed latitude not available")?;
    let lon = input
        .parsed_longitude
        .as_ref()
        .ok_or("Parsed longitude not available")?;
    let datetime = input
        .parsed_datetime
        .as_ref()
        .ok_or("Parsed datetime not available")?;

    let timezone_spec = input
        .global_options
        .timezone
        .as_ref()
        .map(|tz| parse_timezone_spec(tz))
        .transpose()
        .map_err(|e| e.to_string())?;

    let datetime_iter: Box<dyn Iterator<Item = DateTime<FixedOffset>> + 'a> = if is_sunrise_command
        && (matches!(lat, Coordinate::Range { .. }) || matches!(lon, Coordinate::Range { .. }))
    {
        // For sunrise commands with coordinate ranges, use single datetime to avoid Cartesian product time series expansion
        let single_dt =
            datetime_input_to_single_with_timezone(datetime.clone(), timezone_spec.clone());
        Box::new(std::iter::once(single_dt))
    } else {
        // For position commands or sunrise with single coordinates, expand datetime input into time series
        let expanded = crate::time_series::expand_datetime_input_with_watch(
            datetime,
            &step,
            timezone_spec,
            watch_mode,
        )
        .map_err(|e| e.to_string())?;
        Box::new(expanded)
    };

    match (lat, lon) {
        (Coordinate::Single(lat_val), Coordinate::Single(lon_val)) => Ok(Box::new(
            datetime_iter.map(move |dt| calculate_single_position(dt, *lat_val, *lon_val, params)),
        )
            as Box<dyn Iterator<Item = PositionResult> + 'a>),
        _ => {
            // Check if this is a pure coordinate sweep by peeking at the datetime iterator
            let mut datetime_iter = datetime_iter.peekable();
            let first_datetime = datetime_iter.next();
            let is_single_datetime = datetime_iter.peek().is_none();

            if let Some(single_datetime) = first_datetime {
                if is_single_datetime && params.algorithm.to_uppercase() == "SPA" {
                    // Pure coordinate sweep optimization using time-dependent parts caching
                    // Create optimized iterator using direct SPA optimization APIs
                    Ok(Box::new(CoordinateSweepOptimizedIterator::new(
                        lat,
                        lon,
                        single_datetime,
                        params.clone(),
                    ))
                        as Box<dyn Iterator<Item = PositionResult> + 'a>)
                } else {
                    // General case with time-based caching
                    let calculator =
                        Rc::new(RefCell::new(CoordinateSweepCalculator::new(params.clone())));

                    // Reconstruct datetime iterator from consumed elements
                    let datetime_iter = std::iter::once(single_datetime).chain(datetime_iter);

                    Ok(Box::new(datetime_iter.flat_map(move |dt| {
                        let calculator = calculator.clone();
                        let lat_iter = create_coordinate_iterator(lat);
                        lat_iter.flat_map(move |lat_val| {
                            let calculator = calculator.clone();
                            let lon_iter = create_coordinate_iterator(lon);
                            lon_iter.map(move |lon_val| {
                                calculator
                                    .borrow_mut()
                                    .calculate_position(dt, lat_val, lon_val)
                            })
                        })
                    }))
                        as Box<dyn Iterator<Item = PositionResult> + 'a>)
                }
            } else {
                // Empty datetime iterator
                Ok(Box::new(std::iter::empty()) as Box<dyn Iterator<Item = PositionResult> + 'a>)
            }
        }
    }
}

fn create_standard_sunrise_iterator<'a>(
    input: &'a ParsedInput,
    params: &'a SunriseCalculationParameters,
    step: TimeStep,
    is_sunrise_command: bool,
) -> Result<impl Iterator<Item = SunriseResultData> + 'a, String> {
    let lat = input
        .parsed_latitude
        .as_ref()
        .ok_or("Parsed latitude not available")?;
    let lon = input
        .parsed_longitude
        .as_ref()
        .ok_or("Parsed longitude not available")?;
    let datetime = input
        .parsed_datetime
        .as_ref()
        .ok_or("Parsed datetime not available")?;

    let timezone_spec = input
        .global_options
        .timezone
        .as_ref()
        .map(|tz| parse_timezone_spec(tz))
        .transpose()
        .map_err(|e| e.to_string())?;

    let datetime_iter: Box<dyn Iterator<Item = DateTime<FixedOffset>> + 'a> = if is_sunrise_command
        && (matches!(lat, Coordinate::Range { .. })
            || matches!(lon, Coordinate::Range { .. })
            || matches!(datetime, DateTimeInput::PartialDate(_, _, _)))
    {
        let single_dt =
            datetime_input_to_single_with_timezone(datetime.clone(), timezone_spec.clone());
        Box::new(std::iter::once(single_dt))
    } else {
        let expanded =
            expand_datetime_input(datetime, &step, timezone_spec).map_err(|e| e.to_string())?;
        Box::new(expanded)
    };

    match (lat, lon) {
        (Coordinate::Single(lat_val), Coordinate::Single(lon_val)) => Ok(Box::new(
            datetime_iter.map(move |dt| calculate_single_sunrise(dt, *lat_val, *lon_val, params)),
        )
            as Box<dyn Iterator<Item = SunriseResultData> + 'a>),
        _ => Ok(Box::new(datetime_iter.flat_map(move |dt| {
            let lat_iter = create_coordinate_iterator(lat);
            lat_iter.flat_map(move |lat_val| {
                let lon_iter = create_coordinate_iterator(lon);
                lon_iter.map(move |lon_val| calculate_single_sunrise(dt, lat_val, lon_val, params))
            })
        }))
            as Box<dyn Iterator<Item = SunriseResultData> + 'a>),
    }
}

/// Optimized iterator for coordinate sweeps using direct SPA time-dependent parts caching
pub struct CoordinateSweepOptimizedIterator {
    // Direct coordinate ranges to avoid boxing
    lat_end: f64,
    lat_step: f64,
    lon_start: f64,
    lon_end: f64,
    lon_step: f64,
    // Current state
    current_lat: f64,
    current_lon: f64,
    lat_finished: bool,
    first_iteration: bool,
    // Cached values to avoid cloning
    time_parts: solar_positioning::spa::SpaTimeDependent,
    datetime: DateTime<FixedOffset>,
    elevation: f64,
    delta_t: f64,
    refraction: Option<RefractionCorrection>,
}

impl CoordinateSweepOptimizedIterator {
    pub fn new(
        lat: &Coordinate,
        lon: &Coordinate,
        datetime: DateTime<FixedOffset>,
        params: CalculationParameters,
    ) -> Self {
        // Pre-compute time-dependent parts once for coordinate sweep optimization
        let time_parts = solar_positioning::spa::spa_time_dependent_parts(datetime, params.delta_t)
            .expect("Time-dependent parts calculation should not fail");

        // Extract ranges directly to avoid iterator overhead
        let (lat_start, lat_end, lat_step) = match lat {
            Coordinate::Range { start, end, step } => (*start, *end, *step),
            Coordinate::Single(val) => (*val, *val, 1.0),
        };

        let (lon_start, lon_end, lon_step) = match lon {
            Coordinate::Range { start, end, step } => (*start, *end, *step),
            Coordinate::Single(val) => (*val, *val, 1.0),
        };

        let refraction = create_refraction_correction(
            params.pressure,
            params.temperature,
            params.apply_refraction,
        );

        Self {
            lat_end,
            lat_step,
            lon_start,
            lon_end,
            lon_step,
            current_lat: lat_start,
            current_lon: lon_start,
            lat_finished: false,
            first_iteration: true,
            time_parts,
            datetime,
            elevation: params.elevation,
            delta_t: params.delta_t,
            refraction,
        }
    }
}

impl Iterator for CoordinateSweepOptimizedIterator {
    type Item = PositionResult;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        if self.lat_finished {
            return None;
        }

        // Handle first iteration
        if self.first_iteration {
            self.first_iteration = false;
        } else {
            // Advance to next coordinate
            self.advance_coordinates();
            if self.lat_finished {
                return None;
            }
        }

        // Direct SPA calculation with minimal overhead
        let solar_position = solar_positioning::spa::spa_with_time_dependent_parts(
            self.current_lat,
            self.current_lon,
            self.elevation,
            self.refraction,
            &self.time_parts,
        )
        .unwrap();

        // Extract pressure and temperature for PositionResult
        let (pressure, temperature, apply_refraction) =
            if let Some(ref refraction) = self.refraction {
                (refraction.pressure(), refraction.temperature(), true)
            } else {
                (f64::NAN, f64::NAN, false)
            };

        // Optimize PositionResult construction - reorder fields for better cache behavior
        Some(PositionResult {
            position: solar_position,
            latitude: self.current_lat,
            longitude: self.current_lon,
            datetime: self.datetime,
            elevation: self.elevation,
            pressure,
            temperature,
            delta_t: self.delta_t,
            apply_refraction,
        })
    }
}

impl CoordinateSweepOptimizedIterator {
    #[inline(always)]
    fn advance_coordinates(&mut self) {
        // Advance longitude first - optimized for the common case
        self.current_lon += self.lon_step;

        // Fast check without epsilon for performance (most coordinate steps are clean)
        if (self.lon_step > 0.0 && self.current_lon > self.lon_end)
            || (self.lon_step < 0.0 && self.current_lon < self.lon_end)
        {
            // Longitude finished, advance latitude and reset longitude
            self.current_lat += self.lat_step;

            // Check if latitude is finished
            if (self.lat_step > 0.0 && self.current_lat > self.lat_end)
                || (self.lat_step < 0.0 && self.current_lat < self.lat_end)
            {
                self.lat_finished = true;
                return;
            }

            // Reset longitude
            self.current_lon = self.lon_start;
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ascending_range() {
        let mut iter = CoordinateRangeIterator::new(1.0, 3.0, 1.0);
        assert_eq!(iter.next(), Some(1.0));
        assert_eq!(iter.next(), Some(2.0));
        assert_eq!(iter.next(), Some(3.0));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_descending_range() {
        let mut iter = CoordinateRangeIterator::new(3.0, 1.0, -1.0);
        assert_eq!(iter.next(), Some(3.0));
        assert_eq!(iter.next(), Some(2.0));
        assert_eq!(iter.next(), Some(1.0));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_fractional_step() {
        let mut iter = CoordinateRangeIterator::new(0.0, 1.0, 0.5);
        assert_eq!(iter.next(), Some(0.0));
        assert_eq!(iter.next(), Some(0.5));
        assert_eq!(iter.next(), Some(1.0));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_coordinate_range_endpoint_inclusion() {
        let iter = CoordinateRangeIterator::new(10.0, 15.0, 0.1);
        let count = iter.count();
        assert_eq!(count, 51); // Should be exactly 51 values: 10.0, 10.1, ..., 15.0

        let mut iter = CoordinateRangeIterator::new(10.0, 15.0, 0.1);
        assert_eq!(iter.next(), Some(10.0));

        let last = iter.last().unwrap();
        assert!((last - 15.0).abs() < 1e-10); // Last value should be very close to 15.0
    }
}

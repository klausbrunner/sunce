use crate::calculation::{
    CalculationParameters, SunriseCalculationParameters, calculate_single_position,
    calculate_single_sunrise,
};
use crate::datetime_utils::datetime_input_to_single;
use crate::file_input::{
    CoordinateFileIterator, PairedFileIterator, TimeFileIterator, create_file_reader,
};
use crate::output::PositionResult;
use crate::parsing::{Coordinate, InputType, ParsedInput};
use crate::sunrise_output::SunriseResultData;
use crate::time_series::{TimeStep, expand_datetime_input};
use crate::timezone::parse_timezone_to_offset;
use chrono::{DateTime, FixedOffset};
use clap::ArgMatches;

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

/// Create a streaming iterator for a coordinate (single value or range)
pub fn create_coordinate_iterator(coord: &Coordinate) -> Box<dyn Iterator<Item = f64>> {
    match coord {
        Coordinate::Single(val) => Box::new(std::iter::once(*val)),
        Coordinate::Range { start, end, step } => {
            Box::new(CoordinateRangeIterator::new(*start, *end, *step))
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

pub fn create_position_iterator<'a>(
    input: &'a ParsedInput,
    matches: &'a ArgMatches,
    params: &'a CalculationParameters,
) -> Result<Box<dyn Iterator<Item = Result<PositionResult, String>> + 'a>, String> {
    let (cmd_name, cmd_matches) = matches.subcommand().unwrap_or(("position", matches));
    let step = if cmd_name == "position" {
        if let Some(step_str) = cmd_matches.get_one::<String>("step") {
            TimeStep::parse(step_str).map_err(|e| format!("Invalid step parameter: {}", e))?
        } else {
            TimeStep::default()
        }
    } else {
        TimeStep::default()
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
            create_standard_position_iterator(input, params, step, false)?.map(Ok),
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

    Ok(paired_iter.map(move |paired_result| match paired_result {
        Ok((lat, lon, datetime_input)) => {
            let datetime = datetime_input_to_single(datetime_input);
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

    Ok(paired_iter.map(move |paired_result| match paired_result {
        Ok((lat, lon, datetime_input)) => {
            let datetime = datetime_input_to_single(datetime_input);
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
    let datetime = datetime_input_to_single(datetime);

    let reader = create_file_reader(file_path)
        .map_err(|e| format!("Failed to open coordinate file {}: {}", file_path, e))?;
    let coord_iter = CoordinateFileIterator::new(reader);

    Ok(coord_iter.map(move |coord_result| match coord_result {
        Ok((lat, lon)) => Ok(calculate_single_position(datetime, lat, lon, params)),
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
    let datetime = datetime_input_to_single(datetime);

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
    let lat = match input
        .parsed_latitude
        .as_ref()
        .ok_or("Parsed latitude not available")?
    {
        Coordinate::Single(val) => *val,
        Coordinate::Range { .. } => {
            return Err("Range coordinates not supported for time files".to_string());
        }
    };
    let lon = match input
        .parsed_longitude
        .as_ref()
        .ok_or("Parsed longitude not available")?
    {
        Coordinate::Single(val) => *val,
        Coordinate::Range { .. } => {
            return Err("Range coordinates not supported for time files".to_string());
        }
    };

    let file_path = input.datetime.as_ref().unwrap();
    let reader = create_file_reader(file_path)
        .map_err(|e| format!("Failed to open time file {}: {}", file_path, e))?;
    let time_iter = TimeFileIterator::new(reader, input.global_options.timezone.clone());

    Ok(time_iter.map(move |time_result| match time_result {
        Ok(datetime_input) => {
            let datetime = datetime_input_to_single(datetime_input);
            Ok(calculate_single_position(datetime, lat, lon, params))
        }
        Err(e) => Err(format!("Error reading time data: {}", e)),
    }))
}

fn create_time_file_sunrise_iterator<'a>(
    input: &'a ParsedInput,
    params: &'a SunriseCalculationParameters,
) -> Result<impl Iterator<Item = Result<SunriseResultData, String>> + 'a, String> {
    let lat = match input
        .parsed_latitude
        .as_ref()
        .ok_or("Parsed latitude not available")?
    {
        Coordinate::Single(val) => *val,
        Coordinate::Range { .. } => {
            return Err("Range coordinates not supported for time files".to_string());
        }
    };
    let lon = match input
        .parsed_longitude
        .as_ref()
        .ok_or("Parsed longitude not available")?
    {
        Coordinate::Single(val) => *val,
        Coordinate::Range { .. } => {
            return Err("Range coordinates not supported for time files".to_string());
        }
    };

    let file_path = input.datetime.as_ref().unwrap();
    let reader = create_file_reader(file_path)
        .map_err(|e| format!("Failed to open time file {}: {}", file_path, e))?;
    let time_iter = TimeFileIterator::new(reader, input.global_options.timezone.clone());

    Ok(time_iter.map(move |time_result| match time_result {
        Ok(datetime_input) => {
            let datetime = datetime_input_to_single(datetime_input);
            Ok(calculate_single_sunrise(datetime, lat, lon, params))
        }
        Err(e) => Err(format!("Error reading time data: {}", e)),
    }))
}

fn create_standard_position_iterator<'a>(
    input: &'a ParsedInput,
    params: &'a CalculationParameters,
    step: TimeStep,
    is_sunrise_command: bool,
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

    let timezone_override = input
        .global_options
        .timezone
        .as_ref()
        .map(|tz| parse_timezone_to_offset(tz))
        .transpose()
        .map_err(|e| e.to_string())?;

    let datetime_iter: Box<dyn Iterator<Item = DateTime<FixedOffset>> + 'a> = if is_sunrise_command
        && (matches!(lat, Coordinate::Range { .. }) || matches!(lon, Coordinate::Range { .. }))
    {
        // For sunrise commands with coordinate ranges, use single datetime to avoid Cartesian product time series expansion
        let single_dt = datetime_input_to_single(datetime.clone());
        Box::new(std::iter::once(single_dt))
    } else {
        // For position commands or sunrise with single coordinates, expand datetime input into time series
        let expanded =
            expand_datetime_input(datetime, &step, timezone_override).map_err(|e| e.to_string())?;
        Box::new(expanded)
    };

    match (lat, lon) {
        (Coordinate::Single(lat_val), Coordinate::Single(lon_val)) => Ok(Box::new(
            datetime_iter.map(move |dt| calculate_single_position(dt, *lat_val, *lon_val, params)),
        )
            as Box<dyn Iterator<Item = PositionResult> + 'a>),
        _ => Ok(Box::new(datetime_iter.flat_map(move |dt| {
            let lat_iter = create_coordinate_iterator(lat);
            lat_iter.flat_map(move |lat_val| {
                let lon_iter = create_coordinate_iterator(lon);
                lon_iter.map(move |lon_val| calculate_single_position(dt, lat_val, lon_val, params))
            })
        })) as Box<dyn Iterator<Item = PositionResult> + 'a>),
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

    let timezone_override = input
        .global_options
        .timezone
        .as_ref()
        .map(|tz| parse_timezone_to_offset(tz))
        .transpose()
        .map_err(|e| e.to_string())?;

    let datetime_iter: Box<dyn Iterator<Item = DateTime<FixedOffset>> + 'a> = if is_sunrise_command
        && (matches!(lat, Coordinate::Range { .. }) || matches!(lon, Coordinate::Range { .. }))
    {
        let single_dt = datetime_input_to_single(datetime.clone());
        Box::new(std::iter::once(single_dt))
    } else {
        let expanded =
            expand_datetime_input(datetime, &step, timezone_override).map_err(|e| e.to_string())?;
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

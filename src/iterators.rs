use crate::calculation::CalculationEngine;
use crate::datetime_utils::datetime_input_to_single;
use crate::file_input::{
    CoordinateFileIterator, PairedFileIterator, TimeFileIterator, create_file_reader,
};
use crate::parsing::{Coordinate, InputType, ParsedInput};
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
        let iter = CoordinateRangeIterator::new(1.0, 3.0, 1.0);
        let values: Vec<f64> = iter.collect();
        assert_eq!(values, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_descending_range() {
        let iter = CoordinateRangeIterator::new(3.0, 1.0, -1.0);
        let values: Vec<f64> = iter.collect();
        assert_eq!(values, vec![3.0, 2.0, 1.0]);
    }

    #[test]
    fn test_fractional_step() {
        let iter = CoordinateRangeIterator::new(0.0, 1.0, 0.5);
        let values: Vec<f64> = iter.collect();
        assert_eq!(values, vec![0.0, 0.5, 1.0]);
    }

    #[test]
    fn test_coordinate_range_endpoint_inclusion() {
        // Test the specific case that was failing: 10:15:0.1 should have exactly 51 values
        let iter = CoordinateRangeIterator::new(10.0, 15.0, 0.1);
        let values: Vec<f64> = iter.collect();

        // Should be exactly 51 values: 10.0, 10.1, 10.2, ..., 14.9, 15.0
        assert_eq!(values.len(), 51);
        assert_eq!(values[0], 10.0);

        // Last value should be very close to 15.0 (accounting for floating point precision)
        assert!((values[50] - 15.0).abs() < 1e-10);

        // Should NOT include anything significantly beyond 15.0
        assert!(values.iter().all(|&v| v <= 15.0 + 1e-10));
    }
}

pub fn create_calculation_iterator<'a, T>(
    input: &'a ParsedInput,
    matches: &'a ArgMatches,
    engine: &'a dyn CalculationEngine<T>,
) -> Result<Box<dyn Iterator<Item = Result<T, String>> + 'a>, String> {
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

    let is_sunrise_command = cmd_name == "sunrise";

    match input.input_type {
        InputType::PairedDataFile | InputType::StdinPaired => {
            Ok(Box::new(create_paired_file_iterator(input, engine)?))
        }
        InputType::CoordinateFile | InputType::StdinCoords => {
            Ok(Box::new(create_coordinate_file_iterator(input, engine)?))
        }
        InputType::TimeFile | InputType::StdinTimes => {
            Ok(Box::new(create_time_file_iterator(input, engine)?))
        }
        InputType::Standard => Ok(Box::new(
            create_standard_iterator(input, engine, step, is_sunrise_command)?.map(Ok),
        )),
    }
}

fn create_paired_file_iterator<'a, T>(
    input: &'a ParsedInput,
    engine: &'a dyn CalculationEngine<T>,
) -> Result<impl Iterator<Item = Result<T, String>> + 'a, String> {
    let file_path = &input.latitude;
    let reader = create_file_reader(file_path)
        .map_err(|e| format!("Failed to open paired data file {}: {}", file_path, e))?;
    let paired_iter = PairedFileIterator::new(reader, input.global_options.timezone.clone());

    Ok(paired_iter.map(move |paired_result| match paired_result {
        Ok((lat, lon, datetime_input)) => {
            let datetime = datetime_input_to_single(datetime_input);
            Ok(engine.calculate_single(datetime, lat, lon))
        }
        Err(e) => Err(format!("Error reading paired data: {}", e)),
    }))
}

fn create_coordinate_file_iterator<'a, T>(
    input: &'a ParsedInput,
    engine: &'a dyn CalculationEngine<T>,
) -> Result<impl Iterator<Item = Result<T, String>> + 'a, String> {
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
        Ok((lat, lon)) => Ok(engine.calculate_single(datetime, lat, lon)),
        Err(e) => Err(format!("Error reading coordinate data: {}", e)),
    }))
}

fn create_time_file_iterator<'a, T>(
    input: &'a ParsedInput,
    engine: &'a dyn CalculationEngine<T>,
) -> Result<impl Iterator<Item = Result<T, String>> + 'a, String> {
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
            Ok(engine.calculate_single(datetime, lat, lon))
        }
        Err(e) => Err(format!("Error reading time data: {}", e)),
    }))
}

fn create_standard_iterator<'a, T>(
    input: &'a ParsedInput,
    engine: &'a dyn CalculationEngine<T>,
    step: TimeStep,
    is_sunrise_command: bool,
) -> Result<impl Iterator<Item = T> + 'a, String> {
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
            datetime_iter.map(move |dt| engine.calculate_single(dt, *lat_val, *lon_val)),
        )
            as Box<dyn Iterator<Item = T> + 'a>),
        _ => Ok(Box::new(datetime_iter.flat_map(move |dt| {
            let lat_iter = create_coordinate_iterator(lat);
            lat_iter.flat_map(move |lat_val| {
                let lon_iter = create_coordinate_iterator(lon);
                lon_iter.map(move |lon_val| engine.calculate_single(dt, lat_val, lon_val))
            })
        })) as Box<dyn Iterator<Item = T> + 'a>),
    }
}

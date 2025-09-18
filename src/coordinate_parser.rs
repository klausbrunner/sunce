use crate::types::{Coordinate, ParseError};

pub fn parse_coordinate(input: &str, coord_type: &str) -> Result<Coordinate, ParseError> {
    if input.contains(':') {
        parse_coordinate_range(input, coord_type)
    } else {
        parse_single_coordinate(input, coord_type)
    }
}

fn parse_single_coordinate(input: &str, coord_type: &str) -> Result<Coordinate, ParseError> {
    let value: f64 = input.parse().map_err(|_| {
        ParseError::InvalidCoordinate(format!("Invalid {} format: {}", coord_type, input))
    })?;
    validate_coordinate(value, coord_type)?;
    Ok(Coordinate::Single(value))
}

fn parse_coordinate_range(input: &str, coord_type: &str) -> Result<Coordinate, ParseError> {
    let mut parts = input.split(':');
    let start_str = parts.next().ok_or_else(|| {
        ParseError::InvalidRange(format!("Missing start value in range: {}", input))
    })?;
    let end_str = parts.next().ok_or_else(|| {
        ParseError::InvalidRange(format!("Missing end value in range: {}", input))
    })?;
    let step_str = parts.next().ok_or_else(|| {
        ParseError::InvalidRange(format!("Missing step value in range: {}", input))
    })?;

    if parts.next().is_some() {
        return Err(ParseError::InvalidRange(format!(
            "Too many components in range: {}",
            input
        )));
    }

    let start: f64 = start_str.parse().map_err(|_| {
        ParseError::InvalidRange(format!("Invalid start value in range: {}", start_str))
    })?;
    let end: f64 = end_str.parse().map_err(|_| {
        ParseError::InvalidRange(format!("Invalid end value in range: {}", end_str))
    })?;
    let step: f64 = step_str.parse().map_err(|_| {
        ParseError::InvalidRange(format!("Invalid step value in range: {}", step_str))
    })?;

    if step == 0.0 {
        return Err(ParseError::ZeroStep);
    }

    validate_coordinate(start, coord_type)?;
    validate_coordinate(end, coord_type)?;

    if (step > 0.0 && start > end) || (step < 0.0 && start < end) {
        return Err(ParseError::InvalidRange(format!(
            "Step direction incompatible with range: start={}, end={}, step={}",
            start, end, step
        )));
    }

    Ok(Coordinate::Range { start, end, step })
}

fn validate_coordinate(value: f64, coord_type: &str) -> Result<(), ParseError> {
    let (min, max, name) = match coord_type {
        "latitude" => (-90.0, 90.0, "latitude range -90° to 90°"),
        "longitude" => (-180.0, 180.0, "longitude range -180° to 180°"),
        _ => return Ok(()),
    };

    if value < min || value > max {
        Err(ParseError::CoordinateOutOfBounds(value, name.to_string()))
    } else {
        Ok(())
    }
}

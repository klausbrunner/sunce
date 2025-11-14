fn ensure_within(value: f64, min: f64, max: f64, label: &str) -> Result<f64, String> {
    if value < min || value > max {
        Err(format!(
            "{} must be between {} and {} degrees, got {}",
            label, min, max, value
        ))
    } else {
        Ok(value)
    }
}

pub fn validate_latitude(value: f64) -> Result<f64, String> {
    ensure_within(value, -90.0, 90.0, "Latitude")
}

pub fn validate_longitude(value: f64) -> Result<f64, String> {
    ensure_within(value, -180.0, 180.0, "Longitude")
}

pub fn validate_latitude_range(range: (f64, f64, f64)) -> Result<(f64, f64, f64), String> {
    validate_latitude(range.0)?;
    validate_latitude(range.1)?;
    Ok(range)
}

pub fn validate_longitude_range(range: (f64, f64, f64)) -> Result<(f64, f64, f64), String> {
    validate_longitude(range.0)?;
    validate_longitude(range.1)?;
    Ok(range)
}

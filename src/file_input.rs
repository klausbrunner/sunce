use crate::input_parsing::parse_datetime;
use crate::types::{DateTimeInput, ParseError};
use std::fs::File;
use std::io::{self, BufRead, BufReader};

pub fn create_file_reader(file_path: &str) -> Result<Box<dyn BufRead>, io::Error> {
    if file_path == "@-" {
        Ok(Box::new(BufReader::new(io::stdin())))
    } else {
        let path = &file_path[1..]; // Remove the '@' prefix
        let file = File::open(path)?;
        Ok(Box::new(BufReader::new(file)))
    }
}

pub fn parse_coordinate_file_line(line: &str) -> Result<(f64, f64), ParseError> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return Err(ParseError::InvalidCoordinate(
            "Empty or comment line".to_string(),
        ));
    }

    let parts: Vec<&str> = if line.contains(',') {
        line.split(',').collect()
    } else {
        line.split_whitespace().collect()
    };

    if parts.len() != 2 {
        return Err(ParseError::InvalidCoordinate(format!(
            "Expected 2 fields, found {} in: {}",
            parts.len(),
            line
        )));
    }

    let lat_str = parts[0];
    let lon_str = parts[1];

    let lat: f64 = lat_str
        .trim()
        .parse()
        .map_err(|_| ParseError::InvalidCoordinate(format!("Invalid latitude: {}", lat_str)))?;
    let lon: f64 = lon_str
        .trim()
        .parse()
        .map_err(|_| ParseError::InvalidCoordinate(format!("Invalid longitude: {}", lon_str)))?;

    // Validate coordinate ranges
    crate::input_parsing::validate_coordinate(lat, "latitude")?;
    crate::input_parsing::validate_coordinate(lon, "longitude")?;

    Ok((lat, lon))
}

pub fn parse_time_file_line(
    line: &str,
    timezone_override: Option<&str>,
) -> Result<DateTimeInput, ParseError> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return Err(ParseError::InvalidDateTime(
            "Empty or comment line".to_string(),
        ));
    }

    parse_datetime(line, timezone_override)
}

pub fn parse_paired_file_line(
    line: &str,
    timezone_override: Option<&str>,
) -> Result<(f64, f64, DateTimeInput), ParseError> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return Err(ParseError::InvalidDateTime(
            "Empty or comment line".to_string(),
        ));
    }

    let parts: Vec<&str> = if line.contains(',') {
        line.split(',').collect()
    } else {
        line.split_whitespace().collect()
    };

    if parts.len() != 3 {
        return Err(ParseError::InvalidDateTime(format!(
            "Expected 3 fields, found {} in: {}",
            parts.len(),
            line
        )));
    }

    let lat_str = parts[0];
    let lon_str = parts[1];
    let dt_str = parts[2];

    let lat: f64 = lat_str
        .trim()
        .parse()
        .map_err(|_| ParseError::InvalidCoordinate(format!("Invalid latitude: {}", lat_str)))?;
    let lon: f64 = lon_str
        .trim()
        .parse()
        .map_err(|_| ParseError::InvalidCoordinate(format!("Invalid longitude: {}", lon_str)))?;
    let datetime = parse_datetime(dt_str.trim(), timezone_override)?;

    // Validate coordinate ranges
    crate::input_parsing::validate_coordinate(lat, "latitude")?;
    crate::input_parsing::validate_coordinate(lon, "longitude")?;

    Ok((lat, lon, datetime))
}

// Iterator for streaming coordinate files
pub struct CoordinateFileIterator {
    reader: Box<dyn BufRead>,
}

impl CoordinateFileIterator {
    pub fn new(reader: Box<dyn BufRead>) -> Self {
        Self { reader }
    }
}

impl Iterator for CoordinateFileIterator {
    type Item = Result<(f64, f64), ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut line = String::new();
        loop {
            line.clear();
            match self.reader.read_line(&mut line) {
                Ok(0) => return None, // EOF
                Ok(_) => {
                    match parse_coordinate_file_line(&line) {
                        Ok(coords) => return Some(Ok(coords)),
                        Err(ParseError::InvalidCoordinate(msg))
                            if msg.contains("Empty or comment") =>
                        {
                            // Skip empty lines and comments
                            continue;
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
                Err(e) => {
                    return Some(Err(ParseError::InvalidCoordinate(format!(
                        "IO error: {}",
                        e
                    ))));
                }
            }
        }
    }
}

// Iterator for streaming time files
pub struct TimeFileIterator {
    reader: Box<dyn BufRead>,
    timezone_override: Option<String>,
}

impl TimeFileIterator {
    pub fn new(reader: Box<dyn BufRead>, timezone_override: Option<String>) -> Self {
        Self {
            reader,
            timezone_override,
        }
    }
}

impl Iterator for TimeFileIterator {
    type Item = Result<DateTimeInput, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut line = String::new();
        loop {
            line.clear();
            match self.reader.read_line(&mut line) {
                Ok(0) => return None, // EOF
                Ok(_) => {
                    match parse_time_file_line(&line, self.timezone_override.as_deref()) {
                        Ok(datetime) => return Some(Ok(datetime)),
                        Err(ParseError::InvalidDateTime(msg))
                            if msg.contains("Empty or comment") =>
                        {
                            // Skip empty lines and comments
                            continue;
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
                Err(e) => {
                    return Some(Err(ParseError::InvalidDateTime(format!("IO error: {}", e))));
                }
            }
        }
    }
}

// Iterator for streaming paired files
pub struct PairedFileIterator {
    reader: Box<dyn BufRead>,
    timezone_override: Option<String>,
}

impl PairedFileIterator {
    pub fn new(reader: Box<dyn BufRead>, timezone_override: Option<String>) -> Self {
        Self {
            reader,
            timezone_override,
        }
    }
}

impl Iterator for PairedFileIterator {
    type Item = Result<(f64, f64, DateTimeInput), ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut line = String::new();
        loop {
            line.clear();
            match self.reader.read_line(&mut line) {
                Ok(0) => return None, // EOF
                Ok(_) => {
                    match parse_paired_file_line(&line, self.timezone_override.as_deref()) {
                        Ok(record) => return Some(Ok(record)),
                        Err(ParseError::InvalidDateTime(msg))
                            if msg.contains("Empty or comment") =>
                        {
                            // Skip empty lines and comments
                            continue;
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
                Err(e) => {
                    return Some(Err(ParseError::InvalidDateTime(format!("IO error: {}", e))));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coordinate_file_parsing() {
        let data = "52.0,13.4\n59.334 18.063\n# comment\n\n40.42,-3.70\n";
        let reader: Box<dyn BufRead> =
            Box::new(BufReader::new(io::Cursor::new(data.as_bytes().to_vec())));
        let mut iter = CoordinateFileIterator::new(reader);

        assert_eq!(iter.next().unwrap().unwrap(), (52.0, 13.4));
        assert_eq!(iter.next().unwrap().unwrap(), (59.334, 18.063));
        assert_eq!(iter.next().unwrap().unwrap(), (40.42, -3.70));
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_coordinate_file_validation() {
        // Test invalid latitude
        let result = parse_coordinate_file_line("95.0,13.4");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("latitude range"));

        // Test invalid longitude
        let result = parse_coordinate_file_line("52.0,200.0");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("longitude range"));

        // Test valid coordinates
        let result = parse_coordinate_file_line("52.0,13.4");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), (52.0, 13.4));
    }

    #[test]
    fn test_time_file_parsing() {
        let data = "2024-06-21T12:00:00\n2024-06-22T12:00:00\n# comment\n\nnow\n";
        let reader: Box<dyn BufRead> =
            Box::new(BufReader::new(io::Cursor::new(data.as_bytes().to_vec())));
        let mut iter = TimeFileIterator::new(reader, None);

        match iter.next().unwrap().unwrap() {
            DateTimeInput::Single(_) => {}
            _ => panic!("Expected single datetime"),
        }
        match iter.next().unwrap().unwrap() {
            DateTimeInput::Single(_) => {}
            _ => panic!("Expected single datetime"),
        }
        match iter.next().unwrap().unwrap() {
            DateTimeInput::Now => {}
            _ => panic!("Expected now"),
        }
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_paired_file_parsing() {
        let data = "52.0,13.4,2024-06-21T12:00:00\n59.334 18.063 2024-06-22T12:00:00\n";
        let reader: Box<dyn BufRead> =
            Box::new(BufReader::new(io::Cursor::new(data.as_bytes().to_vec())));
        let mut iter = PairedFileIterator::new(reader, None);

        let (lat, lon, _) = iter.next().unwrap().unwrap();
        assert_eq!((lat, lon), (52.0, 13.4));

        let (lat, lon, _) = iter.next().unwrap().unwrap();
        assert_eq!((lat, lon), (59.334, 18.063));

        assert!(iter.next().is_none());
    }
}

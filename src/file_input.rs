use crate::input_parsing::parse_datetime;
use crate::types::{DateTimeInput, ParseError};
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::marker::PhantomData;

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

// Type aliases to simplify complex types
type CoordinateParseFn = fn(&str) -> Result<(f64, f64), ParseError>;
type DateTimeParseFn = Box<dyn FnMut(&str) -> Result<DateTimeInput, ParseError>>;
type PairedParseFn = Box<dyn FnMut(&str) -> Result<(f64, f64, DateTimeInput), ParseError>>;

// Generic file iterator that handles line-by-line parsing with error context
pub struct FileIterator<T, F> {
    reader: Box<dyn BufRead>,
    line_number: usize,
    file_path: Option<String>,
    parse_fn: F,
    _phantom: PhantomData<T>,
}

impl<T, F> FileIterator<T, F>
where
    F: FnMut(&str) -> Result<T, ParseError>,
{
    pub fn with_path(reader: Box<dyn BufRead>, parse_fn: F, path: String) -> Self {
        Self {
            reader,
            line_number: 0,
            file_path: Some(path),
            parse_fn,
            _phantom: PhantomData,
        }
    }
}

impl<T, F> Iterator for FileIterator<T, F>
where
    F: FnMut(&str) -> Result<T, ParseError>,
{
    type Item = Result<T, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut line = String::new();
        loop {
            line.clear();
            match self.reader.read_line(&mut line) {
                Ok(0) => return None, // EOF
                Ok(_) => {
                    self.line_number += 1;
                    match (self.parse_fn)(&line) {
                        Ok(value) => return Some(Ok(value)),
                        Err(e) if self.should_skip_error(&e) => {
                            // Skip empty lines and comments
                            continue;
                        }
                        Err(e) => {
                            let context = if let Some(ref path) = self.file_path {
                                format!("{}:{}", path, self.line_number)
                            } else {
                                format!("line {}", self.line_number)
                            };
                            return Some(Err(self.add_context_to_error(e, &context)));
                        }
                    }
                }
                Err(io_err) => {
                    let context = if let Some(ref path) = self.file_path {
                        format!("{}:{}", path, self.line_number + 1)
                    } else {
                        format!("line {}", self.line_number + 1)
                    };
                    return Some(Err(self.io_error_to_parse_error(io_err, &context)));
                }
            }
        }
    }
}

impl<T, F> FileIterator<T, F> {
    fn should_skip_error(&self, error: &ParseError) -> bool {
        match error {
            ParseError::InvalidCoordinate(msg) | ParseError::InvalidDateTime(msg) => {
                msg.contains("Empty or comment")
            }
            _ => false,
        }
    }

    fn add_context_to_error(&self, error: ParseError, context: &str) -> ParseError {
        match error {
            ParseError::InvalidCoordinate(msg) => {
                ParseError::InvalidCoordinate(format!("Error at {}: {}", context, msg))
            }
            ParseError::InvalidDateTime(msg) => {
                ParseError::InvalidDateTime(format!("Error at {}: {}", context, msg))
            }
            other => other,
        }
    }

    fn io_error_to_parse_error(&self, io_err: io::Error, context: &str) -> ParseError {
        // Determine error type based on what we're parsing
        // This is a bit of a hack but maintains backward compatibility
        ParseError::InvalidCoordinate(format!("IO error at {}: {}", context, io_err))
    }
}

// Wrapper for coordinate file iterator to provide compatible API
pub struct CoordinateFileIterator {
    inner: FileIterator<(f64, f64), CoordinateParseFn>,
}

impl CoordinateFileIterator {
    pub fn with_path(reader: Box<dyn BufRead>, path: String) -> Self {
        Self {
            inner: FileIterator::with_path(
                reader,
                parse_coordinate_file_line as CoordinateParseFn,
                path,
            ),
        }
    }
}

impl Iterator for CoordinateFileIterator {
    type Item = Result<(f64, f64), ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

// Time file iterator needs to capture timezone_override in closure
pub struct TimeFileIterator {
    inner: FileIterator<DateTimeInput, DateTimeParseFn>,
}

impl TimeFileIterator {
    pub fn with_path(
        reader: Box<dyn BufRead>,
        timezone_override: Option<String>,
        path: String,
    ) -> Self {
        let parse_fn =
            Box::new(move |line: &str| parse_time_file_line(line, timezone_override.as_deref()));
        Self {
            inner: FileIterator::with_path(reader, parse_fn, path),
        }
    }
}

impl Iterator for TimeFileIterator {
    type Item = Result<DateTimeInput, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

// Paired file iterator needs to capture timezone_override in closure
pub struct PairedFileIterator {
    inner: FileIterator<(f64, f64, DateTimeInput), PairedParseFn>,
}

impl PairedFileIterator {
    pub fn with_path(
        reader: Box<dyn BufRead>,
        timezone_override: Option<String>,
        path: String,
    ) -> Self {
        let parse_fn =
            Box::new(move |line: &str| parse_paired_file_line(line, timezone_override.as_deref()));
        Self {
            inner: FileIterator::with_path(reader, parse_fn, path),
        }
    }
}

impl Iterator for PairedFileIterator {
    type Item = Result<(f64, f64, DateTimeInput), ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
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
        let mut iter = CoordinateFileIterator::with_path(reader, "test.txt".to_string());

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
        let mut iter = TimeFileIterator::with_path(reader, None, "test.txt".to_string());

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
        let mut iter = PairedFileIterator::with_path(reader, None, "test.txt".to_string());

        let (lat, lon, _) = iter.next().unwrap().unwrap();
        assert_eq!((lat, lon), (52.0, 13.4));

        let (lat, lon, _) = iter.next().unwrap().unwrap();
        assert_eq!((lat, lon), (59.334, 18.063));

        assert!(iter.next().is_none());
    }
}

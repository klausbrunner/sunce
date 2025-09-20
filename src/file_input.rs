use crate::parsing::{DateTimeInput, ParseError, parse_datetime};
use std::fs::File;
use std::io::{self, BufRead, BufReader};

pub enum FileReader {
    Stdin(BufReader<io::Stdin>),
    File(BufReader<File>),
    #[cfg(test)]
    Test(BufReader<io::Cursor<Vec<u8>>>),
}

impl BufRead for FileReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        match self {
            FileReader::Stdin(reader) => reader.fill_buf(),
            FileReader::File(reader) => reader.fill_buf(),
            #[cfg(test)]
            FileReader::Test(reader) => reader.fill_buf(),
        }
    }

    fn consume(&mut self, amt: usize) {
        match self {
            FileReader::Stdin(reader) => reader.consume(amt),
            FileReader::File(reader) => reader.consume(amt),
            #[cfg(test)]
            FileReader::Test(reader) => reader.consume(amt),
        }
    }
}

impl io::Read for FileReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            FileReader::Stdin(reader) => reader.read(buf),
            FileReader::File(reader) => reader.read(buf),
            #[cfg(test)]
            FileReader::Test(reader) => reader.read(buf),
        }
    }
}

pub fn create_file_reader(file_path: &str) -> Result<FileReader, io::Error> {
    if file_path == "@-" {
        Ok(FileReader::Stdin(BufReader::new(io::stdin())))
    } else {
        let path = &file_path[1..]; // Remove the '@' prefix
        let file = File::open(path)?;
        Ok(FileReader::File(BufReader::new(file)))
    }
}

fn parse_fields(line: &str, expected_count: usize) -> Result<Vec<&str>, ParseError> {
    let fields: Vec<&str> = if line.contains(',') {
        line.split(',').collect()
    } else {
        line.split_whitespace().collect()
    };

    if fields.len() != expected_count {
        return Err(ParseError::InvalidCoordinate(format!(
            "Expected {} fields, found {} in: {}",
            expected_count,
            fields.len(),
            line
        )));
    }

    Ok(fields)
}

pub fn parse_coordinate_file_line(line: &str) -> Result<(f64, f64), ParseError> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return Err(ParseError::InvalidCoordinate(
            "Empty or comment line".to_string(),
        ));
    }

    let fields = parse_fields(line, 2)?;

    let lat: f64 = fields[0]
        .trim()
        .parse()
        .map_err(|_| ParseError::InvalidCoordinate(format!("Invalid latitude: {}", fields[0])))?;

    let lon: f64 = fields[1]
        .trim()
        .parse()
        .map_err(|_| ParseError::InvalidCoordinate(format!("Invalid longitude: {}", fields[1])))?;

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

    let fields = parse_fields(line, 3).map_err(|_| {
        ParseError::InvalidDateTime(format!("Invalid paired data format: {}", line))
    })?;

    let lat: f64 = fields[0]
        .trim()
        .parse()
        .map_err(|_| ParseError::InvalidCoordinate(format!("Invalid latitude: {}", fields[0])))?;

    let lon: f64 = fields[1]
        .trim()
        .parse()
        .map_err(|_| ParseError::InvalidCoordinate(format!("Invalid longitude: {}", fields[1])))?;

    let datetime = parse_datetime(fields[2].trim(), timezone_override)?;

    Ok((lat, lon, datetime))
}

// Iterator for streaming coordinate files
pub struct CoordinateFileIterator {
    reader: FileReader,
}

impl CoordinateFileIterator {
    pub fn new(reader: FileReader) -> Self {
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
    reader: FileReader,
    timezone_override: Option<String>,
}

impl TimeFileIterator {
    pub fn new(reader: FileReader, timezone_override: Option<String>) -> Self {
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
    reader: FileReader,
    timezone_override: Option<String>,
}

impl PairedFileIterator {
    pub fn new(reader: FileReader, timezone_override: Option<String>) -> Self {
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
        let reader = FileReader::Test(BufReader::new(io::Cursor::new(data.as_bytes().to_vec())));
        let mut iter = CoordinateFileIterator::new(reader);

        assert_eq!(iter.next().unwrap().unwrap(), (52.0, 13.4));
        assert_eq!(iter.next().unwrap().unwrap(), (59.334, 18.063));
        assert_eq!(iter.next().unwrap().unwrap(), (40.42, -3.70));
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_time_file_parsing() {
        let data = "2024-06-21T12:00:00\n2024-06-22T12:00:00\n# comment\n\nnow\n";
        let reader = FileReader::Test(BufReader::new(io::Cursor::new(data.as_bytes().to_vec())));
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
        let reader = FileReader::Test(BufReader::new(io::Cursor::new(data.as_bytes().to_vec())));
        let mut iter = PairedFileIterator::new(reader, None);

        let (lat, lon, _) = iter.next().unwrap().unwrap();
        assert_eq!((lat, lon), (52.0, 13.4));

        let (lat, lon, _) = iter.next().unwrap().unwrap();
        assert_eq!((lat, lon), (59.334, 18.063));

        assert!(iter.next().is_none());
    }
}

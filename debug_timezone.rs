use chrono::*;

fn parse_timezone_offset(tz: &str) -> Result<FixedOffset, String> {
    println!("Input timezone: '{}'", tz);
    println!("Length: {}", tz.len());
    println!("Starts with +/-: {}", tz.starts_with('+') || tz.starts_with('-'));
    println!("Contains colon: {}", tz.contains(':'));
    
    // Handle formats like "+01:00", "-05:00", "+0100", "-0500"
    let normalized =
        if tz.len() == 6 && (tz.starts_with('+') || tz.starts_with('-')) && tz.contains(':') {
            println!("Using format 1 (6 chars with colon)");
            tz.to_string() // Already in +01:00 format
        } else if tz.len() == 5 && tz.contains(':') {
            println!("Using format 2 (5 chars with colon)");
            format!("+{}", tz) // Handle 01:00 -> +01:00
        } else if tz.len() == 5 && (tz.starts_with('+') || tz.starts_with('-')) && !tz.contains(':')
        {
            println!("Using format 3 (5 chars no colon)");
            format!("{}:{}", &tz[..3], &tz[3..]) // Handle +0100 -> +01:00
        } else {
            println!("No matching format!");
            return Err(format!("Invalid format: {}", tz));
        };

    println!("Normalized: '{}'", normalized);
    
    DateTime::parse_from_str("2000-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S")
        .and_then(|_dt| {
            DateTime::parse_from_str(
                &format!("2000-01-01T00:00:00{}", normalized),
                "%Y-%m-%dT%H:%M:%S%:z",
            )
        })
        .map(|dt| *dt.offset())
        .map_err(|e| format!("Parse error: {}", e))
}

fn main() {
    match parse_timezone_offset("+02:00") {
        Ok(offset) => println!("Success: {:?}", offset),
        Err(e) => println!("Error: {}", e),
    }
}

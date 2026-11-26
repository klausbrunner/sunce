//! Sunce's input conventions for Jiff dates, time zones, and durations.

use jiff::{
    SignedDuration, Timestamp, ZonedDateTime,
    civil::{DateTime, Time},
    fmt::temporal::DateTimeParser,
    tz::{AmbiguousOffset, TimeZone},
};

const PARSER: DateTimeParser = DateTimeParser::new();
// Keep the CLI's existing signed-millisecond range for steps.
const MAX_STEP_SECONDS: i64 = i64::MAX / 1000;

/// Resolve a civil time to its first occurrence, or the transition after a gap.
pub(crate) fn at_or_after_local(zone: &TimeZone, datetime: DateTime) -> Option<Timestamp> {
    let ambiguous = zone.to_ambiguous_timestamp(datetime);
    let gap = matches!(ambiguous.offset(), AmbiguousOffset::Gap { .. });
    let earlier = ambiguous.earlier().ok()?;
    if gap {
        // Moving by the gap's length can overshoot when it began before this time.
        zone.following(earlier).next().map(|t| t.timestamp())
    } else {
        Some(earlier)
    }
}

fn parse_time(
    input: &str,
    zone: Option<&TimeZone>,
    local_zone: impl FnOnce() -> TimeZone,
) -> Result<ZonedDateTime, String> {
    let in_zone = |timestamp: Timestamp, default: TimeZone| {
        timestamp.to_zoned_datetime(zone.cloned().unwrap_or(default))
    };
    if let Ok(seconds) = input.parse::<i64>()
        && seconds.unsigned_abs() >= 10_000
    {
        return Timestamp::from_second(seconds)
            .map(|timestamp| in_zone(timestamp, TimeZone::UTC))
            .map_err(|_| format!("Invalid unix timestamp: {seconds}"));
    }

    let invalid_datetime = |err| format!("Invalid datetime '{input}': {err}");
    let pieces = PARSER.parse_pieces(input).map_err(invalid_datetime)?;
    let offset = pieces.to_numeric_offset();
    if offset.is_some() && pieces.time_zone_annotation().is_some() {
        return PARSER
            .parse_zoned_datetime(input)
            .map(|datetime| match zone {
                Some(zone) => datetime.with_time_zone(zone.clone()),
                None => datetime,
            })
            .map_err(invalid_datetime);
    }
    let date = pieces.date();
    let time = pieces.time();
    let civil = date.to_datetime(time.unwrap_or(Time::MIN));
    if let Some(offset) = offset {
        return offset
            .to_timestamp(civil)
            .map(|timestamp| in_zone(timestamp, offset.to_time_zone()))
            .map_err(|err| err.to_string());
    }

    let input_zone = pieces
        .to_time_zone()
        .map_err(invalid_datetime)?
        .or_else(|| zone.cloned())
        .unwrap_or_else(local_zone);
    let timestamp = if time.is_none() {
        at_or_after_local(&input_zone, civil)
    } else {
        let ambiguous = input_zone.to_ambiguous_timestamp(civil);
        if matches!(ambiguous.offset(), AmbiguousOffset::Gap { .. }) {
            None
        } else {
            ambiguous.earlier().ok()
        }
    };
    timestamp
        .filter(|t| time.is_some() || input_zone.to_datetime(*t).date() == date)
        .map(|t| in_zone(t, input_zone))
        .ok_or_else(|| format!("Datetime does not exist in timezone (likely DST gap): {input}"))
}

pub(crate) fn parse_in_timezone(input: &str, zone: &TimeZone) -> Result<ZonedDateTime, String> {
    if input == "now" {
        return Err("Resolve 'now' to an explicit instant before calling the library".into());
    }
    parse_time(input, Some(zone), || TimeZone::UTC)
}

#[cfg(feature = "cli")]
pub(crate) fn parse_input_time(
    input: &str,
    zone: Option<&TimeZone>,
) -> Result<ZonedDateTime, String> {
    if input == "now" {
        return Ok(Timestamp::now().to_zoned_datetime(get_timezone_info(zone)));
    }
    parse_time(input, zone, || get_timezone_info(None))
}

pub(crate) fn parse_duration_positive(input: &str) -> Result<SignedDuration, String> {
    let (value, factor) = if let Ok(seconds) = input.parse::<i64>() {
        (seconds, 1)
    } else {
        let (index, unit) = input
            .char_indices()
            .next_back()
            .ok_or_else(|| "Expected a step such as 1h or 30m".to_owned())?;
        let factor = match unit {
            's' => 1,
            'm' => 60,
            'h' => 3600,
            'd' => 86400,
            _ => {
                return Err(format!(
                    "Invalid step unit in '{input}'. Supported units: s, m, h, d"
                ));
            }
        };
        let value = input[..index].parse::<i64>().map_err(|_| {
            format!("Invalid step value in '{input}'. Use an integer before the unit (e.g., 15m)")
        })?;
        (value, factor)
    };
    if value <= 0 {
        return Err(format!("Step must be positive, got '{input}'"));
    }
    value
        .checked_mul(factor)
        .filter(|seconds| *seconds <= MAX_STEP_SECONDS)
        .map(SignedDuration::from_secs)
        .ok_or_else(|| format!("Step is too large: '{input}'"))
}

pub(crate) fn is_date_without_time(s: &str) -> bool {
    s.len() == 10
        && s.bytes().enumerate().all(|(i, b)| {
            if matches!(i, 4 | 7) {
                b == b'-'
            } else {
                b.is_ascii_digit()
            }
        })
}

#[cfg(feature = "cli")]
pub(crate) fn is_partial_date(s: &str) -> bool {
    match s.len() {
        4 => s.bytes().all(|b| b.is_ascii_digit()),
        7 => s.bytes().enumerate().all(|(i, b)| {
            if i == 4 {
                b == b'-'
            } else {
                b.is_ascii_digit()
            }
        }),
        _ => false,
    }
}

#[cfg(feature = "cli")]
pub(crate) fn parse_timezone_spec(spec: &str) -> Result<TimeZone, String> {
    if spec.trim().is_empty() {
        return Err("Option --timezone requires a value".into());
    }
    PARSER
        .parse_time_zone(spec.trim())
        .map_err(|_| format!("Invalid timezone: '{spec}'"))
}

#[cfg(feature = "cli")]
pub(crate) fn get_timezone_info(zone: Option<&TimeZone>) -> TimeZone {
    zone.cloned()
        .or_else(|| {
            ["TZ", "SUNCE_SYSTEM_TIMEZONE"]
                .into_iter()
                .find_map(|name| parse_timezone_spec(&std::env::var(name).ok()?).ok())
        })
        .unwrap_or_else(TimeZone::system)
}

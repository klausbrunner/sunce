use chrono::{
    DateTime, Duration, FixedOffset, Local, NaiveDate, NaiveDateTime, Offset, TimeZone, Utc,
};
use chrono_tz::Tz;
use iana_time_zone::get_timezone;
use std::env;
use std::sync::OnceLock;

static SYSTEM_TIMEZONE: OnceLock<TimezoneInfo> = OnceLock::new();
const SYSTEM_TZ_OVERRIDE_ENV: &str = "SUNCE_SYSTEM_TIMEZONE";

fn timezone_gap_error(dt_str: &str) -> String {
    format!(
        "Datetime does not exist in timezone (likely DST gap): {}",
        dt_str
    )
}

#[derive(Clone)]
pub enum TimezoneInfo {
    Fixed(FixedOffset),
    Named(Tz),
}

impl TimezoneInfo {
    pub fn to_datetime_from_utc(&self, dt: &NaiveDateTime) -> DateTime<FixedOffset> {
        match self {
            TimezoneInfo::Fixed(offset) => offset.from_utc_datetime(dt),
            TimezoneInfo::Named(tz) => {
                let dt_utc = Utc.from_utc_datetime(dt);
                dt_utc.with_timezone(tz).fixed_offset()
            }
        }
    }

    pub fn to_datetime_from_local(&self, dt: &NaiveDateTime) -> Option<DateTime<FixedOffset>> {
        match self {
            TimezoneInfo::Fixed(offset) => match offset.from_local_datetime(dt) {
                chrono::LocalResult::Single(dt) => Some(dt),
                chrono::LocalResult::Ambiguous(dt1, _dt2) => Some(dt1),
                chrono::LocalResult::None => {
                    let seconds = offset.local_minus_utc() as i64;
                    let utc_naive = *dt - Duration::seconds(seconds);
                    Some(offset.from_utc_datetime(&utc_naive))
                }
            },
            TimezoneInfo::Named(tz) => match tz.from_local_datetime(dt) {
                chrono::LocalResult::None => None,
                chrono::LocalResult::Single(dt) => Some(dt.fixed_offset()),
                chrono::LocalResult::Ambiguous(dt1, _dt2) => Some(dt1.fixed_offset()),
            },
        }
    }
}

pub fn convert_datetime_to_timezone<Tz: TimeZone>(
    dt: DateTime<Tz>,
    tz_info: &TimezoneInfo,
) -> DateTime<FixedOffset> {
    tz_info.to_datetime_from_utc(&dt.naive_utc())
}

pub fn parse_datetime_string(
    dt_str: &str,
    override_tz: Option<&str>,
) -> Result<DateTime<FixedOffset>, String> {
    if dt_str == "now" {
        let tz_info = get_timezone_info(override_tz);
        return Ok(convert_datetime_to_timezone(Utc::now(), &tz_info));
    }

    let has_space_time = dt_str.contains(' ') && dt_str.matches(':').count() >= 1;
    if dt_str.contains('T') || has_space_time {
        if dt_str.ends_with('Z') {
            let utc_dt = dt_str
                .parse::<DateTime<Utc>>()
                .map_err(|e| format!("Failed to parse UTC datetime: {}", e))?;
            let tz_info = get_timezone_info(override_tz);
            return Ok(convert_datetime_to_timezone(utc_dt, &tz_info));
        } else if dt_str.contains('+') || dt_str.rfind('-').is_some_and(|i| i > 10) {
            let fixed_dt = dt_str
                .parse::<DateTime<FixedOffset>>()
                .map_err(|e| format!("Failed to parse datetime with timezone: {}", e))?;

            if override_tz.is_some() {
                let tz_info = get_timezone_info(override_tz);
                return Ok(convert_datetime_to_timezone(fixed_dt, &tz_info));
            }

            return Ok(fixed_dt);
        } else {
            let naive_dt = NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%dT%H:%M:%S")
                .or_else(|_| NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%dT%H:%M"))
                .or_else(|_| NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%d %H:%M:%S"))
                .or_else(|_| NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%d %H:%M"))
                .map_err(|e| format!("Failed to parse naive datetime: {}", e))?;

            let tz_info = get_timezone_info(override_tz);
            return tz_info
                .to_datetime_from_local(&naive_dt)
                .ok_or_else(|| timezone_gap_error(dt_str));
        }
    }

    if let Ok(timestamp) = dt_str.parse::<i64>()
        && timestamp.abs() >= 10000
    {
        let utc_dt = DateTime::<Utc>::from_timestamp(timestamp, 0)
            .ok_or_else(|| format!("Invalid unix timestamp: {}", timestamp))?;

        if override_tz.is_some() {
            let tz_info = get_timezone_info(override_tz);
            return Ok(convert_datetime_to_timezone(utc_dt, &tz_info));
        }

        return Ok(utc_dt.fixed_offset());
    }

    let naive_date = NaiveDate::parse_from_str(dt_str, "%Y-%m-%d")
        .map_err(|e| format!("Failed to parse date: {}", e))?;
    let naive_dt = naive_date
        .and_hms_opt(0, 0, 0)
        .expect("Midnight time creation cannot fail");
    let tz_info = get_timezone_info(override_tz);
    tz_info
        .to_datetime_from_local(&naive_dt)
        .ok_or_else(|| timezone_gap_error(dt_str))
}

pub fn parse_duration_positive(s: &str) -> Result<Duration, String> {
    let ensure_positive = |num: i64| {
        if num <= 0 {
            Err(format!("Step must be positive, got '{}'", s))
        } else {
            Ok(num)
        }
    };

    if let Ok(raw_seconds) = s.parse::<i64>() {
        let seconds = ensure_positive(raw_seconds)?;
        return Ok(Duration::seconds(seconds));
    }

    if s.len() < 2 {
        return Err(format!(
            "Invalid step format: '{}'. Expected <number><unit> such as 1h or 30m",
            s
        ));
    }

    let (num_str, unit) = s.split_at(s.len() - 1);
    let value = num_str.parse::<i64>().map_err(|_| {
        format!(
            "Invalid step value in '{}'. Use an integer before the unit (e.g., 15m)",
            s
        )
    })?;
    let positive = ensure_positive(value)?;

    let duration = match unit {
        "s" => Duration::seconds(positive),
        "m" => Duration::minutes(positive),
        "h" => Duration::hours(positive),
        "d" => Duration::days(positive),
        _ => {
            return Err(format!(
                "Invalid step unit in '{}'. Supported units: s, m, h, d",
                s
            ));
        }
    };

    Ok(duration)
}

pub fn parse_timezone_spec(spec: &str) -> Option<TimezoneInfo> {
    if spec.is_empty() {
        return None;
    }
    parse_tz_offset(spec)
        .map(TimezoneInfo::Fixed)
        .or_else(|| spec.parse::<Tz>().ok().map(TimezoneInfo::Named))
}

fn detect_system_timezone() -> TimezoneInfo {
    parse_timezone_env(env::var(SYSTEM_TZ_OVERRIDE_ENV).ok())
        .or_else(|| parse_timezone_env(get_timezone().ok()))
        .unwrap_or_else(|| TimezoneInfo::Fixed(Local::now().offset().fix()))
}

pub fn get_timezone_info(override_tz: Option<&str>) -> TimezoneInfo {
    parse_timezone_override(override_tz)
        .or_else(|| parse_timezone_env(env::var("TZ").ok()))
        .unwrap_or_else(|| SYSTEM_TIMEZONE.get_or_init(detect_system_timezone).clone())
}

pub fn parse_tz_offset(tz: &str) -> Option<FixedOffset> {
    let (sign, rest) = match tz.as_bytes().first()? {
        b'+' => (1, &tz[1..]),
        b'-' => (-1, &tz[1..]),
        _ => return None,
    };

    let (hours, minutes) = if let Some((h, m)) = rest.split_once(':') {
        (h.parse::<i32>().ok()?, m.parse::<i32>().ok()?)
    } else {
        (rest.parse::<i32>().ok()?, 0)
    };

    FixedOffset::east_opt(sign * (hours * 3600 + minutes * 60))
}

fn parse_timezone_override(spec: Option<&str>) -> Option<TimezoneInfo> {
    spec.map(str::trim).and_then(parse_timezone_spec)
}

fn parse_timezone_env(value: Option<String>) -> Option<TimezoneInfo> {
    value
        .as_deref()
        .map(str::trim)
        .and_then(parse_timezone_spec)
}

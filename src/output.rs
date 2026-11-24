//! Streaming text, CSV and JSON Lines exports to caller-owned writers.

use crate::{Error, EventKind, EventRecord, PositionRecord, SolarPosition};

/// Formats supported by ordinary `std::io::Write` destinations.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    #[default]
    Text,
    Csv,
    Json,
}

impl std::fmt::Display for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Text => "text",
            Self::Csv => "csv",
            Self::Json => "json",
        })
    }
}

impl std::str::FromStr for Format {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "text" => Ok(Self::Text),
            "csv" => Ok(Self::Csv),
            "json" => Ok(Self::Json),
            _ => Err(Error::Input(format!(
                "Invalid text output format: '{value}'"
            ))),
        }
    }
}

/// Formatting controls. Calculation records retain their full precision.
#[derive(Debug, Clone, Copy)]
pub struct Options {
    pub format: Format,
    pub headers: bool,
    pub show_inputs: bool,
    pub elevation_angle: bool,
    pub flush_each_record: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            format: Format::Text,
            headers: true,
            show_inputs: false,
            elevation_angle: false,
            flush_each_record: false,
        }
    }
}

use ahash::AHashMap;
use chrono::{DateTime, FixedOffset, SecondsFormat};
use serde::Serializer;
use serde::ser::SerializeMap;
use std::fmt::Write as _;

pub(crate) const POSITION_TIME_PRECISION: SecondsFormat = SecondsFormat::AutoSi;
pub(crate) const EVENT_TIME_PRECISION: SecondsFormat = SecondsFormat::Secs;
const DATETIME_CACHE_CAPACITY: usize = 2048;
const FIXED_DECIMAL_CACHE_CAPACITY: usize = 256;

type FixedDecimalCache = AHashMap<(u64, u32), String>;

pub(crate) struct DateTimeCache {
    precision: SecondsFormat,
    values: AHashMap<(DateTime<FixedOffset>, i32), String>,
}

impl DateTimeCache {
    pub(crate) fn new(precision: SecondsFormat) -> Self {
        Self {
            precision,
            values: AHashMap::with_capacity(DATETIME_CACHE_CAPACITY),
        }
    }

    pub(crate) fn format(&mut self, dt: &DateTime<FixedOffset>) -> &str {
        // DateTime equality ignores offsets; formatted timestamps must preserve them.
        let key = (*dt, dt.offset().local_minus_utc());
        if self.values.len() >= DATETIME_CACHE_CAPACITY && !self.values.contains_key(&key) {
            self.values.clear();
        }
        self.values
            .entry(key)
            .or_insert_with(|| dt.to_rfc3339_opts(self.precision, false))
    }
}

fn round_f64(value: f64, decimals: u32) -> f64 {
    let factor = 10_f64.powi(decimals as i32);
    (value * factor).round() / factor
}

fn push_u64_decimal(out: &mut String, mut value: u64) {
    let mut buf = [0_u8; 20];
    let mut idx = buf.len();

    loop {
        idx -= 1;
        buf[idx] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }

    out.push_str(std::str::from_utf8(&buf[idx..]).unwrap());
}

fn push_zero_padded_u64(out: &mut String, mut value: u64, width: usize) {
    let mut buf = [b'0'; 20];
    let mut idx = width;

    while idx > 0 {
        idx -= 1;
        buf[idx] = b'0' + (value % 10) as u8;
        value /= 10;
    }

    out.push_str(std::str::from_utf8(&buf[..width]).unwrap());
}

fn format_f64_fixed_into(out: &mut String, value: f64, decimals: u32) {
    out.clear();

    if !value.is_finite() {
        out.push_str(&value.to_string());
        return;
    }

    let (factor, denom) = match decimals {
        0 => (1.0, 1_i64),
        1 => (10.0, 10_i64),
        2 => (100.0, 100_i64),
        3 => (1_000.0, 1_000_i64),
        4 => (10_000.0, 10_000_i64),
        5 => (100_000.0, 100_000_i64),
        _ => (10_f64.powi(decimals as i32), 10_i64.pow(decimals)),
    };

    let scaled = value * factor;
    if scaled.abs() >= i64::MAX as f64 {
        write!(out, "{value:.decimals$}", decimals = decimals as usize)
            .expect("writing to a string cannot fail");
        return;
    }

    let scaled = scaled.round() as i64;
    let abs = scaled.unsigned_abs();
    let int_part = abs / denom as u64;
    let frac_part = abs % denom as u64;

    if decimals == 0 {
        if scaled < 0 {
            out.push('-');
        }
        push_u64_decimal(out, int_part);
        return;
    }

    if scaled < 0 {
        out.push('-');
    }
    push_u64_decimal(out, int_part);
    out.push('.');
    push_zero_padded_u64(out, frac_part, decimals as usize);
}

fn format_f64_fixed(value: f64, decimals: u32) -> String {
    let mut out = String::with_capacity(24);
    format_f64_fixed_into(&mut out, value, decimals);
    out
}

fn ensure_field(out: &mut Vec<String>, idx: usize) -> &mut String {
    if idx == out.len() {
        out.push(String::new());
    }
    let field = &mut out[idx];
    field.clear();
    field
}

fn set_field(out: &mut Vec<String>, idx: usize, value: &str) {
    ensure_field(out, idx).push_str(value);
}

fn set_cached_f64_fixed(
    out: &mut Vec<String>,
    idx: usize,
    cache: &mut FixedDecimalCache,
    value: f64,
    decimals: u32,
) {
    let key = (value.to_bits(), decimals);
    if cache.len() >= FIXED_DECIMAL_CACHE_CAPACITY && !cache.contains_key(&key) {
        cache.clear();
    }
    let formatted = cache
        .entry(key)
        .or_insert_with(|| format_f64_fixed(value, decimals));
    set_field(out, idx, formatted);
}

fn set_formatted_f64(out: &mut Vec<String>, idx: usize, value: f64, decimals: u32) {
    format_f64_fixed_into(ensure_field(out, idx), value, decimals);
}

fn write_json_f64<W: std::io::Write + ?Sized>(writer: &mut W, value: f64) -> Result<(), String> {
    serde_json::to_writer(writer, &value).map_err(|e| e.to_string())
}

impl OutputRow for PositionRecord {
    type Layout = PositionLayout;
    const TIME_PRECISION: SecondsFormat = POSITION_TIME_PRECISION;

    fn headers(layout: Self::Layout) -> Vec<&'static str> {
        layout.csv_headers()
    }

    fn csv_values(
        &self,
        layout: PositionLayout,
        datetime_cache: &mut DateTimeCache,
        fixed_decimal_cache: &mut FixedDecimalCache,
        out: &mut Vec<String>,
    ) {
        let mut idx = 0;

        if layout.show_inputs {
            set_cached_f64_fixed(out, idx, fixed_decimal_cache, self.location.latitude, 5);
            idx += 1;
            set_cached_f64_fixed(out, idx, fixed_decimal_cache, self.location.longitude, 5);
            idx += 1;
            set_cached_f64_fixed(out, idx, fixed_decimal_cache, self.environment.elevation, 3);
            idx += 1;
            if layout.include_refraction {
                set_cached_f64_fixed(out, idx, fixed_decimal_cache, self.environment.pressure, 3);
                idx += 1;
                set_cached_f64_fixed(
                    out,
                    idx,
                    fixed_decimal_cache,
                    self.environment.temperature,
                    3,
                );
                idx += 1;
            }
        }

        set_field(out, idx, datetime_cache.format(&self.datetime));
        idx += 1;

        if layout.show_inputs {
            set_cached_f64_fixed(out, idx, fixed_decimal_cache, self.deltat, 3);
            idx += 1;
        }

        set_formatted_f64(out, idx, self.position.azimuth(), 4);
        idx += 1;
        set_formatted_f64(out, idx, layout.angle(self.position), 4);
        idx += 1;
        out.truncate(idx);
    }

    fn write_json(
        &self,
        layout: PositionLayout,
        writer: &mut dyn std::io::Write,
        datetime_cache: &mut DateTimeCache,
    ) -> Result<(), String> {
        let mut separator = b'{';

        if layout.show_inputs {
            write_position_json_number(
                writer,
                &mut separator,
                b"\"latitude\":",
                self.location.latitude,
            )?;
            write_position_json_number(
                writer,
                &mut separator,
                b"\"longitude\":",
                self.location.longitude,
            )?;
            write_position_json_number(
                writer,
                &mut separator,
                b"\"elevation\":",
                self.environment.elevation,
            )?;
            if layout.include_refraction {
                write_position_json_number(
                    writer,
                    &mut separator,
                    b"\"pressure\":",
                    self.environment.pressure,
                )?;
                write_position_json_number(
                    writer,
                    &mut separator,
                    b"\"temperature\":",
                    self.environment.temperature,
                )?;
            }
        }

        let datetime = datetime_cache.format(&self.datetime);
        write_position_json_string(writer, &mut separator, b"\"dateTime\":\"", datetime)?;

        if layout.show_inputs {
            write_position_json_number(writer, &mut separator, b"\"deltaT\":", self.deltat)?;
        }

        write_position_json_number(
            writer,
            &mut separator,
            b"\"azimuth\":",
            round_f64(self.position.azimuth(), 4),
        )?;
        let angle_key = if layout.elevation_angle {
            b"\"elevation-angle\":" as &[u8]
        } else {
            b"\"zenith\":"
        };
        write_position_json_number(
            writer,
            &mut separator,
            angle_key,
            round_f64(layout.angle(self.position), 4),
        )?;
        writer.write_all(b"}\n").map_err(|e| e.to_string())
    }
}

fn write_position_json_prefix<W: std::io::Write + ?Sized>(
    writer: &mut W,
    separator: &mut u8,
    key: &[u8],
) -> Result<(), String> {
    writer.write_all(&[*separator]).map_err(|e| e.to_string())?;
    *separator = b',';
    writer.write_all(key).map_err(|e| e.to_string())
}

fn write_position_json_number<W: std::io::Write + ?Sized>(
    writer: &mut W,
    separator: &mut u8,
    key: &[u8],
    value: f64,
) -> Result<(), String> {
    write_position_json_prefix(writer, separator, key)?;
    write_json_f64(writer, value)
}

fn write_position_json_string<W: std::io::Write + ?Sized>(
    writer: &mut W,
    separator: &mut u8,
    key: &[u8],
    value: &str,
) -> Result<(), String> {
    write_position_json_prefix(writer, separator, key)?;
    writer
        .write_all(value.as_bytes())
        .map_err(|e| e.to_string())?;
    writer.write_all(b"\"").map_err(|e| e.to_string())
}

impl OutputRow for EventRecord {
    type Layout = EventLayout;
    const TIME_PRECISION: SecondsFormat = EVENT_TIME_PRECISION;

    fn headers(layout: Self::Layout) -> Vec<&'static str> {
        layout.csv_headers()
    }

    fn csv_values(
        &self,
        layout: EventLayout,
        datetime_cache: &mut DateTimeCache,
        fixed_decimal_cache: &mut FixedDecimalCache,
        out: &mut Vec<String>,
    ) {
        let mut idx = 0;
        if layout.show_inputs {
            set_cached_f64_fixed(out, idx, fixed_decimal_cache, self.lat, 5);
            idx += 1;
            set_cached_f64_fixed(out, idx, fixed_decimal_cache, self.lon, 5);
            idx += 1;
        }
        write!(ensure_field(out, idx), "{}", self.date).expect("writing to a string cannot fail");
        idx += 1;
        if layout.show_inputs {
            set_cached_f64_fixed(out, idx, fixed_decimal_cache, self.deltat, 3);
            idx += 1;
        }
        set_field(out, idx, self.day_state.label());
        set_field(out, idx + 1, self.event.map_or("", EventKind::label));
        let time = self
            .time
            .as_ref()
            .map_or("", |time| datetime_cache.format(time));
        set_field(out, idx + 2, time);
        out.truncate(idx + 3);
    }

    fn write_json(
        &self,
        layout: EventLayout,
        writer: &mut dyn std::io::Write,
        datetime_cache: &mut DateTimeCache,
    ) -> Result<(), String> {
        let mut serializer = serde_json::Serializer::new(&mut *writer);
        let mut map = serializer
            .serialize_map(Some(if layout.show_inputs { 7 } else { 4 }))
            .map_err(|e| e.to_string())?;
        if layout.show_inputs {
            map.serialize_entry("latitude", &self.lat)
                .map_err(|e| e.to_string())?;
            map.serialize_entry("longitude", &self.lon)
                .map_err(|e| e.to_string())?;
        }
        map.serialize_entry("date", &self.date.to_string())
            .map_err(|e| e.to_string())?;
        if layout.show_inputs {
            map.serialize_entry("deltaT", &self.deltat)
                .map_err(|e| e.to_string())?;
        }
        map.serialize_entry("day_state", self.day_state.label())
            .map_err(|e| e.to_string())?;
        map.serialize_entry("event", &self.event.map(EventKind::label))
            .map_err(|e| e.to_string())?;
        map.serialize_entry(
            "time",
            &self.time.as_ref().map(|time| datetime_cache.format(time)),
        )
        .map_err(|e| e.to_string())?;
        map.end().map_err(|e| e.to_string())?;
        writeln!(writer).map_err(|e| e.to_string())
    }
}

#[derive(Copy, Clone)]
pub(crate) struct PositionLayout {
    pub show_inputs: bool,
    pub include_refraction: bool,
    elevation_angle: bool,
}

impl PositionLayout {
    pub(crate) fn new(show_inputs: bool, elevation_angle: bool, refraction: bool) -> Self {
        Self {
            show_inputs,
            include_refraction: show_inputs && refraction,
            elevation_angle,
        }
    }

    pub(crate) fn for_records(
        records: impl IntoIterator<Item = Result<PositionRecord, Error>>,
        show_inputs: bool,
        elevation_angle: bool,
    ) -> (Self, impl Iterator<Item = Result<PositionRecord, Error>>) {
        let mut records = records.into_iter().peekable();
        let refraction = show_inputs
            && match records.peek() {
                Some(Ok(row)) => row.environment.refraction,
                _ => true,
            };
        let records = records.map(move |row| {
            let row = row?;
            if show_inputs && row.environment.refraction != refraction {
                return Err(Error::Output(
                    "Cannot export input columns with mixed refraction settings".into(),
                ));
            }
            Ok(row)
        });
        (Self::new(show_inputs, elevation_angle, refraction), records)
    }

    pub(crate) fn angle(self, position: SolarPosition) -> f64 {
        if self.elevation_angle {
            position.elevation_angle()
        } else {
            position.zenith_angle()
        }
    }

    pub(crate) fn angle_label(self) -> &'static str {
        if self.elevation_angle {
            "elevation-angle"
        } else {
            "zenith"
        }
    }

    pub(crate) fn csv_headers(self) -> Vec<&'static str> {
        let mut headers = Vec::with_capacity(if self.show_inputs {
            if self.include_refraction { 9 } else { 7 }
        } else {
            3
        });

        if self.show_inputs {
            headers.extend(["latitude", "longitude", "elevation"]);
            if self.include_refraction {
                headers.extend(["pressure", "temperature"]);
            }
            headers.extend(["dateTime", "deltaT"]);
        } else {
            headers.push("dateTime");
        }

        headers.push("azimuth");
        headers.push(self.angle_label());
        headers
    }
}

#[derive(Copy, Clone)]
pub(crate) struct EventLayout {
    pub show_inputs: bool,
}

impl EventLayout {
    pub(crate) fn csv_headers(self) -> Vec<&'static str> {
        let mut headers = if self.show_inputs {
            vec!["latitude", "longitude", "date", "deltaT"]
        } else {
            vec!["date"]
        };
        headers.extend(["day_state", "event", "time"]);
        headers
    }
}

trait OutputRow: Sized {
    type Layout: Copy;
    const TIME_PRECISION: SecondsFormat;

    fn headers(layout: Self::Layout) -> Vec<&'static str>;
    fn csv_values(
        &self,
        layout: Self::Layout,
        datetime_cache: &mut DateTimeCache,
        fixed_decimal_cache: &mut FixedDecimalCache,
        out: &mut Vec<String>,
    );
    fn write_json(
        &self,
        layout: Self::Layout,
        writer: &mut dyn std::io::Write,
        datetime_cache: &mut DateTimeCache,
    ) -> Result<(), String>;
}

fn write_csv_line<W: std::io::Write, I, S>(writer: &mut W, fields: I) -> std::io::Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut first = true;
    for field in fields {
        if !first {
            writer.write_all(b",")?;
        }
        first = false;
        writer.write_all(field.as_ref().as_bytes())?;
    }
    writer.write_all(b"\n")
}

fn is_numeric_column(name: &str) -> bool {
    matches!(
        name,
        "latitude"
            | "longitude"
            | "elevation"
            | "pressure"
            | "temperature"
            | "deltaT"
            | "azimuth"
            | "zenith"
            | "elevation-angle"
    )
}

fn suggested_column_width(name: &str) -> usize {
    match name {
        "latitude" | "longitude" => 10,
        "elevation" => 9,
        "pressure" | "temperature" | "deltaT" => 10,
        "dateTime" | "time" => 25,
        "date" => 10,
        "day_state" => 10,
        "event" => 18,
        "azimuth" | "zenith" | "elevation-angle" => 10,
        _ => name.len(),
    }
}

fn write_pretty_header<W: std::io::Write>(
    writer: &mut W,
    headers: &[&str],
    widths: &[usize],
) -> std::io::Result<()> {
    for (idx, (header, width)) in headers.iter().zip(widths).enumerate() {
        if idx > 0 {
            write!(writer, "  ")?;
        }
        write!(writer, "{:<width$}", header, width = *width)?;
    }
    writeln!(writer)?;

    for (idx, width) in widths.iter().enumerate() {
        if idx > 0 {
            write!(writer, "  ")?;
        }
        write!(writer, "{}", "-".repeat(*width))?;
    }
    writeln!(writer)
}

fn write_pretty_row<W: std::io::Write>(
    writer: &mut W,
    headers: &[&str],
    widths: &[usize],
    values: &[String],
) -> std::io::Result<()> {
    for (idx, ((header, width), value)) in headers.iter().zip(widths).zip(values).enumerate() {
        if idx > 0 {
            write!(writer, "  ")?;
        }
        if is_numeric_column(header) {
            write!(writer, "{:>width$}", value, width = *width)?;
        } else {
            write!(writer, "{:<width$}", value, width = *width)?;
        }
    }
    writeln!(writer)
}

/// Export positions, including filtered, limited or previously collected records.
///
/// Input columns use each record's environment and require consistent refraction
/// settings across records. A final flush is attempted; errors propagate.
/// Enable the `parquet` feature for the separate Parquet exporters.
pub fn write_positions<I, W>(records: I, options: Options, destination: W) -> Result<usize, Error>
where
    I: IntoIterator<Item = Result<PositionRecord, Error>>,
    W: std::io::Write,
{
    let (layout, records) =
        PositionLayout::for_records(records, options.show_inputs, options.elevation_angle);
    write_position_rows(records, options, layout, destination)
}

pub(crate) fn write_position_rows(
    records: impl IntoIterator<Item = Result<PositionRecord, Error>>,
    options: Options,
    layout: PositionLayout,
    destination: impl std::io::Write,
) -> Result<usize, Error> {
    write_buffered(records, options, layout, destination)
}

/// Export solar events to text, CSV or JSON Lines, flushing on completion.
pub fn write_events<I, W>(records: I, options: Options, destination: W) -> Result<usize, Error>
where
    I: IntoIterator<Item = Result<EventRecord, Error>>,
    W: std::io::Write,
{
    write_buffered(
        records,
        options,
        EventLayout {
            show_inputs: options.show_inputs,
        },
        destination,
    )
}

fn header_widths(headers: &[&str], values: &[String]) -> Vec<usize> {
    headers
        .iter()
        .zip(values)
        .map(|(header, value)| {
            header
                .len()
                .max(suggested_column_width(header))
                .max(value.len())
        })
        .collect()
}

fn write_buffered<W: std::io::Write, R: OutputRow>(
    results: impl IntoIterator<Item = Result<R, Error>>,
    options: Options,
    layout: R::Layout,
    destination: W,
) -> Result<usize, Error> {
    use std::io::{BufWriter, Write};
    let mut writer = BufWriter::new(destination);
    let headers = R::headers(layout);
    if options.format == Format::Csv && options.headers {
        write_csv_line(&mut writer, &headers)?;
    }
    let mut count = 0;
    let mut datetime_cache = DateTimeCache::new(R::TIME_PRECISION);
    let mut fixed_decimal_cache = FixedDecimalCache::with_capacity(FIXED_DECIMAL_CACHE_CAPACITY);
    let mut row_values = Vec::new();
    let mut widths = Vec::new();

    for result in results {
        let row = result?;
        match options.format {
            Format::Text | Format::Csv => {
                row.csv_values(
                    layout,
                    &mut datetime_cache,
                    &mut fixed_decimal_cache,
                    &mut row_values,
                );
                if options.format == Format::Text {
                    if count == 0 {
                        widths = header_widths(&headers, &row_values);
                        if options.headers {
                            write_pretty_header(&mut writer, &headers, &widths)?;
                        }
                    }
                    write_pretty_row(&mut writer, &headers, &widths, &row_values)?;
                } else {
                    write_csv_line(&mut writer, row_values.iter())?;
                }
            }
            Format::Json => row
                .write_json(layout, &mut writer, &mut datetime_cache)
                .map_err(Error::Output)?,
        }
        count += 1;
        if options.flush_each_record {
            writer.flush()?;
        }
    }

    writer.flush()?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{FixedOffset, TimeZone};
    use std::io::Write;

    #[derive(Default)]
    struct MockWriter {
        buf: Vec<u8>,
        flushes: usize,
        fail_flush: bool,
    }

    impl Write for MockWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.buf.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.flushes += 1;
            if self.fail_flush {
                Err(std::io::Error::other("flush failed"))
            } else {
                Ok(())
            }
        }
    }

    fn position_result(datetime: DateTime<FixedOffset>) -> PositionRecord {
        PositionRecord {
            location: crate::Location {
                latitude: 52.0,
                longitude: 13.4,
            },
            datetime,
            position: SolarPosition::new(180.12345, 45.98765).unwrap(),
            deltat: 69.123,
            environment: crate::Environment::default(),
        }
    }

    #[test]
    fn streaming_text_flushes_each_record_when_requested() {
        let tz = FixedOffset::east_opt(0).unwrap();
        let dt = tz.with_ymd_and_hms(2024, 6, 21, 12, 0, 0).unwrap();

        let options = Options {
            flush_each_record: true,
            ..Options::default()
        };
        let mut writer = MockWriter::default();
        write_positions([Ok(position_result(dt))], options, &mut writer).expect("write succeeds");

        assert_eq!(writer.flushes, 2); // After the record and at completion.
        assert!(!writer.buf.is_empty());
    }

    #[test]
    fn dispatch_reports_final_flush_failure() {
        let tz = FixedOffset::east_opt(0).unwrap();
        let dt = tz.with_ymd_and_hms(2024, 6, 21, 12, 0, 0).unwrap();
        let options = Options {
            format: Format::Csv,
            ..Options::default()
        };
        let writer = MockWriter {
            fail_flush: true,
            ..MockWriter::default()
        };

        let err = write_positions([Ok(position_result(dt))], options, writer)
            .expect_err("flush failure must be reported");

        assert_eq!(err.to_string(), "flush failed");
    }

    #[test]
    fn formatting_caches_stay_bounded() {
        let tz = FixedOffset::east_opt(0).unwrap();
        let start = tz.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let mut datetime_cache = DateTimeCache::new(POSITION_TIME_PRECISION);
        for seconds in 0..=DATETIME_CACHE_CAPACITY {
            datetime_cache.format(&(start + chrono::Duration::seconds(seconds as i64)));
        }
        assert!(datetime_cache.values.len() <= DATETIME_CACHE_CAPACITY);

        let mut decimal_cache = FixedDecimalCache::new();
        let mut fields = Vec::new();
        for value in 0..=FIXED_DECIMAL_CACHE_CAPACITY {
            set_cached_f64_fixed(&mut fields, 0, &mut decimal_cache, value as f64, 3);
        }
        assert!(decimal_cache.len() <= FIXED_DECIMAL_CACHE_CAPACITY);
    }

    #[test]
    fn fixed_formatter_handles_large_finite_values() {
        assert_eq!(format_f64_fixed(-1e20, 3), "-100000000000000000000.000");
    }

    fn split_csv_line(buf: Vec<u8>) -> Vec<String> {
        String::from_utf8(buf)
            .expect("valid utf8")
            .trim_end_matches('\n')
            .split(',')
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn position_text_and_csv_field_order_match() {
        let tz = FixedOffset::east_opt(0).unwrap();
        let dt = tz.with_ymd_and_hms(2024, 6, 21, 12, 0, 0).unwrap();
        let row = position_result(dt);
        let layout = PositionLayout {
            show_inputs: true,
            include_refraction: true,
            elevation_angle: true,
        };

        let mut values = Vec::new();
        let mut datetime_cache = DateTimeCache::new(POSITION_TIME_PRECISION);
        let mut decimal_cache = FixedDecimalCache::new();
        row.csv_values(layout, &mut datetime_cache, &mut decimal_cache, &mut values);

        let expected = vec![
            "52.00000".to_string(),
            "13.40000".to_string(),
            "0.000".to_string(),
            "1013.250".to_string(),
            "15.000".to_string(),
            "2024-06-21T12:00:00+00:00".to_string(),
            "69.123".to_string(),
            "180.1235".to_string(),
            "44.0124".to_string(),
        ];
        assert_eq!(values, expected);

        let mut bytes = Vec::new();
        write_csv_line(&mut bytes, values.iter()).expect("csv write");
        assert_eq!(split_csv_line(bytes), expected);
    }

    #[test]
    fn events_text_and_csv_field_order_match() {
        let tz = FixedOffset::east_opt(0).unwrap();
        let dt = tz.with_ymd_and_hms(2024, 6, 21, 0, 0, 0).unwrap();
        let row = EventRecord {
            lat: 52.0,
            lon: 13.4,
            date: dt.date_naive(),
            deltat: 69.123,
            day_state: crate::events::DayState::Crossing,
            event: Some(EventKind::Sunrise),
            time: Some(dt + chrono::Duration::hours(4)),
        };

        let mut values = Vec::new();
        let mut datetime_cache = DateTimeCache::new(EVENT_TIME_PRECISION);
        let mut decimal_cache = FixedDecimalCache::new();
        row.csv_values(
            EventLayout { show_inputs: true },
            &mut datetime_cache,
            &mut decimal_cache,
            &mut values,
        );

        let expected = vec![
            "52.00000",
            "13.40000",
            "2024-06-21",
            "69.123",
            "CROSSING",
            "sunrise",
            "2024-06-21T04:00:00+00:00",
        ];
        assert_eq!(values, expected);

        let mut bytes = Vec::new();
        write_csv_line(&mut bytes, values.iter()).expect("csv write");
        assert_eq!(split_csv_line(bytes), expected);
    }
}

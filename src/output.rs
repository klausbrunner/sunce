//! Output formatting for CSV, JSON, and text table formats.

use crate::compute::CalculationResult;
use crate::data::{Command, OutputFormat, Parameters};
use crate::error::OutputError;
use chrono::{DateTime, FixedOffset};
use serde::Serializer;
use serde::ser::SerializeMap;
use solar_positioning::SunriseResult;
use std::collections::HashMap;

const RFC3339_NO_MILLIS: &str = "%Y-%m-%dT%H:%M:%S%:z";

type DateTimeCache = HashMap<DateTime<FixedOffset>, String>;
type FixedDecimalCache = HashMap<(u64, u32), String>;

pub(crate) fn format_rfc3339(dt: &DateTime<FixedOffset>) -> String {
    dt.format(RFC3339_NO_MILLIS).to_string()
}

fn round_f64(value: f64, decimals: u32) -> f64 {
    let factor = 10_f64.powi(decimals as i32);
    (value * factor).round() / factor
}

fn format_f64_fixed(value: f64, decimals: u32) -> String {
    if !value.is_finite() {
        return value.to_string();
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

    let scaled = (value * factor).round() as i64;
    let abs = scaled.abs();
    let int_part = abs / denom;
    let frac_part = abs % denom;

    if decimals == 0 {
        if scaled < 0 {
            format!("-{int_part}")
        } else {
            int_part.to_string()
        }
    } else if scaled < 0 {
        format!(
            "-{int_part}.{:0width$}",
            frac_part,
            width = decimals as usize
        )
    } else {
        format!(
            "{int_part}.{:0width$}",
            frac_part,
            width = decimals as usize
        )
    }
}

fn cached_f64_fixed(cache: &mut FixedDecimalCache, value: f64, decimals: u32) -> String {
    let key = (value.to_bits(), decimals);
    if let Some(cached) = cache.get(&key) {
        return cached.clone();
    }
    let formatted = format_f64_fixed(value, decimals);
    cache.insert(key, formatted.clone());
    formatted
}

fn cached_datetime(cache: &mut DateTimeCache, dt: &DateTime<FixedOffset>) -> String {
    cache
        .entry(*dt)
        .or_insert_with(|| format_rfc3339(dt))
        .clone()
}

fn cached_optional_datetime(
    cache: &mut DateTimeCache,
    dt: Option<&DateTime<FixedOffset>>,
) -> String {
    dt.map(|dt| cached_datetime(cache, dt)).unwrap_or_default()
}

fn extract_sunrise_times<T>(result: &SunriseResult<T>) -> (Option<&T>, &T, Option<&T>) {
    match result {
        SunriseResult::RegularDay {
            sunrise,
            transit,
            sunset,
        } => (Some(sunrise), transit, Some(sunset)),
        SunriseResult::AllDay { transit } | SunriseResult::AllNight { transit } => {
            (None, transit, None)
        }
    }
}

fn sunrise_type_str(result: &SunriseResult<impl std::any::Any>) -> &'static str {
    match result {
        SunriseResult::RegularDay { .. } => "NORMAL",
        SunriseResult::AllDay { .. } => "ALL_DAY",
        SunriseResult::AllNight { .. } => "ALL_NIGHT",
    }
}

#[derive(Clone)]
pub(crate) struct PositionRow {
    pub lat: f64,
    pub lon: f64,
    pub datetime: DateTime<FixedOffset>,
    pub deltat: f64,
    pub azimuth: f64,
    pub zenith: f64,
}

impl PositionRow {
    pub(crate) fn angle(&self, elevation_angle: bool) -> f64 {
        if elevation_angle {
            90.0 - self.zenith
        } else {
            self.zenith
        }
    }

    fn fill_csv_values(
        &self,
        params: &Parameters,
        layout: PositionLayout,
        datetime_cache: &mut DateTimeCache,
        fixed_decimal_cache: &mut FixedDecimalCache,
        out: &mut Vec<String>,
    ) {
        out.clear();

        if layout.show_inputs {
            out.push(cached_f64_fixed(fixed_decimal_cache, self.lat, 5));
            out.push(cached_f64_fixed(fixed_decimal_cache, self.lon, 5));
            out.push(cached_f64_fixed(
                fixed_decimal_cache,
                params.environment.elevation,
                3,
            ));
            if layout.include_refraction {
                out.push(cached_f64_fixed(
                    fixed_decimal_cache,
                    params.environment.pressure,
                    3,
                ));
                out.push(cached_f64_fixed(
                    fixed_decimal_cache,
                    params.environment.temperature,
                    3,
                ));
            }
        }

        out.push(cached_datetime(datetime_cache, &self.datetime));

        if layout.show_inputs {
            out.push(cached_f64_fixed(fixed_decimal_cache, self.deltat, 3));
        }

        out.push(format_f64_fixed(self.azimuth, 4));
        out.push(format_f64_fixed(
            self.angle(layout.uses_elevation_angle()),
            4,
        ));
    }

    fn write_json_line(
        &self,
        params: &Parameters,
        layout: PositionLayout,
        writer: &mut dyn std::io::Write,
        datetime_cache: &mut DateTimeCache,
    ) -> Result<(), String> {
        let field_count = match (layout.show_inputs, layout.include_refraction) {
            (true, true) => 9,
            (true, false) => 7,
            (false, _) => 3,
        };

        let mut serializer = serde_json::Serializer::new(&mut *writer);
        let mut map = serializer
            .serialize_map(Some(field_count))
            .map_err(|e| e.to_string())?;

        if layout.show_inputs {
            map.serialize_entry("latitude", &self.lat)
                .map_err(|e| e.to_string())?;
            map.serialize_entry("longitude", &self.lon)
                .map_err(|e| e.to_string())?;
            map.serialize_entry("elevation", &params.environment.elevation)
                .map_err(|e| e.to_string())?;
            if layout.include_refraction {
                map.serialize_entry("pressure", &params.environment.pressure)
                    .map_err(|e| e.to_string())?;
                map.serialize_entry("temperature", &params.environment.temperature)
                    .map_err(|e| e.to_string())?;
            }
        }

        map.serialize_entry("dateTime", &cached_datetime(datetime_cache, &self.datetime))
            .map_err(|e| e.to_string())?;

        if layout.show_inputs {
            map.serialize_entry("deltaT", &self.deltat)
                .map_err(|e| e.to_string())?;
        }

        map.serialize_entry("azimuth", &round_f64(self.azimuth, 4))
            .map_err(|e| e.to_string())?;
        map.serialize_entry(
            layout.angle_label(),
            &round_f64(self.angle(layout.uses_elevation_angle()), 4),
        )
        .map_err(|e| e.to_string())?;
        map.end().map_err(|e| e.to_string())?;
        writeln!(writer).map_err(|e| e.to_string())
    }
}

#[derive(Clone)]
pub(crate) struct SunriseRow {
    pub lat: f64,
    pub lon: f64,
    pub date_time: DateTime<FixedOffset>,
    pub deltat: f64,
    pub type_label: &'static str,
    pub sunrise: Option<DateTime<FixedOffset>>,
    pub transit: DateTime<FixedOffset>,
    pub sunset: Option<DateTime<FixedOffset>>,
    pub civil_start: Option<DateTime<FixedOffset>>,
    pub civil_end: Option<DateTime<FixedOffset>>,
    pub nautical_start: Option<DateTime<FixedOffset>>,
    pub nautical_end: Option<DateTime<FixedOffset>>,
    pub astro_start: Option<DateTime<FixedOffset>>,
    pub astro_end: Option<DateTime<FixedOffset>>,
}

impl SunriseRow {
    fn fill_csv_values(
        &self,
        _params: &Parameters,
        layout: SunriseLayout,
        datetime_cache: &mut DateTimeCache,
        fixed_decimal_cache: &mut FixedDecimalCache,
        out: &mut Vec<String>,
    ) {
        out.clear();

        if layout.show_inputs {
            out.push(cached_f64_fixed(fixed_decimal_cache, self.lat, 5));
            out.push(cached_f64_fixed(fixed_decimal_cache, self.lon, 5));
            out.push(cached_datetime(datetime_cache, &self.date_time));
            out.push(cached_f64_fixed(fixed_decimal_cache, self.deltat, 3));
        } else {
            out.push(cached_datetime(datetime_cache, &self.date_time));
        }

        out.push(self.type_label.to_string());
        out.push(cached_optional_datetime(
            datetime_cache,
            self.sunrise.as_ref(),
        ));
        out.push(cached_datetime(datetime_cache, &self.transit));
        out.push(cached_optional_datetime(
            datetime_cache,
            self.sunset.as_ref(),
        ));

        if layout.include_twilight {
            out.push(cached_optional_datetime(
                datetime_cache,
                self.civil_start.as_ref(),
            ));
            out.push(cached_optional_datetime(
                datetime_cache,
                self.civil_end.as_ref(),
            ));
            out.push(cached_optional_datetime(
                datetime_cache,
                self.nautical_start.as_ref(),
            ));
            out.push(cached_optional_datetime(
                datetime_cache,
                self.nautical_end.as_ref(),
            ));
            out.push(cached_optional_datetime(
                datetime_cache,
                self.astro_start.as_ref(),
            ));
            out.push(cached_optional_datetime(
                datetime_cache,
                self.astro_end.as_ref(),
            ));
        }
    }

    fn write_json_line(
        &self,
        _params: &Parameters,
        layout: SunriseLayout,
        writer: &mut dyn std::io::Write,
        datetime_cache: &mut DateTimeCache,
    ) -> Result<(), String> {
        let field_count =
            if layout.show_inputs { 8 } else { 5 } + usize::from(layout.include_twilight) * 6;

        let mut serializer = serde_json::Serializer::new(&mut *writer);
        let mut map = serializer
            .serialize_map(Some(field_count))
            .map_err(|e| e.to_string())?;

        if layout.show_inputs {
            map.serialize_entry("latitude", &self.lat)
                .map_err(|e| e.to_string())?;
            map.serialize_entry("longitude", &self.lon)
                .map_err(|e| e.to_string())?;
            map.serialize_entry(
                "dateTime",
                &cached_datetime(datetime_cache, &self.date_time),
            )
            .map_err(|e| e.to_string())?;
            map.serialize_entry("deltaT", &self.deltat)
                .map_err(|e| e.to_string())?;
        } else {
            map.serialize_entry(
                "dateTime",
                &cached_datetime(datetime_cache, &self.date_time),
            )
            .map_err(|e| e.to_string())?;
        }

        map.serialize_entry("type", &self.type_label)
            .map_err(|e| e.to_string())?;
        map.serialize_entry(
            "sunrise",
            &self
                .sunrise
                .as_ref()
                .map(|dt| cached_datetime(datetime_cache, dt)),
        )
        .map_err(|e| e.to_string())?;
        map.serialize_entry("transit", &cached_datetime(datetime_cache, &self.transit))
            .map_err(|e| e.to_string())?;
        map.serialize_entry(
            "sunset",
            &self
                .sunset
                .as_ref()
                .map(|dt| cached_datetime(datetime_cache, dt)),
        )
        .map_err(|e| e.to_string())?;

        if layout.include_twilight {
            map.serialize_entry(
                "civil_start",
                &self
                    .civil_start
                    .as_ref()
                    .map(|dt| cached_datetime(datetime_cache, dt)),
            )
            .map_err(|e| e.to_string())?;
            map.serialize_entry(
                "civil_end",
                &self
                    .civil_end
                    .as_ref()
                    .map(|dt| cached_datetime(datetime_cache, dt)),
            )
            .map_err(|e| e.to_string())?;
            map.serialize_entry(
                "nautical_start",
                &self
                    .nautical_start
                    .as_ref()
                    .map(|dt| cached_datetime(datetime_cache, dt)),
            )
            .map_err(|e| e.to_string())?;
            map.serialize_entry(
                "nautical_end",
                &self
                    .nautical_end
                    .as_ref()
                    .map(|dt| cached_datetime(datetime_cache, dt)),
            )
            .map_err(|e| e.to_string())?;
            map.serialize_entry(
                "astronomical_start",
                &self
                    .astro_start
                    .as_ref()
                    .map(|dt| cached_datetime(datetime_cache, dt)),
            )
            .map_err(|e| e.to_string())?;
            map.serialize_entry(
                "astronomical_end",
                &self
                    .astro_end
                    .as_ref()
                    .map(|dt| cached_datetime(datetime_cache, dt)),
            )
            .map_err(|e| e.to_string())?;
        }

        map.end().map_err(|e| e.to_string())?;
        writeln!(writer).map_err(|e| e.to_string())
    }
}

pub(crate) fn position_angle_label(elevation_angle: bool) -> &'static str {
    if elevation_angle {
        "elevation-angle"
    } else {
        "zenith"
    }
}

#[derive(Copy, Clone)]
pub(crate) struct PositionLayout {
    pub show_inputs: bool,
    pub include_refraction: bool,
    elevation_angle: bool,
}

impl PositionLayout {
    pub(crate) fn from_params(params: &Parameters) -> Self {
        Self {
            show_inputs: params.output.should_show_inputs(),
            include_refraction: params.environment.refraction,
            elevation_angle: params.output.elevation_angle,
        }
    }

    pub(crate) fn uses_elevation_angle(self) -> bool {
        self.elevation_angle
    }

    pub(crate) fn angle_label(self) -> &'static str {
        position_angle_label(self.elevation_angle)
    }

    fn csv_headers(self) -> Vec<&'static str> {
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
pub(crate) struct SunriseLayout {
    pub show_inputs: bool,
    pub include_twilight: bool,
}

impl SunriseLayout {
    pub(crate) fn from_params(params: &Parameters) -> Self {
        Self {
            show_inputs: params.output.should_show_inputs(),
            include_twilight: params.calculation.twilight,
        }
    }

    fn csv_headers(self) -> Vec<&'static str> {
        let mut headers = Vec::with_capacity(if self.show_inputs {
            if self.include_twilight { 14 } else { 8 }
        } else if self.include_twilight {
            11
        } else {
            5
        });

        if self.show_inputs {
            headers.extend(["latitude", "longitude", "dateTime", "deltaT"]);
        } else {
            headers.push("dateTime");
        }

        headers.extend(["type", "sunrise", "transit", "sunset"]);
        if self.include_twilight {
            headers.extend([
                "civil_start",
                "civil_end",
                "nautical_start",
                "nautical_end",
                "astronomical_start",
                "astronomical_end",
            ]);
        }
        headers
    }
}

trait OutputRowExt: Sized {
    type Layout: Copy;

    fn normalize(result: &CalculationResult) -> Option<Self>;
    fn headers(layout: Self::Layout) -> Vec<&'static str>;
    fn csv_values(
        &self,
        params: &Parameters,
        layout: Self::Layout,
        datetime_cache: &mut DateTimeCache,
        fixed_decimal_cache: &mut FixedDecimalCache,
        out: &mut Vec<String>,
    );
    fn write_json(
        &self,
        params: &Parameters,
        layout: Self::Layout,
        writer: &mut dyn std::io::Write,
        datetime_cache: &mut DateTimeCache,
    ) -> Result<(), String>;
    fn unexpected_output_error() -> OutputError;
}

impl OutputRowExt for PositionRow {
    type Layout = PositionLayout;

    fn normalize(result: &CalculationResult) -> Option<Self> {
        normalize_position_result(result)
    }

    fn headers(layout: Self::Layout) -> Vec<&'static str> {
        layout.csv_headers()
    }

    fn csv_values(
        &self,
        params: &Parameters,
        layout: Self::Layout,
        datetime_cache: &mut DateTimeCache,
        fixed_decimal_cache: &mut FixedDecimalCache,
        out: &mut Vec<String>,
    ) {
        self.fill_csv_values(params, layout, datetime_cache, fixed_decimal_cache, out);
    }

    fn write_json(
        &self,
        params: &Parameters,
        layout: Self::Layout,
        writer: &mut dyn std::io::Write,
        datetime_cache: &mut DateTimeCache,
    ) -> Result<(), String> {
        self.write_json_line(params, layout, writer, datetime_cache)
    }

    fn unexpected_output_error() -> OutputError {
        OutputError::from("Unexpected calculation result for position output")
    }
}

impl OutputRowExt for SunriseRow {
    type Layout = SunriseLayout;

    fn normalize(result: &CalculationResult) -> Option<Self> {
        normalize_sunrise_result(result)
    }

    fn headers(layout: Self::Layout) -> Vec<&'static str> {
        layout.csv_headers()
    }

    fn csv_values(
        &self,
        params: &Parameters,
        layout: Self::Layout,
        datetime_cache: &mut DateTimeCache,
        fixed_decimal_cache: &mut FixedDecimalCache,
        out: &mut Vec<String>,
    ) {
        self.fill_csv_values(params, layout, datetime_cache, fixed_decimal_cache, out);
    }

    fn write_json(
        &self,
        params: &Parameters,
        layout: Self::Layout,
        writer: &mut dyn std::io::Write,
        datetime_cache: &mut DateTimeCache,
    ) -> Result<(), String> {
        self.write_json_line(params, layout, writer, datetime_cache)
    }

    fn unexpected_output_error() -> OutputError {
        OutputError::from("Unexpected calculation result for sunrise output")
    }
}

pub(crate) fn normalize_position_result(result: &CalculationResult) -> Option<PositionRow> {
    if let CalculationResult::Position {
        lat,
        lon,
        datetime,
        position,
        deltat,
    } = result
    {
        Some(PositionRow {
            lat: *lat,
            lon: *lon,
            datetime: *datetime,
            deltat: *deltat,
            azimuth: position.azimuth(),
            zenith: position.zenith_angle(),
        })
    } else {
        None
    }
}

pub(crate) fn normalize_sunrise_result(result: &CalculationResult) -> Option<SunriseRow> {
    match result {
        CalculationResult::Sunrise {
            lat,
            lon,
            date,
            result,
            deltat,
        } => {
            let (sunrise, transit, sunset) = extract_sunrise_times(result);
            Some(SunriseRow {
                lat: *lat,
                lon: *lon,
                date_time: *date,
                deltat: *deltat,
                type_label: sunrise_type_str(result),
                sunrise: sunrise.copied(),
                transit: *transit,
                sunset: sunset.copied(),
                civil_start: None,
                civil_end: None,
                nautical_start: None,
                nautical_end: None,
                astro_start: None,
                astro_end: None,
            })
        }
        CalculationResult::SunriseWithTwilight {
            lat,
            lon,
            date,
            sunrise_sunset,
            civil,
            nautical,
            astronomical,
            deltat,
        } => {
            let (sunrise, transit, sunset) = extract_sunrise_times(sunrise_sunset);
            let (civil_start, _, civil_end) = extract_sunrise_times(civil);
            let (nautical_start, _, nautical_end) = extract_sunrise_times(nautical);
            let (astro_start, _, astro_end) = extract_sunrise_times(astronomical);

            Some(SunriseRow {
                lat: *lat,
                lon: *lon,
                date_time: *date,
                deltat: *deltat,
                type_label: sunrise_type_str(sunrise_sunset),
                sunrise: sunrise.copied(),
                transit: *transit,
                sunset: sunset.copied(),
                civil_start: civil_start.copied(),
                civil_end: civil_end.copied(),
                nautical_start: nautical_start.copied(),
                nautical_end: nautical_end.copied(),
                astro_start: astro_start.copied(),
                astro_end: astro_end.copied(),
            })
        }
        _ => None,
    }
}

#[cfg(feature = "parquet")]
pub fn write_parquet_output<W: std::io::Write + Send>(
    results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
    command: Command,
    params: &Parameters,
    writer: W,
) -> std::io::Result<usize> {
    crate::parquet::write_parquet(results, command, params, writer)
}

fn write_csv_line<W: std::io::Write, I, S>(writer: &mut W, fields: I) -> Result<(), String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut first = true;
    for field in fields {
        if !first {
            writer.write_all(b",").map_err(|e| e.to_string())?;
        }
        first = false;
        writer
            .write_all(field.as_ref().as_bytes())
            .map_err(|e| e.to_string())?;
    }
    writer.write_all(b"\n").map_err(|e| e.to_string())
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
        "dateTime" => 25,
        "azimuth" | "zenith" | "elevation-angle" => 10,
        "type" => 8,
        "sunrise" | "transit" | "sunset" | "civil_start" | "civil_end" | "nautical_start"
        | "nautical_end" | "astronomical_start" | "astronomical_end" => 25,
        _ => name.len(),
    }
}

fn write_pretty_header<W: std::io::Write>(
    writer: &mut W,
    headers: &[&str],
    widths: &[usize],
) -> Result<(), OutputError> {
    for (idx, (header, width)) in headers.iter().zip(widths).enumerate() {
        if idx > 0 {
            write!(writer, "  ").map_err(OutputError::from)?;
        }
        write!(writer, "{:<width$}", header, width = *width).map_err(OutputError::from)?;
    }
    writeln!(writer).map_err(OutputError::from)?;

    for (idx, width) in widths.iter().enumerate() {
        if idx > 0 {
            write!(writer, "  ").map_err(OutputError::from)?;
        }
        write!(writer, "{}", "-".repeat(*width)).map_err(OutputError::from)?;
    }
    writeln!(writer).map_err(OutputError::from)
}

fn write_pretty_row<W: std::io::Write>(
    writer: &mut W,
    headers: &[&str],
    widths: &[usize],
    values: &[String],
) -> Result<(), OutputError> {
    for (idx, ((header, width), value)) in headers.iter().zip(widths).zip(values).enumerate() {
        if idx > 0 {
            write!(writer, "  ").map_err(OutputError::from)?;
        }
        if is_numeric_column(header) {
            write!(writer, "{:>width$}", value, width = *width).map_err(OutputError::from)?;
        } else {
            write!(writer, "{:<width$}", value, width = *width).map_err(OutputError::from)?;
        }
    }
    writeln!(writer).map_err(OutputError::from)
}

pub fn dispatch_output(
    results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
    command: Command,
    params: &Parameters,
    flush_each_record: bool,
) -> Result<usize, OutputError> {
    match params.output.format {
        #[cfg(feature = "parquet")]
        OutputFormat::Parquet => {
            let stdout = std::io::stdout();
            return write_parquet_output(results, command, params, stdout)
                .map_err(|e| OutputError(e.to_string()));
        }
        _ => {}
    }

    use std::io::{BufWriter, Write};
    let stdout = std::io::stdout();
    let mut writer = BufWriter::new(stdout.lock());
    let result = match command {
        Command::Position => write_rows::<_, PositionRow>(
            results,
            params,
            PositionLayout::from_params(params),
            &mut writer,
            flush_each_record,
        ),
        Command::Sunrise => write_rows::<_, SunriseRow>(
            results,
            params,
            SunriseLayout::from_params(params),
            &mut writer,
            flush_each_record,
        ),
    };
    let _ = writer.flush();
    result
}

fn row_from_result<R: OutputRowExt>(
    result: Result<CalculationResult, String>,
) -> Result<R, OutputError> {
    R::normalize(&result.map_err(OutputError::from)?).ok_or_else(R::unexpected_output_error)
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

fn write_rows<W: std::io::Write, R: OutputRowExt>(
    results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
    params: &Parameters,
    layout: R::Layout,
    writer: &mut W,
    flush_each: bool,
) -> Result<usize, OutputError> {
    let headers = R::headers(layout);
    let mut count = 0;
    let mut header_written = false;
    let mut datetime_cache = DateTimeCache::with_capacity(2048);
    let mut fixed_decimal_cache = FixedDecimalCache::with_capacity(256);
    let mut row_values = Vec::new();

    if params.output.format == OutputFormat::Text {
        let mut iter = results;
        let Some(first) = iter.next() else {
            return Ok(0);
        };

        let first_row = row_from_result::<R>(first)?;
        first_row.csv_values(
            params,
            layout,
            &mut datetime_cache,
            &mut fixed_decimal_cache,
            &mut row_values,
        );

        let widths = header_widths(&headers, &row_values);

        if params.output.headers {
            write_pretty_header(writer, &headers, &widths)?;
        }
        write_pretty_row(writer, &headers, &widths, &row_values)?;
        count += 1;
        if flush_each {
            writer.flush().map_err(OutputError::from)?;
        }

        for result in iter {
            let row = row_from_result::<R>(result)?;
            row.csv_values(
                params,
                layout,
                &mut datetime_cache,
                &mut fixed_decimal_cache,
                &mut row_values,
            );
            write_pretty_row(writer, &headers, &widths, &row_values)?;
            count += 1;
            if flush_each {
                writer.flush().map_err(OutputError::from)?;
            }
        }

        return Ok(count);
    }

    for result in results {
        let row = row_from_result::<R>(result)?;

        match params.output.format {
            OutputFormat::Csv => {
                if params.output.headers && !header_written {
                    write_csv_line(writer, headers.iter()).map_err(OutputError::from)?;
                    header_written = true;
                }
                row.csv_values(
                    params,
                    layout,
                    &mut datetime_cache,
                    &mut fixed_decimal_cache,
                    &mut row_values,
                );
                write_csv_line(writer, row_values.iter()).map_err(OutputError::from)?;
            }
            OutputFormat::Json => row
                .write_json(params, layout, writer, &mut datetime_cache)
                .map_err(OutputError::from)?,
            OutputFormat::Text => unreachable!("handled above"),
            #[cfg(feature = "parquet")]
            OutputFormat::Parquet => return Err(OutputError::from("Unsupported format")),
        }

        count += 1;
        if flush_each {
            writer.flush().map_err(OutputError::from)?;
        }
    }

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::OutputFormat;
    use crate::data::config::OutputOptions;
    use chrono::{FixedOffset, TimeZone};
    use std::io::Write;

    #[derive(Default)]
    struct MockWriter {
        buf: Vec<u8>,
        flushes: usize,
    }

    impl Write for MockWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.buf.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.flushes += 1;
            Ok(())
        }
    }

    #[test]
    fn streaming_text_flushes_each_record_when_requested() {
        let tz = FixedOffset::east_opt(0).unwrap();
        let dt = tz.with_ymd_and_hms(2024, 6, 21, 12, 0, 0).unwrap();

        let params = Parameters {
            output: OutputOptions {
                format: OutputFormat::Text,
                ..OutputOptions::default()
            },
            ..Parameters::default()
        };

        let calc = crate::position::calculate_position(52.0, 13.4, dt, &params).unwrap();
        let results: Box<dyn Iterator<Item = Result<CalculationResult, String>>> =
            Box::new(vec![Ok(calc)].into_iter());

        let mut writer = MockWriter::default();
        write_rows::<_, PositionRow>(
            results,
            &params,
            PositionLayout::from_params(&params),
            &mut writer,
            true,
        )
        .expect("write succeeds");

        assert_eq!(writer.flushes, 1);
        assert!(!writer.buf.is_empty());
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
        let params = Parameters {
            output: OutputOptions {
                show_inputs: Some(true),
                elevation_angle: true,
                ..OutputOptions::default()
            },
            ..Parameters::default()
        };
        let row = PositionRow {
            lat: 52.0,
            lon: 13.4,
            datetime: dt,
            deltat: 69.123,
            azimuth: 180.12345,
            zenith: 45.98765,
        };

        let mut values = Vec::new();
        let mut datetime_cache = DateTimeCache::new();
        let mut decimal_cache = FixedDecimalCache::new();
        row.csv_values(
            &params,
            PositionLayout::from_params(&params),
            &mut datetime_cache,
            &mut decimal_cache,
            &mut values,
        );

        let expected = vec![
            "52.00000".to_string(),
            "13.40000".to_string(),
            "0.000".to_string(),
            "1013.000".to_string(),
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
    fn sunrise_text_and_csv_field_order_match() {
        let tz = FixedOffset::east_opt(0).unwrap();
        let dt = tz.with_ymd_and_hms(2024, 6, 21, 0, 0, 0).unwrap();
        let params = Parameters {
            output: OutputOptions {
                show_inputs: Some(true),
                ..OutputOptions::default()
            },
            calculation: crate::data::config::CalculationOptions {
                twilight: true,
                ..crate::data::config::CalculationOptions::default()
            },
            ..Parameters::default()
        };
        let row = SunriseRow {
            lat: 52.0,
            lon: 13.4,
            date_time: dt,
            deltat: 69.123,
            type_label: "NORMAL",
            sunrise: Some(dt + chrono::Duration::hours(4)),
            transit: dt + chrono::Duration::hours(12),
            sunset: Some(dt + chrono::Duration::hours(20)),
            civil_start: Some(dt + chrono::Duration::hours(3)),
            civil_end: Some(dt + chrono::Duration::hours(21)),
            nautical_start: Some(dt + chrono::Duration::hours(2)),
            nautical_end: Some(dt + chrono::Duration::hours(22)),
            astro_start: Some(dt + chrono::Duration::hours(1)),
            astro_end: Some(dt + chrono::Duration::hours(23)),
        };

        let mut values = Vec::new();
        let mut datetime_cache = DateTimeCache::new();
        let mut decimal_cache = FixedDecimalCache::new();
        row.csv_values(
            &params,
            SunriseLayout::from_params(&params),
            &mut datetime_cache,
            &mut decimal_cache,
            &mut values,
        );

        let expected = vec![
            "52.00000".to_string(),
            "13.40000".to_string(),
            "2024-06-21T00:00:00+00:00".to_string(),
            "69.123".to_string(),
            "NORMAL".to_string(),
            "2024-06-21T04:00:00+00:00".to_string(),
            "2024-06-21T12:00:00+00:00".to_string(),
            "2024-06-21T20:00:00+00:00".to_string(),
            "2024-06-21T03:00:00+00:00".to_string(),
            "2024-06-21T21:00:00+00:00".to_string(),
            "2024-06-21T02:00:00+00:00".to_string(),
            "2024-06-21T22:00:00+00:00".to_string(),
            "2024-06-21T01:00:00+00:00".to_string(),
            "2024-06-21T23:00:00+00:00".to_string(),
        ];
        assert_eq!(values, expected);

        let mut bytes = Vec::new();
        write_csv_line(&mut bytes, values.iter()).expect("csv write");
        assert_eq!(split_csv_line(bytes), expected);
    }
}

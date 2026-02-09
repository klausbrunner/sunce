//! Output formatting for CSV, JSON, and text table formats.

use crate::compute::CalculationResult;
use crate::data::{Command, OutputFormat, Parameters};
use crate::error::OutputError;
use chrono::{DateTime, FixedOffset};
use serde::{Serializer, ser::SerializeMap};
use solar_positioning::SunriseResult;

// Helper functions for time formatting
const RFC3339_NO_MILLIS: &str = "%Y-%m-%dT%H:%M:%S%:z";

pub(crate) fn format_rfc3339(dt: &DateTime<FixedOffset>) -> String {
    dt.format(RFC3339_NO_MILLIS).to_string()
}

fn format_datetime_opt(dt: Option<&DateTime<FixedOffset>>) -> String {
    dt.map_or(String::new(), format_rfc3339)
}

fn round_f64(value: f64, decimals: u32) -> f64 {
    let factor = 10_f64.powi(decimals as i32);
    (value * factor).round() / factor
}

type FixedDecimalCache = std::collections::HashMap<(u64, u32), String>;

fn cached_f64_fixed(cache: &mut FixedDecimalCache, value: f64, decimals: u32) -> String {
    let key = (value.to_bits(), decimals);
    if let Some(cached) = cache.get(&key) {
        return cached.clone();
    }
    let formatted = format_f64_fixed(value, decimals);
    cache.insert(key, formatted.clone());
    formatted
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
    let width = decimals as usize;

    if decimals == 0 {
        if scaled < 0 {
            format!("-{}", int_part)
        } else {
            int_part.to_string()
        }
    } else if scaled < 0 {
        format!("-{}.{:0width$}", int_part, frac_part, width = width)
    } else {
        format!("{}.{:0width$}", int_part, frac_part, width = width)
    }
}

// Helper function to extract times from SunriseResult
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

// Helper function to get type string from SunriseResult
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
    pub fn angle(&self, elevation_angle: bool) -> f64 {
        if elevation_angle {
            90.0 - self.zenith
        } else {
            self.zenith
        }
    }
}

#[derive(Copy, Clone)]
enum AngleKind {
    Zenith,
    Elevation,
}

pub(crate) fn position_angle_label(elevation_angle: bool) -> &'static str {
    if elevation_angle {
        "elevation-angle"
    } else {
        "zenith"
    }
}

impl AngleKind {
    fn from_params(params: &Parameters) -> Self {
        if params.output.elevation_angle {
            AngleKind::Elevation
        } else {
            AngleKind::Zenith
        }
    }

    fn label(&self) -> &'static str {
        position_angle_label(matches!(self, AngleKind::Elevation))
    }
}

#[derive(Copy, Clone)]
pub(crate) struct PositionLayout {
    pub show_inputs: bool,
    pub include_refraction: bool,
    angle_kind: AngleKind,
}

impl PositionLayout {
    pub(crate) fn from_params(params: &Parameters) -> Self {
        Self {
            show_inputs: params.output.should_show_inputs(),
            include_refraction: params.environment.refraction,
            angle_kind: AngleKind::from_params(params),
        }
    }

    pub(crate) fn uses_elevation_angle(self) -> bool {
        matches!(self.angle_kind, AngleKind::Elevation)
    }

    pub(crate) fn angle_label(self) -> &'static str {
        self.angle_kind.label()
    }

    fn csv_headers(self) -> Vec<&'static str> {
        let mut headers = Vec::new();
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
        let mut header = Vec::new();
        if self.show_inputs {
            header.extend(["latitude", "longitude", "dateTime", "deltaT"]);
        } else {
            header.push("dateTime");
        }
        header.extend(["type", "sunrise", "transit", "sunset"]);
        if self.include_twilight {
            header.extend([
                "civil_start",
                "civil_end",
                "nautical_start",
                "nautical_end",
                "astronomical_start",
                "astronomical_end",
            ]);
        }
        header
    }
}

struct PositionFields {
    layout: PositionLayout,
    angle_value: f64,
    lat: f64,
    lon: f64,
    elevation: f64,
    refraction: Option<(f64, f64)>,
    datetime: DateTime<FixedOffset>,
    deltat: f64,
    azimuth: f64,
}

fn position_fields(
    row: &PositionRow,
    params: &Parameters,
    layout: PositionLayout,
) -> PositionFields {
    PositionFields {
        layout,
        angle_value: row.angle(layout.uses_elevation_angle()),
        lat: row.lat,
        lon: row.lon,
        elevation: params.environment.elevation,
        refraction: params
            .environment
            .refraction
            .then_some((params.environment.pressure, params.environment.temperature)),
        datetime: row.datetime,
        deltat: row.deltat,
        azimuth: row.azimuth,
    }
}

impl JsonFields for PositionFields {
    fn map_len(&self) -> usize {
        let mut len = 3; // dateTime, azimuth, angle
        if self.layout.show_inputs {
            len += 4; // latitude, longitude, elevation, deltaT
            if self.layout.include_refraction {
                len += 2; // pressure, temperature
            }
        }
        len
    }

    fn serialize_into_map<S: SerializeMap>(
        &self,
        map: &mut S,
        datetime_cache: &mut std::collections::HashMap<DateTime<FixedOffset>, String>,
    ) -> Result<(), S::Error> {
        if self.layout.show_inputs {
            map.serialize_entry("latitude", &self.lat)?;
            map.serialize_entry("longitude", &self.lon)?;
            map.serialize_entry("elevation", &self.elevation)?;
            if self.layout.include_refraction {
                let (pressure, temperature) = self.refraction.expect("refraction values set");
                map.serialize_entry("pressure", &pressure)?;
                map.serialize_entry("temperature", &temperature)?;
            }
        }
        map.serialize_entry("dateTime", &cached_datetime(datetime_cache, &self.datetime))?;
        if self.layout.show_inputs {
            map.serialize_entry("deltaT", &self.deltat)?;
        }
        let azimuth = round_f64(self.azimuth, 4);
        let angle = round_f64(self.angle_value, 4);
        map.serialize_entry("azimuth", &azimuth)?;
        map.serialize_entry(self.layout.angle_label(), &angle)?;
        Ok(())
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

struct SunriseFields {
    layout: SunriseLayout,
    lat: f64,
    lon: f64,
    date_time: DateTime<FixedOffset>,
    deltat: f64,
    type_label: &'static str,
    sunrise: Option<DateTime<FixedOffset>>,
    transit: DateTime<FixedOffset>,
    sunset: Option<DateTime<FixedOffset>>,
    civil_start: Option<DateTime<FixedOffset>>,
    civil_end: Option<DateTime<FixedOffset>>,
    nautical_start: Option<DateTime<FixedOffset>>,
    nautical_end: Option<DateTime<FixedOffset>>,
    astro_start: Option<DateTime<FixedOffset>>,
    astro_end: Option<DateTime<FixedOffset>>,
}

trait JsonFields {
    fn map_len(&self) -> usize;
    fn serialize_into_map<S: SerializeMap>(
        &self,
        map: &mut S,
        datetime_cache: &mut std::collections::HashMap<DateTime<FixedOffset>, String>,
    ) -> Result<(), S::Error>;
}

fn write_json_fields(
    fields: &impl JsonFields,
    writer: &mut dyn std::io::Write,
    datetime_cache: &mut std::collections::HashMap<DateTime<FixedOffset>, String>,
) -> Result<(), String> {
    let mut serializer = serde_json::Serializer::new(&mut *writer);
    let mut map = serializer
        .serialize_map(Some(fields.map_len()))
        .map_err(|e| e.to_string())?;
    fields
        .serialize_into_map(&mut map, datetime_cache)
        .map_err(|e| e.to_string())?;
    map.end().map_err(|e| e.to_string())?;
    writeln!(writer).map_err(|e| e.to_string())
}

enum OutputRow {
    Position(PositionFields),
    Sunrise(SunriseFields),
}

impl OutputRow {
    fn csv_headers(&self) -> Vec<&'static str> {
        match self {
            OutputRow::Position(fields) => fields.layout.csv_headers(),
            OutputRow::Sunrise(fields) => fields.layout.csv_headers(),
        }
    }

    fn write_json(
        &self,
        writer: &mut dyn std::io::Write,
        datetime_cache: &mut std::collections::HashMap<DateTime<FixedOffset>, String>,
    ) -> Result<(), String> {
        match self {
            OutputRow::Position(fields) => write_json_fields(fields, writer, datetime_cache),
            OutputRow::Sunrise(fields) => write_json_fields(fields, writer, datetime_cache),
        }
    }

    fn csv_values_into(
        &self,
        datetime_cache: &mut std::collections::HashMap<DateTime<FixedOffset>, String>,
        fixed_decimal_cache: &mut FixedDecimalCache,
        out: &mut Vec<String>,
    ) {
        match self {
            OutputRow::Position(fields) => {
                position_csv_values_into(fields, datetime_cache, fixed_decimal_cache, out)
            }
            OutputRow::Sunrise(fields) => sunrise_csv_values_into(fields, fixed_decimal_cache, out),
        }
    }

    fn write_csv<W: std::io::Write>(
        &self,
        writer: &mut W,
        datetime_cache: &mut std::collections::HashMap<DateTime<FixedOffset>, String>,
        fixed_decimal_cache: &mut FixedDecimalCache,
    ) -> Result<(), String> {
        match self {
            OutputRow::Position(fields) => {
                write_position_csv(writer, fields, datetime_cache, fixed_decimal_cache)
            }
            OutputRow::Sunrise(fields) => write_sunrise_csv(writer, fields, fixed_decimal_cache),
        }
    }
}

fn cached_datetime(
    cache: &mut std::collections::HashMap<DateTime<FixedOffset>, String>,
    dt: &DateTime<FixedOffset>,
) -> String {
    cache
        .entry(*dt)
        .or_insert_with(|| format_rfc3339(dt))
        .clone()
}

fn position_csv_values_into(
    fields: &PositionFields,
    datetime_cache: &mut std::collections::HashMap<DateTime<FixedOffset>, String>,
    fixed_decimal_cache: &mut FixedDecimalCache,
    values: &mut Vec<String>,
) {
    let capacity: usize = if fields.layout.show_inputs {
        if fields.layout.include_refraction {
            9
        } else {
            7
        }
    } else {
        3
    };
    values.clear();
    values.reserve(capacity.saturating_sub(values.capacity()));

    if fields.layout.show_inputs {
        values.push(cached_f64_fixed(fixed_decimal_cache, fields.lat, 5));
        values.push(cached_f64_fixed(fixed_decimal_cache, fields.lon, 5));
        values.push(cached_f64_fixed(fixed_decimal_cache, fields.elevation, 3));
        if fields.layout.include_refraction {
            let (pressure, temperature) = fields.refraction.expect("refraction values set");
            values.push(cached_f64_fixed(fixed_decimal_cache, pressure, 3));
            values.push(cached_f64_fixed(fixed_decimal_cache, temperature, 3));
        }
    }

    values.push(cached_datetime(datetime_cache, &fields.datetime));

    if fields.layout.show_inputs {
        values.push(cached_f64_fixed(fixed_decimal_cache, fields.deltat, 3));
    }

    values.push(format_f64_fixed(fields.azimuth, 4));
    values.push(format_f64_fixed(fields.angle_value, 4));
}

fn sunrise_csv_values_into(
    fields: &SunriseFields,
    fixed_decimal_cache: &mut FixedDecimalCache,
    values: &mut Vec<String>,
) {
    let capacity: usize = if fields.layout.show_inputs {
        if fields.layout.include_twilight {
            14
        } else {
            8
        }
    } else if fields.layout.include_twilight {
        11
    } else {
        5
    };
    values.clear();
    values.reserve(capacity.saturating_sub(values.capacity()));

    if fields.layout.show_inputs {
        values.push(cached_f64_fixed(fixed_decimal_cache, fields.lat, 5));
        values.push(cached_f64_fixed(fixed_decimal_cache, fields.lon, 5));
        values.push(format_rfc3339(&fields.date_time));
        values.push(cached_f64_fixed(fixed_decimal_cache, fields.deltat, 3));
    } else {
        values.push(format_rfc3339(&fields.date_time));
    }

    let sunrise_str = format_datetime_opt(fields.sunrise.as_ref());
    let transit_str = format_rfc3339(&fields.transit);
    let sunset_str = format_datetime_opt(fields.sunset.as_ref());

    values.push(fields.type_label.to_string());
    values.push(sunrise_str);
    values.push(transit_str);
    values.push(sunset_str);

    if fields.layout.include_twilight {
        let format_twilight =
            |dt: Option<&DateTime<FixedOffset>>| dt.map(format_rfc3339).unwrap_or_default();
        values.push(format_twilight(fields.civil_start.as_ref()));
        values.push(format_twilight(fields.civil_end.as_ref()));
        values.push(format_twilight(fields.nautical_start.as_ref()));
        values.push(format_twilight(fields.nautical_end.as_ref()));
        values.push(format_twilight(fields.astro_start.as_ref()));
        values.push(format_twilight(fields.astro_end.as_ref()));
    }
}

fn sunrise_fields(row: &SunriseRow, layout: SunriseLayout) -> SunriseFields {
    SunriseFields {
        layout,
        lat: row.lat,
        lon: row.lon,
        date_time: row.date_time,
        deltat: row.deltat,
        type_label: row.type_label,
        sunrise: row.sunrise,
        transit: row.transit,
        sunset: row.sunset,
        civil_start: row.civil_start,
        civil_end: row.civil_end,
        nautical_start: row.nautical_start,
        nautical_end: row.nautical_end,
        astro_start: row.astro_start,
        astro_end: row.astro_end,
    }
}

impl JsonFields for SunriseFields {
    fn map_len(&self) -> usize {
        let mut len = if self.layout.show_inputs { 8 } else { 5 }; // base fields
        if self.layout.include_twilight {
            len += 6;
        }
        len
    }

    fn serialize_into_map<S: SerializeMap>(
        &self,
        map: &mut S,
        datetime_cache: &mut std::collections::HashMap<DateTime<FixedOffset>, String>,
    ) -> Result<(), S::Error> {
        if self.layout.show_inputs {
            map.serialize_entry("latitude", &self.lat)?;
            map.serialize_entry("longitude", &self.lon)?;
            map.serialize_entry(
                "dateTime",
                &cached_datetime(datetime_cache, &self.date_time),
            )?;
            map.serialize_entry("deltaT", &self.deltat)?;
        } else {
            map.serialize_entry(
                "dateTime",
                &cached_datetime(datetime_cache, &self.date_time),
            )?;
        }
        map.serialize_entry("type", &self.type_label)?;

        map.serialize_entry(
            "sunrise",
            &self
                .sunrise
                .as_ref()
                .map(|dt| cached_datetime(datetime_cache, dt)),
        )?;
        map.serialize_entry("transit", &cached_datetime(datetime_cache, &self.transit))?;
        map.serialize_entry(
            "sunset",
            &self
                .sunset
                .as_ref()
                .map(|dt| cached_datetime(datetime_cache, dt)),
        )?;

        if self.layout.include_twilight {
            map.serialize_entry(
                "civil_start",
                &self
                    .civil_start
                    .as_ref()
                    .map(|dt| cached_datetime(datetime_cache, dt)),
            )?;
            map.serialize_entry(
                "civil_end",
                &self
                    .civil_end
                    .as_ref()
                    .map(|dt| cached_datetime(datetime_cache, dt)),
            )?;
            map.serialize_entry(
                "nautical_start",
                &self
                    .nautical_start
                    .as_ref()
                    .map(|dt| cached_datetime(datetime_cache, dt)),
            )?;
            map.serialize_entry(
                "nautical_end",
                &self
                    .nautical_end
                    .as_ref()
                    .map(|dt| cached_datetime(datetime_cache, dt)),
            )?;
            map.serialize_entry(
                "astronomical_start",
                &self
                    .astro_start
                    .as_ref()
                    .map(|dt| cached_datetime(datetime_cache, dt)),
            )?;
            map.serialize_entry(
                "astronomical_end",
                &self
                    .astro_end
                    .as_ref()
                    .map(|dt| cached_datetime(datetime_cache, dt)),
            )?;
        }
        Ok(())
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
        return Some(PositionRow {
            lat: *lat,
            lon: *lon,
            datetime: *datetime,
            deltat: *deltat,
            azimuth: position.azimuth(),
            zenith: position.zenith_angle(),
        });
    }
    None
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
            let type_label = sunrise_type_str(result);
            let (sunrise_opt, transit, sunset_opt) = extract_sunrise_times(result);
            Some(SunriseRow {
                lat: *lat,
                lon: *lon,
                date_time: *date,
                deltat: *deltat,
                type_label,
                sunrise: sunrise_opt.copied(),
                transit: *transit,
                sunset: sunset_opt.copied(),
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
            let type_label = sunrise_type_str(sunrise_sunset);
            let (sunrise_opt, transit, sunset_opt) = extract_sunrise_times(sunrise_sunset);
            let (civil_start_opt, _, civil_end_opt) = extract_sunrise_times(civil);
            let (nautical_start_opt, _, nautical_end_opt) = extract_sunrise_times(nautical);
            let (astro_start_opt, _, astro_end_opt) = extract_sunrise_times(astronomical);

            Some(SunriseRow {
                lat: *lat,
                lon: *lon,
                date_time: *date,
                deltat: *deltat,
                type_label,
                sunrise: sunrise_opt.copied(),
                transit: *transit,
                sunset: sunset_opt.copied(),
                civil_start: civil_start_opt.copied(),
                civil_end: civil_end_opt.copied(),
                nautical_start: nautical_start_opt.copied(),
                nautical_end: nautical_end_opt.copied(),
                astro_start: astro_start_opt.copied(),
                astro_end: astro_end_opt.copied(),
            })
        }
        _ => None,
    }
}

fn to_output_row(
    result: &CalculationResult,
    params: &Parameters,
    command: Command,
    position_layout: PositionLayout,
    sunrise_layout: SunriseLayout,
) -> Result<OutputRow, String> {
    match command {
        Command::Position => normalize_position_result(result)
            .ok_or_else(|| "Unexpected calculation result for position output".to_string())
            .map(|row| OutputRow::Position(position_fields(&row, params, position_layout))),
        Command::Sunrise => normalize_sunrise_result(result)
            .ok_or_else(|| "Unexpected calculation result for sunrise output".to_string())
            .map(|row| OutputRow::Sunrise(sunrise_fields(&row, sunrise_layout))),
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
        if first {
            first = false;
        } else {
            writer.write_all(b",").map_err(|e| e.to_string())?;
        }
        writer
            .write_all(field.as_ref().as_bytes())
            .map_err(|e| e.to_string())?;
    }
    writer.write_all(b"\n").map_err(|e| e.to_string())
}

fn write_csv_field<W: std::io::Write>(
    writer: &mut W,
    first: &mut bool,
    value: &str,
) -> Result<(), String> {
    if *first {
        *first = false;
    } else {
        writer.write_all(b",").map_err(|e| e.to_string())?;
    }
    writer
        .write_all(value.as_bytes())
        .map_err(|e| e.to_string())
}

fn write_position_csv<W: std::io::Write>(
    writer: &mut W,
    fields: &PositionFields,
    datetime_cache: &mut std::collections::HashMap<DateTime<FixedOffset>, String>,
    fixed_decimal_cache: &mut FixedDecimalCache,
) -> Result<(), String> {
    let mut first = true;

    if fields.layout.show_inputs {
        let lat = cached_f64_fixed(fixed_decimal_cache, fields.lat, 5);
        write_csv_field(writer, &mut first, &lat)?;
        let lon = cached_f64_fixed(fixed_decimal_cache, fields.lon, 5);
        write_csv_field(writer, &mut first, &lon)?;
        let elevation = cached_f64_fixed(fixed_decimal_cache, fields.elevation, 3);
        write_csv_field(writer, &mut first, &elevation)?;
        if fields.layout.include_refraction {
            let (pressure, temperature) = fields.refraction.expect("refraction values set");
            let pressure = cached_f64_fixed(fixed_decimal_cache, pressure, 3);
            write_csv_field(writer, &mut first, &pressure)?;
            let temperature = cached_f64_fixed(fixed_decimal_cache, temperature, 3);
            write_csv_field(writer, &mut first, &temperature)?;
        }
    }

    let dt = cached_datetime(datetime_cache, &fields.datetime);
    write_csv_field(writer, &mut first, &dt)?;

    if fields.layout.show_inputs {
        let deltat = cached_f64_fixed(fixed_decimal_cache, fields.deltat, 3);
        write_csv_field(writer, &mut first, &deltat)?;
    }

    let azimuth = format_f64_fixed(fields.azimuth, 4);
    write_csv_field(writer, &mut first, &azimuth)?;
    let angle = format_f64_fixed(fields.angle_value, 4);
    write_csv_field(writer, &mut first, &angle)?;

    writer.write_all(b"\n").map_err(|e| e.to_string())
}

fn write_sunrise_csv<W: std::io::Write>(
    writer: &mut W,
    fields: &SunriseFields,
    fixed_decimal_cache: &mut FixedDecimalCache,
) -> Result<(), String> {
    let mut first = true;

    if fields.layout.show_inputs {
        let lat = cached_f64_fixed(fixed_decimal_cache, fields.lat, 5);
        write_csv_field(writer, &mut first, &lat)?;
        let lon = cached_f64_fixed(fixed_decimal_cache, fields.lon, 5);
        write_csv_field(writer, &mut first, &lon)?;
        let date_time = format_rfc3339(&fields.date_time);
        write_csv_field(writer, &mut first, &date_time)?;
        let deltat = cached_f64_fixed(fixed_decimal_cache, fields.deltat, 3);
        write_csv_field(writer, &mut first, &deltat)?;
    } else {
        let date_time = format_rfc3339(&fields.date_time);
        write_csv_field(writer, &mut first, &date_time)?;
    }

    write_csv_field(writer, &mut first, fields.type_label)?;
    let sunrise = format_datetime_opt(fields.sunrise.as_ref());
    write_csv_field(writer, &mut first, &sunrise)?;
    let transit = format_rfc3339(&fields.transit);
    write_csv_field(writer, &mut first, &transit)?;
    let sunset = format_datetime_opt(fields.sunset.as_ref());
    write_csv_field(writer, &mut first, &sunset)?;

    if fields.layout.include_twilight {
        let format_twilight =
            |dt: Option<&DateTime<FixedOffset>>| dt.map(format_rfc3339).unwrap_or_default();
        let civil_start = format_twilight(fields.civil_start.as_ref());
        write_csv_field(writer, &mut first, &civil_start)?;
        let civil_end = format_twilight(fields.civil_end.as_ref());
        write_csv_field(writer, &mut first, &civil_end)?;
        let nautical_start = format_twilight(fields.nautical_start.as_ref());
        write_csv_field(writer, &mut first, &nautical_start)?;
        let nautical_end = format_twilight(fields.nautical_end.as_ref());
        write_csv_field(writer, &mut first, &nautical_end)?;
        let astro_start = format_twilight(fields.astro_start.as_ref());
        write_csv_field(writer, &mut first, &astro_start)?;
        let astro_end = format_twilight(fields.astro_end.as_ref());
        write_csv_field(writer, &mut first, &astro_end)?;
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
    writeln!(writer).map_err(OutputError::from)?;
    Ok(())
}

fn write_pretty_row<W: std::io::Write>(
    writer: &mut W,
    headers: &[&str],
    widths: &[usize],
    values: &[String],
) -> Result<(), OutputError> {
    for (idx, ((header, width), value)) in headers.iter().zip(widths).zip(values.iter()).enumerate()
    {
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
    output_plan: &crate::planner::OutputPlan,
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
    let res = write_rows(
        results,
        command,
        params,
        &mut writer,
        output_plan.flush_each_record,
    );
    let _ = writer.flush();
    res
}

fn write_rows<W: std::io::Write>(
    results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
    command: Command,
    params: &Parameters,
    writer: &mut W,
    flush_each: bool,
) -> Result<usize, OutputError> {
    let mut count = 0;
    let mut csv_header_written = false;
    let mut datetime_cache: std::collections::HashMap<DateTime<FixedOffset>, String> =
        std::collections::HashMap::with_capacity(2048);
    let mut fixed_decimal_cache: FixedDecimalCache = std::collections::HashMap::with_capacity(256);
    let mut text_row_values: Vec<String> = Vec::new();
    let position_layout = PositionLayout::from_params(params);
    let sunrise_layout = SunriseLayout::from_params(params);

    if params.output.format == OutputFormat::Text {
        let mut iter = results;
        let Some(first) = iter.next() else {
            return Ok(0);
        };

        let first_result = first.map_err(OutputError::from)?;
        let first_row = to_output_row(
            &first_result,
            params,
            command,
            position_layout,
            sunrise_layout,
        )
        .map_err(OutputError::from)?;
        let headers: Vec<&'static str> = first_row.csv_headers();
        first_row.csv_values_into(
            &mut datetime_cache,
            &mut fixed_decimal_cache,
            &mut text_row_values,
        );

        let mut widths: Vec<usize> = headers
            .iter()
            .map(|h| h.len().max(suggested_column_width(h)))
            .collect();
        for (w, v) in widths.iter_mut().zip(text_row_values.iter()) {
            *w = (*w).max(v.len());
        }

        let header_strs = headers.clone();
        if params.output.headers {
            write_pretty_header(writer, &header_strs, &widths)?;
        }

        write_pretty_row(writer, &header_strs, &widths, &text_row_values)?;
        count += 1;
        if flush_each {
            writer.flush().map_err(OutputError::from)?;
        }

        for result_or_err in iter {
            let result = result_or_err.map_err(OutputError::from)?;
            let row = to_output_row(&result, params, command, position_layout, sunrise_layout)
                .map_err(OutputError::from)?;
            row.csv_values_into(
                &mut datetime_cache,
                &mut fixed_decimal_cache,
                &mut text_row_values,
            );
            write_pretty_row(writer, &header_strs, &widths, &text_row_values)?;
            count += 1;
            if flush_each {
                writer.flush().map_err(OutputError::from)?;
            }
        }

        return Ok(count);
    }

    for result_or_err in results {
        let result = result_or_err.map_err(OutputError::from)?;
        let row = to_output_row(&result, params, command, position_layout, sunrise_layout)
            .map_err(OutputError::from)?;

        match params.output.format {
            OutputFormat::Csv => {
                if params.output.headers && !csv_header_written {
                    write_csv_line(writer, row.csv_headers()).map_err(OutputError::from)?;
                    csv_header_written = true;
                }
                row.write_csv(writer, &mut datetime_cache, &mut fixed_decimal_cache)
                    .map_err(OutputError::from)?;
            }
            OutputFormat::Json => row
                .write_json(writer, &mut datetime_cache)
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
    use crate::compute;
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

        let calc = compute::calculate_position(52.0, 13.4, dt, &params).unwrap();
        let results: Box<dyn Iterator<Item = Result<CalculationResult, String>>> =
            Box::new(vec![Ok(calc)].into_iter());

        let mut writer = MockWriter::default();

        write_rows(results, Command::Position, &params, &mut writer, true).expect("write succeeds");

        assert_eq!(writer.flushes, 1);
        assert!(!writer.buf.is_empty());
    }
}

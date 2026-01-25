//! Output formatting for CSV, JSON, and text table formats.

use crate::compute::CalculationResult;
use crate::data::{Command, OutputFormat, Parameters};
use crate::error::OutputError;
use chrono::{DateTime, FixedOffset};
use serde::{Serializer, ser::SerializeMap};
use solar_positioning::SunriseResult;

// Helper functions for time formatting
const RFC3339_NO_MILLIS: &str = "%Y-%m-%dT%H:%M:%S%:z";

fn format_rfc3339(dt: &DateTime<FixedOffset>) -> String {
    dt.format(RFC3339_NO_MILLIS).to_string()
}

fn format_datetime_opt(dt: Option<&DateTime<FixedOffset>>) -> String {
    dt.map_or(String::new(), format_rfc3339)
}

fn round_f64(value: f64, decimals: u32) -> f64 {
    let factor = 10_f64.powi(decimals as i32);
    (value * factor).round() / factor
}

fn format_f64_fixed(value: f64, decimals: u32) -> String {
    if !value.is_finite() {
        return value.to_string();
    }
    let factor = 10_f64.powi(decimals as i32);
    let scaled = (value * factor).round() as i64;
    let abs = scaled.abs();
    let denom = 10_i64.pow(decimals);
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

struct PositionFields {
    show_inputs: bool,
    include_refraction: bool,
    angle_kind: AngleKind,
    angle_value: f64,
    lat: f64,
    lon: f64,
    elevation: f64,
    pressure: Option<f64>,
    temperature: Option<f64>,
    datetime: DateTime<FixedOffset>,
    deltat: f64,
    azimuth: f64,
}

fn position_fields(row: &PositionRow, params: &Parameters, show_inputs: bool) -> PositionFields {
    let include_refraction = params.environment.refraction;
    let angle_kind = AngleKind::from_params(params);

    PositionFields {
        show_inputs,
        include_refraction,
        angle_kind,
        angle_value: row.angle(params.output.elevation_angle),
        lat: row.lat,
        lon: row.lon,
        elevation: params.environment.elevation,
        pressure: include_refraction.then_some(params.environment.pressure),
        temperature: include_refraction.then_some(params.environment.temperature),
        datetime: row.datetime,
        deltat: row.deltat,
        azimuth: row.azimuth,
    }
}

impl JsonFields for PositionFields {
    fn map_len(&self) -> usize {
        let mut len = 3; // dateTime, azimuth, angle
        if self.show_inputs {
            len += 4; // latitude, longitude, elevation, deltaT
            if self.include_refraction {
                len += 2; // pressure, temperature
            }
        }
        len
    }

    fn serialize_into_map<S: SerializeMap>(&self, map: &mut S) -> Result<(), S::Error> {
        if self.show_inputs {
            map.serialize_entry("latitude", &self.lat)?;
            map.serialize_entry("longitude", &self.lon)?;
            map.serialize_entry("elevation", &self.elevation)?;
            if self.include_refraction {
                map.serialize_entry("pressure", &self.pressure)?;
                map.serialize_entry("temperature", &self.temperature)?;
            }
        }
        map.serialize_entry("dateTime", &self.datetime.to_rfc3339())?;
        if self.show_inputs {
            map.serialize_entry("deltaT", &self.deltat)?;
        }
        let azimuth = round_f64(self.azimuth, 4);
        let angle = round_f64(self.angle_value, 4);
        map.serialize_entry("azimuth", &azimuth)?;
        map.serialize_entry(self.angle_kind.label(), &angle)?;
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
    show_inputs: bool,
    include_twilight: bool,
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
    fn serialize_into_map<S: SerializeMap>(&self, map: &mut S) -> Result<(), S::Error>;
}

fn write_json_fields(
    fields: &impl JsonFields,
    writer: &mut dyn std::io::Write,
) -> Result<(), String> {
    let mut serializer = serde_json::Serializer::new(&mut *writer);
    let mut map = serializer
        .serialize_map(Some(fields.map_len()))
        .map_err(|e| e.to_string())?;
    fields
        .serialize_into_map(&mut map)
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
            OutputRow::Position(fields) => {
                let mut header_fields = Vec::new();
                if fields.show_inputs {
                    header_fields.extend(["latitude", "longitude", "elevation"]);
                    if fields.include_refraction {
                        header_fields.push("pressure");
                        header_fields.push("temperature");
                    }
                    header_fields.push("dateTime");
                    header_fields.push("deltaT");
                } else {
                    header_fields.push("dateTime");
                }
                header_fields.push("azimuth");
                header_fields.push(fields.angle_kind.label());
                header_fields
            }
            OutputRow::Sunrise(fields) => {
                let mut header = Vec::new();
                if fields.show_inputs {
                    header.extend(["latitude", "longitude", "dateTime", "deltaT"]);
                } else {
                    header.push("dateTime");
                }
                header.push("type");
                header.push("sunrise");
                header.push("transit");
                header.push("sunset");
                if fields.include_twilight {
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
    }

    fn write_json(&self, writer: &mut dyn std::io::Write) -> Result<(), String> {
        match self {
            OutputRow::Position(fields) => write_json_fields(fields, writer),
            OutputRow::Sunrise(fields) => write_json_fields(fields, writer),
        }
    }

    fn csv_values(
        &self,
        datetime_cache: &mut std::collections::HashMap<DateTime<FixedOffset>, String>,
    ) -> Vec<String> {
        match self {
            OutputRow::Position(fields) => position_csv_values(fields, datetime_cache),
            OutputRow::Sunrise(fields) => sunrise_csv_values(fields),
        }
    }
}

fn cached_datetime(
    cache: &mut std::collections::HashMap<DateTime<FixedOffset>, String>,
    dt: &DateTime<FixedOffset>,
) -> String {
    cache
        .entry(*dt)
        .or_insert_with(|| dt.format("%+").to_string())
        .clone()
}

fn position_csv_values(
    fields: &PositionFields,
    datetime_cache: &mut std::collections::HashMap<DateTime<FixedOffset>, String>,
) -> Vec<String> {
    let capacity = if fields.show_inputs {
        if fields.include_refraction { 9 } else { 7 }
    } else {
        3
    };
    let mut values = Vec::with_capacity(capacity);

    if fields.show_inputs {
        values.push(format_f64_fixed(fields.lat, 5));
        values.push(format_f64_fixed(fields.lon, 5));
        values.push(format_f64_fixed(fields.elevation, 3));
        if fields.include_refraction {
            values.push(format_f64_fixed(
                fields.pressure.expect("pressure required"),
                3,
            ));
            values.push(format_f64_fixed(
                fields.temperature.expect("temperature required"),
                3,
            ));
        }
    }

    values.push(cached_datetime(datetime_cache, &fields.datetime));

    if fields.show_inputs {
        values.push(format_f64_fixed(fields.deltat, 3));
    }

    values.push(format_f64_fixed(fields.azimuth, 4));
    values.push(format_f64_fixed(fields.angle_value, 4));
    values
}

fn sunrise_csv_values(fields: &SunriseFields) -> Vec<String> {
    let capacity = if fields.show_inputs {
        if fields.include_twilight { 14 } else { 8 }
    } else if fields.include_twilight {
        11
    } else {
        5
    };
    let mut values = Vec::with_capacity(capacity);

    if fields.show_inputs {
        values.push(format_f64_fixed(fields.lat, 5));
        values.push(format_f64_fixed(fields.lon, 5));
        values.push(fields.date_time.format("%+").to_string());
        values.push(format_f64_fixed(fields.deltat, 3));
    } else {
        values.push(fields.date_time.format("%+").to_string());
    }

    let sunrise_str = format_datetime_opt(fields.sunrise.as_ref());
    let transit_str = fields.transit.format("%+").to_string();
    let sunset_str = format_datetime_opt(fields.sunset.as_ref());

    values.push(fields.type_label.to_string());
    values.push(sunrise_str);
    values.push(transit_str);
    values.push(sunset_str);

    if fields.include_twilight {
        let format_twilight = |dt: Option<&DateTime<FixedOffset>>| {
            dt.map(|d| d.format("%+").to_string()).unwrap_or_default()
        };
        values.push(format_twilight(fields.civil_start.as_ref()));
        values.push(format_twilight(fields.civil_end.as_ref()));
        values.push(format_twilight(fields.nautical_start.as_ref()));
        values.push(format_twilight(fields.nautical_end.as_ref()));
        values.push(format_twilight(fields.astro_start.as_ref()));
        values.push(format_twilight(fields.astro_end.as_ref()));
    }

    values
}

fn sunrise_fields(row: &SunriseRow, show_inputs: bool, include_twilight: bool) -> SunriseFields {
    SunriseFields {
        show_inputs,
        include_twilight,
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
        let mut len = if self.show_inputs { 8 } else { 5 }; // base fields
        if self.include_twilight {
            len += 6;
        }
        len
    }

    fn serialize_into_map<S: SerializeMap>(&self, map: &mut S) -> Result<(), S::Error> {
        if self.show_inputs {
            map.serialize_entry("latitude", &self.lat)?;
            map.serialize_entry("longitude", &self.lon)?;
            map.serialize_entry("dateTime", &format_rfc3339(&self.date_time))?;
            map.serialize_entry("deltaT", &self.deltat)?;
        } else {
            map.serialize_entry("dateTime", &format_rfc3339(&self.date_time))?;
        }
        map.serialize_entry("type", &self.type_label)?;

        map.serialize_entry("sunrise", &self.sunrise.as_ref().map(format_rfc3339))?;
        map.serialize_entry("transit", &format_rfc3339(&self.transit))?;
        map.serialize_entry("sunset", &self.sunset.as_ref().map(format_rfc3339))?;

        if self.include_twilight {
            map.serialize_entry(
                "civil_start",
                &self.civil_start.as_ref().map(format_rfc3339),
            )?;
            map.serialize_entry("civil_end", &self.civil_end.as_ref().map(format_rfc3339))?;
            map.serialize_entry(
                "nautical_start",
                &self.nautical_start.as_ref().map(format_rfc3339),
            )?;
            map.serialize_entry(
                "nautical_end",
                &self.nautical_end.as_ref().map(format_rfc3339),
            )?;
            map.serialize_entry(
                "astronomical_start",
                &self.astro_start.as_ref().map(format_rfc3339),
            )?;
            map.serialize_entry(
                "astronomical_end",
                &self.astro_end.as_ref().map(format_rfc3339),
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
) -> Result<OutputRow, String> {
    let show_inputs = params.output.should_show_inputs();
    let include_twilight = params.calculation.twilight;
    match command {
        Command::Position => normalize_position_result(result)
            .ok_or_else(|| "Unexpected calculation result for position output".to_string())
            .map(|row| OutputRow::Position(position_fields(&row, params, show_inputs))),
        Command::Sunrise => normalize_sunrise_result(result)
            .ok_or_else(|| "Unexpected calculation result for sunrise output".to_string())
            .map(|row| OutputRow::Sunrise(sunrise_fields(&row, show_inputs, include_twilight))),
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
    let mut iter = fields.into_iter();
    if let Some(first) = iter.next() {
        write!(writer, "{}", first.as_ref()).map_err(|e| e.to_string())?;
        for field in iter {
            write!(writer, ",{}", field.as_ref()).map_err(|e| e.to_string())?;
        }
    }
    writeln!(writer).map_err(|e| e.to_string())
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

    if params.output.format == OutputFormat::Text {
        let mut iter = results;
        let Some(first) = iter.next() else {
            return Ok(0);
        };

        let first_result = first.map_err(OutputError::from)?;
        let first_row = to_output_row(&first_result, params, command).map_err(OutputError::from)?;
        let headers: Vec<&'static str> = first_row.csv_headers();
        let first_values = first_row.csv_values(&mut datetime_cache);

        let mut widths: Vec<usize> = headers
            .iter()
            .map(|h| h.len().max(suggested_column_width(h)))
            .collect();
        for (w, v) in widths.iter_mut().zip(first_values.iter()) {
            *w = (*w).max(v.len());
        }

        let header_strs = headers.clone();
        if params.output.headers {
            write_pretty_header(writer, &header_strs, &widths)?;
        }

        write_pretty_row(writer, &header_strs, &widths, &first_values)?;
        count += 1;
        if flush_each {
            writer.flush().map_err(OutputError::from)?;
        }

        for result_or_err in iter {
            let result = result_or_err.map_err(OutputError::from)?;
            let row = to_output_row(&result, params, command).map_err(OutputError::from)?;
            let values = row.csv_values(&mut datetime_cache);
            write_pretty_row(writer, &header_strs, &widths, &values)?;
            count += 1;
            if flush_each {
                writer.flush().map_err(OutputError::from)?;
            }
        }

        return Ok(count);
    }

    for result_or_err in results {
        let result = result_or_err.map_err(OutputError::from)?;
        let row = to_output_row(&result, params, command).map_err(OutputError::from)?;

        match params.output.format {
            OutputFormat::Csv => {
                if params.output.headers && !csv_header_written {
                    write_csv_line(writer, row.csv_headers()).map_err(OutputError::from)?;
                    csv_header_written = true;
                }
                let values = row.csv_values(&mut datetime_cache);
                write_csv_line(writer, values).map_err(OutputError::from)?;
            }
            OutputFormat::Json => row.write_json(writer).map_err(OutputError::from)?,
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

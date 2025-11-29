//! Output formatting for CSV, JSON, and text table formats.

use crate::compute::CalculationResult;
use crate::data::{Command, OutputFormat, Parameters};
use crate::error::OutputError;
mod formatters;
use chrono::{DateTime, FixedOffset};
use formatters::{CsvFormatter, Formatter, JsonFormatter, TextFormatter};
use solar_positioning::SunriseResult;
use unicode_width::UnicodeWidthStr;

// Helper functions for time formatting
fn format_datetime(dt: &DateTime<FixedOffset>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S%:z").to_string()
}

fn format_datetime_opt(dt: Option<&DateTime<FixedOffset>>) -> String {
    dt.map_or(String::new(), format_datetime)
}

fn format_datetime_json_null(dt: Option<&DateTime<FixedOffset>>) -> String {
    dt.map_or("null".to_string(), |t| {
        format!(r#""{}""#, format_datetime(t))
    })
}

fn format_datetime_text(dt: &DateTime<FixedOffset>) -> String {
    dt.format("%Y-%m-%d %H:%M:%S%:z").to_string()
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

struct PositionFields {
    show_inputs: bool,
    include_refraction: bool,
    angle_label: &'static str,
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
    let angle_label = if params.output.elevation_angle {
        "elevation-angle"
    } else {
        "zenith"
    };

    PositionFields {
        show_inputs,
        include_refraction,
        angle_label,
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
    has_twilight: bool,
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

fn sunrise_fields(row: &SunriseRow, show_inputs: bool) -> SunriseFields {
    let has_twilight =
        row.civil_start.is_some() || row.nautical_start.is_some() || row.astro_start.is_some();

    SunriseFields {
        show_inputs,
        has_twilight,
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

#[cfg(feature = "parquet")]
pub fn write_parquet_output<W: std::io::Write + Send>(
    results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
    command: Command,
    params: &Parameters,
    writer: W,
) -> std::io::Result<usize> {
    crate::parquet::write_parquet(results, command, params, writer)
}

fn write_csv_position<W: std::io::Write>(
    fields: &PositionFields,
    headers: bool,
    first: bool,
    writer: &mut W,
) -> std::io::Result<()> {
    if first && headers {
        if fields.show_inputs {
            let mut header_fields = vec!["latitude", "longitude", "elevation"];
            if fields.include_refraction {
                header_fields.push("pressure");
                header_fields.push("temperature");
            }
            header_fields.extend(["dateTime", "deltaT", "azimuth", fields.angle_label]);
            write!(writer, "{}\r\n", header_fields.join(","))?;
        } else if fields.angle_label == "elevation-angle" {
            write!(writer, "dateTime,azimuth,elevation-angle\r\n")?;
        } else {
            write!(writer, "dateTime,azimuth,zenith\r\n")?;
        }
    }

    if fields.show_inputs {
        write!(
            writer,
            "{:.5},{:.5},{:.3},",
            fields.lat, fields.lon, fields.elevation
        )?;
        if let (Some(pressure), Some(temp)) = (fields.pressure, fields.temperature) {
            write!(writer, "{:.3},{:.3},", pressure, temp)?;
        }
        write!(
            writer,
            "{},{:.3},{:.5},{:.5}\r\n",
            fields.datetime.to_rfc3339(),
            fields.deltat,
            fields.azimuth,
            fields.angle_value
        )?;
    } else {
        write!(
            writer,
            "{},{:.5},{:.5}\r\n",
            fields.datetime.to_rfc3339(),
            fields.azimuth,
            fields.angle_value
        )?;
    }
    Ok(())
}

fn write_csv_sunrise<W: std::io::Write>(
    fields: &SunriseFields,
    headers: bool,
    first: bool,
    writer: &mut W,
) -> std::io::Result<()> {
    if first && headers {
        if fields.has_twilight {
            if fields.show_inputs {
                write!(
                    writer,
                    "latitude,longitude,dateTime,deltaT,type,sunrise,transit,sunset,civil_start,civil_end,nautical_start,nautical_end,astronomical_start,astronomical_end\r\n"
                )?;
            } else {
                write!(
                    writer,
                    "dateTime,type,sunrise,transit,sunset,civil_start,civil_end,nautical_start,nautical_end,astronomical_start,astronomical_end\r\n"
                )?;
            }
        } else if fields.show_inputs {
            write!(
                writer,
                "latitude,longitude,dateTime,deltaT,type,sunrise,transit,sunset\r\n"
            )?;
        } else {
            write!(writer, "dateTime,type,sunrise,transit,sunset\r\n")?;
        }
    }

    let (sunrise_str, transit_str, sunset_str) = (
        format_datetime_opt(fields.sunrise.as_ref()),
        format_datetime(&fields.transit),
        format_datetime_opt(fields.sunset.as_ref()),
    );

    let format_twilight = |dt: Option<&DateTime<FixedOffset>>| format_datetime_opt(dt);
    let civil_start = format_twilight(fields.civil_start.as_ref());
    let civil_end = format_twilight(fields.civil_end.as_ref());
    let nautical_start = format_twilight(fields.nautical_start.as_ref());
    let nautical_end = format_twilight(fields.nautical_end.as_ref());
    let astro_start = format_twilight(fields.astro_start.as_ref());
    let astro_end = format_twilight(fields.astro_end.as_ref());

    if fields.has_twilight {
        if fields.show_inputs {
            write!(
                writer,
                "{:.5},{:.5},{},{:.3},{},{},{},{},{},{},{},{},{},{}\r\n",
                fields.lat,
                fields.lon,
                format_datetime(&fields.date_time),
                fields.deltat,
                fields.type_label,
                sunrise_str,
                transit_str,
                sunset_str,
                civil_start,
                civil_end,
                nautical_start,
                nautical_end,
                astro_start,
                astro_end
            )?;
        } else {
            write!(
                writer,
                "{},{},{},{},{},{},{},{},{},{},{}\r\n",
                format_datetime(&fields.date_time),
                fields.type_label,
                sunrise_str,
                transit_str,
                sunset_str,
                civil_start,
                civil_end,
                nautical_start,
                nautical_end,
                astro_start,
                astro_end
            )?;
        }
    } else if fields.show_inputs {
        write!(
            writer,
            "{:.5},{:.5},{},{:.3},{},{},{},{}\r\n",
            fields.lat,
            fields.lon,
            format_datetime(&fields.date_time),
            fields.deltat,
            fields.type_label,
            sunrise_str,
            transit_str,
            sunset_str
        )?;
    } else {
        write!(
            writer,
            "{},{},{},{},{}\r\n",
            format_datetime(&fields.date_time),
            fields.type_label,
            sunrise_str,
            transit_str,
            sunset_str
        )?;
    }
    Ok(())
}

fn write_json_position<W: std::io::Write>(
    fields: &PositionFields,
    writer: &mut W,
) -> std::io::Result<()> {
    let angle_label = if fields.angle_label == "elevation-angle" {
        "elevation"
    } else {
        "zenith"
    };

    if fields.show_inputs {
        write!(
            writer,
            r#"{{"latitude":{},"longitude":{},"elevation":{}"#,
            fields.lat, fields.lon, fields.elevation
        )?;
        if fields.include_refraction {
            write!(
                writer,
                r#","pressure":{},"temperature":{}"#,
                fields.pressure.unwrap_or_default(),
                fields.temperature.unwrap_or_default()
            )?;
        }
        writeln!(
            writer,
            r#","dateTime":"{}","deltaT":{},"azimuth":{},"{}":{}}}"#,
            fields.datetime.to_rfc3339(),
            fields.deltat,
            fields.azimuth,
            angle_label,
            fields.angle_value
        )
    } else {
        writeln!(
            writer,
            r#"{{"dateTime":"{}","azimuth":{},"{}":{}}}"#,
            fields.datetime.to_rfc3339(),
            fields.azimuth,
            angle_label,
            fields.angle_value
        )
    }
}

fn write_json_sunrise<W: std::io::Write>(
    fields: &SunriseFields,
    writer: &mut W,
) -> std::io::Result<()> {
    let sunrise_str = format_datetime_json_null(fields.sunrise.as_ref());
    let transit_str = format_datetime(&fields.transit);
    let sunset_str = format_datetime_json_null(fields.sunset.as_ref());
    let civil_start = format_datetime_json_null(fields.civil_start.as_ref());
    let civil_end = format_datetime_json_null(fields.civil_end.as_ref());
    let nautical_start = format_datetime_json_null(fields.nautical_start.as_ref());
    let nautical_end = format_datetime_json_null(fields.nautical_end.as_ref());
    let astro_start = format_datetime_json_null(fields.astro_start.as_ref());
    let astro_end = format_datetime_json_null(fields.astro_end.as_ref());

    if fields.has_twilight {
        if fields.show_inputs {
            writeln!(
                writer,
                r#"{{"latitude":{:.5},"longitude":{:.5},"dateTime":"{}","deltaT":{:.3},"type":"{}","sunrise":{},"transit":"{}","sunset":{},"civil_start":{},"civil_end":{},"nautical_start":{},"nautical_end":{},"astronomical_start":{},"astronomical_end":{}}}"#,
                fields.lat,
                fields.lon,
                fields.date_time.to_rfc3339(),
                fields.deltat,
                fields.type_label,
                sunrise_str,
                transit_str,
                sunset_str,
                civil_start,
                civil_end,
                nautical_start,
                nautical_end,
                astro_start,
                astro_end
            )
        } else {
            writeln!(
                writer,
                r#"{{"type":"{}","sunrise":{},"transit":"{}","sunset":{},"civil_start":{},"civil_end":{},"nautical_start":{},"nautical_end":{},"astronomical_start":{},"astronomical_end":{}}}"#,
                fields.type_label,
                sunrise_str,
                transit_str,
                sunset_str,
                civil_start,
                civil_end,
                nautical_start,
                nautical_end,
                astro_start,
                astro_end
            )
        }
    } else if fields.show_inputs {
        writeln!(
            writer,
            r#"{{"latitude":{},"longitude":{},"dateTime":"{}","deltaT":{:.3},"type":"{}","sunrise":{},"transit":"{}","sunset":{}}}"#,
            fields.lat,
            fields.lon,
            fields.date_time.to_rfc3339(),
            fields.deltat,
            fields.type_label,
            sunrise_str,
            transit_str,
            sunset_str
        )
    } else {
        writeln!(
            writer,
            r#"{{"type":"{}","sunrise":{},"transit":"{}","sunset":{}}}"#,
            fields.type_label, sunrise_str, transit_str, sunset_str
        )
    }
}

fn write_text_position<W: std::io::Write>(
    fields: &PositionFields,
    writer: &mut W,
) -> std::io::Result<()> {
    let mut lines = Vec::new();

    if fields.show_inputs {
        lines.push(format!("│ Location   {}, {}", fields.lat, fields.lon));
        lines.push(format!("│ Delta T    {:.1} s", fields.deltat));
    }
    lines.push(format!(
        "│ DateTime:    {}",
        fields.datetime.format("%Y-%m-%d %H:%M:%S%:z")
    ));
    lines.push(format!("│ Azimuth    {:.5}°", fields.azimuth));

    if fields.angle_label == "elevation-angle" {
        lines.push(format!("│ Elevation  {:.5}°", fields.angle_value));
    } else {
        lines.push(format!("│ Zenith     {:.5}°", fields.angle_value));
    }

    let max_width = lines
        .iter()
        .map(|line| UnicodeWidthStr::width(line.as_str()))
        .max()
        .unwrap_or(0);
    let box_width = max_width + 2;

    writeln!(writer, "┌{}┐", "─".repeat(box_width - 2))?;
    for line in lines {
        writeln!(writer, "{}", line)?;
    }
    writeln!(writer, "└{}┘", "─".repeat(box_width - 2))?;
    Ok(())
}

fn write_text_sunrise<W: std::io::Write>(
    fields: &SunriseFields,
    writer: &mut W,
) -> std::io::Result<()> {
    if fields.show_inputs {
        writeln!(writer, "location: {:.5}, {:.5}", fields.lat, fields.lon)?;
        writeln!(
            writer,
            "dateTime: {}",
            format_datetime_text(&fields.date_time)
        )?;
        writeln!(writer, "deltaT : {:.1}", fields.deltat)?;
    }

    match fields.type_label {
        "NORMAL" => {
            writeln!(writer, "type   : normal")?;
            writeln!(
                writer,
                "sunrise: {}",
                format_datetime_text(
                    fields
                        .sunrise
                        .as_ref()
                        .expect("sunrise must exist for normal")
                )
            )?;
            writeln!(writer, "transit: {}", format_datetime_text(&fields.transit))?;
            writeln!(
                writer,
                "sunset : {}",
                format_datetime_text(
                    fields
                        .sunset
                        .as_ref()
                        .expect("sunset must exist for normal")
                )
            )?;
        }
        "ALL_DAY" => {
            writeln!(writer, "type   : all day")?;
            writeln!(writer, "sunrise: ")?;
            writeln!(writer, "transit: {}", format_datetime_text(&fields.transit))?;
            writeln!(writer, "sunset : ")?;
        }
        "ALL_NIGHT" => {
            writeln!(writer, "type   : all night")?;
            writeln!(writer, "sunrise: ")?;
            writeln!(writer, "transit: {}", format_datetime_text(&fields.transit))?;
            writeln!(writer, "sunset : ")?;
        }
        _ => {}
    }

    if let (Some(start), Some(end)) = (fields.civil_start.as_ref(), fields.civil_end.as_ref()) {
        writeln!(
            writer,
            "civil twilight start: {}",
            format_datetime_text(start)
        )?;
        writeln!(
            writer,
            "civil twilight end  : {}",
            format_datetime_text(end)
        )?;
    }

    if let (Some(start), Some(end)) = (fields.nautical_start.as_ref(), fields.nautical_end.as_ref())
    {
        writeln!(
            writer,
            "nautical twilight start: {}",
            format_datetime_text(start)
        )?;
        writeln!(
            writer,
            "nautical twilight end  : {}",
            format_datetime_text(end)
        )?;
    }

    if let (Some(start), Some(end)) = (fields.astro_start.as_ref(), fields.astro_end.as_ref()) {
        writeln!(
            writer,
            "astronomical twilight start: {}",
            format_datetime_text(start)
        )?;
        writeln!(
            writer,
            "astronomical twilight end  : {}",
            format_datetime_text(end)
        )?;
    }

    Ok(())
}

fn write_streaming_text_table<W: std::io::Write>(
    results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
    params: &Parameters,
    source: crate::data::DataSource,
    writer: &mut W,
    flush_each: bool,
) -> std::io::Result<usize> {
    let formatted = format_streaming_text_table(results, params, source);
    let mut record_count = 0;
    for line_result in formatted {
        match line_result {
            Ok((line, is_record)) => {
                write!(writer, "{}", line)?;
                if is_record {
                    record_count += 1;
                    if flush_each {
                        std::io::Write::flush(writer)?;
                    }
                }
            }
            Err(e) => return Err(std::io::Error::other(e)),
        }
    }
    Ok(record_count)
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn write_formatted_output<W: std::io::Write>(
    results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
    command: Command,
    params: &Parameters,
    source: crate::data::DataSource,
    writer: &mut W,
    flush_each: bool,
) -> std::io::Result<usize> {
    write_with_formatter(results, command, params, source, writer, flush_each)
        .map_err(|e| std::io::Error::other(e.to_string()))
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
                .map_err(|e| OutputError::Io(e.to_string()));
        }
        _ => {}
    }

    use std::io::{BufWriter, Write};
    let stdout = std::io::stdout();
    let mut writer = BufWriter::new(stdout.lock());
    let res = write_with_formatter(
        results,
        command,
        params,
        output_plan.data_source.clone(),
        &mut writer,
        output_plan.flush_each_record,
    );
    let _ = writer.flush();
    res
}

fn write_with_formatter<W: std::io::Write>(
    results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
    command: Command,
    params: &Parameters,
    data_source: crate::data::DataSource,
    writer: &mut W,
    flush_each: bool,
) -> Result<usize, OutputError> {
    let mut formatter: Box<dyn Formatter> = match params.output.format {
        OutputFormat::Csv => Box::new(CsvFormatter::new(writer, params, command, flush_each)),
        OutputFormat::Json => Box::new(JsonFormatter::new(writer, params, command, flush_each)),
        _ => Box::new(TextFormatter::new(
            writer,
            params,
            command,
            data_source,
            flush_each,
        )),
    };
    formatter.write(results).map_err(OutputError::from)
}

fn format_streaming_text_table(
    mut results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
    params: &Parameters,
    source: crate::data::DataSource,
) -> Box<dyn Iterator<Item = Result<(String, bool), String>>> {
    use crate::data::{DataSource, LocationSource, TimeSource};

    // Peek at first result to get invariant values and determine table structure
    let first_row = match results.next() {
        Some(Ok(r)) => match normalize_position_result(&r) {
            Some(row) => row,
            None => {
                return Box::new(std::iter::once(Err(
                    "Unexpected calculation result for position output".to_string(),
                )));
            }
        },
        Some(Err(e)) => return Box::new(std::iter::once(Err(e))),
        None => return Box::new(std::iter::empty()),
    };

    let (lat_varies, lon_varies, time_varies) = match &source {
        DataSource::Separate(loc, time) => {
            let (lat_varies, lon_varies) = match loc {
                LocationSource::Single(_, _) => (false, false),
                LocationSource::Range { lat, lon } => {
                    let lat_varies = lat.0 != lat.1;
                    let lon_varies = lon.map(|(s, e, _)| s != e).unwrap_or(false);
                    (lat_varies, lon_varies)
                }
                LocationSource::File(_) => (true, true),
            };
            let time_varies = !matches!(time, TimeSource::Single(_));
            (lat_varies, lon_varies, time_varies)
        }
        DataSource::Paired(_) => (true, true, true),
    };

    // Extract invariant values from first result
    let (lat0, lon0, dt0, deltat0) = (
        first_row.lat,
        first_row.lon,
        first_row.datetime,
        first_row.deltat,
    );

    // Build header section
    let mut header = String::new();
    if !lat_varies {
        header.push_str(&format!("  Latitude:    {:.6}°\n", lat0));
    }
    if !lon_varies {
        header.push_str(&format!("  Longitude:   {:.6}°\n", lon0));
    }
    header.push_str(&format!(
        "  Elevation:   {:.1} m\n",
        params.environment.elevation
    ));
    if params.environment.refraction {
        header.push_str(&format!(
            "  Pressure:    {:.1} hPa\n",
            params.environment.pressure
        ));
        header.push_str(&format!(
            "  Temperature: {:.1}°C\n",
            params.environment.temperature
        ));
    }
    if !time_varies {
        header.push_str(&format!(
            "  DateTime:    {}\n",
            dt0.format("%Y-%m-%d %H:%M:%S%:z")
        ));
    } else if lat_varies {
        // For lat range + time series from partial date, show date in header
        // This works because partial dates (like "2024-06-21") guarantee same date
        if let DataSource::Separate(_, TimeSource::Range(date_str, _)) = &source
            && date_str.len() == 10
        {
            // Full date like "2024-06-21" - all times will be on this date
            header.push_str(&format!(
                "  DateTime:    {}\n",
                dt0.format("%Y-%m-%d %H:%M:%S%:z")
            ));
        }
    }
    header.push_str(&format!("  Delta T:     {:.1} s\n", deltat0));
    header.push('\n');

    // Build table headers
    let mut headers_vec = Vec::new();
    if lat_varies {
        headers_vec.push("Latitude");
    }
    if lon_varies {
        headers_vec.push("Longitude");
    }
    if time_varies {
        headers_vec.push("DateTime");
    }
    headers_vec.push("Azimuth");
    if params.output.elevation_angle {
        headers_vec.push("Elevation");
    } else {
        headers_vec.push("Zenith");
    }

    let col_widths: Vec<usize> = headers_vec
        .iter()
        .map(|h| {
            if time_varies && *h == "DateTime" {
                22 // "YYYY-MM-DD HH:MM±HH:MM"
            } else {
                h.len().max(14)
            }
        })
        .collect();

    // Top border
    header.push('┌');
    for (i, width) in col_widths.iter().enumerate() {
        header.push_str(&"─".repeat(width + 2));
        if i < col_widths.len() - 1 {
            header.push('┬');
        }
    }
    header.push_str("┐\n");

    // Header row
    header.push('│');
    for (h, width) in headers_vec.iter().zip(&col_widths) {
        header.push_str(&format!(" {:<width$} ", h, width = width));
        header.push('│');
    }
    header.push('\n');

    // Separator
    header.push('├');
    for (i, width) in col_widths.iter().enumerate() {
        header.push_str(&"─".repeat(width + 2));
        if i < col_widths.len() - 1 {
            header.push('┼');
        }
    }
    header.push_str("┤\n");

    // Format a single row
    let col_widths_clone = col_widths.clone();
    let elevation_angle = params.output.elevation_angle;
    let format_row = move |row: &PositionRow| -> String {
        let mut output = String::from('│');
        let mut col_idx = 0;

        if lat_varies {
            output.push_str(&format!(
                " {:>width$.5}° ",
                row.lat,
                width = col_widths_clone[col_idx] - 1
            ));
            output.push('│');
            col_idx += 1;
        }
        if lon_varies {
            output.push_str(&format!(
                " {:>width$.5}° ",
                row.lon,
                width = col_widths_clone[col_idx] - 1
            ));
            output.push('│');
            col_idx += 1;
        }
        if time_varies {
            let dt_str = row.datetime.format("%Y-%m-%d %H:%M%:z").to_string();
            output.push_str(&format!(
                " {:<width$} ",
                dt_str,
                width = col_widths_clone[col_idx]
            ));
            output.push('│');
            col_idx += 1;
        }
        output.push_str(&format!(
            " {:>width$.5}° ",
            row.azimuth,
            width = col_widths_clone[col_idx] - 1
        ));
        output.push('│');
        col_idx += 1;

        let angle = row.angle(elevation_angle);
        output.push_str(&format!(
            " {:>width$.5}° ",
            angle,
            width = col_widths_clone[col_idx] - 1
        ));
        output.push_str("│\n");
        output
    };

    // Bottom border
    let mut footer = String::from('└');
    for (i, width) in col_widths.iter().enumerate() {
        footer.push_str(&"─".repeat(width + 2));
        if i < col_widths.len() - 1 {
            footer.push('┴');
        }
    }
    footer.push_str("┘\n");

    // Create streaming iterator: header + first_row + remaining_rows + footer
    let first_line = format_row(&first_row);
    let remaining_rows = results.map(move |r_or_err| match r_or_err {
        Ok(r) => normalize_position_result(&r)
            .ok_or_else(|| "Unexpected calculation result for position output".to_string())
            .map(|row| format_row(&row)),
        Err(e) => Err(e),
    });

    Box::new(
        std::iter::once(Ok((header, false)))
            .chain(std::iter::once(Ok((first_line, true))))
            .chain(remaining_rows.map(|res| res.map(|line| (line, true))))
            .chain(std::iter::once(Ok((footer, false)))),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compute;
    use crate::data::config::OutputOptions;
    use crate::data::{DataSource, LocationSource, OutputFormat, TimeSource};
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

        let source = DataSource::Separate(
            LocationSource::Single(52.0, 13.4),
            TimeSource::Single(dt.to_rfc3339()),
        );

        let mut writer = MockWriter::default();

        write_formatted_output(
            results,
            Command::Position,
            &params,
            source,
            &mut writer,
            true,
        )
        .expect("write succeeds");

        assert_eq!(writer.flushes, 1);
        assert!(!writer.buf.is_empty());
    }
}

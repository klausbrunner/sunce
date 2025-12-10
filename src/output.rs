//! Output formatting for CSV, JSON, and text table formats.

use crate::compute::CalculationResult;
use crate::data::{Command, OutputFormat, Parameters};
use crate::error::OutputError;
use chrono::{DateTime, FixedOffset};
use serde::{Serializer, ser::SerializeMap};
use solar_positioning::SunriseResult;

// Helper functions for time formatting
const RFC3339_NO_MILLIS_SPACE: &str = "%Y-%m-%d %H:%M:%S%:z";
const RFC3339_NO_MILLIS: &str = "%Y-%m-%dT%H:%M:%S%:z";

fn format_rfc3339(dt: &DateTime<FixedOffset>) -> String {
    dt.format(RFC3339_NO_MILLIS).to_string()
}

fn format_datetime_opt(dt: Option<&DateTime<FixedOffset>>) -> String {
    dt.map_or(String::new(), format_rfc3339)
}

fn format_datetime_text(dt: &DateTime<FixedOffset>) -> String {
    dt.format(RFC3339_NO_MILLIS_SPACE).to_string()
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

impl AngleKind {
    fn from_params(params: &Parameters) -> Self {
        if params.output.elevation_angle {
            AngleKind::Elevation
        } else {
            AngleKind::Zenith
        }
    }

    fn label(&self) -> &'static str {
        match self {
            AngleKind::Zenith => "zenith",
            AngleKind::Elevation => "elevation-angle",
        }
    }

    fn field_key(&self) -> &'static str {
        match self {
            AngleKind::Zenith => "zenith",
            AngleKind::Elevation => "elevation",
        }
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

impl PositionFields {
    fn map_len(&self) -> usize {
        let mut len = 4; // dateTime, deltaT, azimuth, angle
        if self.show_inputs {
            len += 3; // latitude, longitude, elevation
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
        map.serialize_entry("deltaT", &self.deltat)?;
        map.serialize_entry("azimuth", &self.azimuth)?;
        map.serialize_entry(self.angle_kind.field_key(), &self.angle_value)?;
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
                if fields.has_twilight {
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
        {
            let mut serializer = serde_json::Serializer::new(&mut *writer);
            match self {
                OutputRow::Position(fields) => {
                    let mut map = serializer
                        .serialize_map(Some(fields.map_len()))
                        .map_err(|e| e.to_string())?;
                    fields
                        .serialize_into_map(&mut map)
                        .map_err(|e| e.to_string())?;
                    map.end().map_err(|e| e.to_string())?;
                }
                OutputRow::Sunrise(fields) => {
                    let mut map = serializer
                        .serialize_map(Some(fields.map_len()))
                        .map_err(|e| e.to_string())?;
                    fields
                        .serialize_into_map(&mut map)
                        .map_err(|e| e.to_string())?;
                    map.end().map_err(|e| e.to_string())?;
                }
            }
        }
        writeln!(writer).map_err(|e| e.to_string())
    }

    fn write_text(&self, writer: &mut dyn std::io::Write) -> Result<(), String> {
        match self {
            OutputRow::Sunrise(fields) => {
                write_text_sunrise(fields, writer).map_err(|e| e.to_string())
            }
            OutputRow::Position(_) => {
                Err("Text output for position is handled by the streaming text table".to_string())
            }
        }
    }

    fn write_csv(
        &self,
        writer: &mut dyn std::io::Write,
        datetime_cache: &mut std::collections::HashMap<DateTime<FixedOffset>, String>,
    ) -> Result<(), String> {
        match self {
            OutputRow::Position(fields) => {
                if fields.show_inputs {
                    write!(
                        writer,
                        "{:.5},{:.5},{:.3},",
                        fields.lat, fields.lon, fields.elevation
                    )
                    .map_err(|e| e.to_string())?;
                    if let (Some(pressure), Some(temp)) = (fields.pressure, fields.temperature) {
                        write!(writer, "{:.3},{:.3},", pressure, temp)
                            .map_err(|e| e.to_string())?;
                    }
                    let dt = datetime_cache
                        .entry(fields.datetime)
                        .or_insert_with(|| fields.datetime.format("%+").to_string());
                    writeln!(
                        writer,
                        "{},{:.3},{:.5},{:.5}",
                        dt, fields.deltat, fields.azimuth, fields.angle_value
                    )
                    .map_err(|e| e.to_string())
                } else {
                    let dt = datetime_cache
                        .entry(fields.datetime)
                        .or_insert_with(|| fields.datetime.format("%+").to_string());
                    writeln!(
                        writer,
                        "{},{:.5},{:.5}",
                        dt, fields.azimuth, fields.angle_value
                    )
                    .map_err(|e| e.to_string())
                }
            }
            OutputRow::Sunrise(fields) => {
                let sunrise_str = format_datetime_opt(fields.sunrise.as_ref());
                let transit_str = fields.transit.format("%+").to_string();
                let sunset_str = format_datetime_opt(fields.sunset.as_ref());

                if fields.show_inputs {
                    write!(
                        writer,
                        "{:.5},{:.5},{},{:.3},{},{},{},{}",
                        fields.lat,
                        fields.lon,
                        fields.date_time.format("%+"),
                        fields.deltat,
                        fields.type_label,
                        sunrise_str,
                        transit_str,
                        sunset_str
                    )
                    .map_err(|e| e.to_string())?;
                } else {
                    write!(
                        writer,
                        "{},{},{},{},{}",
                        fields.date_time.format("%+"),
                        fields.type_label,
                        sunrise_str,
                        transit_str,
                        sunset_str
                    )
                    .map_err(|e| e.to_string())?;
                }

                if fields.has_twilight {
                    let format_twilight = |dt: Option<&DateTime<FixedOffset>>| {
                        dt.map(|d| d.format("%+").to_string()).unwrap_or_default()
                    };
                    write!(
                        writer,
                        ",{},{},{},{},{},{}",
                        format_twilight(fields.civil_start.as_ref()),
                        format_twilight(fields.civil_end.as_ref()),
                        format_twilight(fields.nautical_start.as_ref()),
                        format_twilight(fields.nautical_end.as_ref()),
                        format_twilight(fields.astro_start.as_ref()),
                        format_twilight(fields.astro_end.as_ref())
                    )
                    .map_err(|e| e.to_string())?;
                }
                writeln!(writer).map_err(|e| e.to_string())
            }
        }
    }
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

impl SunriseFields {
    fn map_len(&self) -> usize {
        let mut len = if self.show_inputs { 8 } else { 5 }; // base fields
        if self.has_twilight {
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

        if self.has_twilight {
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
    match command {
        Command::Position => normalize_position_result(result)
            .ok_or_else(|| "Unexpected calculation result for position output".to_string())
            .map(|row| OutputRow::Position(position_fields(&row, params, show_inputs))),
        Command::Sunrise => normalize_sunrise_result(result)
            .ok_or_else(|| "Unexpected calculation result for sunrise output".to_string())
            .map(|row| OutputRow::Sunrise(sunrise_fields(&row, show_inputs))),
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

fn write_text_sunrise<W: std::io::Write + ?Sized>(
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

    let type_label = match fields.type_label {
        "ALL_DAY" => "all day",
        "ALL_NIGHT" => "all night",
        _ => "normal",
    };
    writeln!(writer, "type   : {}", type_label)?;

    let write_opt =
        |label: &str, value: Option<&DateTime<FixedOffset>>, out: &mut W| -> std::io::Result<()> {
            if let Some(v) = value {
                writeln!(out, "{}: {}", label, format_datetime_text(v))
            } else {
                writeln!(out, "{}: ", label)
            }
        };

    write_opt("sunrise", fields.sunrise.as_ref(), writer)?;
    writeln!(writer, "transit: {}", format_datetime_text(&fields.transit))?;
    write_opt("sunset ", fields.sunset.as_ref(), writer)?;

    for (label, start, end) in [
        (
            "civil twilight",
            fields.civil_start.as_ref(),
            fields.civil_end.as_ref(),
        ),
        (
            "nautical twilight",
            fields.nautical_start.as_ref(),
            fields.nautical_end.as_ref(),
        ),
        (
            "astronomical twilight",
            fields.astro_start.as_ref(),
            fields.astro_end.as_ref(),
        ),
    ] {
        if let (Some(s), Some(e)) = (start, end) {
            writeln!(writer, "{} start: {}", label, format_datetime_text(s))?;
            writeln!(writer, "{} end  : {}", label, format_datetime_text(e))?;
        }
    }
    Ok(())
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
        output_plan.data_source.clone(),
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
    data_source: crate::data::DataSource,
    writer: &mut W,
    flush_each: bool,
) -> Result<usize, OutputError> {
    if matches!(command, Command::Position) && params.output.format == OutputFormat::Text {
        return text_table::write_streaming_text_table(
            results,
            params,
            data_source,
            writer,
            flush_each,
        )
        .map_err(OutputError::from);
    }

    let mut count = 0;
    let mut csv_header_written = false;
    let mut csv_datetime_cache: std::collections::HashMap<DateTime<FixedOffset>, String> =
        std::collections::HashMap::with_capacity(2048);

    for result_or_err in results {
        let result = result_or_err.map_err(OutputError::from)?;
        let row = to_output_row(&result, params, command).map_err(OutputError::from)?;

        match params.output.format {
            OutputFormat::Csv => {
                if params.output.headers && !csv_header_written {
                    write_csv_line(writer, row.csv_headers()).map_err(OutputError::from)?;
                    csv_header_written = true;
                }
                row.write_csv(writer, &mut csv_datetime_cache)
                    .map_err(OutputError::from)?;
            }
            OutputFormat::Json => row.write_json(writer).map_err(OutputError::from)?,
            OutputFormat::Text => row.write_text(writer).map_err(OutputError::from)?,
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

mod text_table {
    use super::*;
    use crate::data::{DataSource, LocationSource, TimeSource};

    #[derive(Clone)]
    struct TableInvariants {
        lat_varies: bool,
        lon_varies: bool,
        time_varies: bool,
    }

    #[derive(Clone)]
    struct TableLayout {
        headers: Vec<&'static str>,
        col_widths: Vec<usize>,
        elevation_angle: bool,
    }

    fn detect_invariants(source: &DataSource) -> TableInvariants {
        match source {
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
                TableInvariants {
                    lat_varies,
                    lon_varies,
                    time_varies,
                }
            }
            DataSource::Paired(_) => TableInvariants {
                lat_varies: true,
                lon_varies: true,
                time_varies: true,
            },
        }
    }

    fn invariants_header(
        params: &Parameters,
        source: &DataSource,
        invariants: &TableInvariants,
        first_row: &PositionRow,
    ) -> String {
        let mut header = String::new();
        if !invariants.lat_varies {
            header.push_str(&format!("  Latitude:    {:.6}°\n", first_row.lat));
        }
        if !invariants.lon_varies {
            header.push_str(&format!("  Longitude:   {:.6}°\n", first_row.lon));
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
        if !invariants.time_varies {
            header.push_str(&format!(
                "  DateTime:    {}\n",
                first_row.datetime.format("%Y-%m-%d %H:%M:%S%:z")
            ));
        } else if invariants.lat_varies
            && let DataSource::Separate(_, TimeSource::Range(date_str, _)) = source
            && date_str.len() == 10
        {
            header.push_str(&format!(
                "  DateTime:    {}\n",
                first_row.datetime.format("%Y-%m-%d %H:%M:%S%:z")
            ));
        }
        header.push_str(&format!("  Delta T:     {:.1} s\n", first_row.deltat));
        header.push('\n');
        header
    }

    fn build_table_layout(invariants: &TableInvariants, elevation_angle: bool) -> TableLayout {
        let mut headers = Vec::new();
        if invariants.lat_varies {
            headers.push("Latitude");
        }
        if invariants.lon_varies {
            headers.push("Longitude");
        }
        if invariants.time_varies {
            headers.push("DateTime");
        }
        headers.push("Azimuth");
        headers.push(if elevation_angle {
            "Elevation"
        } else {
            "Zenith"
        });

        let col_widths: Vec<usize> = headers
            .iter()
            .map(|h| {
                if invariants.time_varies && *h == "DateTime" {
                    22 // "YYYY-MM-DD HH:MM±HH:MM"
                } else {
                    h.len().max(14)
                }
            })
            .collect();

        TableLayout {
            headers,
            col_widths,
            elevation_angle,
        }
    }

    fn render_table_header(layout: &TableLayout) -> String {
        let mut header = String::new();

        header.push('┌');
        for (i, width) in layout.col_widths.iter().enumerate() {
            header.push_str(&"─".repeat(width + 2));
            if i < layout.col_widths.len() - 1 {
                header.push('┬');
            }
        }
        header.push_str("┐\n");

        header.push('│');
        for (h, width) in layout.headers.iter().zip(&layout.col_widths) {
            header.push_str(&format!(" {:<width$} ", h, width = width));
            header.push('│');
        }
        header.push('\n');

        header.push('├');
        for (i, width) in layout.col_widths.iter().enumerate() {
            header.push_str(&"─".repeat(width + 2));
            if i < layout.col_widths.len() - 1 {
                header.push('┼');
            }
        }
        header.push_str("┤\n");

        header
    }

    fn render_table_footer(layout: &TableLayout) -> String {
        let mut footer = String::from('└');
        for (i, width) in layout.col_widths.iter().enumerate() {
            footer.push_str(&"─".repeat(width + 2));
            if i < layout.col_widths.len() - 1 {
                footer.push('┴');
            }
        }
        footer.push_str("┘\n");
        footer
    }

    fn format_position_row(
        row: &PositionRow,
        invariants: &TableInvariants,
        layout: &TableLayout,
    ) -> String {
        let mut output = String::from('│');
        let mut col_idx = 0;

        if invariants.lat_varies {
            output.push_str(&format!(
                " {:>width$.5}° ",
                row.lat,
                width = layout.col_widths[col_idx] - 1
            ));
            output.push('│');
            col_idx += 1;
        }
        if invariants.lon_varies {
            output.push_str(&format!(
                " {:>width$.5}° ",
                row.lon,
                width = layout.col_widths[col_idx] - 1
            ));
            output.push('│');
            col_idx += 1;
        }
        if invariants.time_varies {
            let dt_str = row.datetime.format("%Y-%m-%d %H:%M%:z").to_string();
            output.push_str(&format!(
                " {:<width$} ",
                dt_str,
                width = layout.col_widths[col_idx]
            ));
            output.push('│');
            col_idx += 1;
        }
        output.push_str(&format!(
            " {:>width$.5}° ",
            row.azimuth,
            width = layout.col_widths[col_idx] - 1
        ));
        output.push('│');
        col_idx += 1;

        let angle = row.angle(layout.elevation_angle);
        output.push_str(&format!(
            " {:>width$.5}° ",
            angle,
            width = layout.col_widths[col_idx] - 1
        ));
        output.push_str("│\n");
        output
    }

    fn format_streaming_text_table(
        mut results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
        params: &Parameters,
        source: DataSource,
    ) -> Box<dyn Iterator<Item = Result<(String, bool), String>>> {
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

        let invariants = detect_invariants(&source);
        let layout = build_table_layout(&invariants, params.output.elevation_angle);
        let mut header = invariants_header(params, &source, &invariants, &first_row);
        header.push_str(&render_table_header(&layout));

        let invariants_for_rows = invariants.clone();
        let layout_for_rows = layout.clone();
        let format_row = move |row: &PositionRow| -> String {
            format_position_row(row, &invariants_for_rows, &layout_for_rows)
        };
        let footer = render_table_footer(&layout);

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

    pub fn write_streaming_text_table<W: std::io::Write>(
        results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
        params: &Parameters,
        source: DataSource,
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

        write_rows(
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

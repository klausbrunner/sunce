//! Output formatting for CSV, JSON, and text table formats.

use crate::compute::CalculationResult;
use crate::data::{Command, Parameters};
use chrono::{DateTime, FixedOffset};
use solar_positioning::SunriseResult;

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
fn sunrise_type_str(result: &SunriseResult<impl std::any::Any>, is_json: bool) -> &'static str {
    match result {
        SunriseResult::RegularDay { .. } => "NORMAL",
        SunriseResult::AllDay { .. } => "ALL_DAY",
        SunriseResult::AllNight { .. } => {
            if is_json {
                "NO_DAY"
            } else {
                "ALL_NIGHT"
            }
        }
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
    result: &CalculationResult,
    show_inputs: bool,
    headers: bool,
    first: bool,
    params: &Parameters,
    writer: &mut W,
) -> std::io::Result<()> {
    match result {
        CalculationResult::Position {
            lat,
            lon,
            datetime,
            position,
            deltat,
        } => {
            if first && headers {
                if show_inputs {
                    if params.elevation_angle {
                        write!(
                            writer,
                            "latitude,longitude,elevation,pressure,temperature,dateTime,deltaT,azimuth,elevation-angle\r\n"
                        )?;
                    } else {
                        write!(
                            writer,
                            "latitude,longitude,elevation,pressure,temperature,dateTime,deltaT,azimuth,zenith\r\n"
                        )?;
                    }
                } else if params.elevation_angle {
                    write!(writer, "dateTime,azimuth,elevation-angle\r\n")?;
                } else {
                    write!(writer, "dateTime,azimuth,zenith\r\n")?;
                }
            }

            let angle_value = if params.elevation_angle {
                90.0 - position.zenith_angle()
            } else {
                position.zenith_angle()
            };

            if show_inputs {
                write!(
                    writer,
                    "{:.5},{:.5},{:.3},{:.3},{:.3},{},{:.3},{:.5},{:.5}\r\n",
                    lat,
                    lon,
                    params.elevation,
                    params.pressure,
                    params.temperature,
                    datetime.to_rfc3339(),
                    deltat,
                    position.azimuth(),
                    angle_value
                )?;
            } else {
                write!(
                    writer,
                    "{},{:.5},{:.5}\r\n",
                    datetime.to_rfc3339(),
                    position.azimuth(),
                    angle_value
                )?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn write_csv_sunrise<W: std::io::Write>(
    result: &CalculationResult,
    show_inputs: bool,
    headers: bool,
    first: bool,
    writer: &mut W,
) -> std::io::Result<()> {
    match result {
        CalculationResult::Sunrise {
            lat,
            lon,
            date,
            result: sunrise_result,
            deltat,
        } => {
            if first && headers {
                if show_inputs {
                    write!(
                        writer,
                        "latitude,longitude,dateTime,deltaT,type,sunrise,transit,sunset\r\n"
                    )?;
                } else {
                    write!(writer, "dateTime,type,sunrise,transit,sunset\r\n")?;
                }
            }

            let type_str = sunrise_type_str(sunrise_result, false);
            let (sunrise_opt, transit, sunset_opt) = extract_sunrise_times(sunrise_result);
            let (sunrise_str, transit_str, sunset_str) = (
                format_datetime_opt(sunrise_opt),
                format_datetime(transit),
                format_datetime_opt(sunset_opt),
            );

            if show_inputs {
                write!(
                    writer,
                    "{:.5},{:.5},{},{:.3},{},{},{},{}\r\n",
                    lat,
                    lon,
                    date.to_rfc3339(),
                    deltat,
                    type_str,
                    sunrise_str,
                    transit_str,
                    sunset_str
                )?;
            } else {
                write!(
                    writer,
                    "{},{},{},{},{}\r\n",
                    date.format("%Y-%m-%d"),
                    type_str,
                    sunrise_str,
                    transit_str,
                    sunset_str
                )?;
            }
            Ok(())
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
            if first && headers {
                if show_inputs {
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
            }

            let type_str = sunrise_type_str(sunrise_sunset, false);

            let (sunrise_opt, transit, sunset_opt) = extract_sunrise_times(sunrise_sunset);
            let (sunrise_str, transit_str, sunset_str) = (
                format_datetime_opt(sunrise_opt),
                format_datetime(transit),
                format_datetime_opt(sunset_opt),
            );

            let (civil_start_opt, _, civil_end_opt) = extract_sunrise_times(civil);
            let (civil_start, civil_end) = (
                format_datetime_opt(civil_start_opt),
                format_datetime_opt(civil_end_opt),
            );

            let (nautical_start_opt, _, nautical_end_opt) = extract_sunrise_times(nautical);
            let (nautical_start, nautical_end) = (
                format_datetime_opt(nautical_start_opt),
                format_datetime_opt(nautical_end_opt),
            );

            let (astro_start_opt, _, astro_end_opt) = extract_sunrise_times(astronomical);
            let (astro_start, astro_end) = (
                format_datetime_opt(astro_start_opt),
                format_datetime_opt(astro_end_opt),
            );

            if show_inputs {
                write!(
                    writer,
                    "{:.5},{:.5},{},{:.3},{},{},{},{},{},{},{},{},{},{}\r\n",
                    lat,
                    lon,
                    date.to_rfc3339(),
                    deltat,
                    type_str,
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
                    date.format("%Y-%m-%d"),
                    type_str,
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
            Ok(())
        }
        _ => Ok(()),
    }
}

fn write_json_position<W: std::io::Write>(
    result: &CalculationResult,
    show_inputs: bool,
    params: &Parameters,
    writer: &mut W,
) -> std::io::Result<()> {
    match result {
        CalculationResult::Position {
            lat,
            lon,
            datetime,
            position,
            deltat,
        } => {
            let angle_label = if params.elevation_angle {
                "elevation"
            } else {
                "zenith"
            };
            let angle_value = if params.elevation_angle {
                90.0 - position.zenith_angle()
            } else {
                position.zenith_angle()
            };

            if show_inputs {
                writeln!(
                    writer,
                    r#"{{"latitude":{},"longitude":{},"elevation":{},"pressure":{},"temperature":{},"dateTime":"{}","deltaT":{},"azimuth":{},"{}":{}}}"#,
                    lat,
                    lon,
                    params.elevation,
                    params.pressure,
                    params.temperature,
                    datetime.to_rfc3339(),
                    deltat,
                    position.azimuth(),
                    angle_label,
                    angle_value
                )
            } else {
                writeln!(
                    writer,
                    r#"{{"dateTime":"{}","azimuth":{},"{}":{}}}"#,
                    datetime.to_rfc3339(),
                    position.azimuth(),
                    angle_label,
                    angle_value
                )
            }
        }
        _ => Ok(()),
    }
}

fn write_json_sunrise<W: std::io::Write>(
    result: &CalculationResult,
    show_inputs: bool,
    writer: &mut W,
) -> std::io::Result<()> {
    match result {
        CalculationResult::Sunrise {
            lat,
            lon,
            date,
            result: sunrise_result,
            deltat,
        } => {
            let type_str = sunrise_type_str(sunrise_result, true);
            let (sunrise_opt, transit, sunset_opt) = extract_sunrise_times(sunrise_result);

            if show_inputs {
                writeln!(
                    writer,
                    r#"{{"latitude":{},"longitude":{},"dateTime":"{}","deltaT":{:.3},"type":"{}","sunrise":"{}","transit":"{}","sunset":"{}"}}"#,
                    lat,
                    lon,
                    date.to_rfc3339(),
                    deltat,
                    type_str,
                    format_datetime_opt(sunrise_opt),
                    format_datetime(transit),
                    format_datetime_opt(sunset_opt)
                )
            } else {
                writeln!(
                    writer,
                    r#"{{"type":"{}","sunrise":"{}","transit":"{}","sunset":"{}"}}"#,
                    type_str,
                    format_datetime_opt(sunrise_opt),
                    format_datetime(transit),
                    format_datetime_opt(sunset_opt)
                )
            }
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
            let type_str = sunrise_type_str(sunrise_sunset, true);
            let (sunrise_opt, transit, sunset_opt) = extract_sunrise_times(sunrise_sunset);
            let (civil_start_opt, _, civil_end_opt) = extract_sunrise_times(civil);
            let (nautical_start_opt, _, nautical_end_opt) = extract_sunrise_times(nautical);
            let (astro_start_opt, _, astro_end_opt) = extract_sunrise_times(astronomical);

            if show_inputs {
                writeln!(
                    writer,
                    r#"{{"latitude":{:.5},"longitude":{:.5},"dateTime":"{}","deltaT":{:.3},"type":"{}","sunrise":"{}","transit":"{}","sunset":"{}","civil_start":{},"civil_end":{},"nautical_start":{},"nautical_end":{},"astronomical_start":{},"astronomical_end":{}}}"#,
                    lat,
                    lon,
                    date.to_rfc3339(),
                    deltat,
                    type_str,
                    format_datetime_opt(sunrise_opt),
                    format_datetime(transit),
                    format_datetime_opt(sunset_opt),
                    format_datetime_json_null(civil_start_opt),
                    format_datetime_json_null(civil_end_opt),
                    format_datetime_json_null(nautical_start_opt),
                    format_datetime_json_null(nautical_end_opt),
                    format_datetime_json_null(astro_start_opt),
                    format_datetime_json_null(astro_end_opt)
                )
            } else {
                writeln!(
                    writer,
                    r#"{{"type":"{}","sunrise":"{}","transit":"{}","sunset":"{}","civil_start":{},"civil_end":{},"nautical_start":{},"nautical_end":{},"astronomical_start":{},"astronomical_end":{}}}"#,
                    type_str,
                    format_datetime_opt(sunrise_opt),
                    format_datetime(transit),
                    format_datetime_opt(sunset_opt),
                    format_datetime_json_null(civil_start_opt),
                    format_datetime_json_null(civil_end_opt),
                    format_datetime_json_null(nautical_start_opt),
                    format_datetime_json_null(nautical_end_opt),
                    format_datetime_json_null(astro_start_opt),
                    format_datetime_json_null(astro_end_opt)
                )
            }
        }
        _ => Ok(()),
    }
}

fn write_text_position<W: std::io::Write>(
    result: &CalculationResult,
    show_inputs: bool,
    elevation_angle: bool,
    writer: &mut W,
) -> std::io::Result<()> {
    match result {
        CalculationResult::Position {
            lat,
            lon,
            datetime,
            position,
            ..
        } => {
            let mut lines = Vec::new();

            if show_inputs {
                lines.push(format!("│ Location   {}, {}", lat, lon));
            }
            lines.push(format!(
                "│ DateTime:    {}",
                datetime.format("%Y-%m-%d %H:%M:%S%:z")
            ));
            lines.push(format!("│ Azimuth    {:.5}°", position.azimuth()));

            if elevation_angle {
                lines.push(format!(
                    "│ Elevation  {:.5}°",
                    90.0 - position.zenith_angle()
                ));
            } else {
                lines.push(format!("│ Zenith     {:.5}°", position.zenith_angle()));
            }

            let max_width = lines.iter().map(|line| line.len()).max().unwrap_or(0);
            let box_width = max_width + 2;

            writeln!(writer, "┌{}┐", "─".repeat(box_width - 2))?;
            for line in lines {
                writeln!(writer, "{}", line)?;
            }
            writeln!(writer, "└{}┘", "─".repeat(box_width - 2))?;
            Ok(())
        }
        _ => Ok(()),
    }
}

fn write_text_sunrise<W: std::io::Write>(
    result: &CalculationResult,
    _show_inputs: bool,
    writer: &mut W,
) -> std::io::Result<()> {
    match result {
        CalculationResult::Sunrise {
            result: sunrise_result,
            ..
        } => {
            use solar_positioning::SunriseResult;
            match sunrise_result {
                SunriseResult::RegularDay {
                    sunrise,
                    sunset,
                    transit,
                } => {
                    writeln!(writer, "type   : normal")?;
                    writeln!(writer, "sunrise: {}", format_datetime_text(sunrise))?;
                    writeln!(writer, "transit: {}", format_datetime_text(transit))?;
                    writeln!(writer, "sunset : {}", format_datetime_text(sunset))?;
                }
                SunriseResult::AllDay { transit } => {
                    writeln!(writer, "type   : all day")?;
                    writeln!(writer, "sunrise: ")?;
                    writeln!(writer, "transit: {}", format_datetime_text(transit))?;
                    writeln!(writer, "sunset : ")?;
                }
                SunriseResult::AllNight { transit } => {
                    writeln!(writer, "type   : all night")?;
                    writeln!(writer, "sunrise: ")?;
                    writeln!(writer, "transit: {}", format_datetime_text(transit))?;
                    writeln!(writer, "sunset : ")?;
                }
            }
            Ok(())
        }
        CalculationResult::SunriseWithTwilight {
            sunrise_sunset,
            civil,
            nautical,
            astronomical,
            ..
        } => {
            use solar_positioning::SunriseResult;
            match sunrise_sunset {
                SunriseResult::RegularDay {
                    sunrise,
                    sunset,
                    transit,
                } => {
                    writeln!(writer, "type   : normal")?;
                    writeln!(writer, "sunrise: {}", format_datetime_text(sunrise))?;
                    writeln!(writer, "transit: {}", format_datetime_text(transit))?;
                    writeln!(writer, "sunset : {}", format_datetime_text(sunset))?;
                }
                SunriseResult::AllDay { transit } => {
                    writeln!(writer, "type   : all day")?;
                    writeln!(writer, "sunrise: ")?;
                    writeln!(writer, "transit: {}", format_datetime_text(transit))?;
                    writeln!(writer, "sunset : ")?;
                }
                SunriseResult::AllNight { transit } => {
                    writeln!(writer, "type   : all night")?;
                    writeln!(writer, "sunrise: ")?;
                    writeln!(writer, "transit: {}", format_datetime_text(transit))?;
                    writeln!(writer, "sunset : ")?;
                }
            }

            if let SunriseResult::RegularDay {
                sunrise: civil_start,
                sunset: civil_end,
                ..
            } = civil
            {
                writeln!(
                    writer,
                    "civil twilight start: {}",
                    format_datetime_text(civil_start)
                )?;
                writeln!(
                    writer,
                    "civil twilight end  : {}",
                    format_datetime_text(civil_end)
                )?;
            }

            if let SunriseResult::RegularDay {
                sunrise: naut_start,
                sunset: naut_end,
                ..
            } = nautical
            {
                writeln!(
                    writer,
                    "nautical twilight start: {}",
                    format_datetime_text(naut_start)
                )?;
                writeln!(
                    writer,
                    "nautical twilight end  : {}",
                    format_datetime_text(naut_end)
                )?;
            }

            if let SunriseResult::RegularDay {
                sunrise: astro_start,
                sunset: astro_end,
                ..
            } = astronomical
            {
                writeln!(
                    writer,
                    "astronomical twilight start: {}",
                    format_datetime_text(astro_start)
                )?;
                writeln!(
                    writer,
                    "astronomical twilight end  : {}",
                    format_datetime_text(astro_end)
                )?;
            }

            Ok(())
        }
        _ => Ok(()),
    }
}

fn write_streaming_text_table<W: std::io::Write>(
    results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
    params: &Parameters,
    source: crate::data::DataSource,
    writer: &mut W,
) -> std::io::Result<usize> {
    let formatted = format_streaming_text_table(results, params, source);
    let mut count = 0;
    for line_result in formatted {
        match line_result {
            Ok(line) => {
                write!(writer, "{}", line)?;
                count += 1;
            }
            Err(e) => return Err(std::io::Error::other(e)),
        }
    }
    Ok(count)
}

pub fn write_formatted_output<W: std::io::Write>(
    results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
    command: Command,
    params: &Parameters,
    source: crate::data::DataSource,
    writer: &mut W,
    flush_each: bool,
) -> std::io::Result<usize> {
    let format = params.format.clone();

    if format == "text" && matches!(command, Command::Position) {
        return write_streaming_text_table(results, params, source, writer);
    }

    let show_inputs = params.show_inputs.unwrap_or(false);
    let headers = params.headers;

    let mut count = 0;
    for (index, result_or_err) in results.enumerate() {
        let result = match result_or_err {
            Ok(r) => r,
            Err(e) => return Err(std::io::Error::other(e)),
        };
        let first = index == 0;

        match format.as_str() {
            "csv" => match command {
                Command::Position => {
                    write_csv_position(&result, show_inputs, headers, first, params, writer)?
                }
                Command::Sunrise => {
                    write_csv_sunrise(&result, show_inputs, headers, first, writer)?
                }
            },
            "json" => match command {
                Command::Position => write_json_position(&result, show_inputs, params, writer)?,
                Command::Sunrise => write_json_sunrise(&result, show_inputs, writer)?,
            },
            _ => match command {
                Command::Position => {
                    write_text_position(&result, show_inputs, params.elevation_angle, writer)?
                }
                Command::Sunrise => write_text_sunrise(&result, show_inputs, writer)?,
            },
        }

        if flush_each {
            writer.flush()?;
        }
        count += 1;
    }
    Ok(count)
}

fn format_streaming_text_table(
    mut results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
    params: &Parameters,
    source: crate::data::DataSource,
) -> Box<dyn Iterator<Item = Result<String, String>>> {
    use crate::data::{DataSource, LocationSource, TimeSource};

    // Peek at first result to get invariant values and determine table structure
    let first = match results.next() {
        Some(Ok(r)) => r,
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
    let (lat0, lon0, dt0, deltat0) = match &first {
        CalculationResult::Position {
            lat,
            lon,
            datetime,
            deltat,
            ..
        } => (*lat, *lon, *datetime, *deltat),
        _ => return Box::new(std::iter::empty()),
    };

    // Build header section
    let mut header = String::new();
    if !lat_varies {
        header.push_str(&format!("  Latitude:    {:.6}°\n", lat0));
    }
    if !lon_varies {
        header.push_str(&format!("  Longitude:   {:.6}°\n", lon0));
    }
    header.push_str(&format!("  Elevation:   {:.1} m\n", params.elevation));
    header.push_str(&format!("  Pressure:    {:.1} hPa\n", params.pressure));
    header.push_str(&format!("  Temperature: {:.1}°C\n", params.temperature));
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
    if params.elevation_angle {
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
    let elevation_angle = params.elevation_angle;
    let format_row = move |result: &CalculationResult| -> String {
        if let CalculationResult::Position {
            lat,
            lon,
            datetime,
            position,
            ..
        } = result
        {
            let mut output = String::from('│');
            let mut col_idx = 0;

            if lat_varies {
                output.push_str(&format!(
                    " {:>width$.5}° ",
                    lat,
                    width = col_widths_clone[col_idx] - 1
                ));
                output.push('│');
                col_idx += 1;
            }
            if lon_varies {
                output.push_str(&format!(
                    " {:>width$.5}° ",
                    lon,
                    width = col_widths_clone[col_idx] - 1
                ));
                output.push('│');
                col_idx += 1;
            }
            if time_varies {
                let dt_str = datetime.format("%Y-%m-%d %H:%M%:z").to_string();
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
                position.azimuth(),
                width = col_widths_clone[col_idx] - 1
            ));
            output.push('│');
            col_idx += 1;

            let angle = if elevation_angle {
                90.0 - position.zenith_angle()
            } else {
                position.zenith_angle()
            };
            output.push_str(&format!(
                " {:>width$.5}° ",
                angle,
                width = col_widths_clone[col_idx] - 1
            ));
            output.push_str("│\n");
            output
        } else {
            String::new()
        }
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
    let first_row = format_row(&first);
    let remaining_rows = results.map(move |r_or_err| match r_or_err {
        Ok(r) => Ok(format_row(&r)),
        Err(e) => Err(e),
    });

    Box::new(
        std::iter::once(Ok(header))
            .chain(std::iter::once(Ok(first_row)))
            .chain(remaining_rows)
            .chain(std::iter::once(Ok(footer))),
    )
}

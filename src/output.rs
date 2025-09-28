//! Output formatting for CSV, JSON, and text table formats.

use crate::compute::CalculationResult;
use crate::data::{Command, Parameters};

#[cfg(feature = "parquet")]
pub fn write_parquet_output<W: std::io::Write + Send>(
    results: Box<dyn Iterator<Item = CalculationResult>>,
    command: Command,
    params: &Parameters,
    writer: W,
) -> std::io::Result<usize> {
    crate::parquet::write_parquet(results, command, params, writer)
}

fn format_csv_position(
    result: &CalculationResult,
    show_inputs: bool,
    headers: bool,
    first: bool,
    params: &Parameters,
) -> String {
    match result {
        CalculationResult::Position {
            lat,
            lon,
            datetime,
            position,
            deltat,
        } => {
            let mut output = String::new();

            // Headers on first row if requested - match original exactly
            if first && headers {
                if show_inputs {
                    if params.elevation_angle {
                        output.push_str("latitude,longitude,elevation,pressure,temperature,dateTime,deltaT,azimuth,elevation-angle\n");
                    } else {
                        output.push_str("latitude,longitude,elevation,pressure,temperature,dateTime,deltaT,azimuth,zenith\n");
                    }
                } else if params.elevation_angle {
                    output.push_str("dateTime,azimuth,elevation-angle\n");
                } else {
                    output.push_str("dateTime,azimuth,zenith\n");
                }
            }

            // Data row - match original precision exactly
            let angle_value = if params.elevation_angle {
                90.0 - position.zenith_angle()
            } else {
                position.zenith_angle()
            };

            if show_inputs {
                output.push_str(&format!(
                    "{:.5},{:.5},{:.3},{:.3},{:.3},{},{:.3},{:.5},{:.5}\n",
                    lat,
                    lon,
                    params.elevation,
                    params.pressure,
                    params.temperature,
                    datetime.to_rfc3339(),
                    deltat,
                    position.azimuth(),
                    angle_value
                ));
            } else {
                output.push_str(&format!(
                    "{},{:.5},{:.5}\n",
                    datetime.to_rfc3339(),
                    position.azimuth(),
                    angle_value
                ));
            }
            output
        }
        _ => String::new(),
    }
}

fn format_csv_sunrise(
    result: &CalculationResult,
    show_inputs: bool,
    headers: bool,
    first: bool,
) -> String {
    // Format times without milliseconds to match solarpos
    let format_time = |dt: Option<&chrono::DateTime<chrono::FixedOffset>>| -> String {
        dt.map(|d| d.format("%Y-%m-%dT%H:%M:%S%:z").to_string())
            .unwrap_or_default()
    };

    let extract_times = |result: &solar_positioning::SunriseResult<_>| -> (String, String, String) {
        use solar_positioning::SunriseResult;
        match result {
            SunriseResult::RegularDay {
                sunrise,
                transit,
                sunset,
            } => (
                format_time(Some(sunrise)),
                format_time(Some(transit)),
                format_time(Some(sunset)),
            ),
            SunriseResult::AllDay { transit } => {
                (String::new(), format_time(Some(transit)), String::new())
            }
            SunriseResult::AllNight { transit } => {
                (String::new(), format_time(Some(transit)), String::new())
            }
        }
    };

    match result {
        CalculationResult::Sunrise {
            lat,
            lon,
            date,
            result: sunrise_result,
            deltat,
        } => {
            let mut output = String::new();

            if first && headers {
                if show_inputs {
                    output.push_str(
                        "latitude,longitude,dateTime,deltaT,type,sunrise,transit,sunset\n",
                    );
                } else {
                    output.push_str("dateTime,type,sunrise,transit,sunset\n");
                }
            }

            use solar_positioning::SunriseResult;
            let type_str = match sunrise_result {
                SunriseResult::RegularDay { .. } => "NORMAL",
                SunriseResult::AllDay { .. } => "ALL_DAY",
                SunriseResult::AllNight { .. } => "ALL_NIGHT",
            };

            let (sunrise_str, transit_str, sunset_str) = extract_times(sunrise_result);

            if show_inputs {
                output.push_str(&format!(
                    "{:.5},{:.5},{},{:.3},{},{},{},{}\n",
                    lat,
                    lon,
                    date.to_rfc3339(),
                    deltat,
                    type_str,
                    sunrise_str,
                    transit_str,
                    sunset_str
                ));
            } else {
                output.push_str(&format!(
                    "{},{},{},{},{}\n",
                    date.format("%Y-%m-%d"),
                    type_str,
                    sunrise_str,
                    transit_str,
                    sunset_str
                ));
            }
            output
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
            let mut output = String::new();

            if first && headers {
                if show_inputs {
                    output.push_str("latitude,longitude,dateTime,deltaT,type,sunrise,transit,sunset,civil_start,civil_end,nautical_start,nautical_end,astronomical_start,astronomical_end\n");
                } else {
                    output.push_str("dateTime,type,sunrise,transit,sunset,civil_start,civil_end,nautical_start,nautical_end,astronomical_start,astronomical_end\n");
                }
            }

            use solar_positioning::SunriseResult;
            let type_str = match sunrise_sunset {
                SunriseResult::RegularDay { .. } => "NORMAL",
                SunriseResult::AllDay { .. } => "ALL_DAY",
                SunriseResult::AllNight { .. } => "ALL_NIGHT",
            };

            let (sunrise_str, transit_str, sunset_str) = extract_times(sunrise_sunset);
            let (civil_start, _, civil_end) = extract_times(civil);
            let (nautical_start, _, nautical_end) = extract_times(nautical);
            let (astro_start, _, astro_end) = extract_times(astronomical);

            if show_inputs {
                output.push_str(&format!(
                    "{:.5},{:.5},{},{:.3},{},{},{},{},{},{},{},{},{},{}\n",
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
                ));
            } else {
                output.push_str(&format!(
                    "{},{},{},{},{},{},{},{},{},{},{}\n",
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
                ));
            }
            output
        }
        _ => String::new(),
    }
}

fn format_json_position(
    result: &CalculationResult,
    show_inputs: bool,
    params: &Parameters,
) -> String {
    match result {
        CalculationResult::Position {
            lat,
            lon,
            datetime,
            position,
            deltat,
        } => {
            if show_inputs {
                // Include all input parameters when show_inputs is true
                format!(
                    r#"{{"latitude":{},"longitude":{},"elevation":{},"pressure":{},"temperature":{},"dateTime":"{}","deltaT":{},"azimuth":{},"zenith":{}}}"#,
                    lat,
                    lon,
                    params.elevation,
                    params.pressure,
                    params.temperature,
                    datetime.to_rfc3339(),
                    deltat,
                    position.azimuth(),
                    position.zenith_angle()
                )
            } else {
                // Default: just dateTime, azimuth, zenith
                format!(
                    r#"{{"dateTime":"{}","azimuth":{},"zenith":{}}}"#,
                    datetime.to_rfc3339(),
                    position.azimuth(),
                    position.zenith_angle()
                )
            }
        }
        _ => String::new(),
    }
}

fn format_json_sunrise(result: &CalculationResult, show_inputs: bool) -> String {
    let format_time = |dt: &chrono::DateTime<chrono::FixedOffset>| -> String {
        dt.format("%Y-%m-%dT%H:%M:%S%:z").to_string()
    };

    let format_time_opt = |dt: Option<&chrono::DateTime<chrono::FixedOffset>>| -> String {
        dt.map(format_time).unwrap_or_default()
    };

    match result {
        CalculationResult::Sunrise {
            lat,
            lon,
            date,
            result: sunrise_result,
            deltat,
        } => {
            use solar_positioning::SunriseResult;
            let type_str = match sunrise_result {
                SunriseResult::RegularDay { .. } => "NORMAL",
                SunriseResult::AllDay { .. } => "ALL_DAY",
                SunriseResult::AllNight { .. } => "NO_DAY",
            };

            let (sunrise_opt, transit, sunset_opt) = match sunrise_result {
                SunriseResult::RegularDay {
                    sunrise,
                    transit,
                    sunset,
                } => (Some(sunrise), transit, Some(sunset)),
                SunriseResult::AllDay { transit } | SunriseResult::AllNight { transit } => {
                    (None, transit, None)
                }
            };

            if show_inputs {
                format!(
                    r#"{{"latitude":{},"longitude":{},"dateTime":"{}","deltaT":{:.3},"type":"{}","sunrise":"{}","transit":"{}","sunset":"{}"}}"#,
                    lat,
                    lon,
                    date.to_rfc3339(),
                    deltat,
                    type_str,
                    format_time_opt(sunrise_opt),
                    format_time(transit),
                    format_time_opt(sunset_opt)
                )
            } else {
                format!(
                    r#"{{"type":"{}","sunrise":"{}","transit":"{}","sunset":"{}"}}"#,
                    type_str,
                    format_time_opt(sunrise_opt),
                    format_time(transit),
                    format_time_opt(sunset_opt)
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
            use solar_positioning::SunriseResult;
            let type_str = match sunrise_sunset {
                SunriseResult::RegularDay { .. } => "NORMAL",
                SunriseResult::AllDay { .. } => "ALL_DAY",
                SunriseResult::AllNight { .. } => "NO_DAY",
            };

            let (sunrise_opt, transit, sunset_opt) = match sunrise_sunset {
                SunriseResult::RegularDay {
                    sunrise,
                    transit,
                    sunset,
                } => (Some(sunrise), transit, Some(sunset)),
                SunriseResult::AllDay { transit } | SunriseResult::AllNight { transit } => {
                    (None, transit, None)
                }
            };

            let (civil_start_opt, civil_end_opt) = match civil {
                SunriseResult::RegularDay {
                    sunrise, sunset, ..
                } => (Some(sunrise), Some(sunset)),
                _ => (None, None),
            };

            let (nautical_start_opt, nautical_end_opt) = match nautical {
                SunriseResult::RegularDay {
                    sunrise, sunset, ..
                } => (Some(sunrise), Some(sunset)),
                _ => (None, None),
            };

            let (astro_start_opt, astro_end_opt) = match astronomical {
                SunriseResult::RegularDay {
                    sunrise, sunset, ..
                } => (Some(sunrise), Some(sunset)),
                _ => (None, None),
            };

            if show_inputs {
                format!(
                    r#"{{"latitude":{:.5},"longitude":{:.5},"dateTime":"{}","deltaT":{:.3},"type":"{}","sunrise":"{}","transit":"{}","sunset":"{}","civil_start":{},"civil_end":{},"nautical_start":{},"nautical_end":{},"astronomical_start":{},"astronomical_end":{}}}"#,
                    lat,
                    lon,
                    date.to_rfc3339(),
                    deltat,
                    type_str,
                    format_time_opt(sunrise_opt),
                    format_time(transit),
                    format_time_opt(sunset_opt),
                    civil_start_opt
                        .map(|t| format!(r#""{}""#, format_time(t)))
                        .unwrap_or_else(|| "null".to_string()),
                    civil_end_opt
                        .map(|t| format!(r#""{}""#, format_time(t)))
                        .unwrap_or_else(|| "null".to_string()),
                    nautical_start_opt
                        .map(|t| format!(r#""{}""#, format_time(t)))
                        .unwrap_or_else(|| "null".to_string()),
                    nautical_end_opt
                        .map(|t| format!(r#""{}""#, format_time(t)))
                        .unwrap_or_else(|| "null".to_string()),
                    astro_start_opt
                        .map(|t| format!(r#""{}""#, format_time(t)))
                        .unwrap_or_else(|| "null".to_string()),
                    astro_end_opt
                        .map(|t| format!(r#""{}""#, format_time(t)))
                        .unwrap_or_else(|| "null".to_string())
                )
            } else {
                format!(
                    r#"{{"type":"{}","sunrise":"{}","transit":"{}","sunset":"{}","civil_start":{},"civil_end":{},"nautical_start":{},"nautical_end":{},"astronomical_start":{},"astronomical_end":{}}}"#,
                    type_str,
                    format_time_opt(sunrise_opt),
                    format_time(transit),
                    format_time_opt(sunset_opt),
                    civil_start_opt
                        .map(|t| format!(r#""{}""#, format_time(t)))
                        .unwrap_or_else(|| "null".to_string()),
                    civil_end_opt
                        .map(|t| format!(r#""{}""#, format_time(t)))
                        .unwrap_or_else(|| "null".to_string()),
                    nautical_start_opt
                        .map(|t| format!(r#""{}""#, format_time(t)))
                        .unwrap_or_else(|| "null".to_string()),
                    nautical_end_opt
                        .map(|t| format!(r#""{}""#, format_time(t)))
                        .unwrap_or_else(|| "null".to_string()),
                    astro_start_opt
                        .map(|t| format!(r#""{}""#, format_time(t)))
                        .unwrap_or_else(|| "null".to_string()),
                    astro_end_opt
                        .map(|t| format!(r#""{}""#, format_time(t)))
                        .unwrap_or_else(|| "null".to_string())
                )
            }
        }
        _ => String::new(),
    }
}

fn format_text_position(
    result: &CalculationResult,
    show_inputs: bool,
    elevation_angle: bool,
) -> String {
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

            // Calculate the width of the box
            let max_width = lines.iter().map(|line| line.len()).max().unwrap_or(0);
            let box_width = max_width + 2; // Add padding

            let mut output = String::new();
            output.push_str(&format!("┌{}\n", "─".repeat(box_width - 2)));
            for line in lines {
                output.push_str(&format!("{}\n", line));
            }
            output.push_str(&format!("└{}\n", "─".repeat(box_width - 2)));

            output
        }
        _ => String::new(),
    }
}

fn format_text_sunrise(result: &CalculationResult, _show_inputs: bool) -> String {
    let format_time = |dt: &chrono::DateTime<chrono::FixedOffset>| -> String {
        dt.format("%Y-%m-%d %H:%M:%S%:z").to_string()
    };

    match result {
        CalculationResult::Sunrise {
            result: sunrise_result,
            ..
        } => {
            let mut output = String::new();

            use solar_positioning::SunriseResult;
            match sunrise_result {
                SunriseResult::RegularDay {
                    sunrise,
                    sunset,
                    transit,
                } => {
                    output.push_str("type   : normal\n");
                    output.push_str(&format!("sunrise: {}\n", format_time(sunrise)));
                    output.push_str(&format!("transit: {}\n", format_time(transit)));
                    output.push_str(&format!("sunset : {}\n", format_time(sunset)));
                }
                SunriseResult::AllDay { transit } => {
                    output.push_str("type   : all day\n");
                    output.push_str("sunrise: \n");
                    output.push_str(&format!("transit: {}\n", format_time(transit)));
                    output.push_str("sunset : \n");
                }
                SunriseResult::AllNight { transit } => {
                    output.push_str("type   : all night\n");
                    output.push_str("sunrise: \n");
                    output.push_str(&format!("transit: {}\n", format_time(transit)));
                    output.push_str("sunset : \n");
                }
            }

            output
        }
        CalculationResult::SunriseWithTwilight {
            sunrise_sunset,
            civil,
            nautical,
            astronomical,
            ..
        } => {
            let mut output = String::new();

            use solar_positioning::SunriseResult;
            match sunrise_sunset {
                SunriseResult::RegularDay {
                    sunrise,
                    sunset,
                    transit,
                } => {
                    output.push_str("type   : normal\n");
                    output.push_str(&format!("sunrise: {}\n", format_time(sunrise)));
                    output.push_str(&format!("transit: {}\n", format_time(transit)));
                    output.push_str(&format!("sunset : {}\n", format_time(sunset)));
                }
                SunriseResult::AllDay { transit } => {
                    output.push_str("type   : all day\n");
                    output.push_str("sunrise: \n");
                    output.push_str(&format!("transit: {}\n", format_time(transit)));
                    output.push_str("sunset : \n");
                }
                SunriseResult::AllNight { transit } => {
                    output.push_str("type   : all night\n");
                    output.push_str("sunrise: \n");
                    output.push_str(&format!("transit: {}\n", format_time(transit)));
                    output.push_str("sunset : \n");
                }
            }

            // Add twilight times
            if let SunriseResult::RegularDay {
                sunrise: civil_start,
                sunset: civil_end,
                ..
            } = civil
            {
                output.push_str(&format!(
                    "civil twilight start: {}\n",
                    format_time(civil_start)
                ));
                output.push_str(&format!(
                    "civil twilight end  : {}\n",
                    format_time(civil_end)
                ));
            }

            if let SunriseResult::RegularDay {
                sunrise: naut_start,
                sunset: naut_end,
                ..
            } = nautical
            {
                output.push_str(&format!(
                    "nautical twilight start: {}\n",
                    format_time(naut_start)
                ));
                output.push_str(&format!(
                    "nautical twilight end  : {}\n",
                    format_time(naut_end)
                ));
            }

            if let SunriseResult::RegularDay {
                sunrise: astro_start,
                sunset: astro_end,
                ..
            } = astronomical
            {
                output.push_str(&format!(
                    "astronomical twilight start: {}\n",
                    format_time(astro_start)
                ));
                output.push_str(&format!(
                    "astronomical twilight end  : {}\n",
                    format_time(astro_end)
                ));
            }

            output
        }
        _ => String::new(),
    }
}

pub fn format_stream(
    results: Box<dyn Iterator<Item = CalculationResult>>,
    command: Command,
    params: &Parameters,
    source: crate::data::DataSource,
) -> Box<dyn Iterator<Item = String>> {
    let format = params.format.clone();

    if format == "text" && matches!(command, Command::Position) {
        // Position tables use streaming with header/footer
        return format_streaming_text_table(results, params, source);
    }

    // Stream everything else
    let show_inputs = params.show_inputs.unwrap_or(false);
    let headers = params.headers;
    let params_clone = params.clone();

    Box::new(results.enumerate().map(move |(index, result)| {
        let first = index == 0;

        match format.as_str() {
            "csv" => match command {
                Command::Position => {
                    format_csv_position(&result, show_inputs, headers, first, &params_clone)
                }
                Command::Sunrise => format_csv_sunrise(&result, show_inputs, headers, first),
            },
            "json" => {
                let json = match command {
                    Command::Position => format_json_position(&result, show_inputs, &params_clone),
                    Command::Sunrise => format_json_sunrise(&result, show_inputs),
                };
                format!("{}\n", json)
            }
            _ => match command {
                Command::Position => {
                    format_text_position(&result, show_inputs, params_clone.elevation_angle)
                }
                Command::Sunrise => format_text_sunrise(&result, show_inputs),
            },
        }
    }))
}

fn format_streaming_text_table(
    mut results: Box<dyn Iterator<Item = CalculationResult>>,
    params: &Parameters,
    source: crate::data::DataSource,
) -> Box<dyn Iterator<Item = String>> {
    use crate::data::{DataSource, LocationSource, TimeSource};

    // Peek at first result to get invariant values and determine table structure
    let first = match results.next() {
        Some(r) => r,
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

    let col_widths: Vec<usize> = headers_vec.iter().map(|h| h.len().max(14)).collect();

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
                let dt_str = datetime.format("%Y-%m-%d %H:%M").to_string();
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
    let remaining_rows = results.map(move |r| format_row(&r));

    Box::new(
        std::iter::once(header)
            .chain(std::iter::once(first_row))
            .chain(remaining_rows)
            .chain(std::iter::once(footer)),
    )
}

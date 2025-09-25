use crate::parquet_output::output_position_results_parquet;
use crate::table_format::{TableFormatter, VarianceFlags, write_header_section};
use crate::types::{OutputFormat, format_datetime_solarpos};
use chrono::{DateTime, FixedOffset};
use solar_positioning::types::SolarPosition;
use std::io::{self, BufWriter, Write};

pub struct PositionResult {
    pub datetime: DateTime<FixedOffset>,
    pub position: SolarPosition,
    pub latitude: f64,
    pub longitude: f64,
    pub elevation: f64,
    pub pressure: f64,
    pub temperature: f64,
    pub delta_t: f64,
    pub apply_refraction: bool,
}

pub fn output_position_results<I>(
    results: I,
    format: &OutputFormat,
    show_inputs: bool,
    show_headers: bool,
    elevation_angle: bool,
    is_stdin: bool,
) where
    I: Iterator<Item = PositionResult>,
{
    let stdout = io::stdout().lock();
    let mut writer = BufWriter::new(stdout);

    let result = match format {
        OutputFormat::Human => {
            output_human_format(results, &mut writer, show_inputs, elevation_angle, is_stdin)
        }
        OutputFormat::Csv => output_csv_format(
            results,
            &mut writer,
            show_inputs,
            show_headers,
            elevation_angle,
            is_stdin,
        ),
        OutputFormat::Json => {
            output_json_format(results, &mut writer, show_inputs, elevation_angle, is_stdin)
        }
        OutputFormat::Parquet => {
            let stdout = io::stdout();
            output_position_results_parquet(
                results,
                stdout,
                show_inputs,
                show_headers,
                elevation_angle,
                is_stdin,
            )
        }
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

fn output_human_format<I>(
    mut results: I,
    writer: &mut BufWriter<io::StdoutLock>,
    show_inputs: bool,
    elevation_angle: bool,
    flush_each: bool,
) -> io::Result<()>
where
    I: Iterator<Item = PositionResult>,
{
    let first = match results.next() {
        Some(r) => r,
        None => return Ok(()),
    };

    let second = results.next();

    let (variance, buffered) = match second {
        Some(s) => {
            let v = VarianceFlags::detect(&first, &s);
            (v, Some(s))
        }
        None => (VarianceFlags::default(), None),
    };

    if show_inputs {
        write_header_section(writer, &first, &variance)?;
    }

    let formatter = TableFormatter {
        variance: variance.clone(),
        elevation_angle,
        apply_refraction: first.apply_refraction,
    };

    let headers = formatter.column_headers();
    let widths = formatter.calculate_column_widths(&headers);

    formatter.write_table_header(writer, &headers, &widths)?;
    formatter.write_table_row(writer, &first, &widths)?;

    if flush_each {
        writer.flush()?;
    }

    if let Some(second_result) = buffered {
        formatter.write_table_row(writer, &second_result, &widths)?;
        if flush_each {
            writer.flush()?;
        }
    }

    for result in results {
        formatter.write_table_row(writer, &result, &widths)?;
        if flush_each {
            writer.flush()?;
        }
    }

    formatter.write_table_footer(writer, &widths)?;
    writer.flush()?;
    Ok(())
}

fn output_csv_format<I>(
    results: I,
    writer: &mut BufWriter<io::StdoutLock>,
    show_inputs: bool,
    show_headers: bool,
    elevation_angle: bool,
    flush_each: bool,
) -> io::Result<()>
where
    I: Iterator<Item = PositionResult>,
{
    let mut first_result = true;

    for result in results {
        let angle_value = if elevation_angle {
            result.position.elevation_angle()
        } else {
            result.position.zenith_angle()
        };
        let include_refraction_params = show_inputs && result.apply_refraction;

        // Write headers on first result
        if first_result && show_headers {
            let headers = match (show_inputs, include_refraction_params, elevation_angle) {
                (true, true, true) => {
                    "latitude,longitude,elevation,pressure,temperature,dateTime,deltaT,azimuth,elevation-angle"
                }
                (true, true, false) => {
                    "latitude,longitude,elevation,pressure,temperature,dateTime,deltaT,azimuth,zenith"
                }
                (true, false, true) => {
                    "latitude,longitude,elevation,dateTime,deltaT,azimuth,elevation-angle"
                }
                (true, false, false) => {
                    "latitude,longitude,elevation,dateTime,deltaT,azimuth,zenith"
                }
                (false, _, true) => "dateTime,azimuth,elevation-angle",
                (false, _, false) => "dateTime,azimuth,zenith",
            };
            writeln!(writer, "{}", headers)?;
        }
        first_result = false;

        if show_inputs {
            if include_refraction_params {
                write!(
                    writer,
                    "{:.5},{:.5},{:.3},{:.3},{:.3},",
                    result.latitude,
                    result.longitude,
                    result.elevation,
                    result.pressure,
                    result.temperature,
                )?;
            } else {
                write!(
                    writer,
                    "{:.5},{:.5},{:.3},",
                    result.latitude, result.longitude, result.elevation,
                )?;
            }
            write!(writer, "{}", result.datetime.format("%Y-%m-%dT%H:%M:%S%:z"))?;
            writeln!(
                writer,
                ",{:.3},{:.5},{:.5}",
                result.delta_t,
                result.position.azimuth(),
                angle_value
            )?;
        } else {
            write!(writer, "{}", result.datetime.format("%Y-%m-%dT%H:%M:%S%:z"))?;
            writeln!(
                writer,
                ",{:.5},{:.5}",
                result.position.azimuth(),
                angle_value
            )?;
        }
        if flush_each {
            writer.flush()?;
        }
    }
    writer.flush()?;
    Ok(())
}

fn output_json_format<I>(
    results: I,
    writer: &mut BufWriter<io::StdoutLock>,
    show_inputs: bool,
    elevation_angle: bool,
    flush_each: bool,
) -> io::Result<()>
where
    I: Iterator<Item = PositionResult>,
{
    for result in results {
        let angle_value = if elevation_angle {
            result.position.elevation_angle()
        } else {
            result.position.zenith_angle()
        };
        let angle_name = if elevation_angle {
            "elevation-angle"
        } else {
            "zenith"
        };
        let include_refraction_params = show_inputs && result.apply_refraction;

        if show_inputs {
            if include_refraction_params {
                writeln!(
                    writer,
                    r#"{{"latitude":{:.5},"longitude":{:.5},"elevation":{:.3},"pressure":{:.3},"temperature":{:.3},"dateTime":"{}","deltaT":{:.3},"azimuth":{:.5},"{}":{:.5}}}"#,
                    result.latitude,
                    result.longitude,
                    result.elevation,
                    result.pressure,
                    result.temperature,
                    format_datetime_solarpos(&result.datetime),
                    result.delta_t,
                    result.position.azimuth(),
                    angle_name,
                    angle_value
                )?;
            } else {
                writeln!(
                    writer,
                    r#"{{"latitude":{:.5},"longitude":{:.5},"elevation":{:.3},"dateTime":"{}","deltaT":{:.3},"azimuth":{:.5},"{}":{:.5}}}"#,
                    result.latitude,
                    result.longitude,
                    result.elevation,
                    format_datetime_solarpos(&result.datetime),
                    result.delta_t,
                    result.position.azimuth(),
                    angle_name,
                    angle_value
                )?;
            }
        } else {
            writeln!(
                writer,
                r#"{{"dateTime":"{}","azimuth":{:.5},"{}":{:.5}}}"#,
                format_datetime_solarpos(&result.datetime),
                result.position.azimuth(),
                angle_name,
                angle_value
            )?;
        }
        if flush_each {
            writer.flush()?;
        }
    }
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{FixedOffset, NaiveDate, TimeZone};
    use solar_positioning::types::SolarPosition;

    fn create_test_position_result() -> PositionResult {
        let tz = FixedOffset::east_opt(3600).unwrap(); // +01:00
        let datetime = tz
            .from_local_datetime(
                &NaiveDate::from_ymd_opt(2024, 6, 21)
                    .unwrap()
                    .and_hms_opt(12, 0, 0)
                    .unwrap(),
            )
            .unwrap();

        let position = SolarPosition::new(180.0, 30.0).unwrap(); // azimuth, zenith

        PositionResult {
            datetime,
            position,
            latitude: 52.0,
            longitude: 13.0,
            elevation: 100.0,
            pressure: 1013.25,
            temperature: 20.0,
            delta_t: 69.2,
            apply_refraction: true,
        }
    }

    #[test]
    fn test_format_datetime_solarpos() {
        let tz = FixedOffset::east_opt(3600).unwrap();
        let dt = tz
            .from_local_datetime(
                &NaiveDate::from_ymd_opt(2024, 6, 21)
                    .unwrap()
                    .and_hms_opt(12, 30, 45)
                    .unwrap(),
            )
            .unwrap();

        let formatted = format_datetime_solarpos(&dt);
        assert_eq!(formatted, "2024-06-21T12:30:45+01:00");
    }

    #[test]
    fn test_output_format_from_string() {
        assert!(matches!(
            OutputFormat::from_string("human"),
            Ok(OutputFormat::Human)
        ));
        assert!(matches!(
            OutputFormat::from_string("CSV"),
            Ok(OutputFormat::Csv)
        ));
        assert!(matches!(
            OutputFormat::from_string("json"),
            Ok(OutputFormat::Json)
        ));
        assert!(OutputFormat::from_string("invalid").is_err());
    }

    #[test]
    fn test_position_result_creation() {
        let result = create_test_position_result();
        assert_eq!(result.latitude, 52.0);
        assert_eq!(result.longitude, 13.0);
        assert_eq!(result.elevation, 100.0);
        assert_eq!(result.pressure, 1013.25);
        assert_eq!(result.temperature, 20.0);
        assert_eq!(result.delta_t, 69.2);
    }
}

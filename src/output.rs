use chrono::{DateTime, FixedOffset};
use solar_positioning::types::SolarPosition;

#[derive(Debug, Clone)]
pub enum OutputFormat {
    Human,
    Csv,
    Json,
}

impl OutputFormat {
    pub fn from_string(s: &str) -> Result<Self, String> {
        match s.to_uppercase().as_str() {
            "HUMAN" => Ok(Self::Human),
            "CSV" => Ok(Self::Csv),
            "JSON" => Ok(Self::Json),
            _ => Err(format!("Unknown format: {}. Use HUMAN, CSV, or JSON", s)),
        }
    }
}

pub struct PositionResult {
    pub datetime: DateTime<FixedOffset>,
    pub position: SolarPosition,
}

impl PositionResult {
    pub fn new(datetime: DateTime<FixedOffset>, position: SolarPosition) -> Self {
        Self { datetime, position }
    }
}

pub fn output_position_results<I>(
    results: I,
    format: &OutputFormat,
    show_inputs: bool,
    show_headers: bool,
    elevation_angle: bool,
) where
    I: Iterator<Item = PositionResult>,
{
    match format {
        OutputFormat::Human => output_human_format(results, show_inputs, elevation_angle),
        OutputFormat::Csv => output_csv_format(results, show_inputs, show_headers, elevation_angle),
        OutputFormat::Json => output_json_format(results, show_inputs, elevation_angle),
    }
}

fn output_human_format<I>(results: I, _show_inputs: bool, elevation_angle: bool)
where
    I: Iterator<Item = PositionResult>,
{
    for result in results {
        if elevation_angle {
            println!(
                "date/time      : {}",
                result.datetime.format("%Y-%m-%d %H:%M:%S%:z")
            );
            println!(
                "azimuth        :                    {:.5}°",
                result.position.azimuth()
            );
            println!(
                "elevation-angle:                     {:.5}°",
                result.position.elevation_angle()
            );
        } else {
            println!(
                "date/time: {}",
                result.datetime.format("%Y-%m-%d %H:%M:%S%:z")
            );
            println!(
                "azimuth  :                    {:.5}°",
                result.position.azimuth()
            );
            println!(
                "zenith   :                     {:.5}°",
                result.position.zenith_angle()
            );
        }
        println!();
    }
}

fn output_csv_format<I>(results: I, show_inputs: bool, show_headers: bool, elevation_angle: bool)
where
    I: Iterator<Item = PositionResult>,
{
    if show_headers {
        if show_inputs {
            if elevation_angle {
                println!("latitude,longitude,dateTime,azimuth,elevation");
            } else {
                println!("latitude,longitude,dateTime,azimuth,zenith");
            }
        } else if elevation_angle {
            println!("dateTime,azimuth,elevation");
        } else {
            println!("dateTime,azimuth,zenith");
        }
    }

    for result in results {
        let angle_value = if elevation_angle {
            result.position.elevation_angle()
        } else {
            result.position.zenith_angle()
        };

        if show_inputs {
            // TODO: Add latitude/longitude when we have ranges
            println!(
                "{},{:.5},{:.5}",
                result.datetime.to_rfc3339(),
                result.position.azimuth(),
                angle_value
            );
        } else {
            println!(
                "{},{:.5},{:.5}",
                result.datetime.to_rfc3339(),
                result.position.azimuth(),
                angle_value
            );
        }
    }
}

fn output_json_format<I>(results: I, show_inputs: bool, elevation_angle: bool)
where
    I: Iterator<Item = PositionResult>,
{
    for result in results {
        let (angle_name, angle_value) = if elevation_angle {
            ("elevation", result.position.elevation_angle())
        } else {
            ("zenith", result.position.zenith_angle())
        };

        if show_inputs {
            // TODO: Add latitude/longitude when we have ranges
            println!(
                r#"{{"dateTime":"{}","azimuth":{:.5},"{}":{:.5}}}"#,
                result.datetime.to_rfc3339(),
                result.position.azimuth(),
                angle_name,
                angle_value
            );
        } else {
            println!(
                r#"{{"dateTime":"{}","azimuth":{:.5},"{}":{:.5}}}"#,
                result.datetime.to_rfc3339(),
                result.position.azimuth(),
                angle_name,
                angle_value
            );
        }
    }
}

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
    pub latitude: f64,
    pub longitude: f64,
    pub elevation: f64,
    pub pressure: f64,
    pub temperature: f64,
    pub delta_t: f64,
}

impl PositionResult {
    pub fn new(
        datetime: DateTime<FixedOffset>,
        position: SolarPosition,
        latitude: f64,
        longitude: f64,
        environmental: EnvironmentalParams,
        delta_t: f64,
    ) -> Self {
        Self {
            datetime,
            position,
            latitude,
            longitude,
            elevation: environmental.elevation,
            pressure: environmental.pressure,
            temperature: environmental.temperature,
            delta_t,
        }
    }
}

pub struct EnvironmentalParams {
    pub elevation: f64,
    pub pressure: f64,
    pub temperature: f64,
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
                println!(
                    "latitude,longitude,elevation,pressure,temperature,dateTime,deltaT,azimuth,elevation-angle"
                );
            } else {
                println!(
                    "latitude,longitude,elevation,pressure,temperature,dateTime,deltaT,azimuth,zenith"
                );
            }
        } else if elevation_angle {
            println!("dateTime,azimuth,elevation-angle");
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
            println!(
                "{:.5},{:.5},{:.3},{:.3},{:.3},{},{:.3},{:.5},{:.5}",
                result.latitude,
                result.longitude,
                result.elevation,
                result.pressure,
                result.temperature,
                result.datetime.to_rfc3339(),
                result.delta_t,
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
            ("elevation-angle", result.position.elevation_angle())
        } else {
            ("zenith", result.position.zenith_angle())
        };

        if show_inputs {
            println!(
                r#"{{"latitude":{:.5},"longitude":{:.5},"elevation":{:.3},"pressure":{:.3},"temperature":{:.3},"dateTime":"{}","deltaT":{:.3},"azimuth":{:.5},"{}":{:.5}}}"#,
                result.latitude,
                result.longitude,
                result.elevation,
                result.pressure,
                result.temperature,
                result.datetime.to_rfc3339(),
                result.delta_t,
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

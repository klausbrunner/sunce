use chrono::{DateTime, FixedOffset};
use solar_positioning::types::SolarPosition;

/// Format datetime to match solarpos format (no subseconds)
pub fn format_datetime_solarpos(dt: &DateTime<FixedOffset>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S%:z").to_string()
}

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
                format_datetime_solarpos(&result.datetime),
                result.delta_t,
                result.position.azimuth(),
                angle_value
            );
        } else {
            println!(
                "{},{:.5},{:.5}",
                format_datetime_solarpos(&result.datetime),
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
                format_datetime_solarpos(&result.datetime),
                result.delta_t,
                result.position.azimuth(),
                angle_name,
                angle_value
            );
        } else {
            println!(
                r#"{{"dateTime":"{}","azimuth":{:.5},"{}":{:.5}}}"#,
                format_datetime_solarpos(&result.datetime),
                result.position.azimuth(),
                angle_name,
                angle_value
            );
        }
    }
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
        let env = EnvironmentalParams {
            elevation: 100.0,
            pressure: 1013.25,
            temperature: 20.0,
        };

        PositionResult::new(datetime, position, 52.0, 13.0, env, 69.2)
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

    #[test]
    fn test_environmental_params() {
        let env = EnvironmentalParams {
            elevation: 500.0,
            pressure: 950.0,
            temperature: 15.0,
        };
        assert_eq!(env.elevation, 500.0);
        assert_eq!(env.pressure, 950.0);
        assert_eq!(env.temperature, 15.0);
    }
}

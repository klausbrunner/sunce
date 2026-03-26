//! Configuration types for commands, calculation options, and output settings.

use chrono::Duration;
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Text,
    Csv,
    Json,
    #[cfg(feature = "parquet")]
    Parquet,
}

impl OutputFormat {
    pub(crate) const fn all() -> &'static [&'static str] {
        &[
            "text",
            "csv",
            "json",
            #[cfg(feature = "parquet")]
            "parquet",
        ]
    }
}

impl fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            OutputFormat::Text => "text",
            OutputFormat::Csv => "csv",
            OutputFormat::Json => "json",
            #[cfg(feature = "parquet")]
            OutputFormat::Parquet => "parquet",
        };
        f.write_str(s)
    }
}

impl FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "text" => Ok(OutputFormat::Text),
            "csv" => Ok(OutputFormat::Csv),
            "json" => Ok(OutputFormat::Json),
            #[cfg(feature = "parquet")]
            "parquet" => Ok(OutputFormat::Parquet),
            #[cfg(not(feature = "parquet"))]
            "parquet" => Err(
                "PARQUET format not available in this build (enable feature 'parquet')".to_string(),
            ),
            _ => Err(format!(
                "Invalid format: '{}'. Supported formats: {}",
                s,
                Self::all().join(", ")
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OutputOptions {
    pub format: OutputFormat,
    pub headers: bool,
    pub show_inputs: Option<bool>,
    pub elevation_angle: bool,
}

impl OutputOptions {
    /// Returns whether inputs should be emitted in the output stream.
    pub fn should_show_inputs(&self) -> bool {
        self.show_inputs.unwrap_or(false)
    }
}

impl Default for OutputOptions {
    fn default() -> Self {
        Self {
            format: OutputFormat::Text,
            headers: true,
            show_inputs: None,
            elevation_angle: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CalculationAlgorithm {
    Spa,
    Grena3,
}

impl fmt::Display for CalculationAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            CalculationAlgorithm::Spa => "spa",
            CalculationAlgorithm::Grena3 => "grena3",
        };
        f.write_str(s)
    }
}

impl FromStr for CalculationAlgorithm {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "spa" => Ok(CalculationAlgorithm::Spa),
            "grena3" => Ok(CalculationAlgorithm::Grena3),
            _ => Err(format!(
                "Invalid algorithm: '{}'. Supported algorithms: spa, grena3",
                s
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Environment {
    pub refraction: bool,
    pub elevation: f64,
    pub temperature: f64,
    pub pressure: f64,
}

impl Default for Environment {
    fn default() -> Self {
        Self {
            refraction: true,
            elevation: 0.0,
            temperature: 15.0,
            pressure: 1013.0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CalculationOptions {
    pub algorithm: CalculationAlgorithm,
    pub horizon: Option<f64>,
    pub twilight: bool,
}

impl Default for CalculationOptions {
    fn default() -> Self {
        Self {
            algorithm: CalculationAlgorithm::Spa,
            horizon: None,
            twilight: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Parameters {
    pub deltat: Option<f64>,
    pub output: OutputOptions,
    pub environment: Environment,
    pub calculation: CalculationOptions,
    pub wait: bool,
    pub perf: bool,
    pub step: Option<Step>,
    pub timezone: Option<TimezoneOverride>,
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            deltat: Some(0.0),
            output: OutputOptions::default(),
            environment: Environment::default(),
            calculation: CalculationOptions::default(),
            wait: false,
            perf: false,
            step: None,
            timezone: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Command {
    Position,
    Sunrise,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Predicate {
    IsDaylight,
    IsCivilTwilight,
    IsNauticalTwilight,
    IsAstronomicalTwilight,
    IsAstronomicalNight,
    AfterSunset,
    SunAbove(f64),
    SunBelow(f64),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Step(pub Duration);

impl From<Step> for Duration {
    fn from(value: Step) -> Self {
        value.0
    }
}

impl FromStr for Step {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        crate::data::time_utils::parse_duration_positive(s).map(Step)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimezoneOverride(String);

impl TimezoneOverride {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for TimezoneOverride {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.trim().is_empty() {
            return Err("Option --timezone requires a value".to_string());
        }
        crate::data::time_utils::parse_timezone_spec(s)
            .map(|_| TimezoneOverride(s.to_string()))
            .ok_or_else(|| format!("Invalid timezone: '{}'", s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_format_parsing_and_defaults_work() {
        assert_eq!("text".parse::<OutputFormat>().unwrap(), OutputFormat::Text);
        assert_eq!("CSV".parse::<OutputFormat>().unwrap(), OutputFormat::Csv);
        assert_eq!("json".parse::<OutputFormat>().unwrap(), OutputFormat::Json);
        #[cfg(feature = "parquet")]
        assert_eq!(
            "parquet".parse::<OutputFormat>().unwrap(),
            OutputFormat::Parquet
        );
        assert!("wat".parse::<OutputFormat>().is_err());

        let defaults = OutputOptions::default();
        assert_eq!(defaults.format, OutputFormat::Text);
        assert!(defaults.headers);
        assert_eq!(defaults.show_inputs, None);
        assert!(!defaults.should_show_inputs());
        assert!(!defaults.elevation_angle);
    }

    #[test]
    fn algorithm_step_and_timezone_parsing_work() {
        assert_eq!(
            "spa".parse::<CalculationAlgorithm>().unwrap(),
            CalculationAlgorithm::Spa
        );
        assert_eq!(
            "GRENA3".parse::<CalculationAlgorithm>().unwrap(),
            CalculationAlgorithm::Grena3
        );
        assert!("bad".parse::<CalculationAlgorithm>().is_err());

        let step = "1h".parse::<Step>().unwrap();
        assert_eq!(Duration::from(step), Duration::hours(1));
        assert!("0s".parse::<Step>().is_err());
        assert!("bad".parse::<Step>().is_err());

        let fixed = "+05:30".parse::<TimezoneOverride>().unwrap();
        assert_eq!(fixed.as_str(), "+05:30");
        let named = "Europe/Berlin".parse::<TimezoneOverride>().unwrap();
        assert_eq!(named.as_str(), "Europe/Berlin");
        assert!("".parse::<TimezoneOverride>().is_err());
        assert!("Not/AZone".parse::<TimezoneOverride>().is_err());
    }

    #[test]
    fn parameter_defaults_are_sensible() {
        let defaults = Parameters::default();
        assert_eq!(defaults.deltat, Some(0.0));
        assert!(!defaults.wait);
        assert!(!defaults.perf);
        assert!(defaults.step.is_none());
        assert!(defaults.timezone.is_none());
        assert_eq!(defaults.output.format, OutputFormat::Text);
        assert!(defaults.output.headers);
        assert_eq!(defaults.calculation.algorithm, CalculationAlgorithm::Spa);
        assert!(defaults.calculation.horizon.is_none());
        assert!(!defaults.calculation.twilight);
        assert!(defaults.environment.refraction);
        assert_eq!(defaults.environment.elevation, 0.0);
        assert_eq!(defaults.environment.temperature, 15.0);
        assert_eq!(defaults.environment.pressure, 1013.0);
    }
}

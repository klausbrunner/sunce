//! Configuration types for commands, calculation options, and output settings.

use chrono::Duration;
use std::fmt;
use std::str::FromStr;

#[cfg(feature = "cli")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Standard(crate::output::Format),
    #[cfg(feature = "parquet")]
    Parquet,
}

#[cfg(feature = "cli")]
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

#[cfg(feature = "cli")]
impl fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutputFormat::Standard(format) => format.fmt(f),
            #[cfg(feature = "parquet")]
            OutputFormat::Parquet => f.write_str("parquet"),
        }
    }
}

#[cfg(feature = "cli")]
impl FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "text" => Ok(OutputFormat::Standard(crate::output::Format::Text)),
            "csv" => Ok(OutputFormat::Standard(crate::output::Format::Csv)),
            "json" => Ok(OutputFormat::Standard(crate::output::Format::Json)),
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

#[cfg(feature = "cli")]
#[derive(Debug, Clone)]
pub struct OutputOptions {
    pub format: OutputFormat,
    pub headers: bool,
    pub show_inputs: Option<bool>,
    pub elevation_angle: bool,
}

#[cfg(feature = "cli")]
impl OutputOptions {
    /// Returns whether inputs should be emitted in the output stream.
    pub fn should_show_inputs(&self) -> bool {
        self.show_inputs.unwrap_or(false)
    }
}

#[cfg(feature = "cli")]
impl Default for OutputOptions {
    fn default() -> Self {
        Self {
            format: OutputFormat::Standard(crate::output::Format::Text),
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

/// Observer and atmospheric settings for position calculations.
#[derive(Debug, Clone, Copy)]
pub struct Environment {
    /// Apply atmospheric refraction; defaults to `true`.
    pub refraction: bool,
    /// Observer elevation in metres; defaults to 0. Grena3 requires 0.
    pub elevation: f64,
    /// Air temperature in degrees Celsius; defaults to 15. Used with refraction.
    pub temperature: f64,
    /// Atmospheric pressure in hPa; defaults to 1013.25. Used with refraction.
    pub pressure: f64,
}

impl Default for Environment {
    fn default() -> Self {
        Self {
            refraction: true,
            elevation: 0.0,
            temperature: 15.0,
            pressure: 1013.25,
        }
    }
}

/// Options for solar positions at explicit instants.
/// Defaults to SPA and delta-T of 0 seconds.
#[derive(Debug, Clone, Copy)]
pub struct PositionOptions {
    pub algorithm: CalculationAlgorithm,
    /// Delta-T in seconds; `None` estimates it for each input date.
    pub deltat: Option<f64>,
    pub environment: Environment,
}

impl Default for PositionOptions {
    fn default() -> Self {
        Self {
            algorithm: CalculationAlgorithm::Spa,
            deltat: Some(0.0),
            environment: Environment::default(),
        }
    }
}

impl PositionOptions {
    pub(crate) fn validate(&self) -> Result<(), crate::Error> {
        validate_deltat(self.deltat)?;
        if !self.environment.elevation.is_finite() {
            return Err(crate::Error::Input(
                "Observer elevation must be finite".into(),
            ));
        }
        if self.algorithm == CalculationAlgorithm::Grena3 && self.environment.elevation != 0.0 {
            return Err(crate::Error::Input(
                "Grena3 requires zero observer elevation".into(),
            ));
        }
        Ok(())
    }
}

/// Options for solar events on local calendar dates.
/// Defaults to SPA, delta-T of 0 seconds and the sunrise/sunset horizon.
#[derive(Debug, Clone, Copy)]
pub struct EventOptions {
    pub algorithm: CalculationAlgorithm,
    /// Delta-T in seconds; `None` estimates it for each input date.
    pub deltat: Option<f64>,
    /// A custom horizon in degrees; `None` uses the sunrise/sunset horizon.
    pub horizon: Option<f64>,
    /// Include civil, nautical and astronomical dawn/dusk; incompatible with `horizon`.
    pub twilight: bool,
}

impl Default for EventOptions {
    fn default() -> Self {
        Self {
            algorithm: CalculationAlgorithm::Spa,
            deltat: Some(0.0),
            horizon: None,
            twilight: false,
        }
    }
}

impl EventOptions {
    pub(crate) fn validate(&self) -> Result<(), crate::Error> {
        validate_deltat(self.deltat)?;
        if let Some(horizon) = self.horizon {
            if self.twilight {
                return Err(crate::Error::Input(
                    "Custom horizon cannot be combined with twilight".into(),
                ));
            }
            if !horizon.is_finite() || !(-90.0..=90.0).contains(&horizon) {
                return Err(crate::Error::Input(
                    "Horizon must be between -90 and 90 degrees".into(),
                ));
            }
        }
        Ok(())
    }
}

fn validate_deltat(deltat: Option<f64>) -> Result<(), crate::Error> {
    if deltat.is_some_and(|value| !value.is_finite()) {
        return Err(crate::Error::Input("Delta-T must be finite".into()));
    }
    Ok(())
}

#[cfg(feature = "cli")]
#[derive(Debug, Clone, Default)]
pub struct Parameters {
    pub output: OutputOptions,
    pub environment: Environment,
    pub calculation: EventOptions,
    pub wait: bool,
    pub verbose: bool,
    pub perf: bool,
    pub step: Option<Step>,
    pub at: Option<chrono::NaiveTime>,
    pub timezone: Option<TimezoneOverride>,
}

#[cfg(feature = "cli")]
impl Parameters {
    pub(crate) fn position_options(&self) -> PositionOptions {
        PositionOptions {
            algorithm: self.calculation.algorithm,
            deltat: self.calculation.deltat,
            environment: self.environment,
        }
    }
}

#[cfg(feature = "cli")]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Command {
    Position,
    Events,
}

#[cfg(feature = "cli")]
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

/// Elapsed sampling interval. Parses positive seconds or an integer with `s`, `m`, `h` or `d`.
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

#[cfg(feature = "cli")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimezoneOverride(String);

#[cfg(feature = "cli")]
impl TimezoneOverride {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(feature = "cli")]
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

    #[cfg(feature = "cli")]
    #[test]
    fn output_format_parsing_and_defaults_work() {
        assert_eq!(
            "text".parse::<OutputFormat>().unwrap(),
            OutputFormat::Standard(crate::output::Format::Text)
        );
        assert_eq!(
            "CSV".parse::<OutputFormat>().unwrap(),
            OutputFormat::Standard(crate::output::Format::Csv)
        );
        assert_eq!(
            "json".parse::<OutputFormat>().unwrap(),
            OutputFormat::Standard(crate::output::Format::Json)
        );
        #[cfg(feature = "parquet")]
        assert_eq!(
            "parquet".parse::<OutputFormat>().unwrap(),
            OutputFormat::Parquet
        );
        assert!("wat".parse::<OutputFormat>().is_err());

        let defaults = OutputOptions::default();
        assert_eq!(
            defaults.format,
            OutputFormat::Standard(crate::output::Format::Text)
        );
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

        #[cfg(feature = "cli")]
        {
            let fixed = "+05:30".parse::<TimezoneOverride>().unwrap();
            assert_eq!(fixed.as_str(), "+05:30");
            let named = "Europe/Berlin".parse::<TimezoneOverride>().unwrap();
            assert_eq!(named.as_str(), "Europe/Berlin");
            assert!("".parse::<TimezoneOverride>().is_err());
            assert!("+00:99".parse::<TimezoneOverride>().is_err());
            assert!("Not/AZone".parse::<TimezoneOverride>().is_err());
        }
    }

    #[cfg(feature = "cli")]
    #[test]
    fn parameter_defaults_are_sensible() {
        let defaults = Parameters::default();
        assert_eq!(defaults.calculation.deltat, Some(0.0));
        assert!(!defaults.wait);
        assert!(!defaults.verbose);
        assert!(!defaults.perf);
        assert!(defaults.step.is_none());
        assert!(defaults.timezone.is_none());
        assert_eq!(
            defaults.output.format,
            OutputFormat::Standard(crate::output::Format::Text)
        );
        assert!(defaults.output.headers);
        assert_eq!(defaults.calculation.algorithm, CalculationAlgorithm::Spa);
        assert!(defaults.calculation.horizon.is_none());
        assert!(!defaults.calculation.twilight);
        assert!(defaults.environment.refraction);
        assert_eq!(defaults.environment.elevation, 0.0);
        assert_eq!(defaults.environment.temperature, 15.0);
        assert_eq!(defaults.environment.pressure, 1013.25);
    }
}

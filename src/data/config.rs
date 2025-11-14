#[derive(Debug, Clone)]
pub struct OutputOptions {
    pub format: String,
    pub headers: bool,
    pub show_inputs: Option<bool>,
    pub elevation_angle: bool,
}

impl Default for OutputOptions {
    fn default() -> Self {
        Self {
            format: "text".to_string(),
            headers: true,
            show_inputs: None,
            elevation_angle: false,
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
    pub algorithm: String,
    pub horizon: Option<f64>,
    pub twilight: bool,
}

impl Default for CalculationOptions {
    fn default() -> Self {
        Self {
            algorithm: "spa".to_string(),
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
    pub perf: bool,
    pub step: Option<String>,
    pub timezone: Option<String>,
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            deltat: Some(0.0),
            output: OutputOptions::default(),
            environment: Environment::default(),
            calculation: CalculationOptions::default(),
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

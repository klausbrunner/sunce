use crate::output::PositionResult;
use crate::sunrise_formatters::SunriseResultData;
use chrono::Utc;
use clap::ArgMatches;
use solar_positioning::{RefractionCorrection, grena3, spa};
use std::collections::HashMap;

#[derive(Clone)]
pub enum DeltaTMode {
    Fixed(f64),
    Estimate,
    Default,
}

impl DeltaTMode {
    pub fn resolve(&self, datetime: chrono::DateTime<chrono::FixedOffset>) -> f64 {
        match self {
            DeltaTMode::Fixed(value) => *value,
            DeltaTMode::Estimate => {
                solar_positioning::time::DeltaT::estimate_from_date_like(datetime)
                    .expect("Failed to estimate delta-T for the given date")
            }
            DeltaTMode::Default => 0.0,
        }
    }
}

#[derive(Clone)]
pub struct CalculationParameters {
    pub algorithm: String,
    pub elevation: f64,
    pub delta_t: DeltaTMode,
    pub pressure: f64,
    pub temperature: f64,
    pub apply_refraction: bool,
}

#[derive(Clone)]
pub struct SunriseCalculationParameters {
    pub elevation: f64,
    pub delta_t: DeltaTMode,
    pub show_twilight: bool,
}

pub fn get_calculation_parameters(
    input: &crate::ParsedInput,
    matches: &ArgMatches,
) -> Result<CalculationParameters, String> {
    let (_, cmd_matches) = matches.subcommand().unwrap_or(("position", matches));
    let pos_options = crate::parse_position_options(cmd_matches);

    let algorithm = pos_options
        .algorithm
        .as_deref()
        .unwrap_or("SPA")
        .to_string();

    // Validate algorithm
    match algorithm.to_uppercase().as_str() {
        "SPA" | "GRENA3" => {}
        _ => {
            return Err(format!(
                "Unknown algorithm: {}. Use SPA or GRENA3",
                algorithm
            ));
        }
    }

    let delta_t = match input.global_options.deltat.as_deref() {
        Some("") => DeltaTMode::Estimate, // --deltat flag without value
        Some(s) => DeltaTMode::Fixed(
            s.parse()
                .map_err(|_| format!("Invalid delta-T value: {}", s))?,
        ), // --deltat=value
        None => DeltaTMode::Default,      // no --deltat flag
    };

    Ok(CalculationParameters {
        algorithm,
        elevation: match pos_options.elevation.as_deref() {
            Some(s) => s
                .parse()
                .map_err(|_| format!("Invalid elevation value: {}", s))?,
            None => 0.0,
        },
        delta_t,
        pressure: match pos_options.pressure.as_deref() {
            Some(s) => s
                .parse()
                .map_err(|_| format!("Invalid pressure value: {}", s))?,
            None => 1013.0,
        },
        temperature: match pos_options.temperature.as_deref() {
            Some(s) => s
                .parse()
                .map_err(|_| format!("Invalid temperature value: {}", s))?,
            None => 15.0,
        },
        apply_refraction: pos_options.refraction.unwrap_or(true),
    })
}

pub fn get_sunrise_calculation_parameters(
    input: &crate::ParsedInput,
    matches: &ArgMatches,
    show_twilight: bool,
) -> Result<SunriseCalculationParameters, String> {
    let (_, cmd_matches) = matches.subcommand().unwrap_or(("sunrise", matches));
    let _sunrise_options = crate::parse_sunrise_options(cmd_matches);

    let delta_t = match input.global_options.deltat.as_deref() {
        Some("") => DeltaTMode::Estimate, // --deltat flag without value
        Some(s) => DeltaTMode::Fixed(
            s.parse()
                .map_err(|_| format!("Invalid delta-T value: {}", s))?,
        ), // --deltat=value
        None => DeltaTMode::Default,      // no --deltat flag
    };

    Ok(SunriseCalculationParameters {
        elevation: 0.0,
        delta_t,
        show_twilight,
    })
}

/// Calculate a single position result (the core calculation)
pub fn calculate_single_position(
    datetime: chrono::DateTime<chrono::FixedOffset>,
    lat: f64,
    lon: f64,
    params: &CalculationParameters,
) -> PositionResult {
    // Convert to UTC for solar calculations
    let utc_datetime = datetime.with_timezone(&Utc);

    let delta_t = params.delta_t.resolve(datetime);

    let solar_position = match params.algorithm.to_uppercase().as_str() {
        "GRENA3" => {
            let refraction = create_refraction_correction(
                params.pressure,
                params.temperature,
                params.apply_refraction,
            );
            grena3::solar_position(utc_datetime, lat, lon, delta_t, refraction)
        }
        "SPA" => {
            let refraction = create_refraction_correction(
                params.pressure,
                params.temperature,
                params.apply_refraction,
            );
            spa::solar_position(
                utc_datetime,
                lat,
                lon,
                params.elevation,
                delta_t,
                refraction,
            )
        }
        _ => unreachable!(
            "Algorithm validation should prevent this: {}",
            params.algorithm
        ),
    }
    .expect("Solar calculation should not fail with validated inputs");

    PositionResult {
        datetime,
        position: solar_position,
        latitude: lat,
        longitude: lon,
        elevation: params.elevation,
        pressure: params.pressure,
        temperature: params.temperature,
        delta_t,
        apply_refraction: params.apply_refraction,
    }
}

/// Calculate a single sunrise result (the core calculation)
pub fn calculate_single_sunrise(
    datetime: chrono::DateTime<chrono::FixedOffset>,
    lat: f64,
    lon: f64,
    params: &SunriseCalculationParameters,
) -> SunriseResultData {
    use crate::sunrise_formatters::TwilightResults;
    use solar_positioning::types::Horizon;

    let delta_t = params.delta_t.resolve(datetime);
    let _elevation = params.elevation;

    // Use the input datetime directly for sunrise calculation
    // This matches solarpos behavior

    // Calculate sunrise for standard horizon
    let sunrise_result = solar_positioning::spa::sunrise_sunset_for_horizon(
        datetime,
        lat,
        lon,
        delta_t,
        Horizon::SunriseSunset,
    )
    .unwrap();

    // Calculate twilight if requested
    let twilight_results = if params.show_twilight {
        let civil = solar_positioning::spa::sunrise_sunset_for_horizon(
            datetime,
            lat,
            lon,
            delta_t,
            Horizon::CivilTwilight,
        )
        .unwrap();
        let nautical = solar_positioning::spa::sunrise_sunset_for_horizon(
            datetime,
            lat,
            lon,
            delta_t,
            Horizon::NauticalTwilight,
        )
        .unwrap();
        let astronomical = solar_positioning::spa::sunrise_sunset_for_horizon(
            datetime,
            lat,
            lon,
            delta_t,
            Horizon::AstronomicalTwilight,
        )
        .unwrap();

        Some(TwilightResults {
            civil,
            nautical,
            astronomical,
        })
    } else {
        None
    };

    SunriseResultData {
        datetime,
        latitude: lat,
        longitude: lon,
        delta_t,
        sunrise_result,
        twilight_results,
    }
}

/// Optimized calculation for coordinate sweeps with time-based caching
pub struct CoordinateSweepCalculator {
    params: CalculationParameters,
    time_cache: HashMap<chrono::DateTime<chrono::Utc>, spa::SpaTimeDependent>,
}

impl CoordinateSweepCalculator {
    pub fn new(params: CalculationParameters) -> Self {
        Self {
            params,
            time_cache: HashMap::new(),
        }
    }

    pub fn calculate_position(
        &mut self,
        datetime: chrono::DateTime<chrono::FixedOffset>,
        lat: f64,
        lon: f64,
    ) -> PositionResult {
        // Convert to UTC for cache key
        let utc_datetime = datetime.with_timezone(&Utc);

        // Only use caching for SPA algorithm (GRENA3 doesn't benefit as much)
        if self.params.algorithm.to_uppercase() == "SPA" {
            // Get or compute time-dependent parts (cache grows as needed for large sweeps)
            let time_parts = self.time_cache.entry(utc_datetime).or_insert_with(|| {
                // Calculate time-dependent parts with proper delta-T
                let delta_t = self.params.delta_t.resolve(utc_datetime.into());
                spa::spa_time_dependent_parts(utc_datetime, delta_t)
                    .expect("SPA time-dependent calculation should not fail with valid inputs")
            });

            // Calculate position using cached time parts
            let refraction = create_refraction_correction(
                self.params.pressure,
                self.params.temperature,
                self.params.apply_refraction,
            );

            let solar_position = spa::spa_with_time_dependent_parts(
                lat,
                lon,
                self.params.elevation,
                refraction,
                time_parts,
            )
            .expect("Solar calculation should not fail with validated inputs");

            PositionResult {
                datetime,
                position: solar_position,
                latitude: lat,
                longitude: lon,
                elevation: self.params.elevation,
                pressure: self.params.pressure,
                temperature: self.params.temperature,
                delta_t: self.params.delta_t.resolve(datetime),
                apply_refraction: self.params.apply_refraction,
            }
        } else {
            // Fall back to regular calculation for GRENA3
            calculate_single_position(datetime, lat, lon, &self.params)
        }
    }
}

const INVALID_ATMOSPHERIC_PARAMS: &str = "Invalid atmospheric parameters: pressure must be 1-2000 hPa, temperature must be -273.15 to 100°C";

pub fn create_refraction_correction(
    pressure: f64,
    temperature: f64,
    apply: bool,
) -> Option<RefractionCorrection> {
    if apply {
        Some(RefractionCorrection::new(pressure, temperature).expect(INVALID_ATMOSPHERIC_PARAMS))
    } else {
        None
    }
}

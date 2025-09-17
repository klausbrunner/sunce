use crate::output::PositionResult;
use crate::sunrise_output::SunriseResultData;
use chrono::{DateTime, FixedOffset, Utc};
use clap::ArgMatches;
use solar_positioning::{grena3, spa};

/// Generic calculation trait for streaming architecture
pub trait CalculationEngine<T>: Sync {
    fn calculate_single(&self, datetime: DateTime<FixedOffset>, lat: f64, lon: f64) -> T;
}

pub struct PositionCalculationEngine {
    pub params: CalculationParameters,
}

impl CalculationEngine<PositionResult> for PositionCalculationEngine {
    fn calculate_single(
        &self,
        datetime: DateTime<FixedOffset>,
        lat: f64,
        lon: f64,
    ) -> PositionResult {
        calculate_single_position(datetime, lat, lon, &self.params)
    }
}

pub struct SunriseCalculationEngine {
    pub params: SunriseCalculationParameters,
}

impl CalculationEngine<SunriseResultData> for SunriseCalculationEngine {
    fn calculate_single(
        &self,
        datetime: DateTime<FixedOffset>,
        lat: f64,
        lon: f64,
    ) -> SunriseResultData {
        calculate_single_sunrise(datetime, lat, lon, &self.params)
    }
}

#[derive(Clone)]
pub struct CalculationParameters {
    pub algorithm: String,
    pub elevation: f64,
    pub delta_t: f64,
    pub pressure: f64,
    pub temperature: f64,
    pub apply_refraction: bool,
}

#[derive(Clone)]
pub struct SunriseCalculationParameters {
    #[allow(dead_code)]
    pub algorithm: String,
    pub elevation: f64,
    pub delta_t: f64,
    #[allow(dead_code)]
    pub pressure: f64,
    #[allow(dead_code)]
    pub temperature: f64,
    #[allow(dead_code)]
    pub apply_refraction: bool,
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

    Ok(CalculationParameters {
        algorithm,
        elevation: pos_options
            .elevation
            .as_deref()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0),
        delta_t: input
            .global_options
            .deltat
            .as_deref()
            .and_then(|s| {
                if s == "ESTIMATE" {
                    Some(69.0)
                } else {
                    s.parse::<f64>().ok()
                }
            })
            .unwrap_or(0.0),
        pressure: pos_options
            .pressure
            .as_deref()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(1013.0),
        temperature: pos_options
            .temperature
            .as_deref()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(15.0),
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

    Ok(SunriseCalculationParameters {
        algorithm: "SPA".to_string(), // Sunrise always uses SPA
        elevation: 0.0,               // Sunrise uses sea level by default
        delta_t: input
            .global_options
            .deltat
            .as_deref()
            .and_then(|s| {
                if s == "ESTIMATE" {
                    Some(69.0)
                } else {
                    s.parse::<f64>().ok()
                }
            })
            .unwrap_or(0.0),
        pressure: 1013.0,       // Default atmospheric pressure
        temperature: 15.0,      // Default temperature
        apply_refraction: true, // Sunrise typically includes refraction
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

    let solar_position = match params.algorithm.to_uppercase().as_str() {
        "GRENA3" => {
            if params.apply_refraction {
                grena3::solar_position_with_refraction(
                    utc_datetime,
                    lat,
                    lon,
                    params.delta_t,
                    Some(params.pressure),
                    Some(params.temperature),
                )
            } else {
                grena3::solar_position(utc_datetime, lat, lon, params.delta_t)
            }
        }
        "SPA" => {
            if params.apply_refraction {
                spa::solar_position(
                    utc_datetime,
                    lat,
                    lon,
                    params.elevation,
                    params.delta_t,
                    params.pressure,
                    params.temperature,
                )
            } else {
                spa::solar_position_no_refraction(
                    utc_datetime,
                    lat,
                    lon,
                    params.elevation,
                    params.delta_t,
                )
            }
        }
        _ => panic!("Unknown algorithm: {}", params.algorithm),
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
        delta_t: params.delta_t,
    }
}

/// Calculate a single sunrise result (the core calculation)
pub fn calculate_single_sunrise(
    datetime: chrono::DateTime<chrono::FixedOffset>,
    lat: f64,
    lon: f64,
    params: &SunriseCalculationParameters,
) -> SunriseResultData {
    use crate::sunrise_output::TwilightResults;
    use solar_positioning::types::Horizon;

    let delta_t = params.delta_t;
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

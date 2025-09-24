use solar_positioning::RefractionCorrection;

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

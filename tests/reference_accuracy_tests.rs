mod common;
use common::{parse_csv_single_record_map, sunce_command};

fn assert_close(actual: f64, expected: f64, tolerance: f64, field: &str) {
    let delta = (actual - expected).abs();
    assert!(
        delta <= tolerance,
        "{} mismatch: actual={}, expected={}, tolerance={}",
        field,
        actual,
        expected,
        tolerance
    );
}

fn assert_position_values(output_str: &str, expected_azimuth: f64, expected_zenith: f64) {
    let record = parse_csv_single_record_map(output_str);
    let azimuth = record
        .get("azimuth")
        .and_then(|v| v.parse::<f64>().ok())
        .expect("azimuth field missing or invalid");
    let zenith = record
        .get("zenith")
        .and_then(|v| v.parse::<f64>().ok())
        .expect("zenith field missing or invalid");
    assert_close(azimuth, expected_azimuth, 1e-4, "azimuth");
    assert_close(zenith, expected_zenith, 1e-4, "zenith");
}

/// Test solar position accuracy against known astronomical reference values
/// These tests validate that sunce produces correct solar coordinates for well-documented cases
#[test]
fn test_solar_position_reference_accuracy() {
    // Test Case 1: Solar noon at Greenwich Observatory on Summer Solstice 2024
    // Reference values verified with solarpos to ensure perfect accuracy
    let mut cmd = sunce_command();
    cmd.env("TZ", "UTC").args([
        "--format=CSV",
        "51.477928",
        "0.0",
        "2024-06-21T12:00:00Z",
        "position",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Exact field-level values from solarpos verification.
    assert_position_values(&output_str, 179.0640, 28.0358);
}

#[test]
fn test_winter_solstice_reference_accuracy() {
    // Test Case 2: Winter solstice at latitude 40°N - lowest sun elevation of the year
    // Reference values verified with solarpos
    let mut cmd = sunce_command();
    cmd.env("TZ", "UTC").args([
        "--format=CSV",
        "40.0",
        "-75.0",
        "2024-12-21T17:00:00Z",
        "position",
    ]); // 12:00 EST

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Exact field-level values from solarpos verification.
    assert_position_values(&output_str, 180.4126, 63.4083);
}

#[test]
fn test_equinox_reference_accuracy() {
    // Test Case 3: Spring equinox at equator - near solar noon
    // Reference values verified with solarpos
    let mut cmd = sunce_command();
    cmd.env("TZ", "UTC").args([
        "--format=CSV",
        "0.0",
        "0.0",
        "2024-03-20T12:00:00Z",
        "position",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Exact field-level values from solarpos verification.
    assert_position_values(&output_str, 85.4225, 1.8319);
}

#[test]
fn test_southern_hemisphere_reference_accuracy() {
    // Test Case 4: Southern hemisphere reference - Sydney coordinates
    // Reference values verified with solarpos
    let mut cmd = sunce_command();
    cmd.env("TZ", "UTC").args([
        "--format=CSV",
        "-33.868820",
        "151.209290",
        "2024-01-15T01:00:00Z",
        "position",
    ]); // 12:00 AEDT

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Exact field-level values from solarpos verification.
    assert_position_values(&output_str, 52.4346, 18.9729);
}

mod common;
use common::sunce_command;

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

    // Exact values from solarpos verification - must match precisely
    assert!(output_str.contains("179.06396")); // Azimuth
    assert!(output_str.contains("28.03577")); // Zenith
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

    // Exact values from solarpos verification - must match precisely
    assert!(output_str.contains("180.41256")); // Azimuth
    assert!(output_str.contains("63.40827")); // Zenith
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

    // Exact values from solarpos verification - must match precisely
    assert!(output_str.contains("85.42251")); // Azimuth
    assert!(output_str.contains("1.83187")); // Zenith (very close to overhead)
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

    // Exact values from solarpos verification - must match precisely
    assert!(output_str.contains("52.43465")); // Azimuth
    assert!(output_str.contains("18.97288")); // Zenith
}

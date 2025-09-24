use assert_cmd::Command;
use predicates::prelude::*;

/// Test basic sunrise calculation
#[test]
fn test_basic_sunrise() {
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["52.0", "13.4", "2024-06-21", "sunrise"]);

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("sunrise"))
        .stdout(predicate::str::contains("sunset"));
}

/// Test sunrise output formats
#[test]
fn test_sunrise_output_formats() {
    // Test HUMAN format (default)
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["52.0", "13.4", "2024-06-21", "sunrise"]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("sunrise"))
        .stdout(predicate::str::contains("sunset"));

    // Test CSV format
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["--format=CSV", "52.0", "13.4", "2024-06-21", "sunrise"]);
    cmd.assert().success().stdout(predicate::str::contains(
        "latitude,longitude,dateTime,deltaT,type,sunrise,transit,sunset",
    ));

    // Test JSON format
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["--format=JSON", "52.0", "13.4", "2024-06-21", "sunrise"]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("\"dateTime\""))
        .stdout(predicate::str::contains("\"sunrise\""));
}

/// Test sunrise edge cases (Arctic midnight sun and polar night)
#[test]
fn test_sunrise_edge_cases() {
    // Test Arctic summer (midnight sun)
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["80.0", "0.0", "2024-06-21", "sunrise"]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("all day"));

    // Test Arctic winter (polar night)
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["80.0", "0.0", "2024-12-21", "sunrise"]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("all night"));
}

/// Test sunrise time series
#[test]
fn test_sunrise_time_series() {
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.env("TZ", "Europe/Berlin")
        .args(["--format=CSV", "52.0", "13.4", "2024-06", "sunrise"]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should have daily series for the entire month
    assert!(output_str.contains("2024-06-01T00:00:00+02:00"));
    assert!(output_str.contains("2024-06-15T00:00:00+02:00"));
    assert!(output_str.contains("2024-06-30T00:00:00+02:00"));
}

/// Test sunrise coordinate ranges
#[test]
fn test_sunrise_coordinate_ranges() {
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.env("TZ", "Europe/Berlin").args([
        "--format=CSV",
        "52:53:1",
        "13:14:1",
        "2024-06-21",
        "sunrise",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should have 4 coordinate combinations (2x2 grid)
    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 5); // Header + 4 data rows
}

/// Test sunrise with timezone
#[test]
fn test_sunrise_timezone() {
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["--timezone=+02:00", "52.0", "13.4", "2024-06-21", "sunrise"]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("+02:00"));
}

/// Test sunrise validation
#[test]
fn test_sunrise_validation() {
    // Test invalid horizon
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["52.0", "13.4", "2024-06-21", "sunrise", "--horizon=invalid"]);
    cmd.assert().failure();
}

/// Test sunrise show-inputs
#[test]
fn test_sunrise_show_inputs() {
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.env("TZ", "Europe/Berlin").args([
        "--format=CSV",
        "--show-inputs",
        "52.0",
        "13.4",
        "2024-06-21",
        "sunrise",
    ]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("latitude,longitude"));
}

/// Test sunrise DST handling
#[test]
fn test_sunrise_dst() {
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.env("TZ", "Europe/Berlin")
        .args(["52.0", "13.4", "2024-03-31", "sunrise"]);
    cmd.assert().success();
}

/// Test sunrise accuracy against known reference values to prevent horizon value regression
#[test]
fn test_sunrise_accuracy_regression() {
    // Test the specific case that revealed the Horizon::Custom(-0.833) vs Horizon::SunriseSunset bug
    // These values were verified against solarpos to be exact matches
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.env("TZ", "Europe/Berlin")
        .args(["--format=CSV", "53.0", "13.0", "2024-06-21", "sunrise"]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // These exact values must match solarpos output - any deviation indicates regression
    assert!(output_str.contains("2024-06-21T04:41:52+02:00")); // sunrise
    assert!(output_str.contains("2024-06-21T13:09:54+02:00")); // transit
    assert!(output_str.contains("2024-06-21T21:37:55+02:00")); // sunset

    // Test second problematic case that also revealed the bug
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.env("TZ", "Europe/Berlin")
        .args(["--format=CSV", "52.0", "13.4", "2024-06-02", "sunrise"]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // These exact values must match solarpos output
    assert!(output_str.contains("2024-06-02T04:51:15+02:00")); // sunrise
    assert!(output_str.contains("2024-06-02T13:04:28+02:00")); // transit
    assert!(output_str.contains("2024-06-02T21:18:17+02:00")); // sunset
}

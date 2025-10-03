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
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("type,sunrise,transit,sunset"));

    // Test JSON format
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["--format=JSON", "52.0", "13.4", "2024-06-21", "sunrise"]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("\"type\""))
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

/// Test sunrise with timezone - verifies timezone is correctly applied to specific dates
#[test]
fn test_sunrise_timezone() {
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["--timezone=+02:00", "52.0", "13.4", "2024-06-21", "sunrise"]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("+02:00"))
        .stdout(predicate::str::contains("sunrise"))
        .stdout(predicate::str::contains("sunset"));
}

/// Test timezone fix for specific dates - regression test for timezone bug
#[test]
fn test_sunrise_timezone_specific_date_fix() {
    // Test case 1: +02:00 timezone
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "--timezone=+02:00",
        "52.0",
        "13.4",
        "2024-06-21",
        "sunrise",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Verify timezone appears in all timestamps
    assert!(
        output_str.contains("+02:00"),
        "Timezone +02:00 should appear in output: {}",
        output_str
    );
    let plus_two_count = output_str.matches("+02:00").count();
    assert!(
        plus_two_count >= 3,
        "Should have at least 3 timestamps with +02:00: {}",
        output_str
    );

    // Test case 2: -05:00 timezone
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "--timezone=-05:00",
        "40.7",
        "-74.0",
        "2024-12-21",
        "sunrise",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Verify negative timezone
    assert!(
        output_str.contains("-05:00"),
        "Timezone -05:00 should appear in output: {}",
        output_str
    );
    let minus_five_count = output_str.matches("-05:00").count();
    assert!(
        minus_five_count >= 3,
        "Should have at least 3 timestamps with -05:00: {}",
        output_str
    );

    // Verify single row for specific date (not time series)
    let lines: Vec<&str> = output_str.trim().split('\n').collect();
    assert_eq!(
        lines.len(),
        2,
        "Should have header + 1 data row for specific date"
    );
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
    // Note: Values updated after timezone bug fix - timezone is now properly applied
    assert!(output_str.contains("04:41:") && output_str.contains("+02:00")); // sunrise with timezone
    assert!(output_str.contains("13:09:") && output_str.contains("+02:00")); // transit with timezone
    assert!(output_str.contains("21:37:") && output_str.contains("+02:00")); // sunset with timezone

    // Test second problematic case that also revealed the bug
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.env("TZ", "Europe/Berlin")
        .args(["--format=CSV", "52.0", "13.4", "2024-06-02", "sunrise"]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // These exact values must match solarpos output - timezone bug fixed
    assert!(output_str.contains("04:51:") && output_str.contains("+02:00")); // sunrise with timezone
    assert!(output_str.contains("13:04:") && output_str.contains("+02:00")); // transit with timezone
    assert!(output_str.contains("21:18:") && output_str.contains("+02:00")); // sunset with timezone
}

/// Test sunrise date parsing bug fix: specific dates should produce single results
#[test]
fn test_sunrise_specific_date_single_result() {
    // Bug: "2024-01-01" for sunrise was generating 24 rows instead of 1
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["--format=CSV", "52.0", "13.4", "2024-01-01", "sunrise"]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = output_str.trim().split('\n').collect();

    // Should be exactly 2 lines: header + 1 data row
    assert_eq!(
        lines.len(),
        2,
        "Expected header + 1 data row, got {} lines:\n{}",
        lines.len(),
        output_str
    );

    // Verify header
    assert!(lines[0].contains("type"));
    assert!(lines[0].contains("sunrise"));
    assert!(lines[0].contains("sunset"));

    // Verify single data row contains NORMAL type
    assert!(lines[1].contains("NORMAL"));
}

/// Test that partial dates generate daily time series for sunrise
/// Regression test: Sunrise times don't change hourly, must be daily
#[test]
fn test_sunrise_partial_date_time_series() {
    // "2024-01" should generate a DAILY time series (31 days in January)
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["--format=CSV", "52.0", "13.4", "2024-01", "sunrise"]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = output_str.trim().split('\n').collect();

    // Should be exactly 32 lines: header + 31 daily data rows (not hourly!)
    assert_eq!(
        lines.len(),
        32,
        "Expected header + 31 daily rows for January, got {}",
        lines.len()
    );

    // Verify header includes input columns (show-inputs auto-enabled)
    assert!(lines[0].contains("latitude"));
    assert!(lines[0].contains("longitude"));
    assert!(lines[0].contains("dateTime"));

    // Verify first and last days
    assert!(lines[1].contains("52.00000"));
    assert!(lines[1].contains("13.40000"));
    assert!(lines[1].contains("2024-01-01"));
    assert!(lines[31].contains("2024-01-31"));
}

/// Test position command still generates time series for specific dates (expected behavior)
#[test]
fn test_position_specific_date_time_series() {
    // Position command should always generate time series for specific dates like "2024-01-01"
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["--format=CSV", "52.0", "13.4", "2024-01-01", "position"]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = output_str.trim().split('\n').collect();

    // Should be 25 lines: header + 24 hourly data rows
    assert_eq!(
        lines.len(),
        25,
        "Expected header + 24 hourly rows, got {}",
        lines.len()
    );

    // Verify header includes input columns (show-inputs auto-enabled for position)
    assert!(lines[0].contains("latitude"));
    assert!(lines[0].contains("longitude"));
    assert!(lines[0].contains("dateTime"));
    assert!(lines[0].contains("azimuth"));
    assert!(lines[0].contains("zenith"));
}

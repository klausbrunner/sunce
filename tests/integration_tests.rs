use assert_cmd::Command;
use predicates::prelude::*;

/// Test basic position calculation
#[test]
fn test_basic_position_calculation() {
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["52.0", "13.4", "2024-01-01T12:00:00", "position"]);

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("date/time"))
        .stdout(predicate::str::contains("azimuth"))
        .stdout(predicate::str::contains("zenith"));
}

/// Test position with different algorithms
#[test]
fn test_position_algorithms() {
    // Test SPA algorithm
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
        "--algorithm=SPA",
    ]);
    cmd.assert().success();

    // Test GRENA3 algorithm
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
        "--algorithm=GRENA3",
    ]);
    cmd.assert().success();
}

/// Test different output formats
#[test]
fn test_output_formats() {
    // Test HUMAN format (default)
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["52.0", "13.4", "2024-01-01T12:00:00", "position"]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("date/time"));

    // Test CSV format
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
    ]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("dateTime,azimuth,zenith"));

    // Test JSON format
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=JSON",
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
    ]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("\"dateTime\""))
        .stdout(predicate::str::contains("\"azimuth\""));
}

/// Test elevation angle vs zenith angle
#[test]
fn test_elevation_vs_zenith() {
    // Test default (zenith angle)
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
    ]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("zenith"));

    // Test elevation angle
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
        "--elevation-angle",
    ]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("elevation-angle"));
}

/// Test coordinate ranges (geographic sweeps)
#[test]
fn test_coordinate_ranges() {
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "52:53:1",
        "13:14:1",
        "2024-01-01T12:00:00",
        "position",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should have 4 coordinate combinations (2x2 grid)
    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 5); // Header + 4 data rows

    // Check some coordinate values
    assert!(output_str.contains("52.00000,13.00000"));
    assert!(output_str.contains("52.00000,14.00000"));
    assert!(output_str.contains("53.00000,13.00000"));
    assert!(output_str.contains("53.00000,14.00000"));
}

/// Test time series generation
#[test]
fn test_time_series() {
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-01-01",
        "position",
        "--step=6h",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should have times at 6-hour intervals
    assert!(output_str.contains("2024-01-01T00:00:00"));
    assert!(output_str.contains("2024-01-01T06:00:00"));
    assert!(output_str.contains("2024-01-01T12:00:00"));
    assert!(output_str.contains("2024-01-01T18:00:00"));
}

/// Test partial date inputs
#[test]
fn test_partial_dates() {
    // Test year input
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "52.0",
        "13.4",
        "2024",
        "position",
        "--step=24h",
    ]);
    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();
    assert!(output_str.contains("2024-01-01T00:00:00"));
    assert!(output_str.contains("2024-12-31T00:00:00"));

    // Test year-month input
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-06",
        "position",
        "--step=24h",
    ]);
    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();
    assert!(output_str.contains("2024-06-01T00:00:00"));
    assert!(output_str.contains("2024-06-30T00:00:00"));
}

/// Test show-inputs functionality
#[test]
fn test_show_inputs() {
    // Test auto-enabling for ranges
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "52:53:1",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
    ]);
    cmd.assert().success().stdout(predicate::str::contains(
        "latitude,longitude,elevation,pressure,temperature",
    ));

    // Test explicit disable
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "--no-show-inputs",
        "52:53:1",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
    ]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("dateTime,azimuth,zenith"));
}

/// Test environmental parameters
#[test]
fn test_environmental_parameters() {
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "--show-inputs",
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
        "--elevation=1000",
        "--pressure=900",
        "--temperature=25",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    assert!(output_str.contains("1000.000")); // elevation
    assert!(output_str.contains("900.000")); // pressure
    assert!(output_str.contains("25.000")); // temperature
}

/// Test refraction correction
#[test]
fn test_refraction_correction() {
    // Test with refraction (default)
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["52.0", "13.4", "2024-01-01T12:00:00", "position"]);
    let output1 = cmd.assert().success().get_output().stdout.clone();

    // Test without refraction
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
        "--no-refraction",
    ]);
    let output2 = cmd.assert().success().get_output().stdout.clone();

    // Results should be slightly different due to refraction correction
    assert_ne!(output1, output2);
}

/// Test timezone handling
#[test]
fn test_timezone_handling() {
    // Test with timezone override
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--timezone=+02:00",
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
    ]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("+02:00"));

    // Test with named timezone
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--timezone=UTC",
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
    ]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("+00:00"));
}

/// Test different time step formats
#[test]
fn test_time_step_formats() {
    // Test seconds
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
        "--step=30s",
    ]);
    cmd.assert().success();

    // Test minutes
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
        "--step=15m",
    ]);
    cmd.assert().success();

    // Test hours
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
        "--step=2h",
    ]);
    cmd.assert().success();

    // Test days
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-01",
        "position",
        "--step=7d",
    ]);
    cmd.assert().success();
}

/// Test coordinate validation
#[test]
fn test_coordinate_validation() {
    // Test invalid latitude
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["91.0", "13.4", "2024-01-01T12:00:00", "position"]);
    cmd.assert().failure();

    // Test invalid longitude
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["52.0", "181.0", "2024-01-01T12:00:00", "position"]);
    cmd.assert().failure();
}

/// Test datetime parsing
#[test]
fn test_datetime_parsing() {
    // Test various datetime formats
    let formats = [
        "2024-01-01T12:00:00",
        "2024-01-01 12:00:00",
        "2024-01-01T12:00:00Z",
        "2024-01-01T12:00:00+01:00",
    ];

    for format in &formats {
        let mut cmd = Command::cargo_bin("sunce").unwrap();
        cmd.args(["52.0", "13.4", format, "position"]);
        cmd.assert().success();
    }
}

/// Test edge cases
#[test]
fn test_edge_cases() {
    // Test North Pole
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["90.0", "0.0", "2024-06-21T12:00:00", "position"]);
    cmd.assert().success();

    // Test South Pole
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["-90.0", "0.0", "2024-12-21T12:00:00", "position"]);
    cmd.assert().success();

    // Test International Date Line
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["0.0", "180.0", "2024-01-01T12:00:00", "position"]);
    cmd.assert().success();
}

/// Test combined range and time series
#[test]
fn test_combined_range_and_time_series() {
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "52:53:1",
        "13:14:1",
        "2024-01-01",
        "position",
        "--step=12h",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should have cartesian product: 2 lats × 2 lons × 3 times = 12 rows + header
    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 13); // Header + 12 data rows
}

/// Test now datetime
#[test]
fn test_now_datetime() {
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["52.0", "13.4", "now", "position"]);
    cmd.assert().success();
}

/// Test headers in CSV output
#[test]
fn test_csv_headers() {
    // Test with headers (default)
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
    ]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("dateTime,azimuth,zenith"));

    // Test without headers
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "--no-headers",
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
    ]);
    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();
    assert!(!output_str.contains("dateTime,azimuth,zenith"));
}

/// Test delta T parameter
#[test]
fn test_delta_t() {
    // Test with explicit delta T
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "--show-inputs",
        "--deltat=69.2",
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
    ]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("69.200"));

    // Test with delta T estimation
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--deltat",
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
    ]);
    cmd.assert().success();
}

/// Test error handling
#[test]
fn test_error_handling() {
    // Test missing arguments
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["52.0"]);
    cmd.assert().failure();

    // Test invalid time step
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
        "--step=invalid",
    ]);
    cmd.assert().failure();

    // Test invalid algorithm
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
        "--algorithm=INVALID",
    ]);
    cmd.assert().failure();
}

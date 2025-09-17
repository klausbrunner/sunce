mod common;
use common::*;

/// Test basic position calculation
#[test]
fn test_basic_position_calculation() {
    position_test().assert_success_contains_all(&["date/time", "azimuth", "zenith"]);
}

/// Test position with different algorithms
#[test]
fn test_position_algorithms() {
    // Test SPA algorithm
    position_test().arg("--algorithm=SPA").assert_success();

    // Test GRENA3 algorithm
    position_test().arg("--algorithm=GRENA3").assert_success();
}

/// Test different output formats
#[test]
fn test_output_formats() {
    // Test HUMAN format (default)
    position_test().assert_success_contains("date/time");

    // Test CSV format
    position_test_with_format("CSV").assert_success_contains("dateTime,azimuth,zenith");

    // Test JSON format
    position_test_with_format("JSON").assert_success_contains_all(&["\"dateTime\"", "\"azimuth\""]);
}

/// Test elevation angle vs zenith angle
#[test]
fn test_elevation_vs_zenith() {
    // Test default (zenith angle)
    position_test_with_format("CSV").assert_success_contains("zenith");

    // Test elevation angle
    position_test_with_elevation().assert_success_contains("elevation-angle");
}

/// Test coordinate ranges (geographic sweeps)
#[test]
fn test_coordinate_ranges() {
    let output = coordinate_range_test()
        .command()
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
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
    let output = time_series_test("2024-01-01", "6h")
        .command()
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
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
    let output = time_series_test("2024", "24h")
        .command()
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let output_str = String::from_utf8(output).unwrap();
    assert!(output_str.contains("2024-01-01T00:00:00"));
    assert!(output_str.contains("2024-12-31T00:00:00"));

    // Test year-month input
    let output = time_series_test("2024-06", "24h")
        .command()
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let output_str = String::from_utf8(output).unwrap();
    assert!(output_str.contains("2024-06-01T00:00:00"));
    assert!(output_str.contains("2024-06-30T00:00:00"));
}

/// Test show-inputs functionality
#[test]
fn test_show_inputs() {
    // Test auto-enabling for ranges
    show_inputs_lat_range_test()
        .assert_success_contains("latitude,longitude,elevation,pressure,temperature");

    // Test explicit disable
    show_inputs_disabled_test().assert_success_contains("dateTime,azimuth,zenith");
}

/// Test environmental parameters
#[test]
fn test_environmental_parameters() {
    let output = environmental_params_test()
        .command()
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let output_str = String::from_utf8(output).unwrap();

    assert!(output_str.contains("1000.000")); // elevation
    assert!(output_str.contains("900.000")); // pressure
    assert!(output_str.contains("25.000")); // temperature
}

/// Test refraction correction
#[test]
fn test_refraction_correction() {
    // Test with refraction (default)
    let output1 = position_test()
        .command()
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    // Test without refraction
    let output2 = position_no_refraction_test()
        .command()
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    // Results should be slightly different due to refraction correction
    assert_ne!(output1, output2);
}

/// Test timezone handling
#[test]
fn test_timezone_handling() {
    // Test with timezone override
    position_with_timezone("+02:00").assert_success_contains("+02:00");

    // Test with named timezone
    position_with_timezone("UTC").assert_success_contains("+00:00");
}

/// Test different time step formats
#[test]
fn test_time_step_formats() {
    // Test seconds
    time_series_test("2024-01-01T12:00:00", "30s").assert_success();

    // Test minutes
    time_series_test("2024-01-01T12:00:00", "15m").assert_success();

    // Test hours
    time_series_test("2024-01-01T12:00:00", "2h").assert_success();

    // Test days
    time_series_test("2024-01", "7d").assert_success();
}

/// Test coordinate validation
#[test]
fn test_coordinate_validation() {
    // Test invalid latitude
    custom_position("91.0", "13.4", "2024-01-01T12:00:00").assert_failure();

    // Test invalid longitude
    custom_position("52.0", "181.0", "2024-01-01T12:00:00").assert_failure();
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
        custom_position("52.0", "13.4", format).assert_success();
    }
}

/// Test edge cases
#[test]
fn test_edge_cases() {
    // Test North Pole
    custom_position("90.0", "0.0", "2024-06-21T12:00:00").assert_success();

    // Test South Pole
    custom_position("-90.0", "0.0", "2024-12-21T12:00:00").assert_success();

    // Test International Date Line
    custom_position("0.0", "180.0", "2024-01-01T12:00:00").assert_success();
}

/// Test combined range and time series
#[test]
fn test_combined_range_and_time_series() {
    let output = combined_range_time_test()
        .command()
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should have cartesian product: 2 lats × 2 lons × 2 times (00:00, 12:00) = 8 rows + header
    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 9); // Header + 8 data rows
}

/// Test now datetime
#[test]
fn test_now_datetime() {
    custom_position("52.0", "13.4", "now").assert_success();
}

/// Test headers in CSV output
#[test]
fn test_csv_headers() {
    // Test with headers (default)
    position_test_with_format("CSV").assert_success_contains("dateTime,azimuth,zenith");

    // Test without headers
    let output = position_csv_no_headers()
        .command()
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let output_str = String::from_utf8(output).unwrap();
    assert!(!output_str.contains("dateTime,azimuth,zenith"));
}

/// Test delta T parameter
#[test]
fn test_delta_t() {
    // Test with explicit delta T
    position_with_deltat("69.2").assert_success_contains("69.200");

    // Test with delta T estimation
    position_with_deltat_estimation().assert_success();
}

/// Test error handling
#[test]
fn test_error_handling() {
    // Test missing arguments
    missing_args_test().assert_failure();

    // Test invalid time step
    invalid_step_test().assert_failure();

    // Test invalid algorithm
    invalid_algorithm_test().assert_failure();
}

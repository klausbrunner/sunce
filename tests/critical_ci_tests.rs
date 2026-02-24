/// Critical CI tests for production reliability
/// These tests verify essential functionality that could cause production failures
mod common;
use common::*;

fn csv_number_field(stdout: &str, field: &str) -> f64 {
    let record = parse_csv_single_record_map(stdout);
    record
        .get(field)
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or_else(|| panic!("missing or invalid numeric field: {}", field))
}

fn json_number_field(stdout: &str, field: &str) -> f64 {
    let json = parse_json_output(stdout);
    json.get(field)
        .and_then(serde_json::Value::as_f64)
        .unwrap_or_else(|| panic!("missing or invalid numeric JSON field: {}", field))
}

fn csv_string_field(stdout: &str, field: &str) -> String {
    let record = parse_csv_single_record_map(stdout);
    record
        .get(field)
        .cloned()
        .unwrap_or_else(|| panic!("missing CSV field: {}", field))
}

fn csv_headers(stdout: &str) -> Vec<String> {
    let (headers, _rows) = parse_csv_output(stdout);
    headers
}

/// Test 1: Verify exact functional compatibility with solarpos
/// Uses hardcoded reference data captured from Java solarpos to ensure drop-in replacement capability
#[test]
fn test_solarpos_exact_functional_match() {
    // Reference data captured from: solarpos 52.0 13.4 2024-06-21T12:00:00+02:00 position --format=CSV --show-inputs
    // Expected reference values (approximate, captured from solarpos)
    // Latitude: 52.0, Longitude: 13.4, Date: 2024-06-21 noon in Berlin

    let output = SunceTest::new()
        .args([
            "--format=CSV",
            "--show-inputs",
            "52.0",
            "13.4",
            "2024-06-21T12:00:00+02:00",
            "position",
        ])
        .get_output();

    let stdout = String::from_utf8(output.stdout).unwrap();

    let headers = csv_headers(&stdout);
    assert_eq!(
        headers,
        fields(&[
            "latitude",
            "longitude",
            "elevation",
            "pressure",
            "temperature",
            "dateTime",
            "deltaT",
            "azimuth",
            "zenith",
        ])
    );

    assert_eq!(csv_string_field(&stdout, "latitude"), "52.00000");
    assert_eq!(csv_string_field(&stdout, "longitude"), "13.40000");
    assert!(csv_string_field(&stdout, "dateTime").starts_with("2024-06-21T12:00:00"));
    assert!(csv_string_field(&stdout, "dateTime").ends_with("+02:00"));

    // Verify exact baseline values from solarpos-compatible output.
    let azimuth = csv_number_field(&stdout, "azimuth");
    let zenith = csv_number_field(&stdout, "zenith");
    assert!(
        (azimuth - 148.8808).abs() <= 1e-4,
        "azimuth {} does not match baseline 148.8808",
        azimuth
    );
    assert!(
        (zenith - 31.4083).abs() <= 1e-4,
        "zenith {} does not match baseline 31.4083",
        zenith
    );
}

/// Test sunrise output compatibility
#[test]
fn test_solarpos_sunrise_compatibility() {
    // Reference from: solarpos 52.0 13.4 2024-06-21 sunrise --format=CSV --show-inputs

    let output = SunceTest::new()
        .args([
            "--format=CSV",
            "--show-inputs",
            "--timezone=UTC",
            "52.0",
            "13.4",
            "2024-06-21",
            "sunrise",
        ])
        .get_output();

    let stdout = String::from_utf8(output.stdout).unwrap();

    let headers = csv_headers(&stdout);
    assert_eq!(
        headers,
        fields(&[
            "latitude",
            "longitude",
            "dateTime",
            "deltaT",
            "type",
            "sunrise",
            "transit",
            "sunset",
        ])
    );

    assert_eq!(csv_string_field(&stdout, "latitude"), "52.00000");
    assert_eq!(csv_string_field(&stdout, "longitude"), "13.40000");
    assert_eq!(csv_string_field(&stdout, "type"), "NORMAL");

    // Verify exact baseline times for UTC output.
    let sunrise = csv_string_field(&stdout, "sunrise");
    let transit = csv_string_field(&stdout, "transit");
    let sunset = csv_string_field(&stdout, "sunset");
    assert_time_close(&sunrise, "2024-06-21T02:46:15+00:00", 0);
    assert_time_close(&transit, "2024-06-21T11:08:18+00:00", 0);
    assert_time_close(&sunset, "2024-06-21T19:30:20+00:00", 0);
}

/// Test JSON output structure compatibility
#[test]
fn test_solarpos_json_structure() {
    let output = SunceTest::new()
        .args([
            "--format=JSON",
            "--timezone=UTC",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ])
        .get_output();

    let stdout = String::from_utf8(output.stdout).unwrap();

    // Verify reasonable numerical values (UTC timezone)
    let json = parse_json_output(&stdout);
    assert!(json.get("dateTime").is_some());
    assert!(json.get("azimuth").is_some());
    assert!(json.get("zenith").is_some());
    let azimuth = json_number_field(&stdout, "azimuth");
    let zenith = json_number_field(&stdout, "zenith");
    assert!(
        (191.0..=192.0).contains(&azimuth),
        "azimuth {} not in range 191-192°",
        azimuth
    );
    assert!(
        (75.0..=76.0).contains(&zenith),
        "zenith {} not in range 75-76°",
        zenith
    );
}

/// Test 2: Malformed input error handling
/// Verifies graceful failure on invalid inputs rather than panics/crashes
#[test]
fn test_malformed_input_handling() {
    // Invalid coordinates
    SunceTest::new()
        .args(["999", "13.4", "2024-01-01T12:00:00", "position"])
        .assert_failure();

    SunceTest::new()
        .args(["52.0", "999", "2024-01-01T12:00:00", "position"])
        .assert_failure();

    // Invalid datetime
    SunceTest::new()
        .args(["52.0", "13.4", "invalid-date", "position"])
        .assert_failure();

    // Invalid command
    SunceTest::new()
        .args(["52.0", "13.4", "2024-01-01T12:00:00", "invalid"])
        .assert_failure();

    // Missing required arguments
    SunceTest::new().args(["52.0"]).assert_failure();

    // Invalid algorithm
    SunceTest::new()
        .args([
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "--algorithm=INVALID",
        ])
        .assert_failure();

    // Invalid format
    SunceTest::new()
        .args([
            "--format=INVALID",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ])
        .assert_failure();
}

/// Test file input error handling
#[test]
fn test_file_input_error_handling() {
    // Non-existent file
    SunceTest::new()
        .args(["@nonexistent.txt", "position"])
        .assert_failure();

    // Mixed file types (should be invalid)
    SunceTest::new()
        .args(["@coords.txt", "@times.txt", "position"])
        .assert_failure();
}

/// Test 3: CLI option precedence and edge cases
/// Verifies consistent behavior with conflicting or edge-case arguments
#[test]
fn test_cli_option_precedence() {
    // Test that conflicting deltat options are rejected (clap behavior)
    SunceTest::new()
        .args([
            "--format=CSV",
            "--show-inputs",
            "--deltat=69.2",
            "--deltat",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ])
        .assert_failure(); // Should fail with "cannot be used multiple times"

    // Test specific deltat value works
    let output1 = SunceTest::new()
        .args([
            "--format=CSV",
            "--show-inputs",
            "--deltat=69.2",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ])
        .get_output();

    let stdout1 = String::from_utf8(output1.stdout).unwrap();
    assert_eq!(csv_string_field(&stdout1, "deltaT"), "69.200");

    // Test deltat estimation flag works
    let output2 = SunceTest::new()
        .args([
            "--format=CSV",
            "--show-inputs",
            "--deltat",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ])
        .get_output();

    let stdout2 = String::from_utf8(output2.stdout).unwrap();
    let delta_t = csv_number_field(&stdout2, "deltaT");
    assert!(
        (70.0..=72.0).contains(&delta_t),
        "Expected estimated deltaT around 71 for 2024-01-01, got {}",
        delta_t
    );
    assert_ne!(delta_t, 69.2);
}

/// Test delta-T handling: default zero value
#[test]
fn test_deltat_default_zero() {
    let output = SunceTest::new()
        .args([
            "--format=CSV",
            "--show-inputs",
            "52.0",
            "13.4",
            "2024-06-21T12:00:00+02:00",
            "position",
        ])
        .get_output();

    let stdout = String::from_utf8(output.stdout).unwrap();
    let delta_t = csv_number_field(&stdout, "deltaT");
    assert_eq!(delta_t, 0.0, "Default delta-T should be 0.0");
}

/// Test delta-T handling: explicitly set value
#[test]
fn test_deltat_explicit_value() {
    let output = SunceTest::new()
        .args([
            "--deltat=69.2",
            "--format=CSV",
            "--show-inputs",
            "52.0",
            "13.4",
            "2024-06-21T12:00:00+02:00",
            "position",
        ])
        .get_output();

    let stdout = String::from_utf8(output.stdout).unwrap();
    let delta_t = csv_number_field(&stdout, "deltaT");
    assert_eq!(delta_t, 69.2, "Explicit delta-T should be 69.2");
}

/// Test delta-T handling: request estimation
#[test]
fn test_deltat_estimation() {
    let output = SunceTest::new()
        .args([
            "--deltat",
            "--format=CSV",
            "--show-inputs",
            "52.0",
            "13.4",
            "2024-06-21T12:00:00+02:00",
            "position",
        ])
        .get_output();

    let stdout = String::from_utf8(output.stdout).unwrap();
    let delta_t = csv_number_field(&stdout, "deltaT");

    // For 2024, delta-T should be estimated around 69-72 seconds
    assert!(
        (68.0..=72.0).contains(&delta_t),
        "Estimated delta-T {} should be in range 68-72 seconds for 2024",
        delta_t
    );
    assert_ne!(delta_t, 0.0, "Estimated delta-T should not be zero");
    assert_ne!(
        delta_t, 69.2,
        "Estimated delta-T should not be hardcoded 69.2"
    );
}

/// Test show-inputs precedence
#[test]
fn test_show_inputs_precedence() {
    // --no-show-inputs should override auto-enabling for ranges
    let output = SunceTest::new()
        .args([
            "--format=CSV",
            "--no-show-inputs",
            "52:53:1", // This would normally auto-enable show-inputs
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ])
        .get_output();

    let stdout = String::from_utf8(output.stdout).unwrap();

    let (headers, rows) = parse_csv_output(&stdout);
    assert!(!headers.contains(&"latitude".to_string()));
    assert!(!headers.contains(&"longitude".to_string()));
    assert!(headers.contains(&"azimuth".to_string()));
    assert!(headers.contains(&"zenith".to_string()));
    assert!(rows.len() >= 2);
}

/// Test global vs command option positioning
#[test]
fn test_option_positioning() {
    // Global options before positional args should work
    SunceTest::new()
        .args([
            "--format=CSV",
            "--timezone=UTC",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "--algorithm=SPA",
        ])
        .assert_success();

    // Command options after command should work
    SunceTest::new()
        .args([
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "--algorithm=SPA",
            "--elevation-angle",
        ])
        .assert_success();
}

/// Test parameter validation error handling
/// Verifies that invalid parameter values cause proper errors instead of silent fallbacks
#[test]
fn test_parameter_validation_errors() {
    // Test invalid elevation
    SunceTest::new()
        .args([
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "--elevation=invalid",
        ])
        .assert_failure();

    // Test invalid pressure
    SunceTest::new()
        .args([
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "--pressure=not_a_number",
        ])
        .assert_failure();

    // Test invalid temperature
    SunceTest::new()
        .args([
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "--temperature=xyz",
        ])
        .assert_failure();

    // Test invalid delta-T value
    SunceTest::new()
        .args([
            "--deltat=invalid",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ])
        .assert_failure();
}

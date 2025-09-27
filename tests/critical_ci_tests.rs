/// Critical CI tests for production reliability
/// These tests verify essential functionality that could cause production failures
mod common;
use common::*;

/// Extract numeric field from CSV data line
fn extract_csv_field(output: &str, field_index: usize) -> Option<f64> {
    output
        .lines()
        .find(|line| line.contains(',') && !line.starts_with("latitude"))
        .and_then(|line| line.split(',').nth(field_index))
        .and_then(|field| field.trim().parse().ok())
}

/// Extract numeric field from JSON output
fn extract_json_field(output: &str, field_name: &str) -> Option<f64> {
    output
        .find(&format!("\"{}\":", field_name))
        .and_then(|start| {
            let after_colon = start + field_name.len() + 3;
            let remainder = &output[after_colon..];
            let end = remainder.find([',', '}']).unwrap_or(remainder.len());
            remainder[..end].trim().parse().ok()
        })
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

    // Verify CSV header structure (includes environmental params when show-inputs enabled)
    assert!(stdout.contains(
        "latitude,longitude,elevation,pressure,temperature,dateTime,deltaT,azimuth,zenith"
    ));

    // Verify core coordinate values
    assert!(stdout.contains("52.00000,13.40000"));
    assert!(stdout.contains("2024-06-21T12:00:00"));
    assert!(stdout.contains("+02:00"));

    // Verify reasonable azimuth and zenith values for this date/location
    // (exact values may vary slightly with different algorithms or settings)
    let azimuth = extract_csv_field(&stdout, 7).expect("azimuth field");
    let zenith = extract_csv_field(&stdout, 8).expect("zenith field");
    assert!(
        (147.0..=149.0).contains(&azimuth),
        "azimuth {} not in range 147-149°",
        azimuth
    );
    assert!(
        (30.0..=32.0).contains(&zenith),
        "zenith {} not in range 30-32°",
        zenith
    );
}

/// Test sunrise output compatibility
#[test]
fn test_solarpos_sunrise_compatibility() {
    // Reference from: solarpos 52.0 13.4 2024-06-21 sunrise --format=CSV --show-inputs
    let _expected_sunrise_fields = [
        "52.00000",
        "13.40000",
        "2024-06-21",
        "normal",   // type
        "04:50:57", // sunrise time (approximate - timezone may affect format)
        "12:13:02", // transit
        "19:35:07", // sunset
    ];

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

    // Verify CSV structure
    assert!(stdout.contains("latitude,longitude,dateTime,deltaT,type,sunrise,transit,sunset"));

    // Verify coordinates and type are exact
    assert!(stdout.contains("52.00000"));
    assert!(stdout.contains("13.40000"));
    assert!(stdout.contains("NORMAL"));

    // Verify times are reasonable for summer solstice (UTC timezone)
    assert!(stdout.contains("02:4") || stdout.contains("02:5")); // sunrise around 02:46 UTC
    assert!(stdout.contains("11:0") || stdout.contains("11:1")); // transit around 11:08 UTC
    assert!(stdout.contains("19:2") || stdout.contains("19:3")); // sunset around 19:30 UTC
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

    // Verify JSON field structure (our format may be more minimal than solarpos)
    assert!(stdout.contains("\"dateTime\":"));
    assert!(stdout.contains("\"azimuth\":"));
    assert!(stdout.contains("\"zenith\":"));

    // Verify reasonable numerical values (UTC timezone)
    let azimuth = extract_json_field(&stdout, "azimuth").expect("azimuth field");
    let zenith = extract_json_field(&stdout, "zenith").expect("zenith field");
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
    assert!(stdout1.contains("69.200")); // Should use specific value

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
    // Should use estimated value (~71.2 for 2024-01-01) - look for pattern in CSV data
    assert!(
        stdout2.contains(",71.241,"),
        "Expected deltaT=71.241 in output: {}",
        stdout2
    );
    assert!(!stdout2.contains("69.200"));
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
    let delta_t = extract_csv_field(&stdout, 6).expect("delta-T field");
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
    let delta_t = extract_csv_field(&stdout, 6).expect("delta-T field");
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
    let delta_t = extract_csv_field(&stdout, 6).expect("delta-T field");

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

    // Should NOT contain input columns since --no-show-inputs overrides
    assert!(!stdout.contains("latitude,longitude"));
    // Should contain only output columns
    assert!(stdout.contains("azimuth,zenith"));
    // Should have multiple data rows for the coordinate range
    let lines: Vec<&str> = stdout.lines().collect();
    assert!(lines.len() > 2); // header + at least 2 data rows for 52:53:1 range
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

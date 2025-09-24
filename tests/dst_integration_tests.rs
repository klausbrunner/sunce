use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn test_dst_spring_forward_single_datetime() {
    // Test Europe/Berlin DST spring forward: 2024-03-31 02:00:00 doesn't exist
    // With explicit timezone, this should be treated as still being in winter time (CET)
    // until the actual DST transition at 02:00, so 02:00:00 shows as +01:00
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--timezone=+01:00",
        "--show-inputs",
        "52.0",
        "13.4",
        "2024-03-31T02:00:00",
        "position",
    ]);

    cmd.assert()
        .success()
        .stdout(predicate::str::contains(
            "DateTime:    2024-03-31 02:00:00+01:00",
        ))
        .stdout(predicate::str::contains("31.64778°"));
}

#[test]
fn test_dst_spring_forward_time_series() {
    // Test Europe/Berlin DST spring forward time series: should skip 02:00:00
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--timezone=+01:00",
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-03-31",
        "position",
        "--step=1h",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Fixed offset timezone doesn't have DST transitions, should have all hours
    assert!(output_str.contains("2024-03-31T00:00:00+01:00"));
    assert!(output_str.contains("2024-03-31T01:00:00+01:00"));
    assert!(output_str.contains("2024-03-31T02:00:00+01:00"));
    assert!(output_str.contains("2024-03-31T03:00:00+01:00"));

    // With fixed offset, 02:00:00 should exist (no DST gap)
}

#[test]
fn test_dst_fall_back_single_datetime() {
    // Test fixed offset timezone: 2024-10-27 02:00:00 with +01:00
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--timezone=+01:00",
        "--show-inputs",
        "52.0",
        "13.4",
        "2024-10-27T02:00:00",
        "position",
    ]);

    cmd.assert().success().stdout(predicate::str::contains(
        "DateTime:    2024-10-27 02:00:00+01:00",
    ));
}

#[test]
fn test_dst_fall_back_time_series() {
    // Test fixed offset timezone time series (no DST transitions)
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--timezone=+01:00",
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-10-27",
        "position",
        "--step=1h",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should contain normal hourly progression with fixed +01:00 offset
    assert!(output_str.contains("2024-10-27T01:00:00+01:00"));
    assert!(output_str.contains("2024-10-27T02:00:00+01:00"));
    assert!(output_str.contains("2024-10-27T03:00:00+01:00"));
}

#[test]
fn test_dst_normal_summer_time() {
    // Test normal summer time (CEST) - no DST transition
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--timezone=+02:00",
        "--show-inputs",
        "52.0",
        "13.4",
        "2024-07-15T12:00:00",
        "position",
    ]);

    cmd.assert().success().stdout(predicate::str::contains(
        "DateTime:    2024-07-15 12:00:00+02:00",
    ));
}

#[test]
fn test_dst_normal_winter_time() {
    // Test normal winter time (CET) - no DST transition
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--timezone=+01:00",
        "--show-inputs",
        "52.0",
        "13.4",
        "2024-01-15T12:00:00",
        "position",
    ]);

    cmd.assert().success().stdout(predicate::str::contains(
        "DateTime:    2024-01-15 12:00:00+01:00",
    ));
}

#[test]
fn test_dst_different_timezone_us_eastern() {
    // Test US Eastern timezone with fixed offset -05:00
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--timezone=-05:00",
        "--show-inputs",
        "40.7",
        "-74.0",
        "2024-03-10T02:00:00",
        "position",
    ]);

    cmd.assert().success().stdout(predicate::str::contains(
        "DateTime:    2024-03-10 02:00:00-05:00",
    ));
}

#[test]
fn test_dst_timezone_override() {
    // Test timezone override with DST
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--timezone=+02:00",
        "--show-inputs",
        "52.0",
        "13.4",
        "2024-03-31T02:00:00",
        "position",
    ]);

    // With fixed offset +02:00, there's no DST transition - 02:00:00 should be valid
    cmd.assert().success().stdout(predicate::str::contains(
        "DateTime:    2024-03-31 02:00:00+02:00",
    ));
}

#[test]
fn test_dst_partial_date_time_series() {
    // Test partial date (year-month) with fixed offset timezone
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--timezone=+01:00",
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-03",
        "position",
        "--step=24h",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should contain daily entries throughout March with consistent +01:00 offset
    assert!(output_str.contains("2024-03-30T00:00:00+01:00"));
    assert!(output_str.contains("2024-03-31T00:00:00+01:00"));
    // With fixed offset, all entries should have +01:00
    assert!(output_str.contains("+01:00"));
}

#[test]
fn test_dst_year_time_series() {
    // Test year input with fixed offset timezone
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--timezone=+01:00",
        "--format=CSV",
        "52.0",
        "13.4",
        "2024",
        "position",
        "--step=24h",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should have consistent +01:00 offset throughout the year
    assert!(output_str.contains("2024-03-30T00:00:00+01:00"));
    assert!(output_str.contains("2024-04-01T00:00:00+01:00"));
    assert!(output_str.contains("2024-10-26T00:00:00+01:00"));
    assert!(output_str.contains("2024-10-28T00:00:00+01:00"));
}

#[test]
fn test_dst_edge_case_31st_march_exact_time() {
    // Test 30-minute time steps with fixed offset timezone
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--timezone=+01:00",
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-03-31",
        "position",
        "--step=30m",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should have all times including 02:00 and 02:30 with fixed +01:00 offset
    assert!(output_str.contains("2024-03-31T01:00:00+01:00"));
    assert!(output_str.contains("2024-03-31T02:00:00+01:00"));
    assert!(output_str.contains("2024-03-31T02:30:00+01:00"));
    assert!(output_str.contains("2024-03-31T03:00:00+01:00"));
    // Should have consistent +01:00 offset
    assert!(output_str.contains("+01:00"));
}

#[test]
fn test_dst_comparison_with_utc() {
    // Test that UTC doesn't have DST transitions
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--timezone=UTC",
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-03-31",
        "position",
        "--step=1h",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // UTC should have all hours including 02:00:00
    assert!(output_str.contains("2024-03-31T01:00:00+00:00"));
    assert!(output_str.contains("2024-03-31T02:00:00+00:00"));
    assert!(output_str.contains("2024-03-31T03:00:00+00:00"));
}

#[test]
fn test_dst_named_timezone_spring_forward() {
    // Test Europe/Berlin with DST spring forward: 02:00 doesn't exist on 2024-03-31
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--timezone=Europe/Berlin",
        "--format=CSV",
        "--no-headers",
        "52.0",
        "13.4",
        "2024-03-31",
        "position",
        "--step=1h",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should have times before DST transition
    assert!(output_str.contains("2024-03-31T00:00:00+01:00"));
    assert!(output_str.contains("2024-03-31T01:00:00+01:00"));

    // Should NOT have 02:00 (DST gap - clocks jump to 03:00)
    assert!(!output_str.contains("2024-03-31T02:00:00"));

    // Should have time after DST transition with +02:00 offset
    assert!(output_str.contains("2024-03-31T03:00:00+02:00"));
    assert!(output_str.contains("2024-03-31T04:00:00+02:00"));
}

#[test]
fn test_dst_named_timezone_fall_back() {
    // Test Europe/Berlin with DST fall back: 02:00 occurs twice on 2024-10-27
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--timezone=Europe/Berlin",
        "--format=CSV",
        "--no-headers",
        "52.0",
        "13.4",
        "2024-10-27",
        "position",
        "--step=1h",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should have summer time before fall-back
    assert!(output_str.contains("2024-10-27T01:00:00+02:00"));

    // 02:00 is ambiguous - should show BOTH occurrences (like solarpos/Java)
    assert!(output_str.contains("2024-10-27T02:00:00+02:00")); // First 02:00 (summer)
    assert!(output_str.contains("2024-10-27T02:00:00+01:00")); // Second 02:00 (winter)

    // After fall-back should be winter time (+01:00)
    assert!(output_str.contains("2024-10-27T03:00:00+01:00"));
    assert!(output_str.contains("2024-10-27T04:00:00+01:00"));
}

#[test]
fn test_dst_named_timezone_us_eastern() {
    // Test US/Eastern DST spring forward: 2024-03-10 02:00 doesn't exist
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--timezone=America/New_York",
        "--format=CSV",
        "--no-headers",
        "40.7",
        "-74.0",
        "2024-03-10",
        "position",
        "--step=1h",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Before DST: EST (-05:00)
    assert!(output_str.contains("2024-03-10T01:00:00-05:00"));

    // Should NOT have 02:00 (DST gap)
    assert!(!output_str.contains("2024-03-10T02:00:00"));

    // After DST: EDT (-04:00)
    assert!(output_str.contains("2024-03-10T03:00:00-04:00"));
}

#[test]
fn test_system_timezone_detection() {
    // Test that system timezone detection works properly without any TZ override
    // This verifies that iana-time-zone works correctly on all platforms
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--show-inputs",
        "52.0",
        "13.4",
        "2024-01-15T12:00:00",
        "position",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // The output should contain a valid timezone offset
    // We can't assert the exact timezone since it depends on the system, but we can verify
    // that it produces a valid datetime with timezone information
    assert!(output_str.contains("DateTime:    2024-01-15 12:00:00"));

    // Should contain some timezone offset (either + or -)
    let has_timezone = output_str.contains("+") || output_str.contains("-");
    assert!(
        has_timezone,
        "Output should contain timezone information: {}",
        output_str
    );

    // Should not be malformed
    assert!(!output_str.contains("Invalid"));
    assert!(!output_str.contains("Error"));

    // Additional check: if we're on Windows CI (which is likely UTC), that's acceptable
    // The key is that timezone detection doesn't crash and produces valid output
    if cfg!(windows) {
        // On Windows, we accept either a proper timezone or UTC (common in CI environments)
        // The important thing is that the system timezone detection works without crashing
        println!("Windows system timezone detection result: {}", output_str);
    }
}

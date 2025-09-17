use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn test_dst_spring_forward_single_datetime() {
    // Test Europe/Berlin DST spring forward: 2024-03-31 02:00:00 doesn't exist
    // With system timezone, this should be treated as still being in winter time (CET)
    // until the actual DST transition at 02:00, so 02:00:00 shows as +01:00
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.env("TZ", "Europe/Berlin")
        .args(["52.0", "13.4", "2024-03-31T02:00:00", "position"]);

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("2024-03-31 02:00:00+01:00"))
        .stdout(predicate::str::contains("31.64778°"));
}

#[test]
fn test_dst_spring_forward_time_series() {
    // Test Europe/Berlin DST spring forward time series: should skip 02:00:00
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.env("TZ", "Europe/Berlin").args([
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-03-31",
        "position",
        "--step=1h",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should contain 00:00:00+01:00, 01:00:00+01:00, and 03:00:00+02:00
    assert!(output_str.contains("2024-03-31T00:00:00+01:00"));
    assert!(output_str.contains("2024-03-31T01:00:00+01:00"));
    assert!(output_str.contains("2024-03-31T03:00:00+02:00"));

    // Should NOT contain 02:00:00 (skipped during DST transition)
    assert!(!output_str.contains("2024-03-31T02:00:00"));
}

#[test]
fn test_dst_fall_back_single_datetime() {
    // Test Europe/Berlin DST fall back: 2024-10-27 02:00:00 is ambiguous
    // Should choose first occurrence which is +01:00 (winter time)
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.env("TZ", "Europe/Berlin")
        .args(["52.0", "13.4", "2024-10-27T02:00:00", "position"]);

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("2024-10-27 02:00:00+01:00"));
}

#[test]
#[ignore] // TODO: Fix fall-back DST transition (should show both 02:00:00+02:00 and 02:00:00+01:00)
fn test_dst_fall_back_time_series() {
    // Test Europe/Berlin DST fall back time series: should show both occurrences
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.env("TZ", "Europe/Berlin").args([
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-10-27",
        "position",
        "--step=1h",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should contain the transition: 01:00:00+02:00, 02:00:00+02:00, 02:00:00+01:00, 03:00:00+01:00
    assert!(output_str.contains("2024-10-27T01:00:00+02:00"));
    assert!(output_str.contains("2024-10-27T02:00:00+02:00"));
    assert!(output_str.contains("2024-10-27T02:00:00+01:00"));
    assert!(output_str.contains("2024-10-27T03:00:00+01:00"));
}

#[test]
fn test_dst_normal_summer_time() {
    // Test normal summer time (CEST) - no DST transition
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.env("TZ", "Europe/Berlin")
        .args(["52.0", "13.4", "2024-07-15T12:00:00", "position"]);

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("2024-07-15 12:00:00+02:00"));
}

#[test]
fn test_dst_normal_winter_time() {
    // Test normal winter time (CET) - no DST transition
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.env("TZ", "Europe/Berlin")
        .args(["52.0", "13.4", "2024-01-15T12:00:00", "position"]);

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("2024-01-15 12:00:00+01:00"));
}

#[test]
fn test_dst_different_timezone_us_eastern() {
    // Test US Eastern DST spring forward: 2024-03-10 02:00:00 doesn't exist
    // With system timezone, this should be treated as still being in standard time (EST)
    // until the actual DST transition at 02:00, so 02:00:00 shows as -05:00
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.env("TZ", "America/New_York")
        .args(["40.7", "-74.0", "2024-03-10T02:00:00", "position"]);

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("2024-03-10 02:00:00-05:00"));
}

#[test]
fn test_dst_timezone_override() {
    // Test timezone override with DST
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--timezone=+02:00",
        "52.0",
        "13.4",
        "2024-03-31T02:00:00",
        "position",
    ]);

    // With fixed offset +02:00, there's no DST transition - 02:00:00 should be valid
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("2024-03-31 02:00:00+02:00"));
}

#[test]
fn test_dst_partial_date_time_series() {
    // Test partial date (year-month) that spans DST transition
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.env("TZ", "Europe/Berlin").args([
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-03",
        "position",
        "--step=24h",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should contain daily entries throughout March, including DST transition
    assert!(output_str.contains("2024-03-30T00:00:00+01:00")); // Day before DST
    assert!(output_str.contains("2024-03-31T00:00:00+01:00")); // DST transition day
    // With 24h steps, we get daily entries at midnight, not end-of-day
    assert!(output_str.contains("+01:00")); // Before DST (March 30 and earlier)
    // March 31 is still +01:00 at midnight since DST happens at 02:00
}

#[test]
fn test_dst_year_time_series() {
    // Test year input that includes both DST transitions
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.env("TZ", "Europe/Berlin").args([
        "--format=CSV",
        "52.0",
        "13.4",
        "2024",
        "position",
        "--step=24h",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should handle both spring forward and fall back transitions
    assert!(output_str.contains("2024-03-30T00:00:00+01:00")); // Before spring DST
    assert!(output_str.contains("2024-04-01T00:00:00+02:00")); // After spring DST
    assert!(output_str.contains("2024-10-26T00:00:00+02:00")); // Before fall DST
    assert!(output_str.contains("2024-10-28T00:00:00+01:00")); // After fall DST
}

#[test]
fn test_dst_edge_case_31st_march_exact_time() {
    // Test exact DST transition moment for different time steps
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.env("TZ", "Europe/Berlin").args([
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-03-31",
        "position",
        "--step=30m",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should properly handle DST transition - skip 02:00 and 02:30, jump to 03:00
    assert!(output_str.contains("2024-03-31T01:00:00+01:00"));
    assert!(output_str.contains("2024-03-31T03:00:00+02:00"));
    assert!(!output_str.contains("2024-03-31T02:00:00"));
    assert!(!output_str.contains("2024-03-31T02:30:00"));
    // Should have proper timezone transitions
    assert!(output_str.contains("+01:00")); // Before DST
    assert!(output_str.contains("+02:00")); // After DST
}

#[test]
fn test_dst_comparison_with_utc() {
    // Test that UTC doesn't have DST transitions
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.env("TZ", "UTC").args([
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

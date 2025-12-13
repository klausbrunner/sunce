mod common;
use common::sunce_command;
use predicates::prelude::*;

#[test]
fn test_dst_spring_forward_single_datetime() {
    // Test Europe/Berlin DST spring forward: 2024-03-31 02:00:00 doesn't exist
    // With explicit timezone, this should be treated as still being in winter time (CET)
    // until the actual DST transition at 02:00, so 02:00:00 shows as +01:00
    let mut cmd = sunce_command();
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
        .stdout(predicate::str::contains("2024-03-31T02:00:00+01:00"))
        .stdout(predicate::str::contains("31.6478"));
}

#[test]
fn test_dst_spring_forward_time_series() {
    // Test Europe/Berlin DST spring forward time series: should skip 02:00:00
    let mut cmd = sunce_command();
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
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=+01:00",
        "--show-inputs",
        "52.0",
        "13.4",
        "2024-10-27T02:00:00",
        "position",
    ]);

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("2024-10-27T02:00:00+01:00"));
}

#[test]
fn test_dst_fall_back_time_series() {
    // Test fixed offset timezone time series (no DST transitions)
    let mut cmd = sunce_command();
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
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=+02:00",
        "--show-inputs",
        "52.0",
        "13.4",
        "2024-07-15T12:00:00",
        "position",
    ]);

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("2024-07-15T12:00:00+02:00"));
}

#[test]
fn test_dst_normal_winter_time() {
    // Test normal winter time (CET) - no DST transition
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=+01:00",
        "--show-inputs",
        "52.0",
        "13.4",
        "2024-01-15T12:00:00",
        "position",
    ]);

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("2024-01-15T12:00:00+01:00"));
}

#[test]
fn test_dst_different_timezone_us_eastern() {
    // Test US Eastern timezone with fixed offset -05:00
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=-05:00",
        "--show-inputs",
        "40.7",
        "-74.0",
        "2024-03-10T02:00:00",
        "position",
    ]);

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("2024-03-10T02:00:00-05:00"));
}

#[test]
fn test_dst_timezone_override() {
    // Test timezone override with DST
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=+02:00",
        "--show-inputs",
        "52.0",
        "13.4",
        "2024-03-31T02:00:00",
        "position",
    ]);

    // With fixed offset +02:00, there's no DST transition - 02:00:00 should be valid
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("2024-03-31T02:00:00+02:00"));
}

#[test]
fn test_named_timezone_override_uses_dst_offset_summer() {
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=America/New_York",
        "--format=json",
        "--no-show-inputs",
        "0",
        "0",
        "2025-06-21T12:00:00Z",
        "position",
    ]);

    cmd.assert().success().stdout(predicate::str::contains(
        r#""dateTime":"2025-06-21T08:00:00-04:00""#,
    ));
}

#[test]
fn test_named_timezone_override_uses_dst_offset_winter() {
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=America/New_York",
        "--format=json",
        "--no-show-inputs",
        "0",
        "0",
        "2025-01-15T12:00:00Z",
        "position",
    ]);

    cmd.assert().success().stdout(predicate::str::contains(
        r#""dateTime":"2025-01-15T07:00:00-05:00""#,
    ));
}

#[test]
fn test_system_timezone_detection_without_tz_env_summer() {
    let mut cmd = sunce_command();
    cmd.env_remove("TZ")
        .env("SUNCE_SYSTEM_TIMEZONE", "Europe/Berlin")
        .args([
            "--format=json",
            "--no-show-inputs",
            "0",
            "0",
            "2024-07-01T12:00:00",
            "position",
        ]);

    cmd.assert().success().stdout(predicate::str::contains(
        r#""dateTime":"2024-07-01T12:00:00+02:00""#,
    ));
}

#[test]
fn test_system_timezone_detection_without_tz_env_winter() {
    let mut cmd = sunce_command();
    cmd.env_remove("TZ")
        .env("SUNCE_SYSTEM_TIMEZONE", "Europe/Berlin")
        .args([
            "--format=json",
            "--no-show-inputs",
            "0",
            "0",
            "2024-01-10T12:00:00",
            "position",
        ]);

    cmd.assert().success().stdout(predicate::str::contains(
        r#""dateTime":"2024-01-10T12:00:00+01:00""#,
    ));
}

#[test]
fn test_dst_partial_date_time_series() {
    // Test partial date (year-month) with fixed offset timezone
    let mut cmd = sunce_command();
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
    let mut cmd = sunce_command();
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
    let mut cmd = sunce_command();
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
    let mut cmd = sunce_command();
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
    let mut cmd = sunce_command();
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
    let mut cmd = sunce_command();
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

    // 02:00 appears with both offsets due to 1-hour absolute time intervals
    // 00:00 UTC -> 02:00+02:00
    // 01:00 UTC -> 02:00+01:00 (wall clock repeats but absolute time advances)
    assert!(output_str.contains("2024-10-27T02:00:00+02:00"));
    assert!(output_str.contains("2024-10-27T02:00:00+01:00"));

    // After fall-back should be winter time (+01:00)
    assert!(output_str.contains("2024-10-27T03:00:00+01:00"));
    assert!(output_str.contains("2024-10-27T04:00:00+01:00"));
}

#[test]
fn test_dst_named_timezone_us_eastern() {
    // Test US/Eastern DST spring forward: 2024-03-10 02:00 doesn't exist
    let mut cmd = sunce_command();
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
    let mut cmd = sunce_command();
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
    assert!(output_str.contains("2024-01-15T12:00:00"));

    // Should contain a timezone offset (either +HH:MM or -HH:MM)
    let has_timezone = output_str.contains("T12:00:00+") || output_str.contains("T12:00:00-");
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

#[test]
fn test_now_respects_tz_env() {
    // CRITICAL: Test that 'now' uses TZ environment variable, not UTC default
    // This catches the bug where get_timezone_info() defaulted to UTC instead of detecting local timezone
    let mut cmd = sunce_command();
    cmd.env("TZ", "America/New_York"); // Set TZ to non-UTC timezone
    cmd.args(["--format=CSV", "40.7", "-74.0", "now", "position"]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should use America/New_York timezone (either -05:00 or -04:00 depending on DST)
    // NOT +00:00 (UTC)
    let has_ny_tz = output_str.contains("-05:00") || output_str.contains("-04:00");
    assert!(
        has_ny_tz,
        "Should use America/New_York timezone (-05:00 or -04:00), not UTC. Output:\n{}",
        output_str
    );

    // Must NOT contain UTC offset
    assert!(
        !output_str.contains("+00:00"),
        "Should not default to UTC when TZ=America/New_York is set. Output:\n{}",
        output_str
    );
}

#[test]
fn test_now_respects_tz_env_fixed_offset() {
    // Test that 'now' uses fixed offset TZ environment variable
    let mut cmd = sunce_command();
    cmd.env("TZ", "+05:30"); // Set TZ to India Standard Time
    cmd.args(["--format=CSV", "28.6", "77.2", "now", "position"]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should use +05:30 timezone, NOT +00:00 (UTC)
    assert!(
        output_str.contains("+05:30"),
        "Should use TZ=+05:30, not UTC. Output:\n{}",
        output_str
    );

    assert!(
        !output_str.contains("+00:00"),
        "Should not default to UTC when TZ=+05:30 is set. Output:\n{}",
        output_str
    );
}

#[test]
fn test_now_table_format_shows_timezone() {
    // Test that text format shows timezone in the dateTime column
    let mut cmd = sunce_command();
    cmd.env("TZ", "Europe/Paris");
    cmd.args(["48.8", "2.3", "now", "position"]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should contain timezone offset in the table (either +01:00 or +02:00)
    let has_paris_tz = output_str.contains("+01:00") || output_str.contains("+02:00");
    assert!(
        has_paris_tz,
        "Text table should show Europe/Paris timezone offset. Output:\n{}",
        output_str
    );

    assert!(
        output_str.contains("dateTime"),
        "Should have dateTime column header"
    );
}

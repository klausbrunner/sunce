use assert_cmd::Command;
use predicates::prelude::*;
use std::fs::File;
use std::io::Write;
use tempfile::tempdir;

/// Test coordinate file input for position command
#[test]
fn test_coordinate_file_position() {
    let dir = tempdir().unwrap();
    let coords_file = dir.path().join("coords.txt");

    let mut file = File::create(&coords_file).unwrap();
    writeln!(file, "52.0,13.4").unwrap();
    writeln!(file, "59.334,18.063").unwrap();
    writeln!(file, "40.42,-3.70").unwrap();

    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        &format!("@{}", coords_file.to_str().unwrap()),
        "2024-06-21T12:00:00",
        "position",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should show all three coordinate pairs with headers
    assert!(output_str.contains("latitude,longitude"));
    assert!(output_str.contains("52.00000,13.40000"));
    assert!(output_str.contains("59.33400,18.06300"));
    assert!(output_str.contains("40.42000,-3.70000"));

    // Should have 4 lines (header + 3 data rows)
    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 4);
}

/// Test time file input for position command
#[test]
fn test_time_file_position() {
    let dir = tempdir().unwrap();
    let times_file = dir.path().join("times.txt");

    let mut file = File::create(&times_file).unwrap();
    writeln!(file, "2024-06-21T12:00:00").unwrap();
    writeln!(file, "2024-06-21T18:00:00").unwrap();
    writeln!(file, "2024-12-21T12:00:00").unwrap();

    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "52.0",
        "13.4",
        &format!("@{}", times_file.to_str().unwrap()),
        "position",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should show same coordinates with different times
    assert!(output_str.contains("52.00000,13.40000"));
    assert!(output_str.contains("2024-06-21T12:00:00"));
    assert!(output_str.contains("2024-06-21T18:00:00"));
    assert!(output_str.contains("2024-12-21T12:00:00"));

    // Should have 4 lines (header + 3 data rows)
    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 4);
}

/// Test paired data file input for position command
#[test]
fn test_paired_file_position() {
    let dir = tempdir().unwrap();
    let paired_file = dir.path().join("paired.txt");

    let mut file = File::create(&paired_file).unwrap();
    writeln!(file, "52.0,13.4,2024-06-21T12:00:00").unwrap();
    writeln!(file, "59.334,18.063,2024-06-21T18:00:00").unwrap();
    writeln!(file, "40.42,-3.70,2024-12-21T12:00:00").unwrap();

    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        &format!("@{}", paired_file.to_str().unwrap()),
        "position",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should show different coordinates and times
    assert!(output_str.contains("52.00000,13.40000"));
    assert!(output_str.contains("59.33400,18.06300"));
    assert!(output_str.contains("40.42000,-3.70000"));
    assert!(output_str.contains("2024-06-21T12:00:00"));
    assert!(output_str.contains("2024-06-21T18:00:00"));
    assert!(output_str.contains("2024-12-21T12:00:00"));

    // Should have 4 lines (header + 3 data rows)
    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 4);
}

/// Test coordinate file input for sunrise command
#[test]
fn test_coordinate_file_sunrise() {
    let dir = tempdir().unwrap();
    let coords_file = dir.path().join("coords.txt");

    let mut file = File::create(&coords_file).unwrap();
    writeln!(file, "52.0,13.4").unwrap();
    writeln!(file, "59.334,18.063").unwrap();

    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        &format!("@{}", coords_file.to_str().unwrap()),
        "2024-06-21",
        "sunrise",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should show sunrise times for different coordinates
    assert!(output_str.contains("latitude,longitude"));
    assert!(output_str.contains("52.00000,13.40000"));
    assert!(output_str.contains("59.33400,18.06300"));
    assert!(output_str.contains("sunrise"));
    assert!(output_str.contains("sunset"));

    // Should have 3 lines (header + 2 data rows)
    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 3);
}

/// Test paired data file input for sunrise command
#[test]
fn test_paired_file_sunrise() {
    let dir = tempdir().unwrap();
    let paired_file = dir.path().join("paired.txt");

    let mut file = File::create(&paired_file).unwrap();
    writeln!(file, "52.0,13.4,2024-06-21").unwrap();
    writeln!(file, "40.42,-3.70,2024-12-21").unwrap();

    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        &format!("@{}", paired_file.to_str().unwrap()),
        "sunrise",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should show sunrise times for different coordinates and dates
    assert!(output_str.contains("52.00000,13.40000"));
    assert!(output_str.contains("40.42000,-3.70000"));
    assert!(output_str.contains("2024-06-21"));
    assert!(output_str.contains("2024-12-21"));

    // Should have 3 lines (header + 2 data rows)
    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 3);
}

/// Test stdin input with coordinate data
#[test]
fn test_stdin_paired_input() {
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["--format=CSV", "@-", "position"]);
    cmd.write_stdin("52.0,13.4,2024-06-21T12:00:00\n");

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    assert!(output_str.contains("52.00000,13.40000"));
    assert!(output_str.contains("2024-06-21T12:00:00"));
}

/// Test file input with comments and empty lines
#[test]
fn test_file_input_with_comments() {
    let dir = tempdir().unwrap();
    let coords_file = dir.path().join("coords_with_comments.txt");

    let mut file = File::create(&coords_file).unwrap();
    writeln!(file, "# Test coordinate file").unwrap();
    writeln!(file, "52.0,13.4").unwrap();
    writeln!(file).unwrap(); // Empty line
    writeln!(file, "# Another comment").unwrap();
    writeln!(file, "59.334,18.063").unwrap();

    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        &format!("@{}", coords_file.to_str().unwrap()),
        "2024-06-21T12:00:00",
        "position",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should skip comments and empty lines
    assert!(output_str.contains("52.00000,13.40000"));
    assert!(output_str.contains("59.33400,18.06300"));
    assert!(!output_str.contains("#"));

    // Should have 3 lines (header + 2 data rows)
    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 3);
}

/// Test both space and comma separated coordinate formats
#[test]
fn test_mixed_coordinate_formats() {
    let dir = tempdir().unwrap();
    let coords_file = dir.path().join("mixed_coords.txt");

    let mut file = File::create(&coords_file).unwrap();
    writeln!(file, "52.0,13.4").unwrap(); // Comma separated
    writeln!(file, "59.334 18.063").unwrap(); // Space separated
    writeln!(file, "40.42 -3.70").unwrap(); // Space separated with negative

    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        &format!("@{}", coords_file.to_str().unwrap()),
        "2024-06-21T12:00:00",
        "position",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should handle both formats correctly
    assert!(output_str.contains("52.00000,13.40000"));
    assert!(output_str.contains("59.33400,18.06300"));
    assert!(output_str.contains("40.42000,-3.70000"));

    // Should have 4 lines (header + 3 data rows)
    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 4);
}

/// Test timezone handling with file inputs - regression test for timezone-dropping bugs
#[test]
fn test_file_input_timezone_handling() {
    let dir = tempdir().unwrap();

    // Test 1: Time file with --timezone option (TimeFileIterator)
    let times_file = dir.path().join("times.txt");
    let mut file = File::create(&times_file).unwrap();
    writeln!(file, "2024-06-21T12:00:00").unwrap(); // Naive datetime
    writeln!(file, "2024-12-21T15:30:00").unwrap();

    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "--timezone=+02:00", // This should be applied to naive datetimes
        "52.0",
        "13.4",
        &format!("@{}", times_file.to_str().unwrap()),
        "position",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Verify timezone appears in output (proves timezone was applied)
    assert!(
        output_str.contains("+02:00"),
        "TimeFile should apply --timezone to naive datetimes: {}",
        output_str
    );

    // Test 2: Paired file with --timezone option (PairedFileIterator)
    let paired_file = dir.path().join("paired.txt");
    let mut file = File::create(&paired_file).unwrap();
    writeln!(file, "52.0,13.4,2024-06-21T12:00:00").unwrap(); // Naive datetime
    writeln!(file, "40.42,-3.70,2024-12-21T15:30:00").unwrap();

    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "--timezone=-05:00", // Different timezone
        &format!("@{}", paired_file.to_str().unwrap()),
        "position",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Verify timezone appears in output
    assert!(
        output_str.contains("-05:00"),
        "PairedFile should apply --timezone to naive datetimes: {}",
        output_str
    );

    // Test 3: Coordinate file with --timezone option (coordinate file functions)
    let coords_file = dir.path().join("coords.txt");
    let mut file = File::create(&coords_file).unwrap();
    writeln!(file, "52.0,13.4").unwrap();
    writeln!(file, "40.42,-3.70").unwrap();

    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "--timezone=+09:00", // Tokyo timezone
        &format!("@{}", coords_file.to_str().unwrap()),
        "2024-06-21T12:00:00", // Naive datetime
        "sunrise",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Verify timezone appears in output
    assert!(
        output_str.contains("+09:00"),
        "CoordinateFile should apply --timezone to naive datetimes: {}",
        output_str
    );
}

/// Test mixed timezone formats in file inputs
#[test]
fn test_file_input_mixed_timezones() {
    let dir = tempdir().unwrap();

    // Time file with mixed timezone formats
    let times_file = dir.path().join("mixed_times.txt");
    let mut file = File::create(&times_file).unwrap();
    writeln!(file, "2024-06-21T12:00:00+02:00").unwrap(); // Explicit timezone
    writeln!(file, "2024-06-21T15:00:00").unwrap(); // Naive - should use --timezone
    writeln!(file, "2024-06-21T18:00:00Z").unwrap(); // UTC

    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "--timezone=+05:00", // Should only apply to naive datetime
        "52.0",
        "13.4",
        &format!("@{}", times_file.to_str().unwrap()),
        "position",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Verify all timezone formats preserved/applied correctly
    assert!(
        output_str.contains("+02:00"),
        "Explicit timezone should be preserved: {}",
        output_str
    );
    assert!(
        output_str.contains("+05:00"),
        "Naive datetime should get --timezone: {}",
        output_str
    );
    assert!(
        output_str.contains("+00:00"),
        "UTC should be preserved as +00:00: {}",
        output_str
    );
}

/// Test file input error handling
#[test]
fn test_file_input_errors() {
    // Test non-existent file
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args(["@/non/existent/file.txt", "2024-06-21", "position"]);
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("Failed to open coordinate file"));

    // Test invalid coordinate format
    let dir = tempdir().unwrap();
    let invalid_coords = dir.path().join("invalid.txt");

    let mut file = File::create(&invalid_coords).unwrap();
    writeln!(file, "invalid,data").unwrap();

    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        &format!("@{}", invalid_coords.to_str().unwrap()),
        "2024-06-21",
        "position",
    ]);
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("Error reading coordinate data"));
}

/// Test show-inputs auto-enabling for file inputs
#[test]
fn test_file_input_show_inputs_auto() {
    let dir = tempdir().unwrap();
    let coords_file = dir.path().join("coords.txt");

    let mut file = File::create(&coords_file).unwrap();
    writeln!(file, "52.0,13.4").unwrap();

    // Should auto-enable show-inputs for file inputs
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        &format!("@{}", coords_file.to_str().unwrap()),
        "2024-06-21T12:00:00",
        "position",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should include input parameters in CSV header
    assert!(output_str.contains("latitude,longitude,elevation,pressure,temperature"));

    // Test explicit --no-show-inputs override
    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "--no-show-inputs",
        &format!("@{}", coords_file.to_str().unwrap()),
        "2024-06-21T12:00:00",
        "position",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should NOT include input parameters when explicitly disabled
    assert!(output_str.contains("dateTime,azimuth,zenith"));
    assert!(!output_str.contains("latitude,longitude"));
}

/// Test solarpos compatibility with coordinate file input
#[test]
fn test_solarpos_compatibility_coordinate_file() {
    let dir = tempdir().unwrap();
    let coords_file = dir.path().join("coords.txt");

    let mut file = File::create(&coords_file).unwrap();
    writeln!(file, "52.0,13.4").unwrap();
    writeln!(file, "59.334,18.063").unwrap();

    // Test sunce output
    let mut sunce_cmd = Command::cargo_bin("sunce").unwrap();
    sunce_cmd.env("TZ", "UTC").args([
        "--format=CSV",
        &format!("@{}", coords_file.to_str().unwrap()),
        "2024-06-21T12:00:00",
        "position",
    ]);

    let sunce_output = sunce_cmd.assert().success().get_output().stdout.clone();
    let sunce_str = String::from_utf8(sunce_output).unwrap();

    // Verify key values are present (these should match solarpos exactly)
    assert!(sunce_str.contains("204.04406,30.22402")); // Berlin coordinates
    assert!(sunce_str.contains("206.76827,37.97019")); // Stockholm coordinates
    assert!(sunce_str.contains("52.00000,13.40000"));
    assert!(sunce_str.contains("59.33400,18.06300"));
}

/// Test solarpos compatibility with time file input
#[test]
fn test_solarpos_compatibility_time_file() {
    let dir = tempdir().unwrap();
    let times_file = dir.path().join("times.txt");

    let mut file = File::create(&times_file).unwrap();
    writeln!(file, "2024-06-21T12:00:00").unwrap();
    writeln!(file, "2024-06-21T18:00:00").unwrap();

    let mut sunce_cmd = Command::cargo_bin("sunce").unwrap();
    sunce_cmd.env("TZ", "UTC").args([
        "--format=CSV",
        "52.0",
        "13.4",
        &format!("@{}", times_file.to_str().unwrap()),
        "position",
    ]);

    let sunce_output = sunce_cmd.assert().success().get_output().stdout.clone();
    let sunce_str = String::from_utf8(sunce_output).unwrap();

    // Verify exact values that should match solarpos
    assert!(sunce_str.contains("204.04406,30.22402")); // 12:00 position
    assert!(sunce_str.contains("294.43563,79.12831")); // 18:00 position
}

/// Test solarpos compatibility with paired file input
#[test]
fn test_solarpos_compatibility_paired_file() {
    let dir = tempdir().unwrap();
    let paired_file = dir.path().join("paired.txt");

    let mut file = File::create(&paired_file).unwrap();
    writeln!(file, "52.0,13.4,2024-06-21T12:00:00").unwrap();
    writeln!(file, "40.42,-3.70,2024-12-21T12:00:00").unwrap();

    let mut sunce_cmd = Command::cargo_bin("sunce").unwrap();
    sunce_cmd.env("TZ", "UTC").args([
        "--format=CSV",
        &format!("@{}", paired_file.to_str().unwrap()),
        "position",
    ]);

    let sunce_output = sunce_cmd.assert().success().get_output().stdout.clone();
    let sunce_str = String::from_utf8(sunce_output).unwrap();

    // Verify exact values that should match solarpos
    assert!(sunce_str.contains("204.04406,30.22402")); // Berlin summer
    assert!(sunce_str.contains("176.65798,63.89946")); // Madrid winter
}

/// Test solarpos compatibility with sunrise coordinate file
#[test]
fn test_solarpos_compatibility_sunrise_coordinate_file() {
    let dir = tempdir().unwrap();
    let coords_file = dir.path().join("coords.txt");

    let mut file = File::create(&coords_file).unwrap();
    writeln!(file, "52.0,13.4").unwrap();
    writeln!(file, "40.42,-3.70").unwrap();

    let mut sunce_cmd = Command::cargo_bin("sunce").unwrap();
    sunce_cmd.env("TZ", "UTC").args([
        "--format=CSV",
        &format!("@{}", coords_file.to_str().unwrap()),
        "2024-06-21",
        "sunrise",
    ]);

    let sunce_output = sunce_cmd.assert().success().get_output().stdout.clone();
    let sunce_str = String::from_utf8(sunce_output).unwrap();

    // Verify exact sunrise times that should match solarpos
    assert!(sunce_str.contains("2024-06-21T02:46:15+00:00")); // Berlin sunrise
    assert!(sunce_str.contains("2024-06-21T19:30:20+00:00")); // Berlin sunset
    assert!(sunce_str.contains("2024-06-21T04:44:49+00:00")); // Madrid sunrise
    assert!(sunce_str.contains("2024-06-21T19:48:36+00:00")); // Madrid sunset
}

/// Test stdin compatibility with solarpos format
#[test]
fn test_solarpos_compatibility_stdin() {
    let mut sunce_cmd = Command::cargo_bin("sunce").unwrap();
    sunce_cmd
        .env("TZ", "UTC")
        .args(["--format=CSV", "@-", "position"]);
    sunce_cmd.write_stdin("52.0,13.4,2024-06-21T12:00:00\n");

    let sunce_output = sunce_cmd.assert().success().get_output().stdout.clone();
    let sunce_str = String::from_utf8(sunce_output).unwrap();

    // Should match exact solarpos values
    assert!(sunce_str.contains("204.04406,30.22402"));
    assert!(sunce_str.contains("52.00000,13.40000"));
}

/// Test coordinate ranges with time files for position command
#[test]
fn test_coordinate_ranges_with_time_files_position() {
    let dir = tempdir().unwrap();
    let times_file = dir.path().join("times.txt");

    let mut file = File::create(&times_file).unwrap();
    writeln!(file, "2024-06-21T12:00:00").unwrap();
    writeln!(file, "2024-06-21T18:00:00").unwrap();

    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "52.0:52.1:0.1", // Latitude range
        "13.4:13.5:0.1", // Longitude range
        &format!("@{}", times_file.to_str().unwrap()),
        "position",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should contain header with coordinates and other fields
    assert!(output_str.contains("latitude,longitude,elevation"));

    // Should contain coordinate range values for both times
    assert!(output_str.contains("52.00000,13.40000"));
    assert!(output_str.contains("52.10000,13.50000"));
    assert!(output_str.contains("2024-06-21T12:00:00"));
    assert!(output_str.contains("2024-06-21T18:00:00"));

    // Should have the right number of rows:
    // Header + (2 lat values * 2 lon values * 2 time values) = 1 + 8 = 9 lines
    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 9);
}

/// Test coordinate ranges with time files for sunrise command
#[test]
fn test_coordinate_ranges_with_time_files_sunrise() {
    let dir = tempdir().unwrap();
    let times_file = dir.path().join("dates.txt");

    let mut file = File::create(&times_file).unwrap();
    writeln!(file, "2024-06-21").unwrap(); // Summer solstice
    writeln!(file, "2024-12-21").unwrap(); // Winter solstice

    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "50.0:51.0:1.0", // Latitude range (2 values)
        "10.0:11.0:1.0", // Longitude range (2 values)
        &format!("@{}", times_file.to_str().unwrap()),
        "sunrise",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should contain headers with coordinates and sunrise fields
    assert!(output_str.contains("latitude,longitude,dateTime"));
    assert!(output_str.contains("sunrise,transit,sunset"));

    // Should contain range coordinate values for both dates
    assert!(output_str.contains("50.00000,10.00000"));
    assert!(output_str.contains("51.00000,11.00000"));
    assert!(output_str.contains("2024-06-21"));
    assert!(output_str.contains("2024-12-21"));

    // Should have the right number of rows:
    // Header + (2 lat values * 2 lon values * 2 date values) = 1 + 8 = 9 lines
    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 9);
}

/// Test single coordinate with time file range (ensure backward compatibility)
#[test]
fn test_single_coordinates_with_time_files() {
    let dir = tempdir().unwrap();
    let times_file = dir.path().join("times.txt");

    let mut file = File::create(&times_file).unwrap();
    writeln!(file, "2024-06-21T12:00:00").unwrap();
    writeln!(file, "2024-06-21T15:00:00").unwrap();

    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "52.0", // Single latitude
        "13.4", // Single longitude
        &format!("@{}", times_file.to_str().unwrap()),
        "position",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should work exactly as before (backward compatibility)
    assert!(output_str.contains("52.00000,13.40000"));
    assert!(output_str.contains("2024-06-21T12:00:00"));
    assert!(output_str.contains("2024-06-21T15:00:00"));

    // Should have 3 lines (header + 2 time values)
    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 3);
}

/// Test mixed ranges (lat range, lon single) with time files
#[test]
fn test_mixed_coordinate_ranges_with_time_files() {
    let dir = tempdir().unwrap();
    let times_file = dir.path().join("times.txt");

    let mut file = File::create(&times_file).unwrap();
    writeln!(file, "2024-06-21T12:00:00").unwrap();

    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "52.0:53.0:1.0", // Latitude range (2 values)
        "13.4",          // Single longitude
        &format!("@{}", times_file.to_str().unwrap()),
        "position",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should contain both latitude values with same longitude
    assert!(output_str.contains("52.00000,13.40000"));
    assert!(output_str.contains("53.00000,13.40000"));
    assert!(output_str.contains("2024-06-21T12:00:00"));

    // Should have 3 lines (header + 2 latitude values * 1 longitude * 1 time)
    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 3);
}

/// Test coordinate file + time file for position command (NEW!)
#[test]
fn test_coordinate_file_time_file_position() {
    let dir = tempdir().unwrap();
    let coords_file = dir.path().join("coords.txt");
    let times_file = dir.path().join("times.txt");

    let mut file = File::create(&coords_file).unwrap();
    writeln!(file, "52.0,13.4").unwrap();
    writeln!(file, "53.0,14.4").unwrap();

    let mut file = File::create(&times_file).unwrap();
    writeln!(file, "2024-06-21T12:00:00").unwrap();
    writeln!(file, "2024-12-21T12:00:00").unwrap();

    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        &format!("@{}", coords_file.to_str().unwrap()),
        &format!("@{}", times_file.to_str().unwrap()),
        "position",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should contain cartesian product: 2 coords × 2 times = 4 results + header = 5 lines
    assert!(output_str.contains("latitude,longitude"));
    assert!(output_str.contains("52.00000,13.40000"));
    assert!(output_str.contains("53.00000,14.40000"));
    assert!(output_str.contains("2024-06-21T12:00:00"));
    assert!(output_str.contains("2024-12-21T12:00:00"));

    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 5); // Header + 4 cartesian product results
}

/// Test coordinate file + time file for sunrise command (NEW!)
#[test]
fn test_coordinate_file_time_file_sunrise() {
    let dir = tempdir().unwrap();
    let coords_file = dir.path().join("coords.txt");
    let dates_file = dir.path().join("dates.txt");

    let mut file = File::create(&coords_file).unwrap();
    writeln!(file, "52.0,13.4").unwrap();
    writeln!(file, "59.334,18.063").unwrap(); // Stockholm

    let mut file = File::create(&dates_file).unwrap();
    writeln!(file, "2024-06-21").unwrap(); // Summer solstice
    writeln!(file, "2024-12-21").unwrap(); // Winter solstice

    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        &format!("@{}", coords_file.to_str().unwrap()),
        &format!("@{}", dates_file.to_str().unwrap()),
        "sunrise",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should contain cartesian product: 2 coords × 2 dates = 4 results + header = 5 lines
    assert!(output_str.contains("latitude,longitude,dateTime"));
    assert!(output_str.contains("52.00000,13.40000"));
    assert!(output_str.contains("59.33400,18.06300"));
    assert!(output_str.contains("2024-06-21"));
    assert!(output_str.contains("2024-12-21"));
    assert!(output_str.contains("sunrise"));

    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 5); // Header + 4 cartesian product results
}

/// Test stdin coordinates + time file (FIXED! - Now properly handles cartesian product)
#[test]
fn test_stdin_coordinates_time_file() {
    let dir = tempdir().unwrap();
    let times_file = dir.path().join("times.txt");

    let mut file = File::create(&times_file).unwrap();
    writeln!(file, "2024-06-21T12:00:00").unwrap();
    writeln!(file, "2024-12-21T12:00:00").unwrap();

    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        "@-",
        &format!("@{}", times_file.to_str().unwrap()),
        "position",
    ]);
    cmd.write_stdin("52.0,13.4\n53.0,14.4\n");

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // FIXED: coordinates are now read once and applied to all times (proper cartesian product)
    assert!(output_str.contains("52.00000,13.40000"));
    assert!(output_str.contains("53.00000,14.40000"));
    assert!(output_str.contains("2024-06-21"));
    assert!(output_str.contains("2024-12-21")); // Now works - coordinates read once, applied to all times

    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 5); // Header + 4 cartesian product results (2 coords × 2 times)
}

/// Test coordinate file + stdin times (NEW!)
#[test]
fn test_coordinate_file_stdin_times() {
    let dir = tempdir().unwrap();
    let coords_file = dir.path().join("coords.txt");

    let mut file = File::create(&coords_file).unwrap();
    writeln!(file, "52.0,13.4").unwrap();
    writeln!(file, "53.0,14.4").unwrap();

    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=CSV",
        &format!("@{}", coords_file.to_str().unwrap()),
        "@-",
        "position",
    ]);
    cmd.write_stdin("2024-06-21T12:00:00\n2024-12-21T12:00:00\n");

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should work: coord file + stdin times
    assert!(output_str.contains("52.00000,13.40000"));
    assert!(output_str.contains("53.00000,14.40000"));
    assert!(output_str.contains("2024-06-21"));
    assert!(output_str.contains("2024-12-21"));

    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 5); // Header + 4 results
}

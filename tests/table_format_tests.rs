mod common;
use common::sunce_command;
use predicates::prelude::*;

#[test]
fn test_single_value_table_format() {
    sunce_command()
        .args(["52.0", "13.4", "2024-06-21T12:00:00+02:00", "position"])
        .assert()
        .success()
        .stdout(predicate::str::contains("┌"))
        .stdout(predicate::str::contains("│ Azimuth"))
        .stdout(predicate::str::contains("│ Zenith"))
        .stdout(predicate::str::contains("└"));
}

#[test]
fn test_time_series_table_format() {
    sunce_command()
        .args([
            "--show-inputs",
            "52.0",
            "13.4",
            "2024-06-21",
            "position",
            "--step=3h",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Latitude:    52.000000°"))
        .stdout(predicate::str::contains("Longitude:   13.400000°"))
        .stdout(predicate::str::contains("│ DateTime"))
        .stdout(predicate::str::contains("│ Azimuth"))
        .stdout(predicate::str::contains("│ Zenith"));
}

#[test]
fn test_coordinate_sweep_table_format() {
    sunce_command()
        .args([
            "--show-inputs",
            "52:53:0.5",
            "13.4",
            "2024-06-21T12:00:00+02:00",
            "position",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Longitude:   13.400000°"))
        .stdout(predicate::str::contains(
            "DateTime:    2024-06-21 12:00:00+02:00",
        ))
        .stdout(predicate::str::contains("│ Latitude"))
        .stdout(predicate::str::contains("│ Azimuth"))
        .stdout(predicate::str::contains("│ Zenith"));
}

#[test]
fn test_elevation_angle_table_format() {
    sunce_command()
        .args([
            "52.0",
            "13.4",
            "2024-06-21T12:00:00+02:00",
            "position",
            "--elevation-angle",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("│ Elevation"));

    sunce_command()
        .args([
            "52.0",
            "13.4",
            "2024-06-21T12:00:00+02:00",
            "position",
            "--elevation-angle",
        ])
        .assert()
        .success()
        .stdout(predicate::function(|s: &str| !s.contains("│ Zenith")));
}

#[test]
fn test_mixed_variance_detection() {
    let output = sunce_command()
        .args([
            "--show-inputs",
            "52:54:1",
            "13.4",
            "2024-06-21",
            "position",
            "--step=6h",
        ])
        .output()
        .unwrap();

    let stdout = String::from_utf8(output.stdout).unwrap();

    assert!(stdout.contains("Longitude:"));
    assert!(stdout.contains("DateTime:    2024-06-21"));
    assert!(stdout.contains("│ Latitude"));
}

#[test]
fn test_header_section_with_invariants() {
    let output = sunce_command()
        .args([
            "--show-inputs",
            "52.0",
            "13.4",
            "2024-06-21",
            "position",
            "--step=1h",
        ])
        .output()
        .unwrap();

    let stdout = String::from_utf8(output.stdout).unwrap();
    let lines: Vec<&str> = stdout.lines().collect();

    assert!(lines[0].starts_with("  Latitude:"));
    assert!(lines[1].starts_with("  Longitude:"));
    assert!(lines[2].starts_with("  Elevation:"));
    assert!(lines[3].starts_with("  Pressure:"));
    assert!(lines[4].starts_with("  Temperature:"));
    assert!(lines[5].starts_with("  Delta T:"));

    assert_eq!(lines[6], "");

    assert!(lines[7].contains("┌"));
}

#[test]
fn test_perf_reports_true_record_count_in_text_mode() {
    sunce_command()
        .args([
            "--format=text",
            "--perf",
            "52.0",
            "13.4",
            "2024-06-21T12:00:00+02:00",
            "position",
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains("Processed 1 records"));
}

#[test]
fn test_text_table_omits_refraction_fields_when_disabled() {
    let output = sunce_command()
        .args([
            "52.0",
            "13.4",
            "2024-06-21T12:00:00+02:00",
            "position",
            "--no-refraction",
        ])
        .output()
        .unwrap();

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(!stdout.contains("Pressure:"));
    assert!(!stdout.contains("Temperature:"));
}

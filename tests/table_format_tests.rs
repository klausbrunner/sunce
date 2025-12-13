mod common;
use common::sunce_command;
use predicates::prelude::*;

#[test]
fn test_text_output_has_headers() {
    sunce_command()
        .args(["52.0", "13.4", "2024-06-21T12:00:00+02:00", "position"])
        .assert()
        .success()
        .stdout(predicate::str::contains("dateTime"))
        .stdout(predicate::str::contains("azimuth"))
        .stdout(predicate::str::contains("zenith"))
        .stdout(predicate::function(|s: &str| !s.contains('┌')))
        .stdout(predicate::function(|s: &str| !s.contains('│')));
}

#[test]
fn test_text_output_with_show_inputs_includes_inputs() {
    sunce_command()
        .args([
            "--show-inputs",
            "52.0",
            "13.4",
            "2024-06-21T12:00:00+02:00",
            "position",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("latitude"))
        .stdout(predicate::str::contains("longitude"))
        .stdout(predicate::str::contains("elevation"))
        .stdout(predicate::str::contains("deltaT"));
}

#[test]
fn test_elevation_angle_label() {
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
        .stdout(predicate::str::contains("elevation-angle"))
        .stdout(predicate::function(|s: &str| !s.contains("zenith")));
}

#[test]
fn test_text_output_omits_refraction_columns_when_disabled() {
    sunce_command()
        .args([
            "--show-inputs",
            "52.0",
            "13.4",
            "2024-06-21T12:00:00+02:00",
            "position",
            "--no-refraction",
        ])
        .assert()
        .success()
        .stdout(predicate::function(|s: &str| !s.contains("pressure")))
        .stdout(predicate::function(|s: &str| !s.contains("temperature")));
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

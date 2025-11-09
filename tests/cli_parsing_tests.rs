use predicates::prelude::*;

mod common;
use common::sunce_command;

#[test]
fn test_options_before_positionals() {
    sunce_command()
        .args([
            "--format=csv",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ])
        .assert()
        .success();
}

#[test]
fn test_options_after_positionals() {
    sunce_command()
        .args([
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "--format=csv",
        ])
        .assert()
        .success();
}

#[test]
fn test_options_mixed_positions() {
    sunce_command()
        .args([
            "--format=csv",
            "52.0",
            "13.4",
            "--no-headers",
            "2024-01-01T12:00:00",
            "position",
            "--no-refraction",
        ])
        .assert()
        .success();
}

#[test]
fn test_command_specific_option_before_command() {
    sunce_command()
        .args([
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "--algorithm=grena3",
            "position",
        ])
        .assert()
        .success();
}

#[test]
fn test_command_specific_option_after_command() {
    sunce_command()
        .args([
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "--algorithm=grena3",
        ])
        .assert()
        .success();
}

#[test]
fn test_global_and_command_options_mixed() {
    sunce_command()
        .args([
            "--format=csv",
            "52.0",
            "13.4",
            "2024-01-01",
            "--step=1h",
            "position",
            "--no-headers",
        ])
        .assert()
        .success();
}

#[test]
fn test_step_with_full_datetime_rejected() {
    sunce_command()
        .args([
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "--step=1h",
            "position",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Option --step requires date-only input",
        ));
}

#[test]
fn test_negative_step_rejected() {
    sunce_command()
        .args(["52.0", "13.4", "2024-01-01", "--step=-1h", "position"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("Step must be positive"));
}

#[test]
fn test_invalid_option_for_position_command() {
    sunce_command()
        .args(["52.0", "13.4", "2024-01-01", "--twilight", "position"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "--twilight not valid for position",
        ));
}

#[test]
fn test_invalid_option_for_sunrise_command() {
    sunce_command()
        .args(["52.0", "13.4", "2024-01-01", "--step=1h", "sunrise"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--step not valid for sunrise"));
}

#[test]
fn test_horizon_invalid_for_position() {
    sunce_command()
        .args(["52.0", "13.4", "2024-01-01", "position", "--horizon=-6.0"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--horizon not valid for position"));
}

#[test]
fn test_algorithm_invalid_for_sunrise() {
    sunce_command()
        .args([
            "52.0",
            "13.4",
            "2024-01-01",
            "sunrise",
            "--algorithm=grena3",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "--algorithm not valid for sunrise",
        ));
}

#[test]
fn test_invalid_timezone_datetime_surfaces_error() {
    sunce_command()
        .args([
            "40.0",
            "-74.0",
            "2024-03-10T02:30:00",
            "--timezone=America/New_York",
            "position",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Datetime does not exist in timezone",
        ));
}

#[test]
fn test_invalid_refraction_inputs_surface_error() {
    sunce_command()
        .args([
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "--pressure=-10",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("Invalid refraction parameters"));
}

#[test]
fn test_options_anywhere_with_file_input() {
    sunce_command()
        .args(["--format=json", "@-", "position", "--no-headers"])
        .write_stdin("52.0 13.4 2024-01-01T12:00:00\n")
        .assert()
        .success();
}

#[test]
fn test_deltat_before_and_after_positionals() {
    sunce_command()
        .args([
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "--deltat=69.2",
            "position",
        ])
        .assert()
        .success();

    sunce_command()
        .args([
            "--deltat=69.2",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ])
        .assert()
        .success();
}

#[test]
fn test_multiple_deltat_still_errors() {
    sunce_command()
        .args([
            "--deltat=69.2",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "--deltat=70.0",
            "position",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be used multiple times"));
}

#[test]
fn test_all_position_options_anywhere() {
    sunce_command()
        .args([
            "--format=csv",
            "52.0",
            "--algorithm=grena3",
            "13.4",
            "2024-01-01",
            "--elevation=100.0",
            "position",
            "--temperature=20.0",
            "--pressure=1000.0",
            "--step=2h",
        ])
        .assert()
        .success();
}

#[test]
fn test_sunrise_with_options_anywhere() {
    sunce_command()
        .args([
            "--format=csv",
            "52.0",
            "13.4",
            "--twilight",
            "2024-01-01",
            "sunrise",
            "--horizon=-6.0",
        ])
        .assert()
        .success();
}

#[test]
fn test_step_without_unit() {
    let output = sunce_command()
        .args([
            "52.0",
            "13.4",
            "2024-01-01",
            "position",
            "--step=3600",
            "--format=csv",
            "--no-headers",
        ])
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = stdout.lines().collect();

    assert_eq!(lines.len(), 24);
    assert!(lines[0].contains("2024-01-01T00:00:00"));
    assert!(lines[1].contains("2024-01-01T01:00:00"));
}

#[test]
fn test_step_with_unit_still_works() {
    let output = sunce_command()
        .args([
            "52.0",
            "13.4",
            "2024-01-01",
            "position",
            "--step=1h",
            "--format=csv",
            "--no-headers",
        ])
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = stdout.lines().collect();

    assert_eq!(lines.len(), 24);
    assert!(lines[0].contains("2024-01-01T00:00:00"));
    assert!(lines[1].contains("2024-01-01T01:00:00"));
}

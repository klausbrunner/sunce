use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn test_options_before_positionals() {
    Command::cargo_bin("sunce")
        .unwrap()
        .args(&[
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
    Command::cargo_bin("sunce")
        .unwrap()
        .args(&[
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
    Command::cargo_bin("sunce")
        .unwrap()
        .args(&[
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
    Command::cargo_bin("sunce")
        .unwrap()
        .args(&[
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
    Command::cargo_bin("sunce")
        .unwrap()
        .args(&[
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
    Command::cargo_bin("sunce")
        .unwrap()
        .args(&[
            "--format=csv",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "--step=1h",
            "position",
            "--no-headers",
        ])
        .assert()
        .success();
}

#[test]
fn test_invalid_option_for_position_command() {
    Command::cargo_bin("sunce")
        .unwrap()
        .args(&["52.0", "13.4", "2024-01-01", "--twilight", "position"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "--twilight not valid for position",
        ));
}

#[test]
fn test_invalid_option_for_sunrise_command() {
    Command::cargo_bin("sunce")
        .unwrap()
        .args(&["52.0", "13.4", "2024-01-01", "--step=1h", "sunrise"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--step not valid for sunrise"));
}

#[test]
fn test_horizon_invalid_for_position() {
    Command::cargo_bin("sunce")
        .unwrap()
        .args(&["52.0", "13.4", "2024-01-01", "position", "--horizon=-6.0"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--horizon not valid for position"));
}

#[test]
fn test_algorithm_invalid_for_sunrise() {
    Command::cargo_bin("sunce")
        .unwrap()
        .args(&[
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
fn test_options_anywhere_with_file_input() {
    Command::cargo_bin("sunce")
        .unwrap()
        .args(&["--format=json", "@-", "position", "--no-headers"])
        .write_stdin("52.0 13.4 2024-01-01T12:00:00\n")
        .assert()
        .success();
}

#[test]
fn test_deltat_before_and_after_positionals() {
    Command::cargo_bin("sunce")
        .unwrap()
        .args(&[
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "--deltat=69.2",
            "position",
        ])
        .assert()
        .success();

    Command::cargo_bin("sunce")
        .unwrap()
        .args(&[
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
    Command::cargo_bin("sunce")
        .unwrap()
        .args(&[
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
    Command::cargo_bin("sunce")
        .unwrap()
        .args(&[
            "--format=csv",
            "52.0",
            "--algorithm=grena3",
            "13.4",
            "2024-01-01T12:00:00",
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
    Command::cargo_bin("sunce")
        .unwrap()
        .args(&[
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

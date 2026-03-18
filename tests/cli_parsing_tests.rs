use predicates::prelude::*;

mod common;
use common::{parse_csv_no_headers_output, sunce_command};

fn assert_success(args: &[&str], stdin: Option<&str>) {
    let mut cmd = sunce_command();
    cmd.args(args);
    if let Some(stdin) = stdin {
        cmd.write_stdin(stdin);
    }
    cmd.assert().success();
}

fn assert_failure(args: &[&str], expected_stderr: &str) {
    sunce_command()
        .args(args)
        .assert()
        .failure()
        .stderr(predicate::str::contains(expected_stderr));
}

fn step_rows(step: &str) -> Vec<Vec<String>> {
    let output = sunce_command()
        .args([
            "52.0",
            "13.4",
            "2024-01-01",
            "position",
            step,
            "--format=csv",
            "--no-headers",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    parse_csv_no_headers_output(&String::from_utf8(output.stdout).unwrap())
}

#[test]
fn test_position_option_placement_variants() {
    for args in [
        vec![
            "--format=csv",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ],
        vec![
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "--format=csv",
        ],
        vec![
            "--format=csv",
            "52.0",
            "13.4",
            "--no-headers",
            "2024-01-01T12:00:00",
            "position",
            "--no-refraction",
        ],
        vec![
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "--algorithm=grena3",
            "position",
        ],
        vec![
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "--algorithm=grena3",
        ],
        vec![
            "--format=csv",
            "52.0",
            "13.4",
            "2024-01-01",
            "--step=1h",
            "position",
            "--no-headers",
        ],
        vec![
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "--deltat=69.2",
            "position",
        ],
        vec![
            "--deltat=69.2",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ],
        vec![
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
        ],
    ] {
        assert_success(&args, None);
    }
}

#[test]
fn test_sunrise_option_placement_variants() {
    assert_success(
        &[
            "--format=csv",
            "52.0",
            "13.4",
            "--twilight",
            "2024-01-01",
            "sunrise",
            "--horizon=-6.0",
        ],
        None,
    );
    assert_success(
        &["--format=json", "@-", "position", "--no-headers"],
        Some("52.0 13.4 2024-01-01T12:00:00\n"),
    );
}

#[test]
fn test_invalid_cli_combinations() {
    for (args, expected_stderr) in [
        (
            vec![
                "52.0",
                "13.4",
                "2024-01-01T12:00:00",
                "--step=1h",
                "position",
            ],
            "Option --step requires date-only input",
        ),
        (
            vec!["52.0", "13.4", "2024-01-01", "--step=-1h", "position"],
            "Step must be positive",
        ),
        (
            vec!["52.0", "13.4", "2024-01-01", "--twilight", "position"],
            "--twilight not valid for position",
        ),
        (
            vec!["52.0", "13.4", "2024-01-01", "--step=1h", "sunrise"],
            "--step not valid for sunrise",
        ),
        (
            vec!["52.0", "13.4", "2024-01-01", "position", "--horizon=-6.0"],
            "--horizon not valid for position",
        ),
        (
            vec![
                "52.0",
                "13.4",
                "2024-01-01",
                "sunrise",
                "--algorithm=grena3",
            ],
            "--algorithm not valid for sunrise",
        ),
        (
            vec![
                "40.0",
                "-74.0",
                "2024-03-10T02:30:00",
                "--timezone=America/New_York",
                "position",
            ],
            "Datetime does not exist in timezone",
        ),
        (
            vec![
                "52.0",
                "13.4",
                "2024-01-01T12:00:00",
                "position",
                "--pressure=-10",
            ],
            "Invalid refraction parameters",
        ),
        (
            vec![
                "--deltat=69.2",
                "52.0",
                "13.4",
                "2024-01-01T12:00:00",
                "--deltat=70.0",
                "position",
            ],
            "cannot be used multiple times",
        ),
    ] {
        assert_failure(&args, expected_stderr);
    }
}

#[test]
fn test_help_and_version_paths() {
    sunce_command()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Usage:"));

    sunce_command()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("sunce"));

    sunce_command()
        .args(["help", "position"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Calculates topocentric solar coordinates.",
        ));

    sunce_command()
        .args(["help", "sunrise"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Calculates sunrise, transit, sunset",
        ));

    sunce_command()
        .args(["help", "nonsense"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Unknown command: nonsense"));
}

#[test]
fn test_unknown_option_and_argument_shape_errors() {
    for (args, expected_stderr) in [
        (vec!["--wat"], "Unknown option: --wat"),
        (vec!["52.0"], "No command found"),
        (vec!["52.0", "2024-01-01"], "No command found"),
        (
            vec!["52.0", "13.4", "2024-01-01", "extra", "position"],
            "Too many arguments",
        ),
        (
            vec!["@coords.txt", "13.4", "2024-01-01", "position"],
            "Coordinate files must be provided as a single @file argument",
        ),
        (
            vec!["52:53", "13.4", "2024-01-01T12:00:00", "position"],
            "Range must be start:end:step",
        ),
        (
            vec!["52:53:0", "13.4", "2024-01-01T12:00:00", "position"],
            "Range step must be non-zero",
        ),
        (
            vec!["52:53:-1", "13.4", "2024-01-01T12:00:00", "position"],
            "Range step must be positive for ascending ranges",
        ),
        (
            vec!["53:52:1", "13.4", "2024-01-01T12:00:00", "position"],
            "Range step must be negative for descending ranges",
        ),
    ] {
        assert_failure(&args, expected_stderr);
    }
}

#[test]
fn test_step_without_unit_and_with_unit_both_work() {
    for step in ["--step=3600", "--step=1h"] {
        let rows = step_rows(step);
        assert_eq!(rows.len(), 24);
        assert!(
            rows[0]
                .iter()
                .any(|field| field.contains("2024-01-01T00:00:00"))
        );
        assert!(
            rows[1]
                .iter()
                .any(|field| field.contains("2024-01-01T01:00:00"))
        );
    }
}

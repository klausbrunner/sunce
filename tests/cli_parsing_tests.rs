use predicates::ord;
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

fn assert_failure_code(args: &[&str], code: i32, expected_stderr: &str) {
    sunce_command()
        .args(args)
        .assert()
        .code(code)
        .stderr(predicate::str::contains(expected_stderr));
}

fn assert_failures(cases: &[(&[&str], &str)]) {
    for (args, expected_stderr) in cases {
        assert_failure(args, expected_stderr);
    }
}

fn assert_failure_code_cases(code: i32, cases: &[(&[&str], &str)]) {
    for (args, expected_stderr) in cases {
        assert_failure_code(args, code, expected_stderr);
    }
}

fn assert_help(args: &[&str], snippets: &[&str]) {
    let mut assertion = sunce_command().args(args).assert().success();
    for snippet in snippets {
        assertion = assertion.stdout(predicate::str::contains(*snippet));
    }
}

fn assert_predicate_result(args: &[&str]) {
    sunce_command()
        .args(args)
        .assert()
        .code(ord::eq(0).or(ord::eq(1)))
        .stdout(predicate::str::is_empty())
        .stderr(predicate::str::is_empty());
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

fn sunrise_predicate_args(datetime: &'static str, predicate: &'static str) -> [&'static str; 5] {
    ["52.0", "13.4", datetime, "sunrise", predicate]
}

fn position_predicate_args(datetime: &'static str, predicate: &'static str) -> [&'static str; 5] {
    ["52.0", "13.4", datetime, "position", predicate]
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
    assert_failures(&[
        (
            &[
                "52.0",
                "13.4",
                "2024-01-01T12:00:00",
                "--step=1h",
                "position",
            ],
            "Option --step requires date-only input",
        ),
        (
            &["52.0", "13.4", "2024-01-01", "--step=-1h", "position"],
            "Step must be positive",
        ),
        (
            &["52.0", "13.4", "2024-01-01", "--twilight", "position"],
            "--twilight not valid for position",
        ),
        (
            &["52.0", "13.4", "2024-01-01", "--step=1h", "sunrise"],
            "--step not valid for sunrise",
        ),
        (
            &["52.0", "13.4", "2024-01-01", "position", "--horizon=-6.0"],
            "--horizon not valid for position",
        ),
        (
            &[
                "52.0",
                "13.4",
                "2024-01-01",
                "sunrise",
                "--algorithm=grena3",
            ],
            "--algorithm not valid for sunrise",
        ),
        (
            &[
                "40.0",
                "-74.0",
                "2024-03-10T02:30:00",
                "--timezone=America/New_York",
                "position",
            ],
            "Datetime does not exist in timezone",
        ),
        (
            &[
                "52.0",
                "13.4",
                "2024-01-01T12:00:00",
                "position",
                "--pressure=-10",
            ],
            "Invalid refraction parameters",
        ),
        (
            &[
                "--deltat=69.2",
                "52.0",
                "13.4",
                "2024-01-01T12:00:00",
                "--deltat=70.0",
                "position",
            ],
            "cannot be used multiple times",
        ),
    ]);
}

#[test]
fn test_help_and_version_paths() {
    assert_help(
        &["--help"],
        &["Usage:", "--is-daylight", "--sun-above=<degrees>", "--wait"],
    );

    sunce_command()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("sunce"));

    assert_help(
        &["help", "position"],
        &[
            "Calculates topocentric solar coordinates.",
            "--sun-above=<degrees>",
            "--sun-below=<degrees>",
        ],
    );
    assert_help(
        &["help", "sunrise"],
        &[
            "Calculates sunrise, transit, sunset",
            "--is-daylight",
            "--is-astronomical-night",
            "--after-sunset",
            "--wait",
        ],
    );
    assert_help(&["help", "nonsense"], &["Unknown command: nonsense"]);
}

#[test]
fn test_unknown_option_and_argument_shape_errors() {
    assert_failures(&[
        (&["--wat"], "Unknown option: --wat"),
        (&["52.0"], "No command found"),
        (&["52.0", "2024-01-01"], "No command found"),
        (
            &["52.0", "13.4", "2024-01-01", "extra", "position"],
            "Too many arguments",
        ),
        (
            &["@coords.txt", "13.4", "2024-01-01", "position"],
            "Coordinate files must be provided as a single @file argument",
        ),
        (
            &["52:53", "13.4", "2024-01-01T12:00:00", "position"],
            "Range must be start:end:step",
        ),
        (
            &["52:53:0", "13.4", "2024-01-01T12:00:00", "position"],
            "Range step must be non-zero",
        ),
        (
            &["52:53:-1", "13.4", "2024-01-01T12:00:00", "position"],
            "Range step must be positive for ascending ranges",
        ),
        (
            &["53:52:1", "13.4", "2024-01-01T12:00:00", "position"],
            "Range step must be negative for descending ranges",
        ),
    ]);
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

#[test]
fn test_predicate_flag_parsing_and_validation() {
    let sunrise_day = sunrise_predicate_args("2024-03-21T12:00:00Z", "--is-daylight");
    let position_above = position_predicate_args("2024-03-21T12:00:00Z", "--sun-above=10");

    for args in [
        sunrise_day,
        sunrise_predicate_args("2024-03-21T04:40:00Z", "--is-civil-twilight"),
        sunrise_predicate_args("2024-03-21T04:00:00Z", "--is-nautical-twilight"),
        sunrise_predicate_args("2024-03-21T03:20:00Z", "--is-astronomical-twilight"),
        sunrise_predicate_args("2024-03-21T01:00:00Z", "--is-astronomical-night"),
        sunrise_predicate_args("2024-03-21T18:00:00Z", "--after-sunset"),
        position_above,
        position_predicate_args("2024-03-21T00:00:00Z", "--sun-below=-5"),
    ] {
        assert_predicate_result(&args);
    }

    let sunrise_format = [
        "--format=csv",
        "52.0",
        "13.4",
        "2024-03-21T12:00:00Z",
        "sunrise",
        "--is-daylight",
    ];
    let position_headers = [
        "--no-headers",
        "52.0",
        "13.4",
        "2024-03-21T12:00:00Z",
        "position",
        "--sun-above=10",
    ];
    let sunrise_show_inputs = [
        "--show-inputs",
        "52.0",
        "13.4",
        "2024-03-21T12:00:00Z",
        "sunrise",
        "--is-daylight",
    ];
    let sunrise_perf = [
        "--perf",
        "52.0",
        "13.4",
        "2024-03-21T12:00:00Z",
        "sunrise",
        "--is-daylight",
    ];

    assert_failure_code_cases(
        2,
        &[
            (
                &[
                    "52.0",
                    "13.4",
                    "2024-03-21T12:00:00Z",
                    "sunrise",
                    "--is-daylight",
                    "--is-astronomical-night",
                ],
                "Predicate options cannot be used multiple times",
            ),
            (
                &position_predicate_args("2024-03-21T12:00:00Z", "--is-daylight"),
                "Sunrise predicates require the sunrise command",
            ),
            (
                &sunrise_predicate_args("2024-03-21T12:00:00Z", "--sun-above=10"),
                "Sun angle predicates require the position command",
            ),
            (
                &sunrise_format,
                "Option --format not valid in predicate mode",
            ),
            (
                &position_headers,
                "Option --headers/--no-headers not valid in predicate mode",
            ),
            (
                &sunrise_show_inputs,
                "Option --show-inputs/--no-show-inputs not valid in predicate mode",
            ),
            (&sunrise_perf, "Option --perf not valid in predicate mode"),
            (
                &[
                    "52.0",
                    "13.4",
                    "2024-03-21T12:00:00Z",
                    "sunrise",
                    "--twilight",
                    "--is-daylight",
                ],
                "Option --twilight not valid in predicate mode",
            ),
            (
                &[
                    "52.0",
                    "13.4",
                    "2024-03-21T12:00:00Z",
                    "sunrise",
                    "--horizon=-6",
                    "--is-daylight",
                ],
                "Option --horizon not valid in predicate mode",
            ),
            (
                &[
                    "52.0",
                    "13.4",
                    "2024-03-21T12:00:00Z",
                    "position",
                    "--elevation-angle",
                    "--sun-above=10",
                ],
                "Option --elevation-angle not valid in predicate mode",
            ),
            (
                &position_predicate_args("2024-03-21T12:00:00Z", "--sun-above=91"),
                "Elevation threshold must be between -90 and 90 degrees",
            ),
            (
                &position_predicate_args("2024-03-21T12:00:00Z", "--sun-below=-91"),
                "Elevation threshold must be between -90 and 90 degrees",
            ),
            (
                &position_predicate_args("2024-03-21T12:00:00Z", "--sun-above=NaN"),
                "Elevation threshold must be a finite number",
            ),
            (
                &position_predicate_args("2024-03-21T12:00:00Z", "--sun-below=inf"),
                "Elevation threshold must be a finite number",
            ),
            (
                &[
                    "52.0",
                    "13.4",
                    "now",
                    "position",
                    "--step=1h",
                    "--sun-above=10",
                ],
                "Option --step not valid in predicate mode",
            ),
            (
                &["52.0", "13.4", "now", "sunrise", "--wait"],
                "Option --wait requires a predicate option",
            ),
            (
                &[
                    "52.0",
                    "13.4",
                    "2024-03-21T12:00:00Z",
                    "sunrise",
                    "--is-daylight",
                    "--wait",
                ],
                "Option --wait requires 'now' in predicate mode",
            ),
            (
                &[
                    "52:53:1",
                    "13.4",
                    "2024-03-21T12:00:00Z",
                    "position",
                    "--sun-above=10",
                ],
                "Predicate mode requires a single latitude/longitude pair",
            ),
            (
                &["52.0", "13.4", "2024-03-21", "sunrise", "--is-daylight"],
                "Predicate mode requires a single explicit instant",
            ),
            (
                &["52.0", "13.4", "2024-03", "sunrise", "--is-daylight"],
                "Predicate mode requires a single explicit instant",
            ),
            (
                &[
                    "@coords.txt",
                    "2024-03-21T12:00:00Z",
                    "position",
                    "--sun-above=10",
                ],
                "Predicate mode does not support coordinate file input",
            ),
            (
                &["52.0", "13.4", "@times.txt", "position", "--sun-above=10"],
                "Predicate mode does not support datetime file input",
            ),
            (
                &["@data.txt", "sunrise", "--is-daylight"],
                "Predicate mode requires explicit latitude, longitude, and datetime arguments",
            ),
        ],
    );
}

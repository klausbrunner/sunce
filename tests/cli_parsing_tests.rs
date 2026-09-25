use predicates::prelude::*;

mod common;
use common::{parse_csv_no_headers_output, sunce_command};

fn assert_success(args: &[&str]) {
    sunce_command().args(args).assert().success();
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
            "--no-headers",
            "2024-01-01T12:00:00",
            "position",
            "--no-refraction",
        ],
        vec![
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "--algorithm=grena3",
        ],
    ] {
        assert_success(&args);
    }
}

#[test]
fn test_rejects_trailing_positional_arguments() {
    assert_failure(
        &[
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "unexpected",
        ],
        "Unexpected arguments after command: unexpected",
    );
}

#[test]
fn test_rejects_step_for_file_inputs() {
    for args in [
        vec!["@-", "position", "--step=1h"],
        vec!["52.0", "13.4", "@-", "position", "--step=1h"],
    ] {
        assert_failure(&args, "Option --step is not valid with file input");
    }
}

#[test]
fn test_sunrise_option_placement_variants() {
    assert_success(&[
        "--format=csv",
        "52.0",
        "13.4",
        "--twilight",
        "2024-01-01",
        "sunrise",
    ]);
    assert_success(&["52.0", "13.4", "2024-01-01", "sunrise", "--horizon=-6.0"]);
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
            &[
                "52.0",
                "13.4",
                "2024-01-01",
                "sunrise",
                "--twilight",
                "--horizon=-6.0",
            ],
            "Option --horizon cannot be used with --twilight",
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
        (
            &["0", "0", "-9223372036854775808", "position"],
            "Invalid unix timestamp",
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
        .stdout(predicate::str::contains("sunce"))
        .stdout(predicate::str::contains("Build:"))
        .stdout(predicate::str::contains("Features:"))
        .stdout(predicate::str::contains("Built:").not());

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
    assert_failure_code(&["help", "nonsense"], 1, "Unknown command: nonsense");
}

#[test]
fn test_contextual_help_and_short_alias() {
    assert_help(&["-h"], &["Usage:", "Commands:"]);
    for command in ["position", "sunrise"] {
        let expected = sunce_command().args(["help", command]).output().unwrap();
        for flag in ["-h", "--help"] {
            for args in [
                vec![command, flag],
                vec![flag, command],
                vec!["52", "13.4", "now", command, flag],
            ] {
                sunce_command()
                    .args(args)
                    .assert()
                    .success()
                    .stdout(expected.stdout.clone())
                    .stderr("");
            }
        }
    }
    // An option value that happens to name a command isn't the help topic.
    assert_help(&["--timezone", "position", "--help"], &["Commands:"]);
    assert_failure(&["help", "position", "extra"], "Usage: sunce help");
    assert_failure(&["--help=yes"], "does not take a value");
}

#[test]
fn test_separated_option_values_match_equals_form() {
    for command in [
        "--format=json --timezone=-05:00 --algorithm=grena3 --elevation=-10 --temperature=-5 --pressure=1000 52 13.4 2024-01-01T12:00:00Z position",
        "52 13.4 2024-01-01 position --step=6h --format=csv",
        "52 13.4 2024-01-01 sunrise --horizon=-6 --format=json",
        "52 13.4 2024-01-01T12:00:00Z position --sun-above=-10",
        "52 13.4 2024-01-01T12:00:00Z position --sun-below=-10",
    ] {
        let expected = sunce_command()
            .args(command.split_whitespace())
            .output()
            .unwrap();
        assert!(matches!(expected.status.code(), Some(0 | 1)));
        assert!(expected.stderr.is_empty());
        let separated = command
            .split_whitespace()
            .flat_map(|arg| arg.splitn(2, '='));
        sunce_command()
            .args(separated)
            .assert()
            .code(expected.status.code().unwrap())
            .stdout(expected.stdout)
            .stderr("");
    }
    // Bare --deltat must not consume the following latitude as its value.
    assert_success(&["--deltat", "52", "13.4", "2024-01-01T12:00:00Z", "position"]);
    for args in [vec!["--format"], vec!["--format", "--no-headers"]] {
        assert_failure_code(&args, 1, "Option --format requires a value");
    }
}

#[test]
fn test_predicate_errors_are_distinct_from_false_regardless_of_option_order() {
    let cases: &[(&[&str], &str)] = &[
        (
            &["91", "13.4", "now", "sunrise"],
            "Latitude must be between",
        ),
        (
            &["52", "181", "now", "sunrise"],
            "Longitude must be between",
        ),
        (
            &["52", "13.4", "now", "sunrise", "--timezone", "bogus"],
            "Invalid timezone",
        ),
        (
            &["52", "13.4", "now", "sunrise", "--format"],
            "requires a value",
        ),
        (&["52", "13.4", "now", "sunrise", "--wat"], "Unknown option"),
        (
            &["52", "13.4", "now", "position", "--twilight"],
            "not valid for position",
        ),
        (&["52", "13.4", "now"], "No command found"),
    ];
    for (args, message) in cases {
        for before in [true, false] {
            let mut full_args = args.to_vec();
            full_args.insert(if before { 0 } else { full_args.len() }, "--is-daylight");
            assert_failure_code(&full_args, 2, message);
        }
    }
    assert_failure_code(&["--is-daylight=yes"], 2, "does not take a value");
    assert_failure_code(&["--sun-above", "--timezone=UTC"], 2, "requires a value");
    // Predicate-looking option values must not change ordinary error codes.
    assert_failure_code(&["--timezone=--is-daylight"], 1, "Invalid timezone");
    for flag in ["--help", "--version"] {
        assert_success(&["--is-daylight", flag]);
    }
    for (instant, code) in [("2024-03-21T12:00:00Z", 0), ("2024-03-21T00:00:00Z", 1)] {
        sunce_command()
            .args(["52", "13.4", instant, "sunrise", "--is-daylight"])
            .assert()
            .code(code)
            .stdout("")
            .stderr("");
    }
}

#[test]
fn test_unknown_option_and_argument_count_errors() {
    assert_failures(&[
        (&["--wat"], "Unknown option: --wat"),
        (&["52.0"], "No command found"),
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
                &position_predicate_args("2024-03-21T12:00:00Z", "--sun-above=NaN"),
                "Invalid sun above value: expected finite number",
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

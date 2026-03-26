mod common;

use common::sunce_command;

fn assert_run(args: &[&str], code: i32, stderr: Option<&str>) {
    let output = sunce_command().args(args).output().unwrap();
    assert_eq!(output.status.code(), Some(code), "args: {args:?}");
    assert!(
        output.stdout.is_empty(),
        "stdout was not empty for {args:?}"
    );
    match stderr {
        Some(expected) => assert!(
            String::from_utf8_lossy(&output.stderr).contains(expected),
            "stderr did not contain {:?}: {}",
            expected,
            String::from_utf8_lossy(&output.stderr)
        ),
        None => assert!(
            output.stderr.is_empty(),
            "stderr was not empty for {args:?}"
        ),
    }
}

fn sunrise_args(
    lat: &'static str,
    lon: &'static str,
    datetime: &'static str,
    predicate: &'static str,
) -> [&'static str; 6] {
    ["--timezone=UTC", lat, lon, datetime, "sunrise", predicate]
}

fn berlin_sunrise(datetime: &'static str, predicate: &'static str) -> [&'static str; 6] {
    sunrise_args("52.0", "13.4", datetime, predicate)
}

fn position_args(
    lat: &'static str,
    lon: &'static str,
    datetime: &'static str,
    predicate: &'static str,
) -> [&'static str; 5] {
    [lat, lon, datetime, "position", predicate]
}

fn assert_berlin_state(datetime: &'static str, predicate: &'static str, code: i32) {
    assert_run(&berlin_sunrise(datetime, predicate), code, None);
}

fn assert_position_threshold(datetime: &'static str, predicate: &'static str, code: i32) {
    assert_run(
        &position_args("52.0", "13.4", datetime, predicate),
        code,
        None,
    );
}

fn assert_sunrise_state(
    lat: &'static str,
    lon: &'static str,
    datetime: &'static str,
    predicate: &'static str,
    code: i32,
) {
    assert_run(&sunrise_args(lat, lon, datetime, predicate), code, None);
}

#[test]
fn test_twilight_state_predicates_for_known_states() {
    for (datetime, predicate, code) in [
        ("2024-03-21T12:00:00", "--is-daylight", 0),
        ("2024-03-21T04:40:00", "--is-civil-twilight", 0),
        ("2024-03-21T04:00:00", "--is-nautical-twilight", 0),
        ("2024-03-21T03:20:00", "--is-astronomical-twilight", 0),
        ("2024-03-21T02:30:00", "--is-astronomical-night", 0),
        ("2024-03-21T02:30:00", "--after-sunset", 0),
        ("2024-03-21T18:00:00", "--after-sunset", 0),
        ("2024-03-21T02:30:00", "--is-daylight", 1),
        ("2024-03-21T12:00:00", "--is-astronomical-night", 1),
        ("2024-03-21T12:00:00", "--after-sunset", 1),
    ] {
        assert_berlin_state(datetime, predicate, code);
    }
}

#[test]
fn test_twilight_state_boundaries_are_half_open() {
    for (datetime, predicate, code) in [
        ("2024-03-21T05:05:54", "--is-daylight", 0),
        ("2024-03-21T17:21:59", "--is-daylight", 1),
        ("2024-03-21T04:32:14", "--is-civil-twilight", 0),
        ("2024-03-21T05:05:54", "--is-civil-twilight", 1),
        ("2024-03-21T03:52:20", "--is-nautical-twilight", 0),
        ("2024-03-21T04:32:14", "--is-nautical-twilight", 1),
        ("2024-03-21T03:10:39", "--is-astronomical-twilight", 0),
        ("2024-03-21T03:52:20", "--is-astronomical-twilight", 1),
        ("2024-03-21T03:10:39", "--is-astronomical-night", 1),
        ("2024-03-21T17:21:59", "--after-sunset", 0),
        ("2024-03-21T05:05:54", "--after-sunset", 1),
    ] {
        assert_berlin_state(datetime, predicate, code);
    }
}

#[test]
fn test_angle_predicates_for_known_thresholds() {
    for (predicate, code) in [
        ("--sun-above=50", 0),
        ("--sun-above=60", 1),
        ("--sun-below=60", 0),
        ("--sun-below=50", 1),
    ] {
        assert_position_threshold("2024-06-21T12:00:00+02:00", predicate, code);
    }
}

#[test]
fn test_predicate_timezone_and_dst_behavior() {
    for args in [
        vec![
            "--timezone=Europe/Berlin",
            "52.0",
            "13.4",
            "2024-03-21T13:00:00",
            "sunrise",
            "--is-daylight",
        ],
        vec![
            "52.0",
            "13.4",
            "2024-03-21T13:00:00+01:00",
            "sunrise",
            "--is-daylight",
        ],
    ] {
        assert_run(&args, 0, None);
    }

    assert_run(
        &[
            "--timezone=America/New_York",
            "40.7",
            "-74.0",
            "2024-03-10T02:30:00",
            "position",
            "--sun-above=0",
        ],
        2,
        Some("Datetime does not exist in timezone"),
    );
}

#[test]
fn test_predicate_polar_and_twilight_band_cases() {
    for (lat, lon, datetime, predicate, code) in [
        ("80.0", "0.0", "2024-06-21T12:00:00", "--is-daylight", 0),
        (
            "80.0",
            "0.0",
            "2024-06-21T12:00:00",
            "--is-astronomical-night",
            1,
        ),
        ("80.0", "0.0", "2024-06-21T12:00:00", "--after-sunset", 1),
        (
            "80.0",
            "0.0",
            "2024-12-21T02:00:00",
            "--is-astronomical-night",
            0,
        ),
        ("80.0", "0.0", "2024-12-21T02:00:00", "--after-sunset", 0),
        (
            "78.0",
            "15.0",
            "2024-12-21T08:00:00",
            "--is-astronomical-twilight",
            0,
        ),
        (
            "78.0",
            "15.0",
            "2024-12-21T10:00:00",
            "--is-nautical-twilight",
            0,
        ),
    ] {
        assert_sunrise_state(lat, lon, datetime, predicate, code);
    }
}
